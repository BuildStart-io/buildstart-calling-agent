package media

import (
	"math"
)

// BiquadFilter implements a standard Direct Form II Transposed biquad IIR filter.
type BiquadFilter struct {
	b0, b1, b2 float64
	a1, a2     float64
	z1, z2     float64
}

// NewLowShelfFilter creates a low-shelf filter with the given cutoff frequency (Hz), gain (dB), and Q at sampleRate.
func NewLowShelfFilter(sampleRate, cutoffHz, gainDB, q float64) *BiquadFilter {
	A := math.Pow(10.0, gainDB/40.0)
	w0 := 2.0 * math.Pi * cutoffHz / sampleRate
	cosW0 := math.Cos(w0)
	sinW0 := math.Sin(w0)
	alpha := sinW0 / (2.0 * q)
	sqrtA2 := 2.0 * math.Sqrt(A) * alpha

	a0 := (A + 1.0) + (A-1.0)*cosW0 + sqrtA2
	b0 := A * ((A + 1.0) - (A-1.0)*cosW0 + sqrtA2)
	b1 := 2.0 * A * ((A - 1.0) - (A+1.0)*cosW0)
	b2 := A * ((A + 1.0) - (A-1.0)*cosW0 - sqrtA2)
	a1 := -2.0 * ((A - 1.0) + (A+1.0)*cosW0)
	a2 := (A + 1.0) + (A-1.0)*cosW0 - sqrtA2

	return &BiquadFilter{
		b0: b0 / a0,
		b1: b1 / a0,
		b2: b2 / a0,
		a1: a1 / a0,
		a2: a2 / a0,
	}
}

// NewHighShelfFilter creates a high-shelf filter with the given cutoff frequency (Hz), gain (dB), and Q at sampleRate.
func NewHighShelfFilter(sampleRate, cutoffHz, gainDB, q float64) *BiquadFilter {
	A := math.Pow(10.0, gainDB/40.0)
	w0 := 2.0 * math.Pi * cutoffHz / sampleRate
	cosW0 := math.Cos(w0)
	sinW0 := math.Sin(w0)
	alpha := sinW0 / (2.0 * q)
	sqrtA2 := 2.0 * math.Sqrt(A) * alpha

	a0 := (A + 1.0) - (A-1.0)*cosW0 + sqrtA2
	b0 := A * ((A + 1.0) + (A-1.0)*cosW0 + sqrtA2)
	b1 := -2.0 * A * ((A - 1.0) + (A+1.0)*cosW0)
	b2 := A * ((A + 1.0) + (A-1.0)*cosW0 - sqrtA2)
	a1 := 2.0 * ((A - 1.0) - (A+1.0)*cosW0)
	a2 := (A + 1.0) - (A-1.0)*cosW0 - sqrtA2

	return &BiquadFilter{
		b0: b0 / a0,
		b1: b1 / a0,
		b2: b2 / a0,
		a1: a1 / a0,
		a2: a2 / a0,
	}
}

func (f *BiquadFilter) ProcessSample(x float32) float32 {
	x64 := float64(x)
	y := f.b0*x64 + f.z1
	f.z1 = f.b1*x64 - f.a1*y + f.z2
	f.z2 = f.b2*x64 - f.a2*y
	return float32(y)
}

func (f *BiquadFilter) Reset() {
	f.z1 = 0
	f.z2 = 0
}

// DynamicCompressor implements a transparent soft-knee audio compressor & peak limiter.
type DynamicCompressor struct {
	thresholdDB float64 // e.g. -14.0 dB
	ratio       float64 // e.g. 2.2:1
	kneeDB      float64 // e.g. 4.0 dB
	attackTime  float64 // e.g. 0.010 s
	releaseTime float64 // e.g. 0.080 s
	sampleRate  float64
	envDB       float64
	attackCoef  float64
	releaseCoef float64
}

func NewDynamicCompressor(sampleRate, thresholdDB, ratio, kneeDB, attackSec, releaseSec float64) *DynamicCompressor {
	return &DynamicCompressor{
		thresholdDB: thresholdDB,
		ratio:       ratio,
		kneeDB:      kneeDB,
		attackTime:  attackSec,
		releaseTime: releaseSec,
		sampleRate:  sampleRate,
		envDB:       -96.0,
		attackCoef:  math.Exp(-1.0 / (sampleRate * attackSec)),
		releaseCoef: math.Exp(-1.0 / (sampleRate * releaseSec)),
	}
}

func (c *DynamicCompressor) ProcessSample(x float32) float32 {
	absX := math.Abs(float64(x))
	xDB := -96.0
	if absX > 1e-5 {
		xDB = 20.0 * math.Log10(absX)
	}

	// Smooth envelope detector
	if xDB > c.envDB {
		c.envDB = c.attackCoef*c.envDB + (1.0-c.attackCoef)*xDB
	} else {
		c.envDB = c.releaseCoef*c.envDB + (1.0-c.releaseCoef)*xDB
	}

	// Calculate gain reduction with soft knee
	gainReductionDB := 0.0
	diff := c.envDB - c.thresholdDB
	if 2.0*diff < -c.kneeDB {
		gainReductionDB = 0.0
	} else if 2.0*math.Abs(diff) <= c.kneeDB {
		gainReductionDB = (1.0 - 1.0/c.ratio) * math.Pow(diff+c.kneeDB/2.0, 2.0) / (2.0 * c.kneeDB)
	} else {
		gainReductionDB = (1.0 - 1.0/c.ratio) * diff
	}

	gain := math.Pow(10.0, -gainReductionDB/20.0)
	out := float64(x) * gain

	// Soft-clipping peak limiter to eliminate harsh square waves
	if out > 0.95 {
		out = 0.95 + 0.05*math.Tanh((out-0.95)/0.05)
	} else if out < -0.95 {
		out = -0.95 + 0.05*math.Tanh((out+0.95)/0.05)
	}

	return float32(out)
}

func (c *DynamicCompressor) Reset() {
	c.envDB = -96.0
}

// VoiceMasteringEngine applies low-shelf chest resonance (+2.5dB @ 200Hz),
// high-shelf de-esser (-3.0dB @ 6500Hz), and a warm soft-knee compressor.
type VoiceMasteringEngine struct {
	lowShelf   *BiquadFilter
	highShelf  *BiquadFilter
	compressor *DynamicCompressor
}

func NewVoiceMasteringEngine(sampleRate float64) *VoiceMasteringEngine {
	return &VoiceMasteringEngine{
		lowShelf:   NewLowShelfFilter(sampleRate, 200.0, 2.5, 0.707),
		highShelf:  NewHighShelfFilter(sampleRate, 6500.0, -3.0, 0.707),
		compressor: NewDynamicCompressor(sampleRate, -14.0, 2.2, 4.0, 0.010, 0.080),
	}
}

func (v *VoiceMasteringEngine) Process(pcm []float32) []float32 {
	if len(pcm) == 0 {
		return pcm
	}
	out := make([]float32, len(pcm))
	for i, s := range pcm {
		s1 := v.lowShelf.ProcessSample(s)
		s2 := v.highShelf.ProcessSample(s1)
		out[i] = v.compressor.ProcessSample(s2)
	}
	return out
}

func (v *VoiceMasteringEngine) Reset() {
	v.lowShelf.Reset()
	v.highShelf.Reset()
	v.compressor.Reset()
}
