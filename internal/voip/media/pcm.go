package media

import (
	"encoding/binary"
	"math"
)

func PCMFloat32ToInt16LE(pcm []float32) []byte {
	out := make([]byte, len(pcm)*2)
	for i, s := range pcm {
		binary.LittleEndian.PutUint16(out[i*2:], uint16(floatToInt16(s)))
	}
	return out
}

func PCMInt16LEToFloat32(b []byte) []float32 {
	out := make([]float32, len(b)/2)
	for i := range out {
		v := int16(binary.LittleEndian.Uint16(b[i*2:]))
		out[i] = float32(v) / 32768.0
	}
	return out
}

func floatToInt16(s float32) int16 {
	switch {
	case math.IsNaN(float64(s)):
		return 0
	case s >= 1:
		return math.MaxInt16
	case s <= -1:
		return math.MinInt16
	}
	return int16(s * 32767)
}

// Resampler24kTo16k performs exact 3:2 fractional linear downsampling from 24,000 Hz to 16,000 Hz
// with stateful chunk boundary buffering and [-1.0, 1.0] range clamping.
type Resampler24kTo16k struct {
	buf []float32
}

func NewResampler24kTo16k() *Resampler24kTo16k {
	return &Resampler24kTo16k{
		buf: make([]float32, 0, 4800),
	}
}

func (r *Resampler24kTo16k) Reset() {
	r.buf = r.buf[:0]
}

func (r *Resampler24kTo16k) Process(in []float32) []float32 {
	if len(in) == 0 {
		return nil
	}
	r.buf = append(r.buf, in...)
	blocks := len(r.buf) / 3
	if blocks == 0 {
		return nil
	}

	out := make([]float32, blocks*2)
	for i := 0; i < blocks; i++ {
		s0 := r.buf[i*3]
		s1 := r.buf[i*3+1]
		s2 := r.buf[i*3+2]

		if s0 > 1.0 {
			s0 = 1.0
		} else if s0 < -1.0 {
			s0 = -1.0
		}
		out[i*2] = s0

		sMid := 0.5*s1 + 0.5*s2
		if sMid > 1.0 {
			sMid = 1.0
		} else if sMid < -1.0 {
			sMid = -1.0
		}
		out[i*2+1] = sMid
	}

	consumed := blocks * 3
	r.buf = append([]float32(nil), r.buf[consumed:]...)
	return out
}

func Resample24kTo16k(in []float32) []float32 {
	r := NewResampler24kTo16k()
	return r.Process(in)
}

func ResampleFloat32(in []float32, srcRate, targetRate int) []float32 {
	if srcRate == targetRate || len(in) == 0 || srcRate <= 0 || targetRate <= 0 {
		return in
	}
	if srcRate == 24000 && targetRate == 16000 {
		return Resample24kTo16k(in)
	}
	ratio := float64(srcRate) / float64(targetRate)
	outLen := int(float64(len(in)) / ratio)
	out := make([]float32, outLen)
	for i := range out {
		srcPos := float64(i) * ratio
		idx := int(srcPos)
		frac := float32(srcPos - float64(idx))
		if idx+1 < len(in) {
			out[i] = in[idx]*(1-frac) + in[idx+1]*frac
		} else if idx < len(in) {
			out[i] = in[idx]
		}
	}
	return out
}
