package media

import (
	"math"
	"testing"
)

func TestVoiceMasteringEngine(t *testing.T) {
	dsp := NewVoiceMasteringEngine(16000.0)
	if dsp == nil {
		t.Fatal("expected non-nil mastering engine")
	}

	// Generate 1 second of test signal (16,000 samples) with mixed frequencies (150 Hz bass + 6000 Hz sibilance)
	samples := make([]float32, 16000)
	for i := range samples {
		tSec := float64(i) / 16000.0
		// Mixed waveform with loud peak
		s := 0.5*math.Sin(2.0*math.Pi*150.0*tSec) + 0.3*math.Sin(2.0*math.Pi*6500.0*tSec)
		if i > 8000 && i < 8500 {
			s *= 2.5 // simulate volume spike
		}
		samples[i] = float32(s)
	}

	out := dsp.Process(samples)
	if len(out) != len(samples) {
		t.Fatalf("expected output length %d, got %d", len(samples), len(out))
	}

	// Verify all samples are cleanly bounded within [-1.0, 1.0] with zero NaN/Inf
	for i, s := range out {
		if math.IsNaN(float64(s)) || math.IsInf(float64(s), 0) {
			t.Fatalf("invalid sample at index %d: %v", i, s)
		}
		if s > 1.0 || s < -1.0 {
			t.Fatalf("sample at index %d exceeded [-1.0, 1.0]: %v", i, s)
		}
	}
}
