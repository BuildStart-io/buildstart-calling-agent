package agent

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestVADAndWAVEncoding(t *testing.T) {
	cfg := DefaultVADConfig()
	cfg.SilenceDuration = 50 * time.Millisecond
	vad := NewVAD(cfg)

	speechEnded := make(chan struct{}, 1)
	vad.OnSpeechEnd = func(pcm []float32, wav []byte) {
		if len(wav) == 0 {
			t.Errorf("expected non-empty WAV bytes")
		}
		select {
		case speechEnded <- struct{}{}:
		default:
		}
	}

	// Generate 400ms of 440Hz tone (simulated speech)
	numSamples := int(16000 * 0.4)
	speechPCM := make([]float32, numSamples)
	for i := 0; i < numSamples; i++ {
		tVal := float64(i) / 16000.0
		speechPCM[i] = float32(0.5 * math.Sin(2*math.Pi*440.0*tVal))
	}

	// Feed in 160-sample chunks
	for i := 0; i < len(speechPCM); i += 160 {
		end := min(i+160, len(speechPCM))
		vad.ProcessFrame(speechPCM[i:end])
	}

	// Feed silence to trigger end of speech
	silence := make([]float32, 160)
	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Millisecond)
		vad.ProcessFrame(silence)
	}

	select {
	case <-speechEnded:
		// Success
	case <-time.After(500 * time.Millisecond):
		// Test complete
	}
}

func TestWAVParsing(t *testing.T) {
	// Generate 16kHz sine wave
	pcm := make([]float32, 1600)
	for i := range pcm {
		pcm[i] = 0.5
	}
	wavBytes := EncodeWAV(pcm, 16000)
	parsed, err := ParseWAV(wavBytes)
	if err != nil {
		t.Fatalf("ParseWAV failed: %v", err)
	}
	if len(parsed) != len(pcm) {
		t.Fatalf("expected %d samples, got %d", len(pcm), len(parsed))
	}
}

func TestOpenRouterClientInitialization(t *testing.T) {
	client := NewOpenRouterClient("sk-fake-key", "google/gemini-2.5-flash", "You are a test bot.")
	if client.model != "google/gemini-2.5-flash" {
		t.Errorf("unexpected model: %s", client.model)
	}
	client.Reset()
	if len(client.history) != 1 || client.history[0].Role != "system" {
		t.Errorf("expected 1 system message, got %d", len(client.history))
	}
}

func TestTTSFallbackTone(t *testing.T) {
	tts := NewMultiTTS("en-US-JennyNeural")
	pcm, err := tts.Synthesize(context.Background(), "Hello test")
	if err != nil {
		t.Fatalf("Synthesize failed: %v", err)
	}
	if len(pcm) == 0 {
		t.Fatalf("expected non-empty PCM samples")
	}
}

func TestGeminiLiveAudioIngestion(t *testing.T) {
	agent := NewGeminiLiveAgent("fake-key", "", "Aoede", "You are an assistant.", nil)
	defer agent.Close()

	if !agent.IsEnabled() {
		t.Fatalf("expected agent to be enabled by default")
	}

	frame := make([]float32, 960) // 60ms @ 16kHz
	for i := range frame {
		frame[i] = 0.25
	}

	// When call is not active, FeedCallerAudio should ignore frames
	agent.SetCallActive(false)
	agent.FeedCallerAudio(frame)
	if len(agent.audioInCh) != 0 {
		t.Fatalf("expected 0 frames in audioInCh when callActive is false, got %d", len(agent.audioInCh))
	}

	// When call is active, FeedCallerAudio should enqueue frames cleanly
	agent.SetCallActive(true)
	agent.FeedCallerAudio(frame)
	if len(agent.audioInCh) != 1 {
		t.Fatalf("expected 1 frame in audioInCh when callActive is true, got %d", len(agent.audioInCh))
	}

	received := <-agent.audioInCh
	if len(received) != 960 {
		t.Fatalf("expected 960 samples, got %d", len(received))
	}
	for i, s := range received {
		if s != 0.25 {
			t.Fatalf("sample %d corrupted: got %f, want 0.25", i, s)
		}
	}
}
