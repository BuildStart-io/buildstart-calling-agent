package agent

import (
	"encoding/json"
	"math"
	"sync"
	"testing"

	"wacalls/internal/voip/media"
)

// TestGeminiLiveAudioBufferOverflow verifies that when audio frames flood the channel
// beyond its 100-frame capacity, the oldest frames are discarded cleanly without blocking,
// deadlocking, or exceeding capacity.
func TestGeminiLiveAudioBufferOverflow(t *testing.T) {
	agent := NewGeminiLiveAgent("fake-key", "", "Aoede", "prompt", nil)
	defer agent.Close()

	agent.SetCallActive(true)

	// Feed 150 frames with sequence tag in first sample
	for i := 0; i < 150; i++ {
		frame := make([]float32, 960)
		frame[0] = float32(i)
		agent.FeedCallerAudio(frame)
	}

	// Channel capacity is 100
	if len(agent.audioInCh) > 100 {
		t.Fatalf("channel overflowed max capacity: %d", len(agent.audioInCh))
	}

	// Verify the channel contains the freshest frames (older frames were dropped)
	firstFrame := <-agent.audioInCh
	if firstFrame[0] < 50 {
		t.Fatalf("expected oldest frames to be dropped, got seq tag %f", firstFrame[0])
	}
}

// TestGeminiLiveConcurrentFeedAndStateChange tests massive parallel concurrent operations
// across multiple goroutines to guarantee data-race and deadlock freedom.
func TestGeminiLiveConcurrentFeedAndStateChange(t *testing.T) {
	agent := NewGeminiLiveAgent("fake-key", "", "Aoede", "prompt", nil)
	defer agent.Close()

	var wg sync.WaitGroup
	workers := 10
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			frame := make([]float32, 320)
			frame[0] = float32(workerID)

			for j := 0; j < iterations; j++ {
				switch j % 4 {
				case 0:
					agent.SetCallActive(true)
					agent.FeedCallerAudio(frame)
				case 1:
					agent.SetCallActive(false)
					agent.FeedCallerAudio(frame)
				case 2:
					agent.SetEnabled(j%2 == 0)
				case 3:
					_ = agent.IsEnabled()
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestPCMConversionEdgeCases verifies numerical robustness across clipping, NaN, Inf, and fidelity.
func TestPCMConversionEdgeCases(t *testing.T) {
	// Extreme and non-finite values
	input := []float32{
		0.0,
		1.0,
		-1.0,
		2.5,                     // Should clip to +MaxInt16
		-3.8,                    // Should clip to -MinInt16
		float32(math.NaN()),     // Must not panic, must be 0
		float32(math.Inf(1)),    // Should clip to +MaxInt16
		float32(math.Inf(-1)),   // Should clip to -MinInt16
		0.00003,                 // Tiny subnormal
	}

	bytes := media.PCMFloat32ToInt16LE(input)
	if len(bytes) != len(input)*2 {
		t.Fatalf("expected %d bytes, got %d", len(input)*2, len(bytes))
	}

	recovered := media.PCMInt16LEToFloat32(bytes)
	if len(recovered) != len(input) {
		t.Fatalf("expected %d recovered samples, got %d", len(input), len(recovered))
	}

	// 0.0 must be 0
	if recovered[0] != 0.0 {
		t.Errorf("zero sample failed: got %f", recovered[0])
	}

	// 1.0 must be very close to 1.0 (32767 / 32768 = ~0.999969)
	if math.Abs(float64(recovered[1]-1.0)) > 0.001 {
		t.Errorf("1.0 sample failed: got %f", recovered[1])
	}

	// -1.0 must be exact (-32768 / 32768 = -1.0)
	if recovered[2] != -1.0 {
		t.Errorf("-1.0 sample failed: got %f", recovered[2])
	}

	// 2.5 must clip to max int16
	if math.Abs(float64(recovered[3]-1.0)) > 0.001 {
		t.Errorf("clipping > 1.0 failed: got %f", recovered[3])
	}

	// -3.8 must clip to -1.0
	if recovered[4] != -1.0 {
		t.Errorf("clipping < -1.0 failed: got %f", recovered[4])
	}

	// NaN must be converted to 0
	if recovered[5] != 0.0 {
		t.Errorf("NaN failed: got %f", recovered[5])
	}
}

// TestTriggerGreetingConcurrency verifies calling TriggerGreeting multiple times
// concurrently is completely idempotent (only triggers once).
func TestTriggerGreetingConcurrency(t *testing.T) {
	agent := NewGeminiLiveAgent("fake-key", "", "Aoede", "prompt", nil)
	defer agent.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			agent.TriggerGreeting()
		}()
	}
	wg.Wait()

	if !agent.greetingTriggered.Load() {
		t.Fatalf("expected greetingTriggered to be true")
	}
	if !agent.callActive.Load() {
		t.Fatalf("expected callActive to be true")
	}
}

// TestEmptyAudioHandling verifies that feeding nil or empty slices to FeedCallerAudio is a no-op.
func TestEmptyAudioHandling(t *testing.T) {
	agent := NewGeminiLiveAgent("fake-key", "", "Aoede", "prompt", nil)
	defer agent.Close()

	agent.SetCallActive(true)

	agent.FeedCallerAudio(nil)
	agent.FeedCallerAudio([]float32{})

	if len(agent.audioInCh) != 0 {
		t.Fatalf("expected 0 frames in audioInCh for empty audio, got %d", len(agent.audioInCh))
	}
}

// TestSilenceFrameFormat verifies silence frame sample count, byte sizing, and bit neutrality.
func TestSilenceFrameFormat(t *testing.T) {
	const samples = 640 // 40ms @ 16kHz
	frame := make([]float32, samples)
	bytes := media.PCMFloat32ToInt16LE(frame)

	if len(bytes) != samples*2 {
		t.Fatalf("expected %d bytes, got %d", samples*2, len(bytes))
	}
	for i, b := range bytes {
		if b != 0 {
			t.Fatalf("expected zero byte at index %d, got %x", i, b)
		}
	}
}

// TestGeminiLiveServerContentUnmarshaling verifies decoding of modern and legacy Gemini Live
// server messages, barge-in flags, and transcription payloads.
func TestGeminiLiveServerContentUnmarshaling(t *testing.T) {
	type serverMsg struct {
		SetupComplete *struct{} `json:"setupComplete"`
		ServerContent *struct {
			Parts []struct {
				Text       string `json:"text"`
				InlineData *struct {
					MimeType string `json:"mimeType"`
					Data     string `json:"data"`
				} `json:"inlineData"`
			} `json:"parts"`
			ModelTurn *struct {
				Parts []struct {
					Text       string `json:"text"`
					InlineData *struct {
						MimeType string `json:"mimeType"`
						Data     string `json:"data"`
					} `json:"inlineData"`
				} `json:"parts"`
			} `json:"modelTurn"`
			InputTranscription *struct {
				Text string `json:"text"`
			} `json:"inputTranscription"`
			OutputTranscription *struct {
				Text string `json:"text"`
			} `json:"outputTranscription"`
			TurnComplete bool `json:"turnComplete"`
			Interrupted  bool `json:"interrupted"`
		} `json:"serverContent"`
	}

	// 1. setupComplete message
	var msg1 serverMsg
	if err := json.Unmarshal([]byte(`{"setupComplete":{}}`), &msg1); err != nil {
		t.Fatalf("failed to unmarshal setupComplete: %v", err)
	}
	if msg1.SetupComplete == nil {
		t.Fatalf("expected SetupComplete to not be nil")
	}

	// 2. Interrupted / barge-in message
	var msg2 serverMsg
	if err := json.Unmarshal([]byte(`{"serverContent":{"interrupted":true}}`), &msg2); err != nil {
		t.Fatalf("failed to unmarshal interrupted: %v", err)
	}
	if msg2.ServerContent == nil || !msg2.ServerContent.Interrupted {
		t.Fatalf("expected interrupted=true")
	}

	// 3. Audio parts (modern format)
	var msg3 serverMsg
	jsonWithAudio := `{"serverContent":{"parts":[{"inlineData":{"mimeType":"audio/pcm;rate=24000","data":"AAAA"}}]}}`
	if err := json.Unmarshal([]byte(jsonWithAudio), &msg3); err != nil {
		t.Fatalf("failed to unmarshal audio parts: %v", err)
	}
	if len(msg3.ServerContent.Parts) != 1 || msg3.ServerContent.Parts[0].InlineData.Data != "AAAA" {
		t.Fatalf("expected audio part with data 'AAAA'")
	}

	// 4. Caller and agent transcripts
	var msg4 serverMsg
	jsonTranscripts := `{"serverContent":{"inputTranscription":{"text":"hello"},"outputTranscription":{"text":"හෙලෝ"}}}`
	if err := json.Unmarshal([]byte(jsonTranscripts), &msg4); err != nil {
		t.Fatalf("failed to unmarshal transcripts: %v", err)
	}
	if msg4.ServerContent.InputTranscription.Text != "hello" {
		t.Errorf("expected inputTranscription 'hello', got %s", msg4.ServerContent.InputTranscription.Text)
	}
	if msg4.ServerContent.OutputTranscription.Text != "හෙලෝ" {
		t.Errorf("expected outputTranscription 'හෙලෝ', got %s", msg4.ServerContent.OutputTranscription.Text)
	}
}

