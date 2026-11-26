package call

import (
	"testing"
	"time"

	"wacalls/internal/voip/core"
)

func TestCallManagerIsPlayingAudioAcousticGating(t *testing.T) {
	cm := NewCallManager(nil, nil)

	if cm.IsPlayingAudio() {
		t.Fatal("expected IsPlayingAudio() == false initially")
	}

	// Feed audio: should immediately be recognized as active playback
	pcm := make([]float32, 1600) // 100ms
	cm.FeedCapturedPCM(pcm)

	if !cm.IsPlayingAudio() {
		t.Fatal("expected IsPlayingAudio() == true after FeedCapturedPCM")
	}

	// Flushing PCM should reset playback immediately
	cm.FlushCapturedPCM()
	if cm.IsPlayingAudio() {
		t.Fatal("expected IsPlayingAudio() == false immediately after FlushCapturedPCM")
	}

	// Feed again to test cooldown absorption
	cm.FeedCapturedPCM(pcm)
	cm.mu.Lock()
	cm.playQueue = nil // simulate playQueue drained by RTP loop
	cm.wasPlaying = false
	cm.lastPlayingAt = time.Now()
	cm.mu.Unlock()

	// Should still be considered playing within 350ms acoustic cooldown
	if !cm.IsPlayingAudio() {
		t.Fatal("expected IsPlayingAudio() == true during acoustic echo cooldown window")
	}

	// After cooldown window expires, should return false
	cm.mu.Lock()
	cm.lastPlayingAt = time.Now().Add(-400 * time.Millisecond)
	cm.mu.Unlock()

	if cm.IsPlayingAudio() {
		t.Fatal("expected IsPlayingAudio() == false after acoustic echo cooldown expires")
	}
}

func TestIsAudioPayloadType(t *testing.T) {
	audioPTs := []uint8{120, 121, 122, 96, 127}
	for _, pt := range audioPTs {
		if !core.IsAudioPayloadType(pt) {
			t.Errorf("expected IsAudioPayloadType(%d) == true, got false", pt)
		}
	}

	nonAudioPTs := []uint8{0, 8, 72, 74, 78, 80, 200, 201}
	for _, pt := range nonAudioPTs {
		if core.IsAudioPayloadType(pt) {
			t.Errorf("expected IsAudioPayloadType(%d) == false, got true", pt)
		}
	}
}

func TestOutboundCallPromotesFromRingingToActive(t *testing.T) {
	call := NewOutgoingCall("TESTCALL1", "94769652770@s.whatsapp.net", "94740979087@s.whatsapp.net", core.CallMediaTypeAudio)
	if err := call.ApplyTransition(Transition{Type: TransitionOfferSent}); err != nil {
		t.Fatalf("unexpected TransitionOfferSent err: %v", err)
	}
	if call.StateData.State != core.CallStateRinging {
		t.Fatalf("expected CallStateRinging, got %s", call.StateData.State)
	}

	// Simulate incoming decrypted peer audio arriving before WhatsApp <accept> stanza
	if err := call.ApplyTransition(Transition{Type: TransitionRemoteAccepted}); err != nil {
		t.Fatalf("unexpected TransitionRemoteAccepted err: %v", err)
	}
	if err := call.ApplyTransition(Transition{Type: TransitionMediaConnected}); err != nil {
		t.Fatalf("unexpected TransitionMediaConnected err: %v", err)
	}
	if !call.IsActive() {
		t.Fatalf("expected IsActive() == true, got state %s", call.StateData.State)
	}
	// Late HandleCallAccept should fail TransitionRemoteAccepted gracefully without mutating state
	if err := call.ApplyTransition(Transition{Type: TransitionRemoteAccepted}); err == nil {
		t.Fatal("expected TransitionRemoteAccepted to fail when already active")
	}
	if !call.IsActive() {
		t.Fatalf("expected call to remain active, got %s", call.StateData.State)
	}
}
