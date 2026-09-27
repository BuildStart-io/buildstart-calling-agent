package call

import (
	"testing"
	"time"
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
