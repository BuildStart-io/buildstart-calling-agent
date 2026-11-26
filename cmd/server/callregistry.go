package main

import (
	"sync"
	"time"

	"wacalls/internal/agent"
	"wacalls/internal/voip/call"

	"go.mau.fi/whatsmeow/types"
)

type ToolInvocationRecord struct {
	Name       string         `json:"name"`
	Args       map[string]any `json:"args,omitempty"`
	Result     map[string]any `json:"result,omitempty"`
	DurationMs int64          `json:"duration_ms"`
	Error      string         `json:"error,omitempty"`
}

type activeCall struct {
	cm                *call.CallManager
	bridge            *Bridge
	geminiLive        *agent.GeminiLiveAgent
	callbackJID       types.JID
	peerNum           string
	callDirection     string
	clientRef         string
	peerAudioReceived bool
	audioListenersMu  sync.Mutex
	audioListeners    map[int]func([]float32)
	nextListenerID    int
	greetingMessage   string
	fallbackMessage   string
	maxCallSeconds    int
	recorder          *CallAudioRecorder

	durationMu  sync.Mutex
	warnTimer   *time.Timer
	cutoffTimer *time.Timer

	toolsMu         sync.Mutex
	toolInvocations []ToolInvocationRecord
}

func (ac *activeCall) recordToolInvocation(rec ToolInvocationRecord) {
	ac.toolsMu.Lock()
	ac.toolInvocations = append(ac.toolInvocations, rec)
	ac.toolsMu.Unlock()
}

func (ac *activeCall) getToolInvocations() []ToolInvocationRecord {
	ac.toolsMu.Lock()
	defer ac.toolsMu.Unlock()
	if len(ac.toolInvocations) == 0 {
		return nil
	}
	out := make([]ToolInvocationRecord, len(ac.toolInvocations))
	copy(out, ac.toolInvocations)
	return out
}

func (ac *activeCall) cancelDurationTimers() {
	ac.durationMu.Lock()
	if ac.warnTimer != nil {
		ac.warnTimer.Stop()
		ac.warnTimer = nil
	}
	if ac.cutoffTimer != nil {
		ac.cutoffTimer.Stop()
		ac.cutoffTimer = nil
	}
	ac.durationMu.Unlock()
}

func (ac *activeCall) PushAudio(pcm []float32) {
	if ac.recorder != nil {
		ac.recorder.RecordAgentPCM(pcm)
	}
	if ac.cm != nil {
		ac.cm.FeedCapturedPCM(pcm)
	}
}

func (ac *activeCall) addAudioListener(fn func([]float32)) func() {
	ac.audioListenersMu.Lock()
	if ac.audioListeners == nil {
		ac.audioListeners = make(map[int]func([]float32))
	}
	id := ac.nextListenerID
	ac.nextListenerID++
	ac.audioListeners[id] = fn
	ac.audioListenersMu.Unlock()

	return func() {
		ac.audioListenersMu.Lock()
		delete(ac.audioListeners, id)
		ac.audioListenersMu.Unlock()
	}
}

func (ac *activeCall) broadcastAudio(pcm []float32) {
	ac.audioListenersMu.Lock()
	defer ac.audioListenersMu.Unlock()
	for _, fn := range ac.audioListeners {
		fn(pcm)
	}
}

type callRegistry struct {
	mu    sync.Mutex
	calls map[string]*activeCall
}

func newCallRegistry() *callRegistry {
	return &callRegistry{calls: map[string]*activeCall{}}
}

func (r *callRegistry) add(callID string, ac *activeCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls[callID] = ac
}

func (r *callRegistry) get(callID string) (*activeCall, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	return ac, ok
}

func (r *callRegistry) remove(callID string) (*activeCall, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	if !ok {
		return nil, false
	}
	delete(r.calls, callID)
	return ac, true
}

func (r *callRegistry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *callRegistry) setBridge(callID string, b *Bridge) (*Bridge, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	if !ok {
		return nil, false
	}
	oldB := ac.bridge
	ac.bridge = b
	return oldB, true
}

func (r *callRegistry) drain() []*activeCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*activeCall, 0, len(r.calls))
	for _, ac := range r.calls {
		out = append(out, ac)
	}
	r.calls = map[string]*activeCall{}
	return out
}

func (r *callRegistry) all() []*activeCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*activeCall, 0, len(r.calls))
	for _, ac := range r.calls {
		out = append(out, ac)
	}
	return out
}
