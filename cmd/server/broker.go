package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type CallStatus string

const (
	StatusStarting  CallStatus = "starting"
	StatusRinging   CallStatus = "ringing"
	StatusConnected CallStatus = "connected"
	StatusEnded     CallStatus = "ended"
)

type CallEventLog struct {
	Timestamp int64  `json:"timestamp"`
	Type      string `json:"type"` // "offer", "connected", "speech", "ai", "callback", "end", "error"
	Message   string `json:"message"`
	Details   string `json:"details,omitempty"`
}

type CallTranscriptItem struct {
	Timestamp int64  `json:"timestamp"`
	Role      string `json:"role"` // "user" | "assistant" | "caller" | "agent"
	Text      string `json:"text"`
}

type CallRecord struct {
	SessionID       string               `json:"sessionId"`
	BusinessID      string               `json:"businessId"`
	Platform        string               `json:"platform,omitempty"`
	CallID          string               `json:"callId"`
	Owner           *string              `json:"owner"`
	Direction       string               `json:"direction"` // "inbound", "outbound", "auto-callback"
	Peer            string               `json:"peer"`
	PeerNumber      string               `json:"peerNumber"`
	ClientRef       string               `json:"clientRef,omitempty"`
	StartedAt       int64                `json:"startedAt"`
	Status          CallStatus           `json:"status"`
	ConnectedAt     *int64               `json:"connectedAt,omitempty"`
	EndedAt         *int64               `json:"endedAt,omitempty"`
	DurationSeconds int                  `json:"durationSeconds"`
	EndReason       string               `json:"endReason,omitempty"`
	Outcome         string               `json:"outcome,omitempty"`
	TriggerReason   string               `json:"triggerReason,omitempty"`
	Events          []CallEventLog       `json:"events"`
	Transcripts     []CallTranscriptItem `json:"transcripts"`
	Summary         string               `json:"summary,omitempty"`
}

func publicStreamHost() string {
	if h := strings.TrimSpace(os.Getenv("PUBLIC_HOST")); h != "" {
		return h
	}
	return "localhost:8080"
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func formatPhoneNumber(jidOrPhone string) string {
	cleaned := strings.TrimSuffix(jidOrPhone, "@s.whatsapp.net")
	cleaned = strings.TrimSuffix(cleaned, "@lid")
	if idx := strings.Index(cleaned, ":"); idx != -1 {
		cleaned = cleaned[:idx]
	}
	cleaned = strings.TrimPrefix(cleaned, "+")
	cleaned = strings.TrimSpace(cleaned)
	if isAllDigits(cleaned) && len(cleaned) >= 7 && len(cleaned) <= 15 {
		if len(cleaned) >= 14 && (strings.HasPrefix(cleaned, "1") || strings.HasPrefix(cleaned, "2")) {
			return "LID: " + cleaned
		}
		return "+" + cleaned
	}
	return jidOrPhone
}

type AuthSnapshot struct {
	State  string `json:"state"`
	Paired bool   `json:"paired"`
	QR     string `json:"qr,omitempty"`
}

type SessionInfo struct {
	ID         string `json:"id"`
	BusinessID string `json:"business_id"`
	Platform   string `json:"platform,omitempty"`
	JID        string `json:"jid"`
	State      string `json:"state"`
	Paired     bool   `json:"paired"`
	QR         string `json:"qr,omitempty"`
}

type subscriber struct {
	clientID   string
	platform   string
	sessionID  string
	businessID string
	ch         chan []byte
}

type Broker struct {
	mu      sync.RWMutex
	subs    map[*subscriber]struct{}
	calls   map[string]*CallRecord
	history []CallRecord
	store   *sessionStore

	SnapshotFn func(platform, sessionID, businessID string) []any
}

func NewBroker() *Broker {
	return &Broker{
		subs:  map[*subscriber]struct{}{},
		calls: map[string]*CallRecord{},
	}
}

func (b *Broker) setStore(s *sessionStore) {
	b.mu.Lock()
	b.store = s
	if s != nil {
		if rows, err := s.listCallHistory(context.Background(), "", "", 100); err == nil {
			b.history = rows
		}
	}
	b.mu.Unlock()
}

func (b *Broker) subscribe(clientID, platform, sessionID, businessID string) *subscriber {
	s := &subscriber{
		clientID:   clientID,
		platform:   platform,
		sessionID:  sessionID,
		businessID: businessID,
		ch:         make(chan []byte, 32),
	}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	return s
}

func (b *Broker) unsubscribe(s *subscriber) {
	b.mu.Lock()
	delete(b.subs, s)
	b.mu.Unlock()
	close(s.ch)
}

func (b *Broker) broadcastScoped(platform, sessionID, businessID string, ev any) {
	data, err := json.Marshal(ev)
	if err != nil {
		return
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for s := range b.subs {
		if platform != "" && s.platform != platform {
			continue
		}
		if s.sessionID != "" && sessionID != "" && s.sessionID != sessionID {
			continue
		}
		if s.businessID != "" && businessID != "" && s.businessID != businessID {
			continue
		}
		select {
		case s.ch <- data:
		default:
		}
	}
}

func (b *Broker) emitAuthState(platform, sessionID, businessID string, a AuthSnapshot) {
	b.broadcastScoped(platform, sessionID, businessID, map[string]any{
		"type": "auth-state", "sessionId": sessionID, "businessId": businessID,
		"paired": a.Paired, "state": a.State, "qr": a.QR,
	})
	b.broadcastScoped(platform, sessionID, businessID, map[string]any{
		"type": "state", "sessionId": sessionID, "businessId": businessID,
		"paired": a.Paired, "state": a.State, "qr": a.QR,
	})
}

func (b *Broker) emitSessionListForPlatform(sessions []SessionInfo) {
	byPlatform := map[string][]SessionInfo{}
	for _, s := range sessions {
		p := s.Platform
		if p == "" {
			p = "local"
		}
		byPlatform[p] = append(byPlatform[p], s)
	}
	for plat, list := range byPlatform {
		b.broadcastScoped(plat, "", "", map[string]any{"type": "session-list", "sessions": list})
	}
}

func (b *Broker) emitSessionQR(platform, sessionID, businessID, qr string) {
	b.broadcastScoped(platform, sessionID, businessID, map[string]any{"type": "session-qr", "sessionId": sessionID, "qr": qr})
	b.broadcastScoped(platform, sessionID, businessID, map[string]any{"type": "qr", "sessionId": sessionID, "qr": qr})
}

func (b *Broker) recordCallEvent(callID, eventType, message, details string) {
	b.mu.Lock()
	c, ok := b.calls[callID]
	if !ok {
		for i := range b.history {
			if b.history[i].CallID == callID {
				c = &b.history[i]
				ok = true
				break
			}
		}
	}
	ts := time.Now().UnixMilli()
	ev := CallEventLog{Timestamp: ts, Type: eventType, Message: message, Details: details}
	var plat, sid, bid string
	if ok && c != nil {
		plat = c.Platform
		sid = c.SessionID
		bid = c.BusinessID
		c.Events = append(c.Events, ev)
		if b.store != nil {
			go b.store.saveCallRecord(context.Background(), *c, c.BusinessID)
		}
	}
	b.mu.Unlock()

	if plat != "" {
		b.broadcastScoped(plat, sid, bid, map[string]any{
			"type": "call-event", "callId": callID, "eventType": eventType, "message": message, "details": details, "timestamp": ts,
		})
	}
}

func (b *Broker) setCallOutcome(callID, outcome, summary string) {
	b.mu.Lock()
	if c, ok := b.calls[callID]; ok {
		if outcome != "" {
			c.Outcome = outcome
		}
		if summary != "" {
			c.Summary = summary
		}
		if b.store != nil {
			go b.store.saveCallRecord(context.Background(), *c, c.BusinessID)
		}
	}
	b.mu.Unlock()
}

func (b *Broker) upsertCall(r CallRecord) {
	b.mu.Lock()
	if (r.PeerNumber == "" || strings.Contains(r.PeerNumber, "@lid") || strings.HasPrefix(r.PeerNumber, "LID:")) && b.store != nil {
		if resolved := b.store.resolvePhone(r.Peer); resolved != "" {
			r.PeerNumber = resolved
		}
	}
	if r.PeerNumber == "" && r.Peer != "" {
		r.PeerNumber = formatPhoneNumber(r.Peer)
	}

	var callToSave CallRecord
	existing, ok := b.calls[r.CallID]
	if ok && existing != nil {
		if r.BusinessID != "" {
			existing.BusinessID = r.BusinessID
		}
		if r.Platform != "" {
			existing.Platform = r.Platform
		}
		if r.Direction != "" {
			existing.Direction = r.Direction
		}
		if r.Peer != "" {
			existing.Peer = r.Peer
		}
		if r.PeerNumber != "" {
			existing.PeerNumber = r.PeerNumber
		}
		if r.Status != "" {
			existing.Status = r.Status
		}
		if r.ConnectedAt != nil {
			existing.ConnectedAt = r.ConnectedAt
		}
		if r.EndedAt != nil {
			existing.EndedAt = r.EndedAt
		}
		if r.DurationSeconds > 0 {
			existing.DurationSeconds = r.DurationSeconds
		}
		if r.EndReason != "" {
			existing.EndReason = r.EndReason
		}
		if r.Outcome != "" {
			existing.Outcome = r.Outcome
		}
		if r.TriggerReason != "" {
			existing.TriggerReason = r.TriggerReason
		}
		if r.Summary != "" {
			existing.Summary = r.Summary
		}
		callToSave = *existing
	} else {
		if r.Platform == "" {
			r.Platform = "local"
		}
		cp := r
		b.calls[r.CallID] = &cp
		callToSave = cp
	}

	if b.store != nil && callToSave.BusinessID != "" {
		go b.store.saveCallRecord(context.Background(), callToSave, callToSave.BusinessID)
	}
	b.mu.Unlock()

	b.broadcastCallListForPlatform(callToSave.Platform)
	streamURL := fmt.Sprintf("wss://%s/api/v1/calls/%s/audio", publicStreamHost(), callToSave.CallID)
	b.broadcastScoped(callToSave.Platform, callToSave.SessionID, callToSave.BusinessID, map[string]any{
		"type": "call-status", "sessionId": callToSave.SessionID, "id": callToSave.CallID, "callId": callToSave.CallID, "call_id": callToSave.CallID,
		"owner": callToSave.Owner, "status": callToSave.Status, "peer": callToSave.Peer, "peerNumber": callToSave.PeerNumber,
		"direction": callToSave.Direction, "startedAt": callToSave.StartedAt, "outcome": callToSave.Outcome,
		"triggerReason": callToSave.TriggerReason, "stream_url": streamURL, "streamUrl": streamURL,
	})
}

func (b *Broker) getCall(id string) (*CallRecord, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	c, ok := b.calls[id]
	if !ok {
		for _, h := range b.history {
			if h.CallID == id {
				cp := h
				return &cp, true
			}
		}
		return nil, false
	}
	cp := *c
	return &cp, true
}

func (b *Broker) setOwner(id, owner string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.calls[id]
	if !ok {
		return false
	}
	if c.Owner != nil && *c.Owner != owner {
		return false
	}
	c.Owner = &owner
	return true
}

func (b *Broker) ownerActiveCall(owner string) string {
	if owner == "" {
		return ""
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for id, c := range b.calls {
		if c.Owner != nil && *c.Owner == owner && c.Status != StatusEnded {
			return id
		}
	}
	return ""
}

func (b *Broker) endCall(id, reason string) {
	b.mu.Lock()
	c, ok := b.calls[id]
	if !ok {
		b.mu.Unlock()
		return
	}
	now := time.Now().UnixMilli()
	c.Status = StatusEnded
	c.EndedAt = &now
	c.EndReason = reason
	if c.StartedAt > 0 {
		c.DurationSeconds = int((now - c.StartedAt) / 1000)
	}
	ended := *c
	delete(b.calls, id)
	b.history = append(b.history, ended)
	if len(b.history) > 200 {
		b.history = b.history[len(b.history)-200:]
	}
	if b.store != nil && ended.BusinessID != "" {
		// Synchronous final save guarantees ended status and complete transcripts land cleanly.
		_ = b.store.saveCallRecord(context.Background(), ended, ended.BusinessID)
	}
	owner := c.Owner
	sessionID := c.SessionID
	businessID := c.BusinessID
	plat := c.Platform
	duration := ended.DurationSeconds
	outcome := ended.Outcome
	b.mu.Unlock()

	b.broadcastScoped(plat, sessionID, businessID, map[string]any{
		"type": "call-ended", "sessionId": sessionID, "id": id, "callId": id, "owner": owner, "reason": reason, "endedAt": now,
		"durationSeconds": duration, "outcome": outcome,
	})
	b.broadcastCallListForPlatform(plat)
}

func (b *Broker) broadcastCallListForPlatform(platform string) {
	b.mu.RLock()
	list := make([]CallRecord, 0, len(b.calls))
	for _, c := range b.calls {
		if platform == "" || c.Platform == platform {
			list = append(list, *c)
		}
	}
	b.mu.RUnlock()
	b.broadcastScoped(platform, "", "", map[string]any{"type": "call-list", "calls": list})
}

func (b *Broker) emitIncoming(platform, sessionID, businessID, id, peer string) {
	streamURL := fmt.Sprintf("wss://%s/api/v1/calls/%s/audio", publicStreamHost(), id)
	b.broadcastScoped(platform, sessionID, businessID, map[string]any{
		"type": "incoming", "sessionId": sessionID, "id": id, "callId": id, "call_id": id,
		"peer": peer, "offeredAt": time.Now().UnixMilli(),
		"stream_url": streamURL, "streamUrl": streamURL,
	})
}

func (b *Broker) emitIncomingClaimed(platform, sessionID, businessID, id, owner string) {
	b.broadcastScoped(platform, sessionID, businessID, map[string]any{"type": "incoming-claimed", "sessionId": sessionID, "id": id, "owner": owner})
}

func (b *Broker) emitAgentTranscript(sessionID, callID, role, text string, ts int64) {
	b.mu.Lock()
	var plat, bid string
	if c, ok := b.calls[callID]; ok {
		plat = c.Platform
		bid = c.BusinessID
		c.Transcripts = append(c.Transcripts, CallTranscriptItem{
			Timestamp: ts,
			Role:      role,
			Text:      text,
		})
		c.Events = append(c.Events, CallEventLog{
			Timestamp: ts,
			Type:      "speech",
			Message:   fmt.Sprintf("[%s]: %s", role, text),
		})
		if b.store != nil && c.BusinessID != "" {
			go b.store.saveCallRecord(context.Background(), *c, c.BusinessID)
		}
	}
	b.mu.Unlock()

	if plat != "" {
		b.broadcastScoped(plat, sessionID, bid, map[string]any{
			"type": "agent-transcript", "sessionId": sessionID, "callId": callID,
			"role": role, "text": text, "timestamp": ts,
		})
	}
}

func (b *Broker) emitAgentStatus(platform, sessionID, businessID, callID string, enabled bool, state string) {
	b.broadcastScoped(platform, sessionID, businessID, map[string]any{
		"type": "agent-status", "sessionId": sessionID, "callId": callID,
		"enabled": enabled, "state": state,
	})
}

func (b *Broker) historyRows(platform, sessionID string, limit int) []CallRecord {
	if b.store != nil {
		if rows, err := b.store.listCallHistory(context.Background(), platform, sessionID, limit); err == nil && len(rows) > 0 {
			return rows
		}
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	rows := make([]CallRecord, 0, limit)
	for i := len(b.history) - 1; i >= 0 && len(rows) < limit; i-- {
		h := b.history[i]
		if platform != "" && h.Platform != platform {
			continue
		}
		if sessionID != "" && h.SessionID != sessionID {
			continue
		}
		rows = append(rows, h)
	}
	return rows
}

func (b *Broker) serveSSE(w http.ResponseWriter, r *http.Request, clientID, platform, sessionID, businessID string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher.Flush()

	sub := b.subscribe(clientID, platform, sessionID, businessID)
	defer b.unsubscribe(sub)

	if b.SnapshotFn != nil {
		for _, ev := range b.SnapshotFn(platform, sessionID, businessID) {
			writeSSE(w, flusher, ev)
		}
	}
	b.broadcastCallListForPlatform(platform)

	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-sub.ch:
			if _, err := w.Write(append(append([]byte("data: "), data...), '\n', '\n')); err != nil {
				return
			}
			flusher.Flush()
		case <-keepalive.C:
			_, _ = w.Write([]byte(": ping\n\n"))
			flusher.Flush()
		}
	}
}

func writeSSE(w http.ResponseWriter, f http.Flusher, ev any) {
	data, _ := json.Marshal(ev)
	_, _ = w.Write(append(append([]byte("data: "), data...), '\n', '\n'))
	f.Flush()
}
