package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"

	"github.com/coder/websocket"
	"go.mau.fi/whatsmeow/types"
)

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()

	// Health and Status
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("GET /api/v1/health", s.handleHealth)
	mux.HandleFunc("GET /status", s.handleHealth)
	mux.HandleFunc("GET /api/status", s.handleHealth)
	mux.HandleFunc("GET /api/v1/status", s.handleHealth)

	// Lines / Sessions
	mux.HandleFunc("GET /api/lines", s.handleLines)
	mux.HandleFunc("GET /api/v1/lines", s.handleLines)
	mux.HandleFunc("GET /api/sessions", s.handleSessionList)
	mux.HandleFunc("GET /api/v1/sessions", s.handleSessionList)
	mux.HandleFunc("POST /api/sessions", s.handleSessionCreate)
	mux.HandleFunc("POST /api/v1/sessions", s.handleSessionCreate)
	mux.HandleFunc("DELETE /api/sessions/{sid}", s.handleSessionDelete)
	mux.HandleFunc("DELETE /api/v1/sessions/{sid}", s.handleSessionDelete)
	mux.HandleFunc("POST /api/sessions/{sid}/logout", s.handleSessionLogout)
	mux.HandleFunc("POST /api/sessions/{sid}/pair", s.handleSessionPair)

	// Direct Call Dialing & Listing (no sid needed - uses default connected session)
	mux.HandleFunc("GET /api/calls", s.handleCallsList)
	mux.HandleFunc("GET /api/v1/calls", s.handleCallsList)
	mux.HandleFunc("GET /api/calls/{id}", s.handleCallGet)
	mux.HandleFunc("GET /api/v1/calls/{id}", s.handleCallGet)
	mux.HandleFunc("POST /api/calls/dial", s.handleDirectDial)
	mux.HandleFunc("POST /api/v1/calls/dial", s.handleDirectDial)
	mux.HandleFunc("POST /api/calls", s.handleDirectDial)
	mux.HandleFunc("POST /api/v1/calls", s.handleDirectDial)
	mux.HandleFunc("POST /api/sessions/{sid}/calls", s.handleStartCall)

	// Direct WebRTC & WebSocket Audio Streaming
	mux.HandleFunc("POST /api/calls/{id}/webrtc", s.handleDirectWebRTC)
	mux.HandleFunc("POST /api/v1/calls/{id}/webrtc", s.handleDirectWebRTC)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/webrtc", s.handleWebRTC)
	mux.HandleFunc("GET /api/calls/{id}/audio", s.handleDirectAudioWS)
	mux.HandleFunc("GET /api/v1/calls/{id}/audio", s.handleDirectAudioWS)
	mux.HandleFunc("GET /ws/audio", s.handleDirectAudioWS)

	// Call Control (Accept, Reject, Hangup/End)
	mux.HandleFunc("POST /api/calls/{id}/accept", s.handleDirectAccept)
	mux.HandleFunc("POST /api/v1/calls/{id}/accept", s.handleDirectAccept)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/accept", s.handleAccept)

	mux.HandleFunc("POST /api/calls/{id}/reject", s.handleDirectReject)
	mux.HandleFunc("POST /api/v1/calls/{id}/reject", s.handleDirectReject)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/reject", s.handleReject)

	mux.HandleFunc("POST /api/calls/{id}/hangup", s.handleDirectHangup)
	mux.HandleFunc("POST /api/v1/calls/{id}/hangup", s.handleDirectHangup)
	mux.HandleFunc("POST /api/calls/{id}/end", s.handleDirectHangup)
	mux.HandleFunc("POST /api/v1/calls/{id}/end", s.handleDirectHangup)
	mux.HandleFunc("DELETE /api/calls/{id}", s.handleDirectHangup)
	mux.HandleFunc("DELETE /api/v1/calls/{id}", s.handleDirectHangup)
	mux.HandleFunc("DELETE /api/sessions/{sid}/calls/{id}", s.handleEndCall)

	// History
	mux.HandleFunc("GET /api/history", s.handleDirectHistory)
	mux.HandleFunc("GET /api/v1/history", s.handleDirectHistory)
	mux.HandleFunc("GET /api/sessions/{sid}/history", s.handleHistory)

	// Agent Config

	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/agent", s.handleToggleCallAgent)

	// Real-time Events (SSE)
	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.HandleFunc("GET /api/v1/events", s.handleEvents)

	if s.staticDir != "" {
		if _, err := os.Stat(s.staticDir); err == nil {
			mux.Handle("/", http.FileServer(http.Dir(s.staticDir)))
		}
	}
	return s.withCORS(mux)
}

func (s *server) withCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.log.Info("http request", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Client-Id, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func clientID(r *http.Request) string {
	if id := r.Header.Get("X-Client-Id"); id != "" {
		return id
	}
	if id := r.URL.Query().Get("clientId"); id != "" {
		return id
	}
	return r.URL.Query().Get("session_id")
}

func (s *server) defaultSession() *Session {
	infos := s.sessions.infos()
	for _, info := range infos {
		if sess, ok := s.sessions.Get(info.ID); ok && sess.client != nil && sess.client.IsConnected() {
			return sess
		}
	}
	if len(infos) > 0 {
		if sess, ok := s.sessions.Get(infos[0].ID); ok {
			return sess
		}
	}
	return nil
}

func (s *server) findCall(callID string) (*Session, *activeCall) {
	for _, sess := range s.sessions.allSessions() {
		if ac, ok := sess.reg.get(callID); ok {
			return sess, ac
		}
	}
	return nil, nil
}

func (s *server) sessionByID(w http.ResponseWriter, sid string) *Session {
	sess, ok := s.sessions.Get(sid)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such session"})
		return nil
	}
	return sess
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	infos := s.sessions.infos()
	connectedSessions := 0
	var phones []string
	for _, info := range infos {
		if info.State == "connected" || info.Paired {
			connectedSessions++
			if info.JID != "" {
				phones = append(phones, info.JID)
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":             "online",
		"whatsapp_connected": connectedSessions > 0,
		"sessions_count":     len(infos),
		"connected_count":    connectedSessions,
		"phones":             phones,
		"timestamp":          time.Now().Unix(),
	})
}

func (s *server) handleLines(w http.ResponseWriter, r *http.Request) {
	infos := s.sessions.infos()
	type lineItem struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		Phone     string `json:"phone"`
		Status    string `json:"status"`
		State     string `json:"state"`
		Connected bool   `json:"connected"`
		Paired    bool   `json:"paired"`
	}
	var lines []lineItem
	for _, info := range infos {
		connected := info.State == "connected" || info.Paired
		status := "disconnected"
		if connected {
			status = "ready"
		}
		lines = append(lines, lineItem{
			ID:        info.ID,
			Name:      info.BusinessID,
			Phone:     info.JID,
			Status:    status,
			State:     info.State,
			Connected: connected,
			Paired:    info.Paired,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"lines":    lines,
		"sessions": infos,
	})
}

func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	s.broker.serveSSE(w, r, clientID(r))
}

func (s *server) handleSessionList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"sessions": s.sessions.infos()})
}

func (s *server) handleSessionCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		BusinessID string `json:"business_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	businessID := strings.TrimSpace(body.BusinessID)
	if businessID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "business_id is required"})
		return
	}
	id, err := s.sessions.Create(businessID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": id})
}

func (s *server) handleSessionDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.sessions.Delete(r.Context(), r.PathValue("sid")); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleSessionLogout(w http.ResponseWriter, r *http.Request) {
	if err := s.sessions.Logout(r.Context(), r.PathValue("sid")); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleSessionPair(w http.ResponseWriter, r *http.Request) {
	if err := s.sessions.Pair(r.PathValue("sid")); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleCallsList(w http.ResponseWriter, r *http.Request) {
	s.broker.mu.RLock()
	var list []map[string]any
	host := r.Host
	if host == "" {
		host = "buildstart-calling-agent.buildstart.io"
	}
	for _, c := range s.broker.calls {
		streamURL := fmt.Sprintf("wss://%s/api/v1/calls/%s/audio", host, c.CallID)
		list = append(list, map[string]any{
			"id":         c.CallID,
			"callId":     c.CallID,
			"call_id":    c.CallID,
			"sessionId":  c.SessionID,
			"status":     c.Status,
			"peer":       c.Peer,
			"peerNumber": c.PeerNumber,
			"direction":  c.Direction,
			"startedAt":  c.StartedAt,
			"stream_url": streamURL,
			"streamUrl":  streamURL,
		})
	}
	s.broker.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"calls": list,
		"rows":  list,
	})
}

func (s *server) handleCallGet(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	c, ok := s.broker.getCall(callID)
	if !ok || c == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "call not found"})
		return
	}
	host := r.Host
	if host == "" {
		host = "buildstart-calling-agent.buildstart.io"
	}
	streamURL := fmt.Sprintf("wss://%s/api/v1/calls/%s/audio", host, c.CallID)
	writeJSON(w, http.StatusOK, map[string]any{
		"id":         c.CallID,
		"callId":     c.CallID,
		"call_id":    c.CallID,
		"sessionId":  c.SessionID,
		"status":     c.Status,
		"peer":       c.Peer,
		"peerNumber": c.PeerNumber,
		"direction":  c.Direction,
		"startedAt":  c.StartedAt,
		"stream_url": streamURL,
		"streamUrl":  streamURL,
		"call": map[string]any{
			"id":        c.CallID,
			"callId":    c.CallID,
			"sessionId": c.SessionID,
			"status":    c.Status,
			"streamUrl": streamURL,
		},
	})
}

func (s *server) handleDirectDial(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Phone     string `json:"phone"`
		To        string `json:"to"`
		Number    string `json:"number"`
		Recipient string `json:"recipient"`
		SessionID string `json:"session_id"`
		Greeting  string `json:"initial_greeting"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	rawPhone := body.Phone
	if rawPhone == "" {
		rawPhone = body.To
	}
	if rawPhone == "" {
		rawPhone = body.Number
	}
	if rawPhone == "" {
		rawPhone = body.Recipient
	}
	rawPhone = strings.TrimSpace(rawPhone)
	if rawPhone == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "phone/to is required"})
		return
	}

	var sess *Session
	if body.SessionID != "" {
		sess, _ = s.sessions.Get(body.SessionID)
	}
	if sess == nil {
		sess = s.defaultSession()
	}
	if sess == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no active whatsapp session available"})
		return
	}

	owner := clientID(r)
	if other := s.broker.ownerActiveCall(owner); other != "" {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "operator already on a call"})
		return
	}
	if max := s.sessions.maxCalls; max > 0 && sess.reg.count() >= max {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "max concurrent calls reached"})
		return
	}

	phone := normalizePhone(rawPhone)
	if pn := sess.mgr.store.getPNForLID(r.Context(), phone); pn != "" {
		phone = pn
	} else if pn := sess.mgr.store.getPNForLID(r.Context(), rawPhone); pn != "" {
		phone = pn
	}
	peer := types.NewJID(phone, types.DefaultUserServer)

	var callID string
	var err error
	if body.Greeting != "" {
		callID, err = sess.startOutgoingWithGreeting(r.Context(), peer, false, body.Greeting)
	} else {
		callID, err = sess.startOutgoing(r.Context(), peer, false)
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	peerNum := sess.mgr.store.resolvePhone(peer.User)
	s.broker.upsertCall(CallRecord{
		SessionID: sess.id, BusinessID: sess.businessID, CallID: callID, Owner: &owner, Direction: "outbound", Peer: peer.String(),
		PeerNumber: peerNum, StartedAt: time.Now().UnixMilli(), Status: StatusRinging,
	})

	host := r.Host
	if host == "" {
		host = "buildstart-calling-agent.buildstart.io"
	}
	streamURL := fmt.Sprintf("wss://%s/api/v1/calls/%s/audio", host, callID)

	writeJSON(w, http.StatusOK, map[string]any{
		"call_id":    callID,
		"callId":     callID,
		"id":         callID,
		"status":     "ringing",
		"peer":       peer.String(),
		"stream_url": streamURL,
		"call": map[string]any{
			"callId":    callID,
			"sessionId": sess.id,
			"status":    "ringing",
			"peer":      peer.String(),
			"streamUrl": streamURL,
		},
	})
}

func (s *server) handleStartCall(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doStartCall(sess, w, r)
	}
}

func (s *server) handleDirectWebRTC(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	sess, _ := s.findCall(callID)
	if sess == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	s.doWebRTC(sess, w, r)
}

func (s *server) handleWebRTC(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doWebRTC(sess, w, r)
	}
}

func (s *server) handleDirectAudioWS(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	sess, ac := s.findCall(callID)
	if sess == nil || ac == nil {
		http.Error(w, "call not found", http.StatusNotFound)
		return
	}

	// Silences any local AI agent so only Lovable handles audio
	if ac.geminiLive != nil {
		ac.geminiLive.SetEnabled(false)
	}

	rate := 16000
	if r.URL.Query().Get("rate") == "24000" || r.URL.Query().Get("sample_rate") == "24000" {
		rate = 24000
	}

	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		s.log.Error("ws accept failed", "err", err)
		return
	}
	defer c.Close(websocket.StatusNormalClosure, "call ended")

	ctx := r.Context()
	audioChan := make(chan []byte, 100)
	removeListener := ac.addAudioListener(func(pcm []float32) {
		// WhatsApp caller audio to Gemini Live input is ALWAYS 16 kHz PCM
		data := media.PCMFloat32ToInt16LE(pcm)
		select {
		case audioChan <- data:
		default:
		}
	})
	defer removeListener()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-audioChan:
				if !ok {
					return
				}
				if err := c.Write(ctx, websocket.MessageBinary, data); err != nil {
					return
				}
			}
		}
	}()

	var byteBuf []byte
	var resampleBuf []float32

	for {
		msgType, data, err := c.Read(ctx)
		if err != nil {
			break
		}
		var rawBytes []byte
		if msgType == websocket.MessageBinary && len(data) > 0 {
			rawBytes = data
		} else if msgType == websocket.MessageText && len(data) > 0 {
			var textPayload struct {
				Type   string `json:"type"`
				Event  string `json:"event"`
				Action string `json:"action"`
				Audio  string `json:"audio"`
				Data   string `json:"data"`
				PCM    string `json:"pcm"`
				Chunk  string `json:"chunk"`
			}
			if err := json.Unmarshal(data, &textPayload); err == nil {
				if textPayload.Type == "clear" || textPayload.Type == "interrupt" || textPayload.Type == "flush" ||
					textPayload.Event == "clear" || textPayload.Event == "interrupted" || textPayload.Action == "flush" {
					ac.cm.FlushCapturedPCM()
					byteBuf = nil
					resampleBuf = nil
					continue
				}
				b64 := textPayload.Audio
				if b64 == "" {
					b64 = textPayload.Data
				}
				if b64 == "" {
					b64 = textPayload.PCM
				}
				if b64 == "" {
					b64 = textPayload.Chunk
				}
				if b64 != "" {
					rawBytes, _ = base64.StdEncoding.DecodeString(b64)
				}
			} else {
				rawBytes, _ = base64.StdEncoding.DecodeString(string(data))
			}
		}

		if len(rawBytes) > 0 {
			byteBuf = append(byteBuf, rawBytes...)
			if len(byteBuf) >= 2 {
				validBytes := (len(byteBuf) / 2) * 2
				pcm := media.PCMInt16LEToFloat32(byteBuf[:validBytes])
				byteBuf = append([]byte(nil), byteBuf[validBytes:]...)

				if rate == 24000 {
					resampleBuf = append(resampleBuf, pcm...)
					validLen := (len(resampleBuf) / 3) * 3
					if validLen > 0 {
						toResample := resampleBuf[:validLen]
						resampled := media.Resample24kTo16k(toResample)
						ac.cm.FeedCapturedPCM(resampled)
						resampleBuf = append([]float32(nil), resampleBuf[validLen:]...)
					}
				} else if rate != 16000 {
					pcm = media.ResampleFloat32(pcm, rate, 16000)
					ac.cm.FeedCapturedPCM(pcm)
				} else {
					ac.cm.FeedCapturedPCM(pcm)
				}
			}
		}
	}
}

func (s *server) handleDirectAccept(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	sess, _ := s.findCall(callID)
	if sess == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	s.doAccept(sess, w, r)
}

func (s *server) handleAccept(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doAccept(sess, w, r)
	}
}

func (s *server) handleDirectReject(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	sess, _ := s.findCall(callID)
	if sess == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	s.doReject(sess, w, r)
}

func (s *server) handleReject(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doReject(sess, w, r)
	}
}

func (s *server) handleDirectHangup(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	sess, _ := s.findCall(callID)
	if sess == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ended"})
		return
	}
	s.doEndCall(sess, w, r)
}

func (s *server) handleEndCall(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doEndCall(sess, w, r)
	}
}

func (s *server) handleDirectHistory(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"rows": s.broker.historyRows("", 50)})
}

func (s *server) handleHistory(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		writeJSON(w, http.StatusOK, map[string]any{"rows": s.broker.historyRows(sess.id, 50)})
	}
}

func (s *server) doStartCall(sess *Session, w http.ResponseWriter, r *http.Request) {
	if sess.client.Store.ID == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not paired"})
		return
	}
	var body struct {
		Phone      string `json:"phone"`
		To         string `json:"to"`
		Number     string `json:"number"`
		DurationMs int    `json:"duration_ms"`
		Record     bool   `json:"record"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	rawPhone := body.Phone
	if rawPhone == "" {
		rawPhone = body.To
	}
	if rawPhone == "" {
		rawPhone = body.Number
	}
	if strings.TrimSpace(rawPhone) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "phone required"})
		return
	}
	owner := clientID(r)
	if other := s.broker.ownerActiveCall(owner); other != "" {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "operator already on a call"})
		return
	}
	if max := s.sessions.maxCalls; max > 0 && sess.reg.count() >= max {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "max concurrent calls"})
		return
	}
	phone := normalizePhone(rawPhone)
	if pn := sess.mgr.store.getPNForLID(r.Context(), phone); pn != "" {
		phone = pn
	} else if pn := sess.mgr.store.getPNForLID(r.Context(), rawPhone); pn != "" {
		phone = pn
	}
	peer := types.NewJID(phone, types.DefaultUserServer)

	callID, err := sess.startOutgoing(r.Context(), peer, false)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.broker.upsertCall(CallRecord{
		SessionID: sess.id, BusinessID: sess.businessID, CallID: callID, Owner: &owner, Direction: "outbound", Peer: peer.String(),
		StartedAt: time.Now().UnixMilli(), Status: StatusRinging,
	})
	host := r.Host
	if host == "" {
		host = "buildstart-calling-agent.buildstart.io"
	}
	streamURL := fmt.Sprintf("wss://%s/api/v1/calls/%s/audio", host, callID)

	writeJSON(w, http.StatusOK, map[string]any{
		"call_id":    callID,
		"callId":     callID,
		"id":         callID,
		"status":     "ringing",
		"peer":       peer.String(),
		"stream_url": streamURL,
		"call": map[string]any{
			"callId":    callID,
			"sessionId": sess.id,
			"status":    "ringing",
			"streamUrl": streamURL,
		},
	})
}

func (s *server) doWebRTC(sess *Session, w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	ac, ok := sess.reg.get(callID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	if ac.geminiLive != nil {
		ac.geminiLive.SetEnabled(false)
	}
	var body struct {
		SDPOffer string `json:"sdp_offer"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.SDPOffer == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sdp_offer required"})
		return
	}
	bridge, answer, err := NewBridge(body.SDPOffer, s.log)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	bridge.OnBrowserPCM = func(pcm []float32) {
		ac.cm.FeedCapturedPCM(pcm)
	}
	bridge.OnTerminalICE = func() {
		go sess.terminateCall(callID, core.EndCallReasonUserEnded)
	}
	sess.setBridge(callID, bridge)
	writeJSON(w, http.StatusOK, map[string]string{"sdp_answer": answer})
}

func (s *server) doAccept(sess *Session, w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ac, ok := sess.reg.get(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	owner := clientID(r)
	if other := s.broker.ownerActiveCall(owner); other != "" && other != id {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "operator already on a call"})
		return
	}
	if !s.broker.setOwner(id, owner) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "claimed by another client"})
		return
	}
	s.broker.emitIncomingClaimed(sess.id, id, owner)
	if err := ac.cm.AcceptCall(r.Context(), id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"call": map[string]string{"callId": id}})
}

func (s *server) doReject(sess *Session, w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if ac, ok := sess.reg.get(id); ok {
		_ = ac.cm.RejectCall(r.Context(), id, core.EndCallReasonDeclined)
	}
	sess.removeCall(id)
	s.broker.endCall(id, string(core.EndCallReasonDeclined))
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) doEndCall(sess *Session, w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if ac, ok := sess.reg.get(id); ok {
		_ = ac.cm.EndCall(r.Context(), core.EndCallReasonUserEnded)
	}
	sess.removeCall(id)
	s.broker.endCall(id, string(core.EndCallReasonUserEnded))
	writeJSON(w, http.StatusOK, map[string]string{"status": "ended"})
}

func normalizePhone(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "+")
	var b strings.Builder
	for _, c := range p {
		if c >= '0' && c <= '9' {
			b.WriteRune(c)
		}
	}
	return b.String()
}


func (s *server) handleToggleCallAgent(w http.ResponseWriter, r *http.Request) {
	sid := r.PathValue("sid")
	cid := r.PathValue("id")
	sess := s.sessionByID(w, sid)
	if sess == nil {
		return
	}
	ac, ok := sess.reg.get(cid)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	var body struct {
		Enabled bool `json:"enabled"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if ac.geminiLive != nil {
		ac.geminiLive.SetEnabled(body.Enabled)
		s.broker.emitAgentStatus(sid, cid, body.Enabled, "idle")
	}
	writeJSON(w, http.StatusOK, map[string]any{"callId": cid, "agentEnabled": body.Enabled})
}


