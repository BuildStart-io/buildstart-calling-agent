package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
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
	mux.HandleFunc("POST /api/v1/sessions/{sid}/logout", s.handleSessionLogout)
	mux.HandleFunc("POST /api/sessions/{sid}/pair", s.handleSessionPair)
	mux.HandleFunc("POST /api/v1/sessions/{sid}/pair", s.handleSessionPair)

	// Messaging, Media, Presence & Read Receipts
	mux.HandleFunc("POST /api/sessions/{sid}/messages/send", s.handleSessionSendMessage)
	mux.HandleFunc("POST /api/v1/sessions/{sid}/messages/send", s.handleSessionSendMessage)
	mux.HandleFunc("GET /api/sessions/{sid}/messages/{id}/media", s.handleSessionDownloadMedia)
	mux.HandleFunc("GET /api/v1/sessions/{sid}/messages/{id}/media", s.handleSessionDownloadMedia)
	mux.HandleFunc("POST /api/sessions/{sid}/presence", s.handleSessionPresence)
	mux.HandleFunc("POST /api/v1/sessions/{sid}/presence", s.handleSessionPresence)
	mux.HandleFunc("POST /api/sessions/{sid}/messages/{id}/read", s.handleSessionMarkRead)
	mux.HandleFunc("POST /api/v1/sessions/{sid}/messages/{id}/read", s.handleSessionMarkRead)

	// Call Dialing & Listing (session_id required on /calls/dial for tenant isolation)
	mux.HandleFunc("GET /api/calls", s.handleCallsList)
	mux.HandleFunc("GET /api/v1/calls", s.handleCallsList)
	mux.HandleFunc("GET /api/calls/{id}", s.handleCallGet)
	mux.HandleFunc("GET /api/v1/calls/{id}", s.handleCallGet)
	mux.HandleFunc("POST /api/calls/dial", s.handleDirectDial)
	mux.HandleFunc("POST /api/v1/calls/dial", s.handleDirectDial)
	mux.HandleFunc("POST /api/calls", s.handleDirectDial)
	mux.HandleFunc("POST /api/v1/calls", s.handleDirectDial)
	mux.HandleFunc("POST /api/sessions/{sid}/calls", s.handleStartCall)
	mux.HandleFunc("POST /api/v1/sessions/{sid}/calls", s.handleStartCall)

	// Recordings (Private bucket streaming / presigned access)
	mux.HandleFunc("GET /api/sessions/{sid}/calls/{id}/recording", s.handleSessionGetRecording)
	mux.HandleFunc("GET /api/v1/sessions/{sid}/calls/{id}/recording", s.handleSessionGetRecording)

	// Direct WebRTC & WebSocket Audio Streaming
	mux.HandleFunc("POST /api/calls/{id}/webrtc", s.handleDirectWebRTC)
	mux.HandleFunc("POST /api/v1/calls/{id}/webrtc", s.handleDirectWebRTC)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/webrtc", s.handleWebRTC)
	mux.HandleFunc("POST /api/v1/sessions/{sid}/calls/{id}/webrtc", s.handleWebRTC)
	mux.HandleFunc("GET /api/calls/{id}/audio", s.handleDirectAudioWS)
	mux.HandleFunc("GET /api/v1/calls/{id}/audio", s.handleDirectAudioWS)
	mux.HandleFunc("GET /ws/audio", s.handleDirectAudioWS)

	// Call Control (Accept, Reject, Hangup/End)
	mux.HandleFunc("POST /api/calls/{id}/accept", s.handleDirectAccept)
	mux.HandleFunc("POST /api/v1/calls/{id}/accept", s.handleDirectAccept)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/accept", s.handleAccept)
	mux.HandleFunc("POST /api/v1/sessions/{sid}/calls/{id}/accept", s.handleAccept)

	mux.HandleFunc("POST /api/calls/{id}/reject", s.handleDirectReject)
	mux.HandleFunc("POST /api/v1/calls/{id}/reject", s.handleDirectReject)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/reject", s.handleReject)
	mux.HandleFunc("POST /api/v1/sessions/{sid}/calls/{id}/reject", s.handleReject)

	mux.HandleFunc("POST /api/calls/{id}/hangup", s.handleDirectHangup)
	mux.HandleFunc("POST /api/v1/calls/{id}/hangup", s.handleDirectHangup)
	mux.HandleFunc("POST /api/calls/{id}/end", s.handleDirectHangup)
	mux.HandleFunc("POST /api/v1/calls/{id}/end", s.handleDirectHangup)
	mux.HandleFunc("DELETE /api/calls/{id}", s.handleDirectHangup)
	mux.HandleFunc("DELETE /api/v1/calls/{id}", s.handleDirectHangup)
	mux.HandleFunc("DELETE /api/sessions/{sid}/calls/{id}", s.handleEndCall)
	mux.HandleFunc("DELETE /api/v1/sessions/{sid}/calls/{id}", s.handleEndCall)

	// History
	mux.HandleFunc("GET /api/history", s.handleDirectHistory)
	mux.HandleFunc("GET /api/v1/history", s.handleDirectHistory)
	mux.HandleFunc("GET /api/sessions/{sid}/history", s.handleHistory)

	// Agent Configs
	mux.HandleFunc("PUT /api/agent-configs/{business_id}", s.handlePutAgentConfig)
	mux.HandleFunc("PUT /api/v1/agent-configs/{business_id}", s.handlePutAgentConfig)
	mux.HandleFunc("GET /api/agent-configs/{business_id}", s.handleGetAgentConfig)
	mux.HandleFunc("GET /api/v1/agent-configs/{business_id}", s.handleGetAgentConfig)
	mux.HandleFunc("DELETE /api/agent-configs/{business_id}", s.handleDeleteAgentConfig)
	mux.HandleFunc("DELETE /api/v1/agent-configs/{business_id}", s.handleDeleteAgentConfig)

	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/agent", s.handleToggleCallAgent)

	// Real-time Events (SSE)
	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.HandleFunc("GET /api/v1/events", s.handleEvents)

	if strings.EqualFold(strings.TrimSpace(os.Getenv("SERVE_STATIC_CONSOLE")), "true") && s.staticDir != "" {
		if _, err := os.Stat(s.staticDir); err == nil {
			mux.Handle("/", http.FileServer(http.Dir(s.staticDir)))
		}
	}
	return s.withCORS(s.withPlatformAuth(mux))
}

func (s *server) withCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.log.Info("http request", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Client-Id, Authorization, X-Api-Key")
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

func (s *server) findCall(r *http.Request, callID string) (*Session, *activeCall) {
	plat := callerPlatform(r)
	for _, sess := range s.sessions.allSessions() {
		if plat != "" && sess.platform != plat {
			continue
		}
		if ac, ok := sess.reg.get(callID); ok {
			return sess, ac
		}
	}
	return nil, nil
}

func (s *server) sessionByID(w http.ResponseWriter, r *http.Request, sid string) *Session {
	sess, ok := s.sessions.GetForPlatform(sid, callerPlatform(r))
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such session"})
		return nil
	}
	return sess
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	plat := callerPlatform(r)
	var infos []SessionInfo
	if plat != "" {
		infos = s.sessions.infosForPlatform(plat, "")
	} else {
		infos = s.sessions.infos()
	}

	connectedSessions := 0
	var phones []string
	for _, info := range infos {
		if info.State == "connected" || info.State == "open" || info.Paired {
			connectedSessions++
			if plat != "" && info.JID != "" {
				phones = append(phones, info.JID)
			}
		}
	}

	resp := map[string]any{
		"status":             "online",
		"whatsapp_connected": connectedSessions > 0,
		"sessions_count":     len(infos),
		"connected_count":    connectedSessions,
		"timestamp":          time.Now().Unix(),
	}
	if plat != "" {
		resp["platform"] = plat
		resp["phones"] = phones
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *server) handleLines(w http.ResponseWriter, r *http.Request) {
	infos := s.sessions.infosForPlatform(callerPlatform(r), "")
	type lineItem struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		Phone     string `json:"phone"`
		Status    string `json:"status"`
		State     string `json:"state"`
		Connected bool   `json:"connected"`
		Paired    bool   `json:"paired"`
		Platform  string `json:"platform"`
	}
	var lines []lineItem
	for _, info := range infos {
		connected := info.State == "connected" || info.State == "open" || info.Paired
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
			Platform:  info.Platform,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"lines":    lines,
		"sessions": infos,
	})
}

func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	plat := callerPlatform(r)
	sid := strings.TrimSpace(r.URL.Query().Get("session_id"))
	bid := strings.TrimSpace(r.URL.Query().Get("business_id"))
	if sid != "" {
		if _, ok := s.sessions.GetForPlatform(sid, plat); !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such session"})
			return
		}
	}
	s.broker.serveSSE(w, r, clientID(r), plat, sid, bid)
}

func (s *server) handleSessionList(w http.ResponseWriter, r *http.Request) {
	bid := strings.TrimSpace(r.URL.Query().Get("business_id"))
	writeJSON(w, http.StatusOK, map[string]any{
		"sessions": s.sessions.infosForPlatform(callerPlatform(r), bid),
	})
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
	if !isValidUUID(businessID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid business_id uuid"})
		return
	}
	plat := callerPlatform(r)
	id, reused, err := s.sessions.Create(plat, businessID)
	if err != nil {
		if errors.Is(err, errPlatformMismatch) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":          id,
		"session_id":  id,
		"business_id": businessID,
		"platform":    plat,
		"reused":      reused,
	})
}

func (s *server) handleSessionDelete(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r, r.PathValue("sid"))
	if sess == nil {
		return
	}
	if err := s.sessions.Delete(r.Context(), sess.id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleSessionLogout(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r, r.PathValue("sid"))
	if sess == nil {
		return
	}
	if err := s.sessions.Logout(r.Context(), sess.id); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleSessionPair(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r, r.PathValue("sid"))
	if sess == nil {
		return
	}
	if err := s.sessions.Pair(sess.id); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleCallsList(w http.ResponseWriter, r *http.Request) {
	plat := callerPlatform(r)
	s.broker.mu.RLock()
	var list []map[string]any
	host := r.Host
	if host == "" {
		host = publicStreamHost()
	}
	for _, c := range s.broker.calls {
		if plat != "" && c.Platform != plat {
			continue
		}
		streamURL := fmt.Sprintf("wss://%s/api/v1/calls/%s/audio", host, c.CallID)
		list = append(list, map[string]any{
			"id":         c.CallID,
			"callId":     c.CallID,
			"call_id":    c.CallID,
			"sessionId":  c.SessionID,
			"businessId": c.BusinessID,
			"platform":   c.Platform,
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
	plat := callerPlatform(r)
	callID := r.PathValue("id")
	c, ok := s.broker.getCall(callID)
	if !ok || c == nil || (plat != "" && c.Platform != plat) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "call not found"})
		return
	}
	host := r.Host
	if host == "" {
		host = publicStreamHost()
	}
	streamURL := fmt.Sprintf("wss://%s/api/v1/calls/%s/audio", host, c.CallID)
	writeJSON(w, http.StatusOK, map[string]any{
		"id":         c.CallID,
		"callId":     c.CallID,
		"call_id":    c.CallID,
		"sessionId":  c.SessionID,
		"businessId": c.BusinessID,
		"platform":   c.Platform,
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
		Phone           string `json:"phone"`
		To              string `json:"to"`
		Number          string `json:"number"`
		Recipient       string `json:"recipient"`
		SessionID       string `json:"session_id"`
		Greeting        string `json:"greeting"`
		InitialGreeting string `json:"initial_greeting"`
		Context         string `json:"context"`
		CallContext     string `json:"call_context"`
		ClientRef       string `json:"client_ref"`
		MaxCallSeconds  int    `json:"max_call_seconds"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.MaxCallSeconds < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max_call_seconds must be >= 0"})
		return
	}

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

	sid := strings.TrimSpace(body.SessionID)
	if sid == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "session_id is required"})
		return
	}
	sess := s.sessionByID(w, r, sid)
	if sess == nil {
		return
	}
	if sess.client == nil || sess.client.Store == nil || sess.client.Store.ID == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "whatsapp session is not paired"})
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
	if phone == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid recipient phone"})
		return
	}
	peer := types.NewJID(phone, types.DefaultUserServer)

	greeting := strings.TrimSpace(body.Greeting)
	if greeting == "" {
		greeting = strings.TrimSpace(body.InitialGreeting)
	}
	callCtx := strings.TrimSpace(body.Context)
	if callCtx == "" {
		callCtx = strings.TrimSpace(body.CallContext)
	}
	clientRef := strings.TrimSpace(body.ClientRef)

	callID, err := sess.startOutgoingWithOptions(r.Context(), peer, false, OutboundCallOptions{
		Greeting:       greeting,
		Context:        callCtx,
		ClientRef:      clientRef,
		MaxCallSeconds: body.MaxCallSeconds,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	peerNum := sess.mgr.store.resolvePhone(peer.User)
	s.broker.upsertCall(CallRecord{
		SessionID: sess.id, BusinessID: sess.businessID, Platform: sess.platform, CallID: callID,
		Owner: &owner, Direction: "outbound", Peer: peer.String(),
		PeerNumber: peerNum, ClientRef: clientRef, StartedAt: time.Now().UnixMilli(), Status: StatusRinging,
	})

	host := r.Host
	if host == "" {
		host = publicStreamHost()
	}
	streamURL := fmt.Sprintf("wss://%s/api/v1/calls/%s/audio", host, callID)

	resp := map[string]any{
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
	}
	if clientRef != "" {
		resp["client_ref"] = clientRef
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *server) handleStartCall(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r, r.PathValue("sid")); sess != nil {
		s.doStartCall(sess, w, r)
	}
}

func (s *server) handleDirectWebRTC(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	sess, _ := s.findCall(r, callID)
	if sess == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	s.doWebRTC(sess, w, r)
}

func (s *server) handleWebRTC(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r, r.PathValue("sid")); sess != nil {
		s.doWebRTC(sess, w, r)
	}
}

func (s *server) handleDirectAudioWS(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	sess, ac := s.findCall(r, callID)
	if sess == nil || ac == nil {
		http.Error(w, "call not found", http.StatusNotFound)
		return
	}

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
	sess, _ := s.findCall(r, callID)
	if sess == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	s.doAccept(sess, w, r)
}

func (s *server) handleAccept(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r, r.PathValue("sid")); sess != nil {
		s.doAccept(sess, w, r)
	}
}

func (s *server) handleDirectReject(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	sess, _ := s.findCall(r, callID)
	if sess == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	s.doReject(sess, w, r)
}

func (s *server) handleReject(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r, r.PathValue("sid")); sess != nil {
		s.doReject(sess, w, r)
	}
}

func (s *server) handleDirectHangup(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	sess, _ := s.findCall(r, callID)
	if sess == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ended"})
		return
	}
	s.doEndCall(sess, w, r)
}

func (s *server) handleEndCall(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r, r.PathValue("sid")); sess != nil {
		s.doEndCall(sess, w, r)
	}
}

func (s *server) handleDirectHistory(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"rows": s.broker.historyRows(callerPlatform(r), "", 50)})
}

func (s *server) handleHistory(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r, r.PathValue("sid")); sess != nil {
		writeJSON(w, http.StatusOK, map[string]any{"rows": s.broker.historyRows(sess.platform, sess.id, 50)})
	}
}

func (s *server) doStartCall(sess *Session, w http.ResponseWriter, r *http.Request) {
	if sess.client.Store.ID == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not paired"})
		return
	}
	var body struct {
		Phone           string `json:"phone"`
		To              string `json:"to"`
		Number          string `json:"number"`
		Recipient       string `json:"recipient"`
		Greeting        string `json:"greeting"`
		InitialGreeting string `json:"initial_greeting"`
		Context         string `json:"context"`
		CallContext     string `json:"call_context"`
		ClientRef       string `json:"client_ref"`
		MaxCallSeconds  int    `json:"max_call_seconds"`
		DurationMs      int    `json:"duration_ms"`
		Record          bool   `json:"record"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.MaxCallSeconds < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max_call_seconds must be >= 0"})
		return
	}
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
	if phone == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid recipient phone"})
		return
	}
	peer := types.NewJID(phone, types.DefaultUserServer)

	greeting := strings.TrimSpace(body.Greeting)
	if greeting == "" {
		greeting = strings.TrimSpace(body.InitialGreeting)
	}
	callCtx := strings.TrimSpace(body.Context)
	if callCtx == "" {
		callCtx = strings.TrimSpace(body.CallContext)
	}
	clientRef := strings.TrimSpace(body.ClientRef)

	callID, err := sess.startOutgoingWithOptions(r.Context(), peer, false, OutboundCallOptions{
		Greeting:       greeting,
		Context:        callCtx,
		ClientRef:      clientRef,
		MaxCallSeconds: body.MaxCallSeconds,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.broker.upsertCall(CallRecord{
		SessionID: sess.id, BusinessID: sess.businessID, Platform: sess.platform, CallID: callID,
		Owner: &owner, Direction: "outbound", Peer: peer.String(), ClientRef: clientRef,
		StartedAt: time.Now().UnixMilli(), Status: StatusRinging,
	})
	host := r.Host
	if host == "" {
		host = publicStreamHost()
	}
	streamURL := fmt.Sprintf("wss://%s/api/v1/calls/%s/audio", host, callID)

	resp := map[string]any{
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
	}
	if clientRef != "" {
		resp["client_ref"] = clientRef
	}
	writeJSON(w, http.StatusOK, resp)
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
	s.broker.emitIncomingClaimed(sess.platform, sess.id, sess.businessID, id, owner)
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
	sess := s.sessionByID(w, r, sid)
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
		s.broker.emitAgentStatus(sess.platform, sid, sess.businessID, cid, body.Enabled, "idle")
	}
	writeJSON(w, http.StatusOK, map[string]any{"callId": cid, "agentEnabled": body.Enabled})
}

func (s *server) handleSessionSendMessage(w http.ResponseWriter, r *http.Request) {
	sid := r.PathValue("sid")
	sess := s.sessionByID(w, r, sid)
	if sess == nil {
		return
	}
	if sess.client == nil || sess.client.Store == nil || sess.client.Store.ID == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "session is not paired"})
		return
	}

	var body struct {
		To        string `json:"to"`
		Text      string `json:"text"`
		Message   string `json:"message"`
		MediaURL  string `json:"media_url"`
		MediaType string `json:"media_type"`
		Caption   string `json:"caption"`
		Filename  string `json:"filename"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
		return
	}

	rawTo := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(body.To), "LID:"))
	text := strings.TrimSpace(body.Text)
	if text == "" {
		text = strings.TrimSpace(body.Message)
	}
	mediaURL := strings.TrimSpace(body.MediaURL)
	caption := strings.TrimSpace(body.Caption)
	if caption == "" {
		caption = text
	}

	if rawTo == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "to is required"})
		return
	}
	if text == "" && mediaURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "text or media_url is required"})
		return
	}

	var peerJID types.JID
	resolvedPN := ""
	if sess.mgr != nil && sess.mgr.store != nil {
		resolvedPN = sess.mgr.store.getPNForLID(r.Context(), rawTo)
	}
	if resolvedPN != "" {
		peerJID = types.NewJID(normalizePhone(resolvedPN), types.DefaultUserServer)
	} else if strings.Contains(rawTo, "@") {
		parsed, err := resolveTargetJID(rawTo)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		peerJID = parsed
	} else {
		digits := normalizePhone(rawTo)
		if digits == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid recipient phone"})
			return
		}
		if sess.mgr != nil && sess.mgr.store != nil {
			if pn := sess.mgr.store.getPNForLID(r.Context(), digits); pn != "" {
				digits = normalizePhone(pn)
			}
		}
		peerJID = types.NewJID(digits, types.DefaultUserServer)
	}

	var msgID string
	var err error
	if mediaURL != "" {
		msgID, err = sess.sendMediaURLSync(r.Context(), peerJID, mediaURL, body.MediaType, caption, body.Filename)
	} else {
		msgID, err = sess.sendTextSync(r.Context(), peerJID, text)
	}
	if err != nil {
		s.log.Error("handleSessionSendMessage failed", "session_id", sid, "to", peerJID.String(), "err", err)
		statusCode := http.StatusBadGateway
		msg := err.Error()
		if strings.Contains(msg, "must use https") || strings.Contains(msg, "blocked ") || strings.Contains(msg, "unsupported media_type") || strings.Contains(msg, "invalid media_url") || strings.Contains(msg, "exceeds") {
			statusCode = http.StatusBadRequest
		}
		writeJSON(w, statusCode, map[string]string{"error": msg})
		return
	}

	s.log.Info("outbound WhatsApp message sent via API", "session_id", sid, "to", peerJID.String(), "message_id", msgID, "media", mediaURL != "")
	writeJSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"ok":         true,
		"message_id": msgID,
		"to":         peerJID.String(),
	})
}
