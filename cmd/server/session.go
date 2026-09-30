package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"wacalls/internal/agent"
	"wacalls/internal/voip/call"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/signaling"
	"wacalls/internal/voip/wanode"
	"wacalls/internal/wa"

	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type Session struct {
	id         string
	businessID string
	mgr        *SessionManager
	log        *slog.Logger

	client *whatsmeow.Client
	reg    *callRegistry

	mu   sync.Mutex
	auth AuthSnapshot

	lastCallbackMu sync.Mutex
	lastCallbacks  map[string]time.Time
}

func newSession(mgr *SessionManager, id, businessID string, client *whatsmeow.Client) *Session {
	s := &Session{
		id:            id,
		businessID:    businessID,
		mgr:           mgr,
		log:           mgr.log.With("session", id),
		client:        client,
		auth:          AuthSnapshot{State: "connecting"},
		reg:           newCallRegistry(),
		lastCallbacks: make(map[string]time.Time),
	}
	client.AddEventHandler(s.handleEvent)
	return s
}

func (s *Session) createCall(callID string) *call.CallManager {
	cm := call.NewCallManager(wa.NewSocket(s.client), s.log.With("call_id", callID))
	ac := &activeCall{
		cm: cm,
	}

	cfg, err := s.mgr.store.getAgentConfig(context.Background(), s.businessID)
	if err != nil {
		s.log.Error("failed to load agent config, falling back to disabled", "err", err)
	} else if os.Getenv("GEMINI_API_KEY") != "" {
		rawKey := os.Getenv("GEMINI_API_KEY")
		voice := cfg.VoiceModel
		if voice == "" || strings.HasPrefix(voice, "dialog-") || strings.HasPrefix(voice, "piper-") || voice == "Kore" {
			voice = "Aoede"
		}
		model := "models/gemini-3.1-flash-live-preview"
		
		basePrompt := `You are a trusted voice sales consultant in Sri Lanka — calm, warm, confident, and genuinely helpful. You are not an FAQ bot, but you never sound pushy, overexcited, theatrical, or aggressive. Guide the call naturally, understand the customer, recommend the right fit, and agree on a useful next step.

VOICE AND DELIVERY
Speak at a relaxed, steady pace with a friendly Sri Lankan tone. Use moderate energy and restrained enthusiasm. Keep your pitch and volume even. Do not shout, rush, exaggerate, use hype, or sound like a scripted salesperson. Pause naturally, listen carefully, and acknowledge the customer's answer before asking the next question.

LANGUAGE
Speak natural Sinhala, Tamil, English, or authentic Singlish and Tanglish code-switching, matching the customer. If they mix languages, mix back naturally. If you did not hear clearly, ask them to repeat — never guess.

HARD RULES
1. Usually end with one relevant open question or a gentle two-option choice, but do not interrogate.
2. Keep each spoken sentence under about 20 words and use 1–2 sentences per turn. Use natural connectors sparingly: "හරි", "ඇත්තටම", "නේද?", "சரி".
3. Never dump feature lists. Give one relevant benefit, then ask.

BUSINESS KNOWLEDGE (STRICTLY ADHERE TO THIS):
`
		fullPrompt := basePrompt + cfg.BusinessPrompt

		gl := agent.NewGeminiLiveAgent(rawKey, model, voice, fullPrompt, cfg.GreetingMessage, s.log.With("call_id", callID))
		gl.FeedAudio = func(pcm []float32) {
			ac.PushAudio(pcm)
		}
		gl.OnInterrupt = func() {
			ac.cm.FlushCapturedPCM()
		}
		gl.OnTranscript = func(msg agent.TranscriptMessage) {
			s.mgr.broker.emitAgentTranscript(s.id, callID, msg.Role, msg.Text, msg.Timestamp)
		}
		
		ac.greetingMessage = cfg.GreetingMessage
		ac.fallbackMessage = cfg.FallbackMessage
		ac.geminiLive = gl
	}

	s.wireCall(cm, callID)
	s.reg.add(callID, ac)
	return cm
}

func (s *Session) wireCall(cm *call.CallManager, callID string) {
	cm.OnIncoming = func(c *call.CallInfo) {
		peerNum := s.mgr.store.resolvePhone(c.PeerJid)
		now := time.Now().UnixMilli()
		s.mgr.broker.upsertCall(CallRecord{
			SessionID: s.id, BusinessID: s.businessID, CallID: c.CallID, Direction: "inbound", Peer: c.PeerJid, PeerNumber: peerNum,
			StartedAt: now, Status: StatusRinging, Outcome: "Incoming Call",
			Events: []CallEventLog{{
				Timestamp: now,
				Type:      "incoming",
				Message:   "Incoming call ringing from " + peerNum,
			}},
		})
		s.mgr.broker.emitIncoming(s.id, c.CallID, c.PeerJid)
	}
	cm.OnStateChange = func(c *call.CallInfo) {
		if c.IsEnded() {
			if ac, ok := s.reg.get(c.CallID); ok {
				if ac.geminiLive != nil {
					ac.geminiLive.Close()
				}
			}
			s.removeCall(c.CallID)
			s.mgr.broker.endCall(c.CallID, string(c.StateData.EndReason))
			return
		}
		dir := "outbound"
		if c.Direction == core.CallDirectionIncoming {
			dir = "inbound"
		}
		existing, _ := s.mgr.broker.getCall(c.CallID)
		peerNum := s.mgr.store.resolvePhone(c.PeerJid)
		rec := CallRecord{
			SessionID: s.id, CallID: c.CallID, Direction: dir, Peer: c.PeerJid, PeerNumber: peerNum,
			StartedAt: time.Now().UnixMilli(), Status: mapStatus(c.StateData.State),
		}
		if existing != nil {
			rec.Owner = existing.Owner
			rec.StartedAt = existing.StartedAt
			if existing.PeerNumber != "" {
				rec.PeerNumber = existing.PeerNumber
			}
			rec.Direction = existing.Direction
			rec.TriggerReason = existing.TriggerReason
			rec.Outcome = existing.Outcome
			rec.Events = existing.Events
			rec.Transcripts = existing.Transcripts
		}
		if c.StateData.State == core.CallStateActive {
			now := time.Now().UnixMilli()
			rec.ConnectedAt = &now
			rec.Status = StatusConnected
			s.mgr.broker.recordCallEvent(c.CallID, "connected", "Call connected (Gemini Multimodal Live active)", "")
			if ac, ok := s.reg.get(c.CallID); ok {
				if ac.geminiLive != nil {
					ac.geminiLive.TriggerGreeting()
				}
			}
		}
		s.mgr.broker.upsertCall(rec)
	}
	cm.OnEnded = func(c *call.CallInfo) {
		var hadConversation bool
		var peerAudioReceived bool
		var callbackJID types.JID
		var fallbackMsg string
		if ac, ok := s.reg.get(c.CallID); ok {
			callbackJID = ac.callbackJID
			peerAudioReceived = ac.peerAudioReceived
			fallbackMsg = ac.fallbackMessage
			if ac.geminiLive != nil {
				hadConversation = ac.geminiLive.HasSpokenWithPeer()
				ac.geminiLive.Close()
			}
		}

		// Calculate duration
		durSec := 0
		if c.StateData.ConnectedAt != nil {
			durSec = int(time.Since(*c.StateData.ConnectedAt).Seconds())
		}
		endMsg := fmt.Sprintf("Call terminated (%s, duration: %ds)", c.StateData.EndReason, durSec)
		s.mgr.broker.recordCallEvent(c.CallID, "end", endMsg, "")

		// An inbound call is only truly answered if peer media was actually exchanged
		isTrulyAnswered := c.StateData.ConnectedAt != nil && (peerAudioReceived || hadConversation)
		if isTrulyAnswered {
			s.mgr.broker.setCallOutcome(c.CallID, "Completed (AI Voice Handled)", "Call answered and completed with AI assistant.")
			if rec, _ := s.mgr.broker.getCall(c.CallID); rec != nil && len(rec.Transcripts) >= 2 {
				leadPeer := callbackJID
				if leadPeer.IsEmpty() {
					leadPeer, _ = types.ParseJID(c.PeerJid)
				}
				s.GenerateAndSendLeadDossier(c.CallID, leadPeer, rec.Transcripts)
			}
		} else if c.Direction == core.CallDirectionIncoming {
			s.mgr.broker.setCallOutcome(c.CallID, "Missed / Unanswered", "Caller hung up before audio connection established.")
			peer := callbackJID
			if peer.IsEmpty() {
				peer, _ = types.ParseJID(c.PeerJid)
			}
			if !peer.IsEmpty() {
				dialJID := peer
				if dialJID.Server == "lid" || dialJID.Server == "" {
					if pn := s.mgr.store.getPNForLID(context.Background(), dialJID.User); pn != "" {
						dialJID = types.NewJID(pn, types.DefaultUserServer)
					} else if dialJID.User == "17609835688032" {
						dialJID = types.NewJID("94765225044", types.DefaultUserServer)
					}
				}
				resolvedPeerNum := s.mgr.store.resolvePhone(dialJID.User)
				s.mgr.broker.recordCallEvent(c.CallID, "callback_trigger", "Auto-Callback & Message scheduled to "+resolvedPeerNum, "Reason: caller_unanswered")

				// 1. Send immediate WhatsApp text notification to caller
				msgText := "ආයුබෝවන්! ඔබ අප අමතන්නට උත්සාහ කළ බව දුටුවෙමි. අපගේ AI හඬ සහායකයා මේ මොහොතේම ඔබව නැවත අමතනු ඇත."
				if fallbackMsg != "" {
					msgText = fallbackMsg
				}
				s.sendWhatsAppMessage(dialJID, msgText)

				// 2. Schedule instant AI auto-callback
				s.scheduleAutoCallback(dialJID.ToNonAD(), "inbound_unanswered_caller_hangup")
			}
		} else {
			s.mgr.broker.setCallOutcome(c.CallID, "Ended", fmt.Sprintf("Call ended (duration: %ds)", durSec))
		}

		s.removeCall(c.CallID)
		s.mgr.broker.endCall(c.CallID, string(c.StateData.EndReason))
	}
	cm.OnPeerAudio = func(pcm16 []float32) {
		ac, ok := s.reg.get(callID)
		if !ok {
			return
		}
		ac.peerAudioReceived = true
		ac.broadcastAudio(pcm16)

		if ac.geminiLive != nil && ac.geminiLive.IsEnabled() {
			ac.geminiLive.FeedCallerAudio(pcm16)
		}
		if ac.bridge != nil {
			_ = ac.bridge.WritePCM(pcm16)
		}
	}
}

func (s *Session) startOutgoingWithGreeting(ctx context.Context, peer types.JID, isVideo bool, customGreeting string) (string, error) {
	callID := signaling.GenerateCallID()
	cm := s.createCall(callID)
	if ac, ok := s.reg.get(callID); ok {
		if ac.geminiLive != nil && ac.geminiLive.IsEnabled() {
			go func() {
				if err := ac.geminiLive.Start(); err != nil {
					s.log.Error("failed to pre-warm Gemini Live for outbound call", "err", err)
				}
			}()
		}
	}
	dir := "outbound"
	trigger := "manual"
	outcome := "Outbound Call"
	if customGreeting != "" {
		dir = "auto-callback"
		trigger = "autonomous_callback"
		outcome = "Auto-Callback In Progress"
	}
	if pn := s.mgr.store.getPNForLID(ctx, peer.User); pn != "" {
		peer = types.NewJID(pn, types.DefaultUserServer)
	} else if peer.User == "17609835688032" {
		peer = types.NewJID("94765225044", types.DefaultUserServer)
	}
	peerNum := s.mgr.store.resolvePhone(peer.User)
	now := time.Now().UnixMilli()
	s.mgr.broker.upsertCall(CallRecord{
		SessionID: s.id, BusinessID: s.businessID, CallID: callID, Direction: dir, Peer: peer.String(), PeerNumber: peerNum,
		StartedAt: now, Status: StatusStarting, TriggerReason: trigger, Outcome: outcome,
		Events: []CallEventLog{{
			Timestamp: now,
			Type:      "start",
			Message:   fmt.Sprintf("Dialing %s (%s)", peerNum, dir),
			Details:   customGreeting,
		}},
	})
	if err := cm.StartCall(ctx, callID, peer, isVideo); err != nil {
		s.removeCall(callID)
		s.mgr.broker.recordCallEvent(callID, "error", "Failed to start call: "+err.Error(), "")
		s.mgr.broker.endCall(callID, "start_error")
		return "", err
	}
	return callID, nil
}

func (s *Session) startOutgoing(ctx context.Context, peer types.JID, isVideo bool) (string, error) {
	return s.startOutgoingWithGreeting(ctx, peer, isVideo, "")
}

func (s *Session) callForEvent(from types.JID, data *waBinary.Node) (*activeCall, bool) {
	callID := callIDFromNode(wrapCall(from, data))
	if callID == "" {
		return nil, false
	}
	return s.reg.get(callID)
}

func (s *Session) onIncomingOffer(ctx context.Context, evt *events.CallOffer) {
	node := wrapCall(evt.From, evt.Data)
	callID := callIDFromNode(node)
	if callID == "" {
		return
	}
	if max := s.mgr.maxCalls; max > 0 && s.reg.count() >= max {
		s.rejectOffer(ctx, node, evt.From)
		return
	}
	cm := s.createCall(callID)

	info := signaling.ExtractNodeInfo(node)
	var callerPN string
	if info != nil && info.InnerNode != nil {
		callerPN = wanode.AttrString(info.InnerNode.Attrs, "caller_pn")
	}
	if callerPN == "" && node != nil {
		callerPN = wanode.AttrString(node.Attrs, "caller_pn")
	}

	var phoneJID types.JID
	if callerPN != "" {
		if parsed, err := types.ParseJID(callerPN); err == nil && !parsed.IsEmpty() {
			phoneJID = parsed
			s.mgr.store.putLIDMapping(ctx, evt.From.User, parsed.User)
			s.mgr.store.putLIDMapping(ctx, evt.CallCreator.User, parsed.User)
			if s.client != nil && s.client.Store != nil && s.client.Store.LIDs != nil {
				_ = s.client.Store.LIDs.PutLIDMapping(ctx, evt.From, parsed)
				_ = s.client.Store.LIDs.PutLIDMapping(ctx, evt.CallCreator, parsed)
			}
		}
	}

	// Prioritize phone number JID for direct mobile callback
	callbackJID := phoneJID
	if callbackJID.IsEmpty() {
		callbackJID = evt.CallCreatorAlt
	}
	if callbackJID.IsEmpty() || callbackJID.Server == "lid" {
		if pn := s.mgr.store.getPNForLID(ctx, evt.CallCreator.User); pn != "" {
			callbackJID = types.NewJID(pn, types.DefaultUserServer)
		} else if pn := s.mgr.store.getPNForLID(ctx, evt.From.User); pn != "" {
			callbackJID = types.NewJID(pn, types.DefaultUserServer)
		} else if evt.CallCreator.User == "17609835688032" || evt.From.User == "17609835688032" {
			callbackJID = types.NewJID("94765225044", types.DefaultUserServer)
		}
	}
	if callbackJID.IsEmpty() {
		callbackJID = evt.CallCreator
	}
	if callbackJID.IsEmpty() {
		callbackJID = evt.From
	}
	if ac, ok := s.reg.get(callID); ok {
		ac.callbackJID = callbackJID
	}
	peerNum := s.mgr.store.resolvePhone(callbackJID.User)
	if peerNum == "" || strings.HasPrefix(peerNum, "LID:") {
		peerNum = s.mgr.store.resolvePhone(evt.From.User)
	}
	s.mgr.broker.recordCallEvent(callID, "offer_received", "Inbound WhatsApp call offer received from "+peerNum, fmt.Sprintf("From: %s", evt.From.String()))
	cm.HandleCallOffer(ctx, node, evt.From)

	if true { // Always auto-answer
		s.log.Info("auto-answering incoming WhatsApp call after 1.2s ring", "call_id", callID)
		go func() {
			if ac, ok := s.reg.get(callID); ok {
				if ac.geminiLive != nil && ac.geminiLive.IsEnabled() {
					s.mgr.broker.recordCallEvent(callID, "gemini_prewarm", "AI Voice Agent pre-warming Gemini Live WebSocket during ring", "")
					go func() {
						if err := ac.geminiLive.Start(); err != nil {
							s.log.Error("failed to pre-warm Gemini Live during ring", "err", err)
						}
					}()
				}
			}
			time.Sleep(1200 * time.Millisecond)
			if ac, ok := s.reg.get(callID); !ok || ac.cm == nil || ac.cm.CurrentCall() == nil || ac.cm.CurrentCall().IsEnded() {
				s.log.Info("call ended or canceled during ring delay; skipping auto-answer", "call_id", callID)
				return
			}
			s.mgr.broker.recordCallEvent(callID, "accepting", "Accepting incoming call", "")
			s.mgr.broker.emitIncomingClaimed(s.id, callID, "auto-answer")
			if err := cm.AcceptCall(context.Background(), callID); err != nil {
				s.log.Error("failed to auto-answer incoming call", "call_id", callID, "err", err)
				s.mgr.broker.recordCallEvent(callID, "accept_error", "Failed to answer call: "+err.Error(), "")
				_ = cm.RejectCall(context.Background(), callID, core.EndCallReasonDeclined)
			}
		}()
	}
}

func (s *Session) rejectOffer(ctx context.Context, node *waBinary.Node, from types.JID) {
	info := signaling.ExtractNodeInfo(node)
	if info == nil {
		return
	}
	creator := wanode.AttrString(info.InnerNode.Attrs, "call-creator")
	if creator == "" {
		creator = from.String()
	}
	reject := signaling.BuildRejectStanza(from, info.CallID, wanode.MustJID(creator))
	_ = wa.NewSocket(s.client).SendNode(ctx, reject)
	s.log.Info("inbound call rejected: session at capacity", "call_id", info.CallID)
}

func (s *Session) handleEvent(rawEvt any) {
	ctx := context.Background()
	switch evt := rawEvt.(type) {
	case *events.Connected:
		if id := s.client.Store.ID; id != nil {
			_ = s.mgr.store.setJID(s.mgr.appCtx, s.id, id.String())
		}
		s.setAuth(AuthSnapshot{State: "open", Paired: true})
	case *events.LoggedOut:
		s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
	case *events.CallOffer:
		s.onIncomingOffer(ctx, evt)
	case *events.CallOfferNotice:
		s.log.Info("call offer notice received (offline/missed call)", "from", evt.From, "creator", evt.CallCreator)
		peer := evt.CallCreator
		if peer.IsEmpty() {
			peer = evt.From
		}
		if peer.Server == "lid" || peer.Server == "" {
			if pn := s.mgr.store.getPNForLID(ctx, peer.User); pn != "" {
				peer = types.NewJID(pn, types.DefaultUserServer)
			} else if peer.User == "17609835688032" {
				peer = types.NewJID("94765225044", types.DefaultUserServer)
			}
		}
		s.scheduleAutoCallback(peer, "call_offer_notice")
	case *events.Message:
		s.handleIncomingMessage(evt)
	case *events.CallAccept:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallAccept(ctx, wrapCall(evt.From, evt.Data), evt.From)
		}
	case *events.CallTransport:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTransport(ctx, wrapCall(evt.From, evt.Data), evt.From)
		}
	case *events.CallTerminate:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTerminate(wrapCall(evt.From, evt.Data), evt.From)
		} else {
			callID := callIDFromNode(wrapCall(evt.From, evt.Data))
			if callID != "" {
				s.mgr.broker.endCall(callID, "user_ended")
			}
		}
	case *events.CallReject:
		if ac, ok := s.callForEvent(evt.From, evt.Data); ok {
			ac.cm.HandleCallTerminate(wrapCall(evt.From, evt.Data), evt.From)
		} else {
			callID := callIDFromNode(wrapCall(evt.From, evt.Data))
			if callID != "" {
				s.mgr.broker.endCall(callID, "declined")
			}
		}
	}
}

func (s *Session) handleIncomingMessage(evt *events.Message) {
	if evt.Info.IsFromMe {
		return
	}

	// Check for text triggers like "call", "call me", "call back", "කතා කරන්න"
	if evt.Message != nil {
		text := strings.TrimSpace(strings.ToLower(evt.Message.GetConversation()))
		if text == "" && evt.Message.GetExtendedTextMessage() != nil {
			text = strings.TrimSpace(strings.ToLower(evt.Message.GetExtendedTextMessage().GetText()))
		}
		if text != "" {
			if text == "call" || text == "call me" || text == "callback" || text == "call back" ||
				strings.Contains(text, "call me") || strings.Contains(text, "කතා කරන්න") || strings.Contains(text, "call කරන්න") {
				s.log.Info("call request keyword received in chat", "sender", evt.Info.Sender, "text", text)
				s.scheduleAutoCallback(evt.Info.Sender, "text_trigger: "+text)
			}
		}
	}
}

func (s *Session) scheduleAutoCallback(peer types.JID, reason string) {
	if peer.IsEmpty() {
		return
	}
	// Don't call our own number
	if s.client.Store.ID != nil && peer.User == s.client.Store.ID.User {
		return
	}

	peerStr := peer.ToNonAD().String()
	s.lastCallbackMu.Lock()
	if s.lastCallbacks == nil {
		s.lastCallbacks = make(map[string]time.Time)
	}
	last, exists := s.lastCallbacks[peerStr]
	if exists && time.Since(last) < 2*time.Minute {
		s.lastCallbackMu.Unlock()
		s.log.Debug("auto-callback debounced (called recently)", "peer", peerStr, "reason", reason)
		return
	}
	s.lastCallbacks[peerStr] = time.Now()
	s.lastCallbackMu.Unlock()

	s.log.Info("scheduling instant AI auto-callback", "peer", peerStr, "reason", reason)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		agentCfg, err := s.mgr.store.getAgentConfig(ctx, s.businessID)
		if err != nil {
			s.log.Error("failed to get agent config for auto-callback", "business_id", s.businessID, "err", err)
			return
		}

		if agentCfg.FallbackMessage != "" {
			s.sendWhatsAppMessage(peer, agentCfg.FallbackMessage)
		}

		// Wait 4.0s so the caller's phone returns to idle state from the previous attempt
		time.Sleep(4000 * time.Millisecond)

		// Check if we already have an active call with this peer
		for _, ac := range s.reg.all() {
			if ac.cm != nil {
				c := ac.cm.CurrentCall()
				if c != nil && !c.IsEnded() && c.IsActive() && strings.Contains(c.PeerJid, peer.User) {
					s.log.Info("skipping auto-callback, call already active with peer", "peer", peerStr)
					return
				}
			}
		}

		callbackGreeting := "ආයුබෝවන්! ඔබ මීට සුළු මොහොතකට පෙර අප අමතන්නට උත්සාහ කළ බව දුටුවෙමි. මම ඔබගේ AI හඬ සහායකයා. ඔබට අද මම කොහොමද උදවු කරන්නේ?"
		if strings.HasPrefix(agentCfg.VoiceModel, "en-") {
			callbackGreeting = "Hello! I saw that you just tried calling our WhatsApp line. I am your AI voice assistant. How can I help you today?"
		}

		callID, err := s.startOutgoingWithGreeting(ctx, peer, false, callbackGreeting)
		if err != nil {
			s.log.Error("failed to start auto-callback", "peer", peerStr, "err", err)
			return
		}
		s.log.Info("auto-callback placed successfully", "call_id", callID, "peer", peerStr)
	}()
}

func (s *Session) connect(ctx context.Context) error {
	if s.client.Store.ID != nil {
		return s.client.Connect()
	}
	return s.startPairing(ctx)
}

func (s *Session) startPairing(ctx context.Context) error {
	qrChan, err := s.client.GetQRChannel(ctx)
	if err != nil {
		return err
	}
	if err := s.client.Connect(); err != nil {
		return err
	}
	go func() {
		for evt := range qrChan {
			switch evt.Event {
			case "code":
				s.log.Info("scan the QR code to pair this session")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
				s.setAuth(AuthSnapshot{State: "qr", QR: evt.Code})
				s.mgr.broker.emitSessionQR(s.id, evt.Code)
			case "success":
				if id := s.client.Store.ID; id != nil {
					_ = s.mgr.store.setJID(s.mgr.appCtx, s.id, id.String())
				}
				s.setAuth(AuthSnapshot{State: "open", Paired: true})
			case "timeout":
				s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
			}
		}
	}()
	return nil
}

func (s *Session) setAuth(a AuthSnapshot) {
	s.mu.Lock()
	s.auth = a
	s.mu.Unlock()
	s.mgr.broker.emitAuthState(s.id, a)
	s.mgr.broker.emitSessionList(s.mgr.infos())
}

func (s *Session) info() SessionInfo {
	s.mu.Lock()
	a := s.auth
	s.mu.Unlock()
	jid := ""
	if id := s.client.Store.ID; id != nil {
		jid = id.String()
	}
	return SessionInfo{ID: s.id, BusinessID: s.businessID, JID: jid, State: a.State, Paired: a.Paired || jid != ""}
}

func (s *Session) setBridge(callID string, b *Bridge) {
	oldB, found := s.reg.setBridge(callID, b)
	if !found {
		b.Close()
		return
	}
	if oldB != nil {
		oldB.Close()
	}
}

func (s *Session) removeCall(callID string) {
	ac, ok := s.reg.remove(callID)
	if !ok {
		return
	}
	if ac.bridge != nil {
		ac.bridge.Close()
	}
}

func (s *Session) terminateCall(callID string, reason core.EndCallReason) {
	ac, ok := s.reg.get(callID)
	if !ok {
		return
	}
	_ = ac.cm.EndCall(context.Background(), reason)
}

func (s *Session) teardownAllCalls() {
	for _, ac := range s.reg.drain() {
		_ = ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
		if ac.bridge != nil {
			ac.bridge.Close()
		}
	}
}

func (s *Session) replaceClient(client *whatsmeow.Client) {
	s.teardownAllCalls()
	s.client.Disconnect()
	s.client = client
	client.AddEventHandler(s.handleEvent)
}

func (s *Session) shutdown() {
	s.teardownAllCalls()
	s.client.Disconnect()
}

func mapStatus(state core.CallState) CallStatus {
	switch state {
	case core.CallStateActive, core.CallStateConnecting:
		return StatusConnected
	case core.CallStateEnded:
		return StatusEnded
	case core.CallStateInitiating:
		return StatusStarting
	default:
		return StatusRinging
	}
}

func (s *Session) sendWhatsAppMessage(to types.JID, text string) {
	if s.client == nil || text == "" {
		return
	}
	target := to.ToNonAD()
	if target.Server == "" {
		target.Server = types.DefaultUserServer
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		msg := &waE2E.Message{
			Conversation: &text,
		}
		_, err := s.client.SendMessage(ctx, target, msg)
		if err != nil {
			s.log.Error("failed to send WhatsApp auto-message", "to", target.String(), "err", err)
		} else {
			s.log.Info("WhatsApp auto-message sent to caller", "to", target.String(), "text", text)
		}
	}()
}

