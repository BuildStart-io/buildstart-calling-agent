package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type Session struct {
	id         string
	businessID string
	platform   string
	mgr        *SessionManager
	log        *slog.Logger

	client *whatsmeow.Client
	reg    *callRegistry

	mu   sync.Mutex
	auth AuthSnapshot

	lastCallbackMu sync.Mutex
	lastCallbacks  map[string]time.Time

	inOfflineSync atomic.Bool
	offlineMu     sync.Mutex
	offlineTimer  *time.Timer

	apiSentMu     sync.Mutex
	apiSentMsgIDs map[string]time.Time

	mediaMu    sync.Mutex
	mediaCache map[string]cachedInboundMedia
	mediaOrder []string

	missedEmittedMu sync.Mutex
	missedEmitted   map[string]time.Time
}

func newSession(mgr *SessionManager, id, businessID, platform string, client *whatsmeow.Client) *Session {
	if platform == "" {
		platform = "local"
	}
	s := &Session{
		id:            id,
		businessID:    businessID,
		platform:      platform,
		mgr:           mgr,
		log:           mgr.log.With("session", id, "platform", platform),
		client:        client,
		auth:          AuthSnapshot{State: "connecting"},
		reg:           newCallRegistry(),
		lastCallbacks: make(map[string]time.Time),
		apiSentMsgIDs: make(map[string]time.Time),
		mediaCache:    make(map[string]cachedInboundMedia),
		missedEmitted: make(map[string]time.Time),
	}
	client.AddEventHandler(s.handleEvent)
	return s
}

func geminiAPIKeyForPlatform(platform string) string {
	if platform == "global" {
		if k := strings.TrimSpace(os.Getenv("GEMINI_API_KEY_GLOBAL")); k != "" {
			return k
		}
	}
	return strings.TrimSpace(os.Getenv("GEMINI_API_KEY"))
}

func (s *Session) buildCallPrompt(cfg AgentConfigRow, peerNum, direction, callContext string) string {
	if s.platform != "global" && strings.TrimSpace(callContext) == "" {
		return cfg.BusinessPrompt
	}
	tzName := strings.TrimSpace(cfg.Timezone)
	if tzName == "" {
		tzName = "UTC"
	}
	loc, err := time.LoadLocation(tzName)
	if err != nil {
		loc = time.UTC
		tzName = "UTC"
	}
	nowLoc := time.Now().In(loc)
	var b strings.Builder
	b.WriteString(fmt.Sprintf(
		"[LIVE CALL CONTEXT]\nCurrent local time: %s (%s)\nTimezone: %s\nDirection: %s\nCustomer phone: %s\n",
		nowLoc.Format("Monday, 2006-01-02 15:04:05 -07:00"),
		tzName,
		tzName,
		direction,
		peerNum,
	))
	if ctxTrim := strings.TrimSpace(callContext); ctxTrim != "" {
		b.WriteString("Call purpose / campaign context: ")
		b.WriteString(ctxTrim)
		b.WriteByte('\n')
	}
	b.WriteString("[END LIVE CALL CONTEXT]\n\n")
	b.WriteString(cfg.BusinessPrompt)
	return b.String()
}

type OutboundCallOptions struct {
	Greeting       string
	Context        string
	ClientRef      string
	MaxCallSeconds int
}

func (s *Session) recordMissedEmitted(callID string) bool {
	if callID == "" {
		return true
	}
	now := time.Now()
	s.missedEmittedMu.Lock()
	defer s.missedEmittedMu.Unlock()
	if s.missedEmitted == nil {
		s.missedEmitted = make(map[string]time.Time)
	}
	for k, t := range s.missedEmitted {
		if now.Sub(t) > 10*time.Minute {
			delete(s.missedEmitted, k)
		}
	}
	if _, seen := s.missedEmitted[callID]; seen {
		return false
	}
	s.missedEmitted[callID] = now
	return true
}

func (s *Session) createCall(callID, peerNum, direction string, preloaded *AgentConfigRow, opts OutboundCallOptions) *call.CallManager {
	cm := call.NewCallManager(wa.NewSocket(s.client), s.log.With("call_id", callID))
	ac := &activeCall{
		cm:            cm,
		peerNum:       peerNum,
		callDirection: direction,
		clientRef:     strings.TrimSpace(opts.ClientRef),
	}

	var cfg AgentConfigRow
	var cfgLoaded bool
	if preloaded != nil {
		cfg = *preloaded
		cfgLoaded = true
	} else {
		loaded, err := s.mgr.store.getAgentConfig(context.Background(), s.businessID)
		if err != nil {
			s.log.Error("failed to load agent config, falling back to disabled", "err", err)
		} else {
			cfg = loaded
			cfgLoaded = true
		}
	}

	if cfgLoaded {
		effectiveGreeting := cfg.GreetingMessage
		if direction == "auto-callback" && strings.TrimSpace(cfg.CallbackGreeting) != "" {
			effectiveGreeting = cfg.CallbackGreeting
		}
		if g := strings.TrimSpace(opts.Greeting); g != "" && g != "auto_callback" {
			effectiveGreeting = g
		}
		effectiveMaxSec := cfg.MaxCallSeconds
		if opts.MaxCallSeconds > 0 {
			effectiveMaxSec = opts.MaxCallSeconds
		}

		ac.greetingMessage = effectiveGreeting
		ac.fallbackMessage = cfg.FallbackMessage
		ac.maxCallSeconds = effectiveMaxSec

		rawKey := geminiAPIKeyForPlatform(s.platform)
		if rawKey != "" {
			voice := strings.TrimSpace(cfg.VoiceModel)
			if voice == "" || strings.HasPrefix(voice, "dialog-") || strings.HasPrefix(voice, "piper-") || strings.HasPrefix(voice, "models/") {
				voice = "Aoede"
			}
			model := strings.TrimSpace(os.Getenv("GEMINI_LIVE_MODEL"))
			if model == "" {
				model = "models/gemini-3.1-flash-live-preview"
			}

			prompt := s.buildCallPrompt(cfg, peerNum, direction, opts.Context)
			gl := agent.NewGeminiLiveAgent(rawKey, model, voice, prompt, effectiveGreeting, s.log.With("call_id", callID))

			if len(cfg.ToolsJSON) > 0 && string(cfg.ToolsJSON) != "null" {
				var tools []agent.ToolDeclaration
				if err := json.Unmarshal(cfg.ToolsJSON, &tools); err != nil {
					s.log.Warn("failed to parse tools_json for call", "call_id", callID, "err", err)
				} else if len(tools) > 0 {
					gl.SetTools(tools)
					gl.OnToolCall = func(ctx context.Context, fc agent.FunctionCall) (map[string]any, error) {
						tStart := time.Now()
						if fc.Name == "end_call" {
							res := map[string]any{"status": "ending_call"}
							ac.recordToolInvocation(ToolInvocationRecord{
								Name:       fc.Name,
								Args:       fc.Args,
								Result:     res,
								DurationMs: time.Since(tStart).Milliseconds(),
							})
							go func() {
								deadline := time.Now().Add(2500 * time.Millisecond)
								time.Sleep(400 * time.Millisecond)
								for time.Now().Before(deadline) {
									if !gl.IsModelSpeaking() {
										break
									}
									time.Sleep(100 * time.Millisecond)
								}
								s.terminateCall(callID, core.EndCallReasonUserEnded)
							}()
							return res, nil
						}

						peerJID := ac.callbackJID
						res, err := s.invokePlatformTool(ctx, callID, ac.peerNum, peerJID, ac.clientRef, fc.ID, fc.Name, fc.Args)
						rec := ToolInvocationRecord{
							Name:       fc.Name,
							Args:       fc.Args,
							Result:     res,
							DurationMs: time.Since(tStart).Milliseconds(),
						}
						if err != nil {
							rec.Error = err.Error()
						}
						ac.recordToolInvocation(rec)
						return res, err
					}
				}
			}

			gl.FeedAudio = func(pcm []float32) {
				ac.PushAudio(pcm)
			}
			gl.OnInterrupt = func() {
				ac.cm.FlushCapturedPCM()
			}
			gl.OnTranscript = func(msg agent.TranscriptMessage) {
				s.mgr.broker.emitAgentTranscript(s.id, callID, msg.Role, msg.Text, msg.Timestamp)
			}

			ac.geminiLive = gl
		}
	}

	s.wireCall(cm, callID)
	s.reg.add(callID, ac)
	return cm
}

func (s *Session) startDurationTimers(callID string, ac *activeCall) {
	if ac == nil || ac.maxCallSeconds <= 0 {
		return
	}
	cutoffDur := time.Duration(ac.maxCallSeconds) * time.Second
	ac.durationMu.Lock()
	defer ac.durationMu.Unlock()
	if ac.cutoffTimer != nil {
		return
	}
	if ac.maxCallSeconds >= 60 {
		warnDelay := cutoffDur - 30*time.Second
		ac.warnTimer = time.AfterFunc(warnDelay, func() {
			if ac.geminiLive != nil {
				ac.geminiLive.SendSystemCue("Polite system reminder: you have about 30 seconds remaining before the maximum call duration limit. Wrap up smoothly and say goodbye.")
			}
		})
	}
	ac.cutoffTimer = time.AfterFunc(cutoffDur, func() {
		s.log.Info("max call duration reached; ending call", "call_id", callID, "max_call_seconds", ac.maxCallSeconds)
		s.mgr.broker.recordCallEvent(callID, "max_duration", fmt.Sprintf("Max call duration reached (%ds)", ac.maxCallSeconds), "")
		s.terminateCall(callID, core.EndCallReasonUserEnded)
	})
}

func (s *Session) wireCall(cm *call.CallManager, callID string) {
	cm.OnIncoming = func(c *call.CallInfo) {
		peerNum := s.mgr.store.resolvePhone(c.PeerJid)
		now := time.Now().UnixMilli()
		s.mgr.broker.upsertCall(CallRecord{
			SessionID: s.id, BusinessID: s.businessID, Platform: s.platform, CallID: c.CallID,
			Direction: "inbound", Peer: c.PeerJid, PeerNumber: peerNum,
			StartedAt: now, Status: StatusRinging, Outcome: "Incoming Call",
			Events: []CallEventLog{{
				Timestamp: now,
				Type:      "incoming",
				Message:   "Incoming call ringing from " + peerNum,
			}},
		})
		s.mgr.broker.emitIncoming(s.platform, s.id, s.businessID, c.CallID, c.PeerJid)
	}

	cm.OnStateChange = func(c *call.CallInfo) {
		if c.IsEnded() {
			if ac, ok := s.reg.get(c.CallID); ok {
				ac.cancelDurationTimers()
				if ac.recorder != nil {
					rec := ac.recorder
					ac.recorder = nil
					go uploadCallRecording(context.Background(), rec, s.platform, s.id, s.mgr.store, s.mgr.outbox, s.log)
				}
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
			SessionID: s.id, BusinessID: s.businessID, Platform: s.platform, CallID: c.CallID,
			Direction: dir, Peer: c.PeerJid, PeerNumber: peerNum,
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
				if ac.recorder == nil {
					ac.recorder = newCallAudioRecorder(c.CallID, s.businessID, s.log.With("call_id", c.CallID))
				}
				if ac.geminiLive != nil {
					ac.geminiLive.TriggerGreeting()
				}
				s.startDurationTimers(c.CallID, ac)
			}
		}
		s.mgr.broker.upsertCall(rec)
	}

	cm.OnEnded = func(c *call.CallInfo) {
		var hadConversation bool
		var peerAudioReceived bool
		var callbackJID types.JID
		var clientRef string
		var toolInvocations []ToolInvocationRecord
		if ac, ok := s.reg.get(c.CallID); ok {
			ac.cancelDurationTimers()
			callbackJID = ac.callbackJID
			peerAudioReceived = ac.peerAudioReceived
			clientRef = ac.clientRef
			toolInvocations = ac.getToolInvocations()
			if ac.recorder != nil {
				rec := ac.recorder
				ac.recorder = nil
				go uploadCallRecording(context.Background(), rec, s.platform, s.id, s.mgr.store, s.mgr.outbox, s.log)
			}
			if ac.geminiLive != nil {
				hadConversation = ac.geminiLive.HasSpokenWithPeer()
				ac.geminiLive.Close()
			}
		}

		durSec := 0
		if c.StateData.ConnectedAt != nil {
			durSec = int(time.Since(*c.StateData.ConnectedAt).Seconds())
		}
		endMsg := fmt.Sprintf("Call terminated (%s, duration: %ds)", c.StateData.EndReason, durSec)
		s.mgr.broker.recordCallEvent(c.CallID, "end", endMsg, "")

		existingRec, _ := s.mgr.broker.getCall(c.CallID)
		endedAt := time.Now().UnixMilli()

		leadPeer := callbackJID
		if leadPeer.IsEmpty() {
			leadPeer, _ = types.ParseJID(c.PeerJid)
		}
		resolvedPeer := s.resolvePhoneJID(context.Background(), leadPeer)

		isTrulyAnswered := c.StateData.ConnectedAt != nil && (peerAudioReceived || hadConversation)
		if isTrulyAnswered {
			s.mgr.broker.setCallOutcome(c.CallID, "Completed (AI Voice Handled)", "Call answered and completed with AI assistant.")
			if existingRec != nil {
				if s.platform == "local" && len(existingRec.Transcripts) >= 2 {
					s.dispatchPostCallWebhook(c.CallID, leadPeer, existingRec.PeerNumber, existingRec.Direction, durSec, existingRec.Transcripts)
				} else if s.platform == "global" {
					if clientRef == "" {
						clientRef = existingRec.ClientRef
					}
					payload := map[string]any{
						"call_id":          c.CallID,
						"peer_jid":         resolvedPeer.String(),
						"caller_number":    existingRec.PeerNumber,
						"direction":        existingRec.Direction,
						"started_at":       existingRec.StartedAt,
						"connected_at":     existingRec.ConnectedAt,
						"ended_at":         endedAt,
						"duration_seconds": durSec,
						"end_reason":       string(c.StateData.EndReason),
						"transcripts":      existingRec.Transcripts,
						"tool_invocations": toolInvocations,
					}
					if clientRef != "" {
						payload["client_ref"] = clientRef
					}
					s.enqueueGlobalEvent(context.Background(), s.id+":"+resolvedPeer.String(), "call.completed", payload)
				}
			}
		} else if c.Direction == core.CallDirectionIncoming {
			s.mgr.broker.setCallOutcome(c.CallID, "Missed / Unanswered", "Caller hung up before audio connection established.")
			peer := leadPeer
			willCallback := false
			if !peer.IsEmpty() {
				dialJID := s.resolvePhoneJID(context.Background(), peer.ToNonAD())
				cfg := defaultAgentConfigRow()
				if loaded, err := s.mgr.store.getAgentConfig(context.Background(), s.businessID); err == nil {
					cfg = loaded
				}
				envCB := !strings.EqualFold(strings.TrimSpace(os.Getenv("AUTO_CALLBACK_ENABLED")), "false")
				if envCB && cfg.VoiceEnabled && cfg.AutoCallback {
					willCallback = true
					resolvedPeerNum := s.mgr.store.resolvePhone(dialJID.User)
					s.mgr.broker.recordCallEvent(c.CallID, "callback_trigger", "Auto-Callback & Message scheduled to "+resolvedPeerNum, "Reason: caller_unanswered")
					s.scheduleAutoCallback(dialJID, "inbound_unanswered_caller_hangup")
				}
			}
			if s.platform == "global" && s.recordMissedEmitted(c.CallID) {
				peerNum := ""
				if existingRec != nil {
					peerNum = existingRec.PeerNumber
				} else {
					peerNum = s.mgr.store.resolvePhone(resolvedPeer.User)
				}
				s.enqueueGlobalEvent(context.Background(), s.id+":"+resolvedPeer.String(), "call.missed", map[string]any{
					"call_id":                 c.CallID,
					"peer_jid":                resolvedPeer.String(),
					"caller_number":           peerNum,
					"reason":                  "caller_unanswered",
					"auto_callback_scheduled": willCallback,
				})
			}
		} else {
			s.mgr.broker.setCallOutcome(c.CallID, "Unanswered", fmt.Sprintf("Outbound call ended without answer (%s)", c.StateData.EndReason))
			if s.platform == "global" {
				dir := "outbound"
				peerNum := s.mgr.store.resolvePhone(resolvedPeer.User)
				var startedAt int64
				if existingRec != nil {
					dir = existingRec.Direction
					if existingRec.PeerNumber != "" {
						peerNum = existingRec.PeerNumber
					}
					startedAt = existingRec.StartedAt
					if clientRef == "" {
						clientRef = existingRec.ClientRef
					}
				}
				payload := map[string]any{
					"call_id":          c.CallID,
					"peer_jid":         resolvedPeer.String(),
					"caller_number":    peerNum,
					"direction":        dir,
					"outcome":          "unanswered",
					"duration_seconds": 0,
					"end_reason":       string(c.StateData.EndReason),
					"started_at":       startedAt,
					"ended_at":         endedAt,
				}
				if clientRef != "" {
					payload["client_ref"] = clientRef
				}
				s.enqueueGlobalEvent(context.Background(), s.id+":"+resolvedPeer.String(), "call.ended", payload)
			}
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

		if ac.recorder != nil {
			ac.recorder.RecordCallerPCM(pcm16)
		}

		if ac.geminiLive != nil && ac.geminiLive.IsEnabled() {
			ac.geminiLive.FeedCallerAudio(pcm16)
		}
		if ac.bridge != nil {
			_ = ac.bridge.WritePCM(pcm16)
		}
	}
}

func (s *Session) startOutgoingWithOptions(ctx context.Context, peer types.JID, isVideo bool, opts OutboundCallOptions) (string, error) {
	if pn := s.mgr.store.getPNForLID(ctx, peer.User); pn != "" {
		peer = types.NewJID(pn, types.DefaultUserServer)
	}
	peerNum := s.mgr.store.resolvePhone(peer.User)
	dir := "outbound"
	trigger := "manual"
	outcome := "Outbound Call"
	if opts.Greeting == "auto_callback" {
		dir = "auto-callback"
		trigger = "autonomous_callback"
		outcome = "Auto-Callback In Progress"
	}

	var preloaded *AgentConfigRow
	if loaded, err := s.mgr.store.getAgentConfig(ctx, s.businessID); err == nil {
		preloaded = &loaded
	}

	callID := signaling.GenerateCallID()
	cm := s.createCall(callID, peerNum, dir, preloaded, opts)
	if ac, ok := s.reg.get(callID); ok {
		ac.callbackJID = peer
		if ac.geminiLive != nil && ac.geminiLive.IsEnabled() {
			go func() {
				if err := ac.geminiLive.Start(); err != nil {
					s.log.Error("failed to pre-warm Gemini Live for outbound call", "err", err)
				}
			}()
		}
	}

	now := time.Now().UnixMilli()
	s.mgr.broker.upsertCall(CallRecord{
		SessionID: s.id, BusinessID: s.businessID, Platform: s.platform, CallID: callID,
		Direction: dir, Peer: peer.String(), PeerNumber: peerNum, ClientRef: strings.TrimSpace(opts.ClientRef),
		StartedAt: now, Status: StatusStarting, TriggerReason: trigger, Outcome: outcome,
		Events: []CallEventLog{{
			Timestamp: now,
			Type:      "start",
			Message:   fmt.Sprintf("Dialing %s (%s)", peerNum, dir),
			Details:   opts.Greeting,
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

func (s *Session) startOutgoingWithGreeting(ctx context.Context, peer types.JID, isVideo bool, customGreeting string) (string, error) {
	return s.startOutgoingWithOptions(ctx, peer, isVideo, OutboundCallOptions{Greeting: customGreeting})
}

func (s *Session) startOutgoing(ctx context.Context, peer types.JID, isVideo bool) (string, error) {
	return s.startOutgoingWithOptions(ctx, peer, isVideo, OutboundCallOptions{})
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
			cli := s.currentClient()
			if cli != nil && cli.Store != nil && cli.Store.LIDs != nil {
				_ = cli.Store.LIDs.PutLIDMapping(ctx, evt.From, parsed)
				_ = cli.Store.LIDs.PutLIDMapping(ctx, evt.CallCreator, parsed)
			}
		}
	}

	callbackJID := phoneJID
	if callbackJID.IsEmpty() {
		callbackJID = evt.CallCreatorAlt
	}
	if callbackJID.IsEmpty() || callbackJID.Server == "lid" {
		if pn := s.mgr.store.getPNForLID(ctx, evt.CallCreator.User); pn != "" {
			callbackJID = types.NewJID(pn, types.DefaultUserServer)
		} else if pn := s.mgr.store.getPNForLID(ctx, evt.From.User); pn != "" {
			callbackJID = types.NewJID(pn, types.DefaultUserServer)
		}
	}
	if callbackJID.IsEmpty() {
		callbackJID = evt.CallCreator
	}
	if callbackJID.IsEmpty() {
		callbackJID = evt.From
	}

	peerNum := s.mgr.store.resolvePhone(callbackJID.User)
	if peerNum == "" || strings.HasPrefix(peerNum, "LID:") {
		peerNum = s.mgr.store.resolvePhone(evt.From.User)
	}

	// Check agent configuration BEFORE touching CallManager signaling state.
	cfg := defaultAgentConfigRow()
	if loaded, err := s.mgr.store.getAgentConfig(ctx, s.businessID); err == nil {
		cfg = loaded
	} else {
		s.log.Warn("agent config not found for incoming call; using defaults", "business_id", s.businessID, "err", err)
	}

	if !cfg.VoiceEnabled {
		s.log.Info("voice agent disabled; ignoring incoming WhatsApp call", "call_id", callID, "peer", peerNum)
		if s.platform == "global" && s.recordMissedEmitted(callID) {
			resolvedPeer := s.resolvePhoneJID(ctx, callbackJID)
			s.enqueueGlobalEvent(ctx, s.id+":"+resolvedPeer.String(), "call.missed", map[string]any{
				"call_id":                 callID,
				"peer_jid":                resolvedPeer.String(),
				"caller_number":           peerNum,
				"reason":                  "agent_disabled",
				"auto_callback_scheduled": false,
			})
		}
		return
	}

	if max := s.mgr.maxCalls; max > 0 && s.reg.count() >= max {
		s.rejectOffer(ctx, node, evt.From)
		return
	}

	cm := s.createCall(callID, peerNum, "inbound", &cfg, OutboundCallOptions{})
	if ac, ok := s.reg.get(callID); ok {
		ac.callbackJID = callbackJID
	}

	s.mgr.broker.recordCallEvent(callID, "offer_received", "Inbound WhatsApp call offer received from "+peerNum, fmt.Sprintf("From: %s", evt.From.String()))
	cm.HandleCallOffer(ctx, node, evt.From)

	if cfg.AutoAnswer {
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
			s.mgr.broker.emitIncomingClaimed(s.platform, s.id, s.businessID, callID, "auto-answer")
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
	case *events.OfflineSyncPreview:
		s.inOfflineSync.Store(true)
		s.offlineMu.Lock()
		if s.offlineTimer != nil {
			s.offlineTimer.Stop()
		}
		s.offlineTimer = time.AfterFunc(60*time.Second, func() {
			s.inOfflineSync.Store(false)
		})
		s.offlineMu.Unlock()
	case *events.OfflineSyncCompleted:
		s.offlineMu.Lock()
		if s.offlineTimer != nil {
			s.offlineTimer.Stop()
			s.offlineTimer = nil
		}
		s.offlineMu.Unlock()
		s.inOfflineSync.Store(false)
	case *events.CallOffer:
		s.onIncomingOffer(ctx, evt)
	case *events.CallOfferNotice:
		s.log.Info("call offer notice received (offline/missed call)", "from", evt.From, "creator", evt.CallCreator)
		peer := evt.CallCreator
		if peer.IsEmpty() {
			peer = evt.From
		}
		peer = s.resolvePhoneJID(ctx, peer)
		cfg := defaultAgentConfigRow()
		if loaded, err := s.mgr.store.getAgentConfig(ctx, s.businessID); err == nil {
			cfg = loaded
		}
		envCB := !strings.EqualFold(strings.TrimSpace(os.Getenv("AUTO_CALLBACK_ENABLED")), "false")
		willCallback := envCB && cfg.VoiceEnabled && cfg.AutoCallback
		if willCallback {
			s.scheduleAutoCallback(peer, "call_offer_notice")
		}
		if s.platform == "global" && s.recordMissedEmitted(evt.CallID) {
			reason := "call_offer_notice"
			if !cfg.VoiceEnabled {
				reason = "agent_disabled"
			}
			s.enqueueGlobalEvent(ctx, s.id+":"+peer.String(), "call.missed", map[string]any{
				"call_id":                 evt.CallID,
				"peer_jid":                peer.String(),
				"caller_number":           s.mgr.store.resolvePhone(peer.User),
				"reason":                  reason,
				"auto_callback_scheduled": willCallback,
			})
		}
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

func (s *Session) scheduleAutoCallback(peer types.JID, reason string) {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("AUTO_CALLBACK_ENABLED")), "false") {
		return
	}
	if peer.IsEmpty() {
		return
	}
	// Don't call our own number
	if s.client.Store.ID != nil && peer.User == s.client.Store.ID.User {
		return
	}

	peerStr := peer.ToNonAD().String()
	hasActiveCallWithPeer := func() bool {
		for _, ac := range s.reg.all() {
			if ac.cm != nil {
				c := ac.cm.CurrentCall()
				if c != nil && !c.IsEnded() && (strings.Contains(c.PeerJid, peer.User) || strings.Contains(ac.callbackJID.User, peer.User)) {
					return true
				}
			}
		}
		return false
	}

	if hasActiveCallWithPeer() {
		s.log.Info("skipping auto-callback, call already active or ringing with peer", "peer", peerStr, "reason", reason)
		return
	}

	s.lastCallbackMu.Lock()
	if s.lastCallbacks == nil {
		s.lastCallbacks = make(map[string]time.Time)
	}
	last, exists := s.lastCallbacks[peerStr]
	if exists && time.Since(last) < 8*time.Second {
		s.lastCallbackMu.Unlock()
		s.log.Info("auto-callback debounced (triggered within 8s)", "peer", peerStr, "reason", reason)
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
		if !agentCfg.VoiceEnabled || !agentCfg.AutoCallback {
			s.log.Info("skipping auto-callback because voice or auto_callback is disabled", "business_id", s.businessID)
			return
		}

		if msgText := strings.TrimSpace(agentCfg.FallbackMessage); msgText != "" {
			s.sendWhatsAppMessage(peer, msgText)
		}

		// Wait 4.0s so the caller's phone returns to idle state from the previous attempt
		time.Sleep(4000 * time.Millisecond)

		if hasActiveCallWithPeer() {
			s.log.Info("skipping auto-callback after delay, call already active with peer", "peer", peerStr)
			return
		}

		callID, err := s.startOutgoingWithGreeting(ctx, peer, false, "auto_callback")
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

func (s *Session) isCurrentClient(c *whatsmeow.Client) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client == c
}

func (s *Session) startPairing(ctx context.Context) error {
	refreshWAVersion(ctx, s.log, false)
	s.mu.Lock()
	client := s.client
	s.mu.Unlock()

	s.setAuth(AuthSnapshot{State: "connecting", Paired: false})
	qrChan, err := client.GetQRChannel(ctx)
	if err != nil {
		return err
	}
	if err := client.Connect(); err != nil {
		return err
	}
	go func() {
		for evt := range qrChan {
			if !s.isCurrentClient(client) {
				return
			}
			switch evt.Event {
			case "code":
				s.log.Info("scan the QR code to pair this session")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
				s.setAuth(AuthSnapshot{State: "qr", QR: evt.Code})
				s.mgr.broker.emitSessionQR(s.platform, s.id, s.businessID, evt.Code)
			case "success":
				if id := client.Store.ID; id != nil {
					_ = s.mgr.store.setJID(s.mgr.appCtx, s.id, id.String())
				}
				s.setAuth(AuthSnapshot{State: "open", Paired: true})
			case "err-client-outdated":
				s.log.Warn("WhatsApp client version outdated during pairing; refreshing version and retrying")
				refreshWAVersion(s.mgr.appCtx, s.log, true)
				go func() {
					if err := s.mgr.Pair(s.id); err != nil {
						s.log.Error("re-pair after version refresh failed", "err", err)
						s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
					}
				}()
				return
			case "timeout":
				s.log.Info("QR pairing timed out")
				s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
			default:
				s.log.Warn("QR pairing ended", "event", evt.Event, "err", evt.Error)
				s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
			}
		}
	}()
	return nil
}

func (s *Session) setAuth(a AuthSnapshot) {
	s.mu.Lock()
	prevState := s.auth.State
	prevPaired := s.auth.Paired
	s.auth = a
	s.mu.Unlock()

	s.mgr.broker.emitAuthState(s.platform, s.id, s.businessID, a)
	s.mgr.broker.emitSessionListForPlatform(s.mgr.infosForPlatform(s.platform, ""))

	if s.platform == "global" && (a.State != prevState || a.Paired != prevPaired) && a.State != "connecting" {
		info := s.info()
		s.enqueueGlobalEvent(context.Background(), s.id, "session.status", map[string]any{
			"state":  a.State,
			"paired": a.Paired,
			"jid":    info.JID,
		})
	}
}

func (s *Session) info() SessionInfo {
	s.mu.Lock()
	a := s.auth
	s.mu.Unlock()
	jid := ""
	if id := s.client.Store.ID; id != nil {
		jid = id.String()
	}
	return SessionInfo{
		ID:         s.id,
		BusinessID: s.businessID,
		Platform:   s.platform,
		JID:        jid,
		State:      a.State,
		Paired:     a.Paired || jid != "",
		QR:         a.QR,
	}
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
	ac.cancelDurationTimers()
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
		ac.cancelDurationTimers()
		_ = ac.cm.EndCall(context.Background(), core.EndCallReasonUserEnded)
		if ac.bridge != nil {
			ac.bridge.Close()
		}
	}
}

func (s *Session) replaceClient(client *whatsmeow.Client) {
	s.teardownAllCalls()
	s.mu.Lock()
	oldClient := s.client
	s.client = client
	s.mu.Unlock()
	if oldClient != nil {
		oldClient.Disconnect()
	}
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
