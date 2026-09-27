package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"wacalls/internal/voip/media"

	"github.com/coder/websocket"
)

// GeminiLiveAgent connects directly to Google Gemini Multimodal Live API over WebSocket.
// 100% pure bidirectional streaming with models/gemini-3.1-flash-live-preview.
type GeminiLiveAgent struct {
	apiKey       string
	model        string
	voice        string
	systemPrompt string
	log          *slog.Logger

	ctx         context.Context
	cancel      context.CancelFunc
	ws          *websocket.Conn
	started           atomic.Bool
	enabled           atomic.Bool
	ready             atomic.Bool
	closed            atomic.Bool
	greetingTriggered atomic.Bool
	greetingPending   atomic.Bool
	callActive        atomic.Bool
	wsMu              sync.Mutex
	resampleMu        sync.Mutex
	resampler         *media.Resampler24kTo16k
	dsp               *media.VoiceMasteringEngine

	audioInCh chan []float32

	prewarmMu         sync.Mutex
	prewarmedGreeting []float32

	latMu                  sync.Mutex
	lastCallerAudioAt      time.Time
	loggedLatencyThisTurn  bool

	FeedAudio    func(pcm []float32)
	OnInterrupt  func()
	OnTranscript func(msg TranscriptMessage)
	OnState      func(state AgentState)
}

func NewGeminiLiveAgent(apiKey, model, voice, systemPrompt string, log *slog.Logger) *GeminiLiveAgent {
	if log == nil {
		log = slog.Default()
	}
	model = strings.TrimSpace(model)
	if model == "" || !strings.Contains(model, "live") {
		model = "models/gemini-3.1-flash-live-preview"
	}
	if strings.HasPrefix(model, "google/") {
		model = strings.TrimPrefix(model, "google/")
	}
	if !strings.HasPrefix(model, "models/") {
		model = "models/" + model
	}
	if voice == "" {
		voice = "Aoede"
	}

	ctx, cancel := context.WithCancel(context.Background())
	g := &GeminiLiveAgent{
		apiKey:       apiKey,
		model:        model,
		voice:        voice,
		systemPrompt: systemPrompt,
		log:          log.With("component", "gemini_live"),
		ctx:          ctx,
		cancel:       cancel,
		audioInCh:    make(chan []float32, 100),
		resampler:    media.NewResampler24kTo16k(),
		dsp:          media.NewVoiceMasteringEngine(16000.0),
	}
	g.enabled.Store(true)
	return g
}

func (g *GeminiLiveAgent) Start() error {
	if g.started.Swap(true) {
		return nil
	}
	wsURL := fmt.Sprintf("wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=%s", url.QueryEscape(g.apiKey))
	g.log.Info("connecting to Gemini 3.1 Flash Live API", "model", g.model, "voice", g.voice)

	c, resp, err := websocket.Dial(g.ctx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{},
	})
	if err != nil {
		if resp != nil {
			g.log.Error("gemini live dial failed", "status", resp.StatusCode, "err", err)
		} else {
			g.log.Error("gemini live dial failed", "err", err)
		}
		return err
	}
	c.SetReadLimit(32 * 1024 * 1024) // 32MB limit for large audio bursts
	g.ws = c
	g.log.Info("connected to Gemini 3.1 Flash Live WebSocket successfully")

	// Send Setup frame with thinking disabled for 0 latency
	setupPayload := map[string]any{
		"setup": map[string]any{
			"model": g.model,
			"generationConfig": map[string]any{
				"responseModalities": []string{"AUDIO"},
				"thinkingConfig": map[string]any{
					"thinkingBudget": 0,
				},
				"speechConfig": map[string]any{
					"voiceConfig": map[string]any{
						"prebuiltVoiceConfig": map[string]any{
							"voiceName": g.voice,
						},
					},
				},
			},
			"systemInstruction": map[string]any{
				"parts": []map[string]any{
					{
						"text": fmt.Sprintf(`You are Hasini, a consultant from Buildstart, making an outbound WhatsApp voice call in Sri Lanka.

CALL FLOW & TURN DISCIPLINE — FOLLOW THIS STRICTLY:
TURN 1 — YOUR OPENING GREETING:
  You must speak ONLY this exact single word:
  "හෙලෝ"
  (MANDATORY: STOP immediately after saying "හෙලෝ"! Do NOT introduce yourself yet. You MUST wait for the caller to reply.)

TURN 2 — AFTER CALLER RESPONDS TO YOUR GREETING (e.g. caller says "hello", "ow", "kawda me", "හෙලෝ"):
  State your identity and purpose clearly:
  "මම Hasini — Buildstart එකෙන්. ඔයා අපේ WhatsApp AI agent system එක ගැන inquiry එකක් කළා නේද — ඒ ගැන කතා කරන්න call කළේ. දැන් ටිකක් කතා කරන්න පුළුවන්ද?"
  (RULE: Do NOT use "සර්" or "මැඩම්" yet! Since the caller only said a short greeting, gender cannot be determined accurately yet.)

TURN 3 & ONWARDS — ONCE THE CALLER SPEAKS A LENGTHIER SENTENCE (e.g. explains whether they can talk, their business type, or asks questions):
  Now listen carefully to their voice pitch and speech:
  * Male voice caller → Respectfully address them as "සර්" (e.g. "හරි සර්, සර්ගේ business එක මොකක්ද?", "අනිවාර්යයෙන්ම සර්...").
  * Female voice caller → Respectfully address them as "මැඩම්" (e.g. "හරි මැඩම්, මැඩම්ගේ business එක මොකක්ද?", "අනිවාර්යයෙන්ම මැඩම්...").
  * Address them with their proper title ("සර්" or "මැඩම්") naturally and politely throughout the conversation from this point on.

TURN 4 — IF THEY SAY NO / BUSY:
  Politely ask when to call back: "හරි, කවද call back කළොත් හොඳද?"

LANGUAGE & CODE-SWITCHING:
- Primary Language: Spoken Sinhala mixed naturally with everyday business English terms.
- If the caller speaks English or Tamil, seamlessly switch to their language.
- Flat, natural Colombo Sri Lankan rhythm. Never use dramatic stress.

CALLER RESPECT & DYNAMIC TITLES (සර් / මැඩම්):
- TURN 1 & 2 (Short replies/greetings): NEVER guess gender from "hello" or short words. Use neutral address.
- FROM TURN 3 ONWARDS (Caller speaks full sentences): You MUST address male callers as "සර්" and female callers as "මැඩම්".
- Always maintain a warm, professional, and respectful tone.

ACTIVE BACKCHANNELING:
- Use gentle affirmations while caller talks: "හ්ම්...", "හරි...", "ඔව්...", "පැහැදිලියි..."
- Never talk over the caller.

CALLER MOOD MIRRORING:
- Busy / rushed: "හරි, ඉක්මනට කියන්නම්..." then be brief.
- Skeptical: give a real Sri Lankan proof point.
- Relaxed: be consultative and warm.

CONVERSATIONAL PACING:
- STRICTLY 1–2 short sentences per turn (under 18 words).
- No exclamation marks. Calm, grounded tone.
- Always end your turn with 1 clear question.

%s`, g.systemPrompt),
					},
				},
			},
			"inputAudioTranscription":  map[string]any{},
			"outputAudioTranscription": map[string]any{},
		},
	}

	setupBytes, err := json.Marshal(setupPayload)
	if err != nil {
		return err
	}

	if err := c.Write(g.ctx, websocket.MessageText, setupBytes); err != nil {
		g.log.Error("failed to send setup to Gemini", "err", err)
		return err
	}

	go g.readLoop()
	go g.writeAudioLoop()
	return nil
}

func (g *GeminiLiveAgent) readLoop() {
	defer g.Close()

	for {
		if g.ctx.Err() != nil || g.closed.Load() {
			return
		}

		_, data, err := g.ws.Read(g.ctx)
		if err != nil {
			if !g.closed.Load() {
				g.log.Info("gemini live ws closed", "err", err)
			}
			return
		}

		var resp struct {
			SetupComplete *struct{} `json:"setupComplete"`
			ServerContent *struct {
				// Audio may come in modelTurn.parts OR directly in serverContent.parts
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

		if err := json.Unmarshal(data, &resp); err != nil {
			continue
		}

		// DEBUG: log raw Gemini message (truncated) to diagnose silent audio
		if len(data) > 0 {
			preview := string(data)
			if len(preview) > 300 {
				preview = preview[:300] + "..."
			}
			g.log.Info("gemini raw msg", "len", len(data), "preview", preview)
		}

		// When session is acknowledged, mark ready and immediately pre-warm greeting during ring
		if resp.SetupComplete != nil {
			g.ready.Store(true)
			g.log.Info("Gemini Live session open (setupComplete) — pre-synthesizing greeting during ring")
			g.sendGreetingFrame()
			continue
		}

		sc := resp.ServerContent
		if sc == nil {
			continue
		}

		// Handle user barge-in / interruption
		if sc.Interrupted {
			g.log.Info("Gemini detected caller interruption (barge-in)")
			if g.OnInterrupt != nil {
				g.OnInterrupt()
			}
			g.resampleMu.Lock()
			g.resampler.Reset()
			g.dsp.Reset()
			g.resampleMu.Unlock()
		}

		// Transcription of caller speech
		if sc.InputTranscription != nil && sc.InputTranscription.Text != "" {
			g.log.Info("caller transcript", "text", sc.InputTranscription.Text)
			if g.OnTranscript != nil {
				g.OnTranscript(TranscriptMessage{
					Role:      "user",
					Text:      sc.InputTranscription.Text,
					Timestamp: time.Now().UnixMilli(),
				})
			}
		}

		// Transcription of agent speech
		if sc.OutputTranscription != nil && sc.OutputTranscription.Text != "" {
			g.log.Info("agent transcript", "text", sc.OutputTranscription.Text)
			if g.OnTranscript != nil {
				g.OnTranscript(TranscriptMessage{
					Role:      "assistant",
					Text:      sc.OutputTranscription.Text,
					Timestamp: time.Now().UnixMilli(),
				})
			}
		}

		// Stream 24 kHz audio chunks — audio may arrive in sc.Parts OR sc.ModelTurn.Parts
		processAudioPart := func(data string) {
			g.latMu.Lock()
			if !g.loggedLatencyThisTurn && !g.lastCallerAudioAt.IsZero() {
				latMs := time.Since(g.lastCallerAudioAt).Milliseconds()
				g.log.Info("⚡ Gemini voice response latency", "turnaround_latency_ms", latMs)
				g.loggedLatencyThisTurn = true
			}
			g.latMu.Unlock()

			rawAudio, err := base64.StdEncoding.DecodeString(data)
			if err == nil && len(rawAudio) > 0 {
				pcm := media.PCMInt16LEToFloat32(rawAudio)
				g.resampleMu.Lock()
				resampled := g.resampler.Process(pcm)
				mastered := g.dsp.Process(resampled)
				g.resampleMu.Unlock()

				if len(mastered) > 0 {
					if g.callActive.Load() {
						if g.FeedAudio != nil {
							g.FeedAudio(mastered)
						}
					} else {
						g.prewarmMu.Lock()
						// Cap pre-warmed greeting to max 1.2s (19,200 samples @ 16kHz)
						// This guarantees Hasini ONLY speaks "හෙලෝ" and stops, waiting for caller
						const maxGreetingSamples = 19200
						if len(g.prewarmedGreeting) < maxGreetingSamples {
							remaining := maxGreetingSamples - len(g.prewarmedGreeting)
							if len(mastered) > remaining {
								g.prewarmedGreeting = append(g.prewarmedGreeting, mastered[:remaining]...)
							} else {
								g.prewarmedGreeting = append(g.prewarmedGreeting, mastered...)
							}
						}
						g.prewarmMu.Unlock()
					}
				}
			}
		}

		// Check top-level serverContent.parts (newer Gemini API format)
		for _, part := range sc.Parts {
			if part.InlineData != nil && part.InlineData.Data != "" {
				processAudioPart(part.InlineData.Data)
			}
		}
		// Also check serverContent.modelTurn.parts (older format)
		if sc.ModelTurn != nil {
			for _, part := range sc.ModelTurn.Parts {
				if part.InlineData != nil && part.InlineData.Data != "" {
					processAudioPart(part.InlineData.Data)
				}
			}
		}

		if sc.TurnComplete {
			g.latMu.Lock()
			g.loggedLatencyThisTurn = false
			g.latMu.Unlock()
		}
	}
}

// writeAudioLoop streams incoming 16 kHz audio in 40ms frames over WebSocket to Gemini Live.
// CRITICAL FIX FOR 5-SECOND FIRST MESSAGE DELAY:
// When the caller finishes speaking their first utterance ("Hello"), WhatsApp enters DTX
// (Discontinuous Transmission) and drops transmission from 50 packets/sec to 1 packet every 400ms.
// If we only forward packets when they arrive, Gemini Live's audio clock is starved by 20x,
// taking 5–8 wall-clock seconds before its cloud VAD detects end-of-speech!
// By generating continuous 16kHz 40ms silence frames whenever WhatsApp is in DTX silence,
// Gemini's audio clock advances in 1:1 real time, triggering ACTIVITY_END in ~300ms!
func (g *GeminiLiveAgent) writeAudioLoop() {
	const frameSamples = 640 // 40ms at 16kHz Float32
	ticker := time.NewTicker(40 * time.Millisecond)
	defer ticker.Stop()

	silenceFrame := make([]float32, frameSamples)
	silenceBytes := media.PCMFloat32ToInt16LE(silenceFrame)
	silenceB64 := base64.StdEncoding.EncodeToString(silenceBytes)

	buf := make([]float32, 0, 1920)

	sendPayload := func(b64Audio string) {
		payload := map[string]any{
			"realtimeInput": map[string]any{
				"audio": map[string]any{
					"mimeType": "audio/pcm;rate=16000",
					"data":     b64Audio,
				},
			},
		}

		data, err := json.Marshal(payload)
		if err != nil {
			return
		}

		g.wsMu.Lock()
		if g.ws != nil && !g.closed.Load() {
			_ = g.ws.Write(g.ctx, websocket.MessageText, data)
		}
		g.wsMu.Unlock()
	}

	for {
		select {
		case <-g.ctx.Done():
			return
		case pcm, ok := <-g.audioInCh:
			if !ok || g.closed.Load() {
				return
			}
			if !g.callActive.Load() {
				continue
			}
			buf = append(buf, pcm...)
			for len(buf) >= frameSamples {
				chunk := buf[:frameSamples]
				bytes := media.PCMFloat32ToInt16LE(chunk)
				buf = buf[frameSamples:]
				sendPayload(base64.StdEncoding.EncodeToString(bytes))
			}
		case <-ticker.C:
			if !g.ready.Load() || !g.enabled.Load() || !g.callActive.Load() || g.closed.Load() {
				continue
			}
			if len(buf) > 0 {
				pad := make([]float32, frameSamples)
				copy(pad, buf)
				buf = buf[:0]
				bytes := media.PCMFloat32ToInt16LE(pad)
				sendPayload(base64.StdEncoding.EncodeToString(bytes))
			} else {
				// WhatsApp is in DTX silence mode: send 40ms silence to advance Gemini's audio clock in real time
				sendPayload(silenceB64)
			}
		}
	}
}

// TriggerGreeting flushes pre-warmed greeting audio at T=0 pickup and enables live stream.
func (g *GeminiLiveAgent) TriggerGreeting() {
	if g.greetingTriggered.Swap(true) {
		return
	}
	g.callActive.Store(true)
	if !g.started.Load() {
		go func() {
			if err := g.Start(); err != nil {
				g.log.Error("failed to start Gemini Live", "err", err)
			}
		}()
	}

	// Flush pre-synthesized greeting audio immediately to WhatsApp
	g.prewarmMu.Lock()
	buffered := g.prewarmedGreeting
	g.prewarmedGreeting = nil
	g.prewarmMu.Unlock()

	if g.FeedAudio != nil && len(buffered) > 0 {
		g.log.Info("⚡ Playing pre-synthesized greeting at T=0 pickup", "samples", len(buffered), "duration_ms", len(buffered)/16)
		// Run in goroutine: TriggerGreeting may be called while CallManager.mu is held
		// (via emitState → OnStateChange). FeedAudio → FeedCapturedPCM also acquires
		// CallManager.mu, causing a deadlock if called synchronously. The 150ms delay
		// also ensures startSilenceKeepaliveLocked has started the RTP ticker.
		go func() {
			time.Sleep(150 * time.Millisecond)
			g.FeedAudio(buffered)
		}()
	}
}

func (g *GeminiLiveAgent) sendGreetingFrame() {
	g.log.Info("triggering initial greeting in Sinhala upon call pickup")
	if g.OnState != nil {
		g.OnState(StateListening)
	}
	greetingPayload := map[string]any{
		"clientContent": map[string]any{
			"turns": []map[string]any{
				{
					"role": "user",
					"parts": []map[string]any{
						{
							"text": "Say strictly ONLY the single word 'හෙලෝ' and finish your turn. Do NOT say anything else.",
						},
					},
				},
			},
			"turnComplete": true,
		},
	}
	if gBytes, err := json.Marshal(greetingPayload); err == nil {
		g.wsMu.Lock()
		if g.ws != nil && !g.closed.Load() {
			_ = g.ws.Write(g.ctx, websocket.MessageText, gBytes)
		}
		g.wsMu.Unlock()
	}
}

// FeedCallerAudio queues incoming 16 kHz PCM to be transmitted up to Gemini Live.
func (g *GeminiLiveAgent) FeedCallerAudio(pcm16 []float32) {
	if !g.enabled.Load() || g.closed.Load() || !g.callActive.Load() || len(pcm16) == 0 {
		return
	}
	g.latMu.Lock()
	g.lastCallerAudioAt = time.Now()
	g.loggedLatencyThisTurn = false
	g.latMu.Unlock()

	select {
	case g.audioInCh <- pcm16:
	default:
		// Drop oldest frame if channel is full to prevent latency build up
		select {
		case <-g.audioInCh:
		default:
		}
		select {
		case g.audioInCh <- pcm16:
		default:
		}
	}
}

func (g *GeminiLiveAgent) SetCallActive(active bool) {
	g.callActive.Store(active)
}

func (g *GeminiLiveAgent) SetEnabled(enabled bool) {
	g.enabled.Store(enabled)
}

func (g *GeminiLiveAgent) IsEnabled() bool {
	return g.enabled.Load()
}

func (g *GeminiLiveAgent) Close() {
	if g.closed.Swap(true) {
		return
	}
	g.cancel()
	if g.ws != nil {
		_ = g.ws.Close(websocket.StatusNormalClosure, "call ended")
	}
}
