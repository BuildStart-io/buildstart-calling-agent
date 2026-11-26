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

type AgentState string

const (
	StateIdle      AgentState = "idle"
	StateListening AgentState = "listening"
	StateThinking  AgentState = "thinking"
	StateSpeaking  AgentState = "speaking"
)

// bargeInMinSpeechMs is how long the caller must speak continuously before Gemini
// treats it as a real barge-in (sent to Gemini as automaticActivityDetection.prefixPaddingMs).
// Raise it if backchannels ("mm", "okay") still cut the agent off; lower it if real
// interruptions feel sluggish.
const bargeInMinSpeechMs = 300

type TranscriptMessage struct {
	Role      string `json:"role"` // "user" or "assistant" or "system"
	Text      string `json:"text"`
	Timestamp int64  `json:"timestamp"`
}

type ToolDeclaration struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type FunctionCall struct {
	ID   string         `json:"id"`
	Name string         `json:"name"`
	Args map[string]any `json:"args"`
}

// GeminiLiveAgent connects directly to Google Gemini Multimodal Live API over WebSocket.
// 100% pure bidirectional streaming with models/gemini-3.1-flash-live-preview.
type GeminiLiveAgent struct {
	apiKey          string
	model           string
	voice           string
	systemPrompt    string
	greetingMessage string
	tools           []ToolDeclaration
	log             *slog.Logger

	ctx                 context.Context
	cancel              context.CancelFunc
	ws                  *websocket.Conn
	started             atomic.Bool
	enabled             atomic.Bool
	ready               atomic.Bool
	closed              atomic.Bool
	greetingTriggered   atomic.Bool
	greetingPending     atomic.Bool
	callActive          atomic.Bool
	interruptedThisTurn atomic.Bool
	isModelSpeaking     atomic.Bool
	hasSpokenWithPeer   atomic.Bool
	wsMu                sync.Mutex
	resumeMu            sync.Mutex
	resumeHandle        string
	isReconnecting      atomic.Bool
	resampleMu          sync.Mutex
	resampler           *media.Resampler24kTo16k
	dsp                 *media.VoiceMasteringEngine

	audioInCh chan []float32

	prewarmMu         sync.Mutex
	prewarmedGreeting []float32

	latMu                 sync.Mutex
	lastCallerAudioAt     time.Time
	loggedLatencyThisTurn bool

	inFlightToolsMu sync.Mutex
	inFlightTools   map[string]context.CancelFunc

	FeedAudio    func(pcm []float32)
	OnInterrupt  func()
	OnTranscript func(msg TranscriptMessage)
	OnState      func(state AgentState)
	OnToolCall   func(ctx context.Context, call FunctionCall) (map[string]any, error)
}

func NewGeminiLiveAgent(apiKey, model, voice, systemPrompt, greetingMessage string, log *slog.Logger) *GeminiLiveAgent {
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
	if greetingMessage == "" {
		greetingMessage = "Hello"
	}

	ctx, cancel := context.WithCancel(context.Background())
	g := &GeminiLiveAgent{
		apiKey:          apiKey,
		model:           model,
		voice:           voice,
		systemPrompt:    systemPrompt,
		greetingMessage: greetingMessage,
		log:             log.With("component", "gemini_live"),
		ctx:             ctx,
		cancel:          cancel,
		audioInCh:       make(chan []float32, 100),
		resampler:       media.NewResampler24kTo16k(),
		dsp:             media.NewVoiceMasteringEngine(16000.0),
		inFlightTools:   make(map[string]context.CancelFunc),
	}
	g.enabled.Store(true)
	return g
}

// SetTools configures Gemini function declarations before Start() is called.
// When empty, setup omits the tools field completely so non-tool sessions stay unchanged.
func (g *GeminiLiveAgent) SetTools(tools []ToolDeclaration) {
	if len(tools) == 0 {
		return
	}
	g.tools = append([]ToolDeclaration(nil), tools...)
}

func (g *GeminiLiveAgent) Start() error {
	if g.started.Swap(true) {
		return nil
	}
	err := g.connect(false)
	if err == nil {
		go g.writeAudioLoop()
	}
	return err
}

func (g *GeminiLiveAgent) reconnect() {
	if g.isReconnecting.Swap(true) {
		return
	}
	defer g.isReconnecting.Store(false)

	g.ready.Store(false)
	g.log.Info("Gemini sent goAway, reconnecting using session resumption...")
	err := g.connect(true)
	if err != nil {
		g.log.Error("failed to reconnect to Gemini API", "err", err)
	}
}

func (g *GeminiLiveAgent) connect(isReconnect bool) error {
	wsURL := fmt.Sprintf("wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=%s", url.QueryEscape(g.apiKey))
	g.log.Info("connecting to Gemini 3.1 Flash Live API", "model", g.model, "voice", g.voice, "reconnect", isReconnect)

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

	g.wsMu.Lock()
	if g.ws != nil {
		g.ws.Close(websocket.StatusNormalClosure, "reconnecting")
	}
	g.ws = c
	g.wsMu.Unlock()
	g.log.Info("connected to Gemini 3.1 Flash Live WebSocket successfully")

	// Send Setup frame with thinking disabled for 0 latency
	setupObj := map[string]any{
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
					"text": g.systemPrompt,
				},
			},
		},
		"inputAudioTranscription":  map[string]any{},
		"outputAudioTranscription": map[string]any{},
		// Barge-in debounce: Gemini only commits start-of-speech (and therefore
		// sends `interrupted`, which flushes the agent's buffered audio) after the
		// caller has spoken continuously for bargeInMinSpeechMs.
		"realtimeInputConfig": map[string]any{
			"automaticActivityDetection": map[string]any{
				"prefixPaddingMs": bargeInMinSpeechMs,
			},
		},
	}

	if len(g.tools) > 0 {
		setupObj["tools"] = []map[string]any{
			{
				"functionDeclarations": g.tools,
			},
		}
	}

	g.resumeMu.Lock()
	if g.resumeHandle != "" {
		setupObj["sessionResumption"] = map[string]any{
			"handle": g.resumeHandle,
		}
	}
	g.resumeMu.Unlock()

	setupPayload := map[string]any{
		"setup": setupObj,
	}

	setupBytes, err := json.Marshal(setupPayload)
	if err != nil {
		return err
	}

	if err := c.Write(g.ctx, websocket.MessageText, setupBytes); err != nil {
		g.log.Error("failed to send setup to Gemini", "err", err)
		return err
	}

	go g.readLoop(c, isReconnect)
	return nil
}

func (g *GeminiLiveAgent) readLoop(conn *websocket.Conn, isReconnect bool) {
	defer func() {
		// Only close the whole agent if this is still the active connection and we aren't mid-reconnect.
		if !g.isReconnecting.Load() {
			g.wsMu.Lock()
			isCurrent := g.ws == conn
			g.wsMu.Unlock()
			if isCurrent {
				g.Close()
			}
		}
	}()

	for {
		if g.ctx.Err() != nil || g.closed.Load() {
			return
		}

		_, data, err := conn.Read(g.ctx)
		if err != nil {
			if !g.closed.Load() && !g.isReconnecting.Load() {
				g.log.Info("gemini live ws closed", "err", err)
			}
			return
		}

		var resp struct {
			SetupComplete *struct{} `json:"setupComplete"`
			VoiceActivity *struct {
				Type        string `json:"type"`
				AudioOffset string `json:"audioOffset"`
			} `json:"voiceActivity"`
			SessionResumptionUpdate *struct {
				NewHandle string `json:"newHandle"`
				Resumable bool   `json:"resumable"`
			} `json:"sessionResumptionUpdate"`
			GoAway   *struct{} `json:"goAway"`
			ToolCall *struct {
				FunctionCalls []FunctionCall `json:"functionCalls"`
			} `json:"toolCall"`
			ToolCallCancellation *struct {
				IDs []string `json:"ids"`
			} `json:"toolCallCancellation"`
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

		if err := json.Unmarshal(data, &resp); err != nil {
			continue
		}

		// Voice activity diagnostics from Gemini Live VAD
		if resp.VoiceActivity != nil {
			g.log.Info("Gemini Live VAD voice activity", "type", resp.VoiceActivity.Type, "offset", resp.VoiceActivity.AudioOffset)
		}

		// DEBUG: log raw Gemini message (truncated) to diagnose silent audio
		if len(data) > 0 {
			preview := string(data)
			if len(preview) > 300 {
				preview = preview[:300] + "..."
			}
			g.log.Info("gemini raw msg", "len", len(data), "preview", preview)
		}

		// When session is acknowledged, mark ready and immediately pre-warm greeting on initial connect
		if resp.SetupComplete != nil {
			g.ready.Store(true)
			if !isReconnect {
				g.log.Info("Gemini Live session open (setupComplete) — pre-synthesizing greeting during ring")
				g.sendGreetingFrame()
			} else {
				g.log.Info("Gemini Live resumed session (setupComplete)")
			}
			continue
		}

		if resp.SessionResumptionUpdate != nil {
			g.resumeMu.Lock()
			if resp.SessionResumptionUpdate.NewHandle != "" {
				g.resumeHandle = resp.SessionResumptionUpdate.NewHandle
			}
			g.resumeMu.Unlock()
			continue
		}

		if resp.GoAway != nil {
			go g.reconnect()
			return // exit this old readLoop
		}

		if resp.ToolCallCancellation != nil && len(resp.ToolCallCancellation.IDs) > 0 {
			g.inFlightToolsMu.Lock()
			for _, id := range resp.ToolCallCancellation.IDs {
				if cancelFn, ok := g.inFlightTools[id]; ok {
					cancelFn()
					delete(g.inFlightTools, id)
				}
			}
			g.inFlightToolsMu.Unlock()
			continue
		}

		if resp.ToolCall != nil && len(resp.ToolCall.FunctionCalls) > 0 {
			for _, fc := range resp.ToolCall.FunctionCalls {
				g.dispatchToolCall(fc)
			}
			continue
		}

		sc := resp.ServerContent
		if sc == nil {
			continue
		}

		// Handle user barge-in / interruption from serverContent
		if sc.Interrupted {
			g.log.Info("Gemini detected caller interruption (barge-in)")
			g.interruptedThisTurn.Store(true)
			g.isModelSpeaking.Store(false)
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
			if g.interruptedThisTurn.Load() {
				return // Discard audio chunks from interrupted turn
			}
			g.isModelSpeaking.Store(true)

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
						g.prewarmedGreeting = append(g.prewarmedGreeting, mastered...)
						g.prewarmMu.Unlock()
					}
				}
			}
		}

		for _, part := range sc.Parts {
			if part.InlineData != nil && part.InlineData.Data != "" {
				processAudioPart(part.InlineData.Data)
			}
		}
		if sc.ModelTurn != nil {
			for _, part := range sc.ModelTurn.Parts {
				if part.InlineData != nil && part.InlineData.Data != "" {
					processAudioPart(part.InlineData.Data)
				}
			}
		}

		if sc.TurnComplete {
			g.isModelSpeaking.Store(false)
			g.interruptedThisTurn.Store(false)
			g.latMu.Lock()
			g.loggedLatencyThisTurn = false
			g.latMu.Unlock()
		}
	}
}

func (g *GeminiLiveAgent) dispatchToolCall(fc FunctionCall) {
	tCtx, cancel := context.WithTimeout(g.ctx, 8*time.Second)
	g.inFlightToolsMu.Lock()
	g.inFlightTools[fc.ID] = cancel
	g.inFlightToolsMu.Unlock()

	go func() {
		defer func() {
			cancel()
			g.inFlightToolsMu.Lock()
			delete(g.inFlightTools, fc.ID)
			g.inFlightToolsMu.Unlock()
		}()

		var result map[string]any
		if g.OnToolCall == nil {
			result = map[string]any{"error": "tool handler not configured"}
		} else {
			res, err := g.OnToolCall(tCtx, fc)
			if tCtx.Err() == context.Canceled {
				return
			}
			if err != nil {
				if tCtx.Err() == context.DeadlineExceeded {
					result = map[string]any{"error": "timeout"}
				} else {
					result = map[string]any{"error": err.Error()}
				}
			} else if res == nil {
				result = map[string]any{"ok": true}
			} else {
				result = res
			}
		}

		// If mid-reconnect on a 10-minute goAway, wait briefly for the resumed session to become ready.
		if g.isReconnecting.Load() || !g.ready.Load() {
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if g.closed.Load() || g.ctx.Err() != nil {
					return
				}
				if g.ready.Load() && !g.isReconnecting.Load() {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
		}

		g.sendToolResponse(fc.ID, fc.Name, result)
	}()
}

func (g *GeminiLiveAgent) sendToolResponse(callID, name string, result map[string]any) {
	payload := map[string]any{
		"toolResponse": map[string]any{
			"functionResponses": []map[string]any{
				{
					"id":       callID,
					"name":     name,
					"response": result,
				},
			},
		},
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return
	}
	g.wsMu.Lock()
	if g.ws != nil && !g.closed.Load() {
		if err := g.ws.Write(g.ctx, websocket.MessageText, b); err != nil {
			g.log.Warn("failed to write toolResponse to Gemini Live", "call_id", callID, "name", name, "err", err)
		}
	}
	g.wsMu.Unlock()
}

// SendSystemCue injects a short system-level turn (e.g. call duration wrap-up warning).
func (g *GeminiLiveAgent) SendSystemCue(text string) {
	if g.closed.Load() || !g.ready.Load() || text == "" {
		return
	}
	payload := map[string]any{
		"clientContent": map[string]any{
			"turns": []map[string]any{
				{
					"role": "user",
					"parts": []map[string]any{
						{"text": text},
					},
				},
			},
			"turnComplete": true,
		},
	}
	if b, err := json.Marshal(payload); err == nil {
		g.wsMu.Lock()
		if g.ws != nil && !g.closed.Load() {
			_ = g.ws.Write(g.ctx, websocket.MessageText, b)
		}
		g.wsMu.Unlock()
	}
}

func (g *GeminiLiveAgent) IsModelSpeaking() bool {
	return g.isModelSpeaking.Load()
}

// writeAudioLoop streams incoming 16 kHz audio directly to Gemini Live.
func (g *GeminiLiveAgent) writeAudioLoop() {
	const silenceFrameSamples = 640 // 40ms at 16kHz Float32
	ticker := time.NewTicker(40 * time.Millisecond)
	defer ticker.Stop()

	silenceFrame := make([]float32, silenceFrameSamples)
	silenceBytes := media.PCMFloat32ToInt16LE(silenceFrame)
	silenceB64 := base64.StdEncoding.EncodeToString(silenceBytes)

	var lastAudioReceivedAt time.Time

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
			if len(pcm) > 0 {
				bytes := media.PCMFloat32ToInt16LE(pcm)
				sendPayload(base64.StdEncoding.EncodeToString(bytes))
				lastAudioReceivedAt = time.Now()
			}
		case <-ticker.C:
			if !g.ready.Load() || !g.enabled.Load() || !g.callActive.Load() || g.closed.Load() {
				continue
			}
			if !lastAudioReceivedAt.IsZero() && time.Since(lastAudioReceivedAt) < 75*time.Millisecond {
				continue
			}
			sendPayload(silenceB64)
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

	g.prewarmMu.Lock()
	buffered := g.prewarmedGreeting
	g.prewarmedGreeting = nil
	g.prewarmMu.Unlock()

	if g.FeedAudio != nil && len(buffered) > 0 {
		g.log.Info("⚡ Playing pre-synthesized greeting at T=0 pickup", "samples", len(buffered), "duration_ms", len(buffered)/16)
		go func() {
			time.Sleep(150 * time.Millisecond)
			g.FeedAudio(buffered)
		}()
	}
}

func (g *GeminiLiveAgent) sendGreetingFrame() {
	g.log.Info("pre-synthesizing initial greeting")
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
							"text": fmt.Sprintf("Say strictly ONLY the following message and finish your turn. Do NOT say anything else: '%s'", g.greetingMessage),
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
	g.hasSpokenWithPeer.Store(true)
	g.latMu.Lock()
	g.lastCallerAudioAt = time.Now()
	g.loggedLatencyThisTurn = false
	g.latMu.Unlock()

	select {
	case g.audioInCh <- pcm16:
	default:
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

func (g *GeminiLiveAgent) HasSpokenWithPeer() bool {
	return g.hasSpokenWithPeer.Load()
}

func (g *GeminiLiveAgent) Close() {
	if g.closed.Swap(true) {
		return
	}
	g.cancel()
	g.inFlightToolsMu.Lock()
	for id, cancelFn := range g.inFlightTools {
		cancelFn()
		delete(g.inFlightTools, id)
	}
	g.inFlightToolsMu.Unlock()
	g.wsMu.Lock()
	if g.ws != nil {
		_ = g.ws.Close(websocket.StatusNormalClosure, "call ended")
	}
	g.wsMu.Unlock()
}
