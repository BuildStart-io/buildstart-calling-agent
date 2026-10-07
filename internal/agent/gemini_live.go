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

// GeminiLiveAgent connects directly to Google Gemini Multimodal Live API over WebSocket.
// 100% pure bidirectional streaming with models/gemini-3.1-flash-live-preview.
type GeminiLiveAgent struct {
	apiKey          string
	model           string
	voice           string
	systemPrompt    string
	greetingMessage string
	log             *slog.Logger

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
	interruptedThisTurn atomic.Bool
	isModelSpeaking     atomic.Bool
	hasSpokenWithPeer   atomic.Bool
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
		greetingMessage = "හෙලෝ"
	}

	ctx, cancel := context.WithCancel(context.Background())
	g := &GeminiLiveAgent{
		apiKey:          apiKey,
		model:           model,
		voice:           voice,
		systemPrompt:    systemPrompt,
		greetingMessage: greetingMessage,
		log:             log.With("component", "gemini_live"),
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
						"text": g.systemPrompt,
					},
				},
			},
			"inputAudioTranscription":  map[string]any{},
			"outputAudioTranscription": map[string]any{},
			// Barge-in debounce: Gemini only commits start-of-speech (and therefore
			// sends `interrupted`, which flushes the agent's buffered audio) after the
			// caller has spoken continuously for bargeInMinSpeechMs. Short sounds such
			// as "mm", "ah", breaths or background noise no longer cut the agent off
			// mid-sentence. Silence/end-of-turn detection is left at Gemini defaults so
			// response latency is unchanged.
			"realtimeInputConfig": map[string]any{
				"automaticActivityDetection": map[string]any{
					"prefixPaddingMs": bargeInMinSpeechMs,
				},
			},
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
			VoiceActivity *struct {
				Type        string `json:"type"`
				AudioOffset string `json:"audioOffset"`
			} `json:"voiceActivity"`
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
						// Accumulate the entire pre-warmed greeting so custom
						// multi-sentence greetings play fully upon pickup.
						g.prewarmedGreeting = append(g.prewarmedGreeting, mastered...)
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
			g.isModelSpeaking.Store(false)
			g.interruptedThisTurn.Store(false)
			g.latMu.Lock()
			g.loggedLatencyThisTurn = false
			g.latMu.Unlock()
		}
	}
}

// writeAudioLoop streams incoming 16 kHz audio directly to Gemini Live.
//
// Root-cause fix for broken voice recognition / silence issue:
// Previously, writeAudioLoop buffered audio looking for 640 samples, while a 40ms ticker
// ran concurrently and padded partial buffers with zeros or injected silence. Because WhatsApp
// delivers ~60ms audio frames (960 samples), the ticker repeatedly sliced caller speech in half,
// padded it with 20ms of zeros, and injected silence packets right in the middle of words.
// This severely corrupted speech waveforms, causing Gemini to either hallucinate random languages
// or fail VAD detection completely (leaving the bot silent).
//
// The fix:
// 1. Every incoming audio frame from WhatsApp is forwarded immediately and contiguously to Gemini.
// 2. The silence ticker only injects silence when WhatsApp is in DTX silence mode (i.e. no audio received for >= 75ms).
// 3. Contiguous speech is NEVER zero-padded, truncated, or interleaved with silence.
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
			// WhatsApp sends ~60ms packets during speech.
			// Only inject DTX silence if no audio has arrived for at least 75ms.
			if !lastAudioReceivedAt.IsZero() && time.Since(lastAudioReceivedAt) < 75*time.Millisecond {
				continue
			}
			// DTX silence mode: advance Gemini's audio clock in real time
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

func (g *GeminiLiveAgent) HasSpokenWithPeer() bool {
	return g.hasSpokenWithPeer.Load()
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
