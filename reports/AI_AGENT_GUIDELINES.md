# 🤖 WaCalls AI Voice Agent: Architecture & Operational Guidelines

This guide documents the architecture, lifecycle, configurations, and troubleshooting procedures for the autonomous AI Voice Agent in WaCalls.

---

## 📋 Table of Contents
1. [System Architecture](#system-architecture)
2. [End-to-End Call Lifecycle](#end-to-end-call-lifecycle)
3. [Autonomous Auto-Callback & WhatsApp Messaging](#autonomous-auto-callback--whatsapp-messaging)
4. [Voice Processing Pipeline](#voice-processing-pipeline)
   - [Voice Activity Detection (VAD)](#1-voice-activity-detection-vad)
   - [Multimodal Speech Understanding (OpenRouter / Gemini)](#2-multimodal-speech-understanding)
   - [High-Fidelity Text-to-Speech (TTS)](#3-high-fidelity-neural-tts)
5. [Configuration & Environment Variables](#configuration--environment-variables)
6. [Dashboard & Call Logging Audit Trail](#dashboard--call-logging-audit-trail)
7. [Troubleshooting & FAQ](#troubleshooting--faq)

---

## 🏛️ System Architecture

```
                                    ┌────────────────────────────┐
                                    │    WhatsApp VoIP Server    │
                                    │ (SRTP Audio / WebRTC Mesh) │
                                    └──────────────┬─────────────┘
                                                   │
                                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          WaCalls Go Server (cmd/server)                     │
│                                                                             │
│   ┌─────────────────────┐     ┌────────────────┐     ┌──────────────────┐   │
│   │ CallManager (VoIP)  │◄───►│ Bridge (16kHz) │◄───►│  AIAgent Engine  │   │
│   └─────────────────────┘     └────────────────┘     └────────┬─────────┘   │
│                                                               │             │
│            ┌─────────────────┬────────────────────────────────┼───────────┐ │
│            ▼                 ▼                                ▼           ▼ │
│      ┌───────────┐    ┌─────────────┐                  ┌──────────┐ ┌─────┴─┐
│      │    VAD    │    │ Pre-warmer  │                  │  Broker  │ │SQLite │
│      │ (0.016RMS)│    │(GreetingTTS)│                  │(SSE Hub) │ │  DB   │
│      └─────┬─────┘    └─────────────┘                  └────┬─────┘ └───────┘
│            │                                                │               │
└────────────┼────────────────────────────────────────────────┼───────────────┘
             ▼                                                ▼
┌─────────────────────────┐                     ┌───────────────────────────┐
│  AI Intelligence Cloud  │                     │ React Web Dashboard (:8080)│
│  - OpenRouter API       │                     │ - Live Audio Waveform     │
│  - Gemini Multimodal    │                     │ - Caller + AI Transcripts │
│  - Local Piper Neural   │                     │ - Auto-Callback Trigger   │
│    Sinhala/English TTS  │                     │ - Audit Timeline Logs     │
└─────────────────────────┘                     └───────────────────────────┘
```

---

## 🔄 End-to-End Call Lifecycle

### 1. Pre-warm & Ringing Phase
* When an inbound call arrives or an outbound callback starts, the agent triggers `PrewarmGreeting()`.
* The TTS engine synthesizes the initial welcome message into float32 PCM in memory while the call is ringing.
* **Latency Benefit:** Eliminates the 2–3 second delay that callers would otherwise experience waiting for AI synthesis after picking up.

### 2. Live Audio Streaming
* When the call transitions to `ACTIVE`, the pre-warmed greeting streams over WhatsApp's relay immediately via 60ms 16kHz frames.
* When the AI finishes speaking, a 400ms echo cooldown ensures playback does not bleed back into the receiver.

### 3. Conversational Turns
* The caller's voice is processed in real time by the Voice Activity Detector (`VAD`).
* When the caller pauses for ≥ 600ms, the speech buffer is converted to WAV and forwarded to the intelligence pipeline.
* The model returns both the **transcript** of what the caller said and the **conversational reply**.
* The reply is spoken via TTS, and the dashboard timeline records both messages.

---

## ⚡ Autonomous Auto-Callback & WhatsApp Messaging

### The Multi-Device Companion Reality
WhatsApp multi-device accounts operate with a primary mobile device and secondary companion clients. When someone calls the account:
* If the primary phone is **offline**, WhatsApp emits an offline `*events.CallOfferNotice` or missed-call notification to companion devices.
* If a caller rings and hangs up before the voice handshake completes, the call is classified as **Unanswered / Missed**.

### Autonomous Response Flow

```
Inbound Call Arrives
        │
        ├──► [Did caller talk & complete conversation?]
        │           │
        │           ├── YES ──► Logged as "Completed (AI Voice Handled)" (No callback)
        │           │
        │           └── NO  ──► Caller hung up / Primary device offline
        │                           │
        │                           ├── 1. WhatsApp Text Message sent to caller
        │                           └── 2. Instant Outbound AI Auto-Callback placed
```

1. **Immediate WhatsApp Notification:**
   Sends a WhatsApp message directly to the caller:
   > *"ආයුබෝවන්! ඔබ අප අමතන්නට උත්සාහ කළ බව දුටුවෙමි. අපගේ AI හඬ සහායකයා මේ මොහොතේම ඔබව නැවත අමතනු ඇත."*
2. **Instant AI Auto-Callback:**
   WaCalls initiates an outbound voice call to the caller's phone number. When the user picks up, the AI voice assistant speaks directly with the customer.

---

## 🎙️ Voice Processing Pipeline

### 1. Voice Activity Detection (VAD)
Located in [`internal/agent/vad.go`](file:///Users/yasiru/Documents/CALL/internal/agent/vad.go):
* **RMS Energy Threshold:** `0.016` (calibrated for standard smartphone microphone input over WhatsApp Opus).
* **Onset Detection:** 2 consecutive frames (120ms) of energy above threshold.
* **Silence Hangover:** 600ms of quiet triggers end-of-turn.
* **Minimum Speech Window:** 3200 samples (200ms) to allow short replies like "ඔව්" or "හරි".

### 2. Multimodal Speech Understanding
Located in [`internal/agent/openrouter.go`](file:///Users/yasiru/Documents/CALL/internal/agent/openrouter.go):
* **Provider:** OpenRouter (`https://openrouter.ai/api/v1/chat/completions`)
* **Default Model:** `google/gemini-2.5-flash`
* **Input Modality:** `input_audio` WAV base64 payload.
* **Extraction:** Structured JSON extraction returning both:
  * `transcription`: Caller's spoken words.
  * `reply`: Colloquial, natural Sinhala response (1–2 sentences).

### 3. High-Fidelity Neural TTS
Located in [`scripts/tts_server.py`](file:///Users/yasiru/Documents/CALL/scripts/tts_server.py) and [`internal/agent/tts.go`](file:///Users/yasiru/Documents/CALL/internal/agent/tts.go):
* **Primary Engine:** Dialog Axiata & University of Moratuwa Nipunika Studio VITS (`dialog-nipunika`, 22,050 Hz, 210,000 steps) running on port `5050`.
* **Offline Neural Fallbacks:** OpenSLR 30 Piper ONNX (`piper-openslr`, 22.05 kHz) and Ashoka Weerawardhana Studio Voice (`piper-ashoka`, 16 kHz).
* **Cloud Fallback Engines:** Microsoft Edge Neural (`si-LK-ThiliniNeural` / `si-LK-SameeraNeural`), Google Cloud TTS, or Azure Speech.
* **Phonetic & Number Normalization:** Automatic expansion of numbers, dates, and colloquial speech into spoken Sinhala words.

---

## ⚙️ Configuration & Environment Variables

### Command Line Flags
| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `-addr` | string | `:8080` | HTTP & WebSocket listen address |
| `-db` | string | `wacalls.db` | SQLite database path |
| `-ai-agent` | bool | `true` | Enable or disable the AI Voice Agent engine |
| `-ai-auto-answer` | bool | `true` | Automatically answer inbound calls with the AI Voice Agent |
| `-ai-model` | string | `google/gemini-2.5-flash` | OpenRouter model identifier |
| `-openrouter-key` | string | `$OPENROUTER_API_KEY` | OpenRouter API Key |
| `-ai-voice` | string | `si-LK-ThiliniNeural` | Default voice identifier |
| `-ai-prompt` | string | `""` | Custom system prompt override |

### Recommended Startup Command
```bash
./server \
  -addr :8080 \
  -openrouter-key "your-openrouter-key" \
  -ai-model "google/gemini-2.5-flash" \
  -ai-voice "si-LK-ThiliniNeural" \
  -ai-agent=true \
  -ai-auto-answer=true
```

---

## 📊 Dashboard & Call Logging Audit Trail

The Web Dashboard ([`http://localhost:8080`](http://localhost:8080)) provides a complete audit trail of every call:

### Card Snippet Preview
Each call card displays:
* **Call Direction Badge:** `Inbound`, `Auto-Callback`, or `Outbound`.
* **Caller Speech:** `Caller: "what the user asked"` (highlighted).
* **AI Reply:** `AI: "what the voice assistant answered"`.
* **Duration & Timestamp:** e.g., `18s • 10:45 AM`.

### Call Detail Modal (`CallDetailDialog`)
Clicking on any call opens a dialog with three detailed tabs:
1. **Conversation Transcripts:** Chat-style bubble view displaying full multi-turn dialog between Caller and AI.
2. **Event Timeline:** Chronological log of signaling events (Offer received, WebRTC relay connected, TTS synthesized, call termination reason).
3. **Technical Info:** Call ID, session JID, media relay IP, and audio codec information.

---

## 🛠️ Troubleshooting & FAQ

### Q: Why did Safari say "Can't connect to server"?
* Make sure you are connecting to port **`8080`** (`http://localhost:8080`), not port 3000.

### Q: The server started but localhost didn't load (stuck silently)
* **Root Cause:** A database deadlock where nested queries occurred inside an open `sql.Rows` loop with `MaxOpenConns=1`.
* **Resolution:** Ensure `rows.Close()` is called before secondary database lookups. This is fixed in [`cmd/server/sessionstore.go`](file:///Users/yasiru/Documents/CALL/cmd/server/sessionstore.go).

### Q: Why didn't an inbound call connect to audio on the caller's phone?
* WhatsApp companion web clients have limitations receiving 1:1 calls when the caller's phone does not establish direct P2P. 
* The system is designed to handle this automatically: when an incoming call does not establish audio, WaCalls immediately logs it as **Unanswered**, sends a WhatsApp notification, and triggers the **instant Auto-Callback** to the caller.
