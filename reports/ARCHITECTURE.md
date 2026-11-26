# BuildStart Calling Agent — System Architecture & Technical Specification

> **Version:** 2.0 (Production)  
> **Target Platform:** Linux VPS (Docker + Native Systemd Service)  
> **Language & Frameworks:** Go 1.26+ (Native Backend), TypeScript & React 18 (Management Dashboard)  
> **Core Integrations:** WhatsApp VoIP Protocol (`whatsmeow`), Google Gemini Multimodal Live API, PostgreSQL (`whatsapp_infra`), MinIO S3 Object Storage

---

## 1. Executive Summary & System Vision

The **BuildStart Calling Agent** is a carrier-grade, native VoIP calling server and autonomous AI voice consultant engineered specifically for WhatsApp. Unlike legacy chatbots or text-to-speech IVR systems that introduce noticeable latency (1.5–3+ seconds), this server delivers **human-grade, natural real-time voice conversations (<500ms turnaround)** directly through standard WhatsApp voice calls.

```
                                  ┌──────────────────────────────────────────────┐
                                  │           WhatsApp Cloud & Relay             │
                                  └──────▲───────────────────────────────▲───────┘
                                         │ Signaling (Node Stanzas)      │ Media (SRTP / STUN)
                                         │                               │
                                  ┌──────▼───────────────────────────────▼───────┐
                                  │       BuildStart Calling Agent Server        │
                                  │  ┌────────────────────────────────────────┐  │
                                  │  │   whatsmeow WhatsApp Protocol Core     │  │
                                  │  ├────────────────────────────────────────┤  │
                                  │  │   VoIP State Machine & SCTP Transport  │  │
                                  │  ├────────────────────────────────────────┤  │
                                  │  │   Pure Go MLow & Opus Audio Codec      │  │
                                  │  ├────────────────────────────────────────┤  │
                                  │  │   Voice Mastering DSP & Resampler      │  │
                                  │  └───────────────────▲────────────────────┘  │
                                  └──────────────────────┼───────────────────────┘
                                                         │
                        Bidirectional PCM Audio Stream   │  WebSocket (BidiGenerateContent)
                                                         ▼
                                  ┌──────────────────────────────────────────────┐
                                  │        Google Gemini Live 3.1 Flash          │
                                  │     (Zero-latency Multimodal Voice AI)       │
                                  └──────────────────────────────────────────────┘
```

### Key Architectural Capabilities
1. **Direct WhatsApp VoIP Stack**: Implements WhatsApp's binary XML signaling protocol and proprietary SCTP/TURN data-channel relay transport on port 3480 without relying on Android emulators or physical mobile screens.
2. **Native Go MLow Codec**: Features a pure Go implementation of Meta's *MLow* (Meta Low Bitrate Audio Codec), including CELP synthesis, LPC analysis, and range decoding, eliminating CGo and dynamic library dependencies.
3. **Sub-Second Voice Turnaround**: Integrates with Google's Gemini Multimodal Live API (`models/gemini-3.1-flash-live-preview`) over WebSockets using zero-latency thinking configurations.
4. **Pre-Warmed Call Pickup**: Synthesizes the AI voice greeting *during the ringing phase* so audio plays at $T=0$ when the user answers.
5. **Intelligent Barge-In & Acoustic Gating**: Debounces user interruptions (300ms speech padding) and enforces a 350ms acoustic echo cooldown to prevent synthetic audio feedback loops.
6. **Autonomous Lead Intelligence**: Extracts structured CRM lead dossiers via `gemini-2.5-flash` after each call and dispatches personalized WhatsApp follow-up messages automatically.
7. **Operator Live-Bridge**: Enables human operators to silently monitor live audio or take over calls in real-time via WebRTC data channels directly from a web browser.

---

## 2. High-Level System Architecture

The calling agent is structured as a modular pipeline separating protocol signaling, media transport, AI orchestration, and persistent storage:

```mermaid
flowchart TD
    subgraph WhatsApp["WhatsApp Network"]
        WAClient["WhatsApp Mobile App"]
        WARelay["WhatsApp Media Relays (Port 3480)"]
    end

    subgraph Server["BuildStart Calling Server (Go)"]
        subgraph Signaling["Signaling & Protocol"]
            WACli["whatsmeow Client"]
            Sig["Signaling Parser & Builder"]
            Socket["VoIP Socket Adapter"]
        end

        subgraph CallMgmt["Call Management & Transport"]
            CM["CallManager State Machine"]
            SCTP["SCTP Relay Transport"]
            SRTP["SRTP AES-128 Engine"]
        end

        subgraph MediaEngine["Audio Processing Engine"]
            MLOW["Native MLow Codec"]
            DSP["Voice Mastering Engine (EQ + Compressor)"]
            Resample["24kHz <-> 16kHz Resampler"]
            Rec["Stereo WAV Call Recorder"]
        end

        subgraph Agent["AI Intelligence Core"]
            GeminiLive["Gemini Live WebSocket Agent"]
            Dossier["Lead Dossier Extractor"]
            Sched["Autonomous Call Scheduler"]
        end

        subgraph API["Web & API Layer"]
            HTTP["REST & WebSocket API"]
            SSE["Server-Sent Events Broker"]
            WebRTC["Browser WebRTC Bridge"]
        end
    end

    subgraph External["External Cloud Services"]
        GeminiAPI["Google Gemini Live API"]
        PG["PostgreSQL (whatsapp_infra)"]
        MinIO["MinIO S3 Storage"]
    end

    WAClient <-->|Call Stanzas| WACli
    WACli --> Sig
    Sig --> Socket
    Socket --> CM
    CM <--> SCTP
    SCTP <-->|SRTP over DataChannel| WARelay
    WARelay <--> WAClient

    SCTP --> SRTP
    SRTP --> MLOW
    MLOW <--> Resample
    Resample <--> DSP
    DSP <--> GeminiLive
    GeminiLive <-->|Bidi WebSockets| GeminiAPI

    CM --> Rec
    Rec -->|Upload Stereo WAV| MinIO
    CM --> Dossier
    Dossier -->|Analyze Transcripts| GeminiAPI
    Dossier -->|Save Logs| PG
    Sched -->|Poll Scheduled Calls| PG
    Sched --> CM

    CM --> SSE
    SSE -->|Live Events| HTTP
    WebRTC <--> CM
```

---

## 3. Directory Structure & Module Breakdown

The repository is organized following clean Go conventions with clear separation between server command executables, domain libraries, and frontend clients:

```
buildstart-calling-agent/
├── cmd/server/                 # Main Server Executable & HTTP Daemon
│   ├── main.go                 # Bootstrapper, flag parsing, companion spoofing, graceful shutdown
│   ├── server.go               # Server struct, DB connection pool, service initialization
│   ├── sessionmanager.go       # Multi-tenant WhatsApp session lifecycle manager
│   ├── session.go              # Session event multiplexer, call wiring, auto-answer, callbacks
│   ├── sessionstore.go         # PostgreSQL data access layer (whatsapp_infra schema)
│   ├── callregistry.go         # Thread-safe in-memory active call registry
│   ├── callrouting.go          # Incoming/outgoing call routing & phone number normalizers
│   ├── broker.go               # Real-time SSE event broker for web dashboards
│   ├── httpapi.go              # REST, WebSocket (/audio), WebRTC (/webrtc) HTTP endpoints
│   ├── recorder.go             # Non-blocking stereo WAV audio recorder & MinIO S3 uploader
│   ├── crm_lead.go             # Gemini Flash CRM Lead Dossier generator & WhatsApp follow-ups
│   ├── scheduler.go            # Background cron processor for scheduled outbound calls
│   ├── bridge.go               # Pion WebRTC data channel adapter for browser audio
│   └── whatsapp.go             # Binary node extraction and helper utilities
│
├── internal/
│   ├── agent/                  # AI Orchestration
│   │   ├── gemini_live.go      # Google Gemini 3.1 Flash Live Bidi streaming WebSocket client
│   │   └── gemini_live_test.go # Mocked unit tests for Live API event handling
│   │
│   ├── voip/                   # WhatsApp VoIP Core
│   │   ├── core/               # Shared domain interfaces & payload definitions
│   │   │   ├── types.go        # CallState, PayloadTypes (120, 121, 122), SrtpKeyingMaterial
│   │   │   └── voipsocket.go   # VoipSocket interface definition
│   │   │
│   │   ├── signaling/          # WhatsApp VoIP Binary XML Protocol
│   │   │   ├── signaling_build.go # Stanza builders: offer, accept, reject, relaylatency, transport
│   │   │   ├── signaling_parse.go # Stanza parsers: attributes, participants, call-id
│   │   │   ├── callkey.go      # Signal-protocol encrypted callKey marshaling & unpad
│   │   │   └── relayack.go     # Relay token, auth-token, and endpoint parser
│   │   │
│   │   ├── transport/          # Network Relays & STUN
│   │   │   ├── sctprelay.go    # SCTP DataChannel relay manager (wa-web-call on port 3480)
│   │   │   ├── stun.go         # STUN binding, WhatsApp ping (0x0801), and pong builder
│   │   │   └── subscriptions.go# SSRC subscription message encoder
│   │   │
│   │   ├── call/               # Call State Machine & Audio Loops
│   │   │   ├── callmanager.go  # Core CallManager coordinator & transitions
│   │   │   ├── callmanager_media.go # RTP packet loop, payload filtering, silence keepalives
│   │   │   ├── callmanager_relay.go # Relay failover, endpoint selection, latency reporting
│   │   │   ├── callmanager_signaling.go # Signaling event handlers (accept, transport, ack)
│   │   │   ├── callmanager_srtp.go  # SRTP session initialization & key derivation
│   │   │   └── callstate.go    # Formal state machine transitions & validations
│   │   │
│   │   ├── media/              # Audio Codecs, DSP, and Packetization
│   │   │   ├── dsp.go          # Voice mastering: Biquad filters, soft-knee compressor, limiter
│   │   │   ├── pcm.go          # Float32 <-> Int16 LE transformations, 24k->16k downsampler
│   │   │   ├── resample.go     # Frame normalization & sample rate conversions
│   │   │   ├── rtp.go          # RTP packetizer & header encoding/decoding
│   │   │   ├── srtp.go         # AES-128-ICM encryption, HMAC-SHA1 auth, ROC counter
│   │   │   ├── ssrc.go         # Deterministic SSRC generator (callID + JID)
│   │   │   ├── codec.go        # Codec interface definition
│   │   │   ├── mlow_codec_native.go # Pure Go native MLow codec wrapper
│   │   │   └── mlow/           # Native Meta MLow Codec Engine (Pure Go)
│   │   │       ├── encoder.go  # Voicing classifier, range coder, entropy encoder
│   │   │       ├── decoder.go  # TOC parser, RED depacker, synthesis pipeline
│   │   │       ├── celp_enc.go # Code-Excited Linear Prediction excitation search
│   │   │       ├── celpdec.go  # CELP decoding & codebook synthesis
│   │   │       ├── lpc.go      # Linear Predictive Coding autocorrelation & Levinson-Durbin
│   │   │       ├── lsf.go      # Line Spectral Frequencies transformation
│   │   │       ├── lsf_quant.go# Multistage vector quantization for LSFs
│   │   │       ├── pitch.go    # Pitch lag detector & open-loop pitch estimator
│   │   │       ├── postfilter.go # Formant & harmonic postfilter for speech clarity
│   │   │       └── vad.go      # Voice Activity Detector for MLow frames
│   │   │
│   │   └── wanode/             # WhatsApp Binary Node Helpers
│   │       ├── nodeutil.go     # Child navigation, attribute extraction (int, string, bytes)
│   │       └── jid.go          # JID cleaner, parser, phone number formatter
│   │
│   └── wa/                     # whatsmeow Bridge
│       └── socket.go           # VoipSocket implementation wrapping DangerousInternals
│
├── client/                     # Operator React Management Dashboard
│   ├── src/
│   │   ├── App.tsx             # Main shell & routing
│   │   ├── pages/CallsPage.tsx # Call dialer, active call cards, audio meter, logs
│   │   ├── lib/webrtc.ts       # WebRTC browser call bridge using Web Audio API
│   │   ├── lib/pcm.ts          # Browser PCM conversion helpers
│   │   └── lib/event-stream.ts # SSE client for real-time dashboard updates
│   └── public/worklets/        # AudioWorklet processors (capture & playback)
│
└── scripts/
    └── e2e_call_eval.go        # Live multi-turn evaluation & latency benchmarking harness
```

---

## 4. WhatsApp VoIP Protocol Deep Dive

Connecting to WhatsApp VoIP without official client SDKs requires reverse-engineering and implementing four interconnected layers:

```mermaid
flowchart LR
    L1["Layer 1: Identity & Pairing<br/>(Companion Registration)"] --> L2["Layer 2: Signaling<br/>(Binary XML over WebSocket)"]
    L2 --> L3["Layer 3: E2E Key Exchange<br/>(Signal Protocol)"]
    L3 --> L4["Layer 4: Media Relay & Transport<br/>(SCTP / TURN Port 3480)"]
```

### Layer 1: Identity & Companion Device Spoofing
WhatsApp restricts VoIP calls to approved platforms (Desktop macOS/Windows and Mobile). The server emulates a native macOS desktop companion during initialization in `cmd/server/main.go`:
```go
store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_DESKTOP.Enum()
store.DeviceProps.Os = proto.String("Mac OS")
store.DeviceProps.RequireFullSync = proto.Bool(false)
store.SetOSInfo("Mac OS", [3]uint32{14, 5, 0})
```
This enables the server to receive `<call>` stanzas that are otherwise suppressed for web clients.

### Layer 2: VoIP Signaling Protocol
WhatsApp signaling is conveyed via binary XML nodes exchanged over the primary WhatsApp WebSocket (`web.whatsapp.com/ws/chat`):

| Stanza Tag | Direction | Purpose | Key Attributes & Children |
|---|---|---|---|
| `<offer>` | Inbound / Outbound | Initiates a call session | `call-id`, `call-creator`, `<audio enc="opus" rate="16000">`, `<destination>`, `<encopt>` |
| `<accept>` | Bidirectional | Accepts ringing call | `call-id`, `call-creator`, `<audio>`, `<enc>` (Signal-encrypted call key) |
| `<transport>` | Bidirectional | Informs peer of transport medium | `transport-message-type="1"`, `p2p-cand-round="1"`, `<net medium="2">` |
| `<relaylatency>` | Bidirectional | Reports measured RTT to WhatsApp relays | `<te relay_name="..." latency="...">` |
| `<terminate>` | Bidirectional | Ends active or ringing call | `call-id`, `call-creator`, `reason="user_ended"` or `"declined"` |

### Layer 3: End-to-End Cryptography (Call Key & SRTP)
WhatsApp calls use a hybrid encryption model:
1. **Call Key Distribution**: The call creator generates a cryptographically random 32-byte master key (`callKey`).
2. **Signal Encryption**: The `callKey` is wrapped into a protobuf message (`waE2E.Call{CallKey}`) and encrypted for each target device using the Signal Protocol (via `DangerousInternals().EncryptMessageForDevices`).
3. **Key Derivation (RFC 3711)**:
   The 32-byte `callKey` is split into:
   - `MasterKey` = 16 bytes (`callKey[0:16]`)
   - `MasterSalt` = 14 bytes (`callKey[16:30]`)
   Using AES-128 in counter mode with specific label constants (`0x00` for Encryption, `0x01` for Authentication, `0x02` for Salt), the SRTP engine derives:
   - 16-byte Session Encryption Key
   - 20-byte HMAC-SHA1 Session Authentication Key
   - 14-byte Session Salt Key
4. **SRTP Packet Protection**:
   Each RTP packet is encrypted using AES-128-ICM (Counter Mode) with an initialization vector (IV) generated from the Session Salt, SSRC, and 48-bit Packet Index (`ROC * 65536 + SequenceNumber`). A 4-byte HMAC-SHA1 authentication tag is appended to the packet.

### Layer 4: Proprietary SCTP Relay Data Channel
Unlike standard SIP or WebRTC VoIP which relies on UDP STUN/TURN holes, WhatsApp Web VoIP channels media through dedicated relay servers on port 3480:
- The server opens a WebRTC `PeerConnection` and creates a data channel named **`wa-web-call`**.
- It captures the local SDP offer and synthesizes a remote SDP answer pointing directly to the IP and port of the assigned WhatsApp relay (`modifySdpForRelay`).
- Once the data channel opens, the server sends a **STUN Registration** containing WhatsApp-specific authentication tokens received in the `<relay>` ack.
- WhatsApp sends periodic ping packets (`0x0801`), which the server immediately answers with a synthesized pong (`BuildWhatsAppPong`) to prevent connection timeouts.
- Both audio RTP packets and RTCP control packets are multiplexed over this single data channel.

#### Dynamic Payload Types Handled:
- **`PT=120`**: Standard WhatsApp Opus (16 kHz).
- **`PT=121`**: Opus alternate / Redundant audio (RED).
- **`PT=122`**: Meta MLow (Meta Low Bitrate Audio Codec, widely used by modern WhatsApp Android/iOS clients).
- **`PT=200..208`**: Multiplexed RTCP control packets (SR, RR, SDES, BYE) — filtered and ignored by the audio pipeline.

---

## 5. Native Meta MLow Audio Codec Engine

To ensure 100% audio compatibility without requiring external C libraries (like `libopus` or `libmlow.so`), the repository includes a complete, pure Go implementation of Meta's **MLow** speech codec in `internal/voip/media/mlow/`.

```mermaid
flowchart TD
    subgraph Encoder["MLow Encoder Pipeline (60ms PCM -> Wire Payload)"]
        PCMIn["60ms Input PCM (960 samples @ 16kHz)"] --> VAD["VAD & Spectral Analysis"]
        VAD --> LPC["LPC Autocorrelation (Order 16)"]
        LPC --> LSF["LSF Vector Quantization (3 Sub-vectors)"]
        LSF --> Pitch["Open-Loop Pitch Search (Lags 34..231)"]
        Pitch --> CELP["CELP Excitation Codebook Search"]
        CELP --> RangeEnc["Arithmetic Range Coder"]
        RangeEnc --> Wire["MLow Packet (TOC + Bitstream)"]
    end

    subgraph Decoder["MLow Decoder Pipeline (Wire Payload -> 60ms PCM)"]
        WireIn["Wire Payload"] --> TOC["TOC Header Parser & RED Depacker"]
        TOC --> RangeDec["Arithmetic Range Decoder"]
        RangeDec --> QLSF["Inverse LSF Quantizer -> LPC Synthesis Filter"]
        RangeDec --> QCELP["CELP Codebook Reconstruction"]
        QCELP & QLSF --> Post["Formant & Harmonic Postfilter"]
        Post --> PCMOut["Clean 60ms PCM (960 samples @ 16kHz)"]
    end
```

### MLow Core Algorithms:
1. **TOC (Table of Contents)**: Evaluates the first byte of each packet to detect frame size (20ms, 40ms, 60ms), voiced/unvoiced classification, and Silence Insertion Descriptor (SID) frames.
2. **CELP (Code-Excited Linear Prediction)**: Models vocal tract resonance using linear predictive filters driven by an algebraic codebook.
3. **Range Coder**: Compresses excitation indices and pitch gains using fractional-bit arithmetic entropy coding.
4. **Adaptive Postfiltering**: Suppresses noise between formant frequencies to maximize synthetic speech intelligibility over phone speakers.

---

## 6. Gemini Multimodal Live API Integration

The server integrates with Google Gemini Live using the bidirectional WebSocket streaming protocol:

```
wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=...
```

```mermaid
sequenceDiagram
    autonumber
    actor Caller as WhatsApp Caller
    participant Server as Calling Agent Server
    participant Gemini as Gemini Live 3.1 Flash

    Note over Caller, Server: Inbound Call Ringing
    Caller->>Server: <offer> Stanza Received
    Server->>Gemini: Dial WebSocket & Send Setup Frame (Thinking Budget=0, Voice=Aoede)
    Gemini-->>Server: SetupComplete
    Server->>Gemini: Send Greeting Prompt ("Say: Hello, welcome to...")
    Gemini-->>Server: 24kHz Audio Chunks (Pre-synthesizing greeting)
    Note over Server: Server buffers pre-warmed audio in RAM

    Caller->>Server: 1.2s Ring Delay Elapsed
    Server->>Caller: Send <accept> & Establish Media Relay
    Server->>Caller: Flush Pre-warmed Audio at T=0 (Instant Voice!)

    loop Continuous Bidirectional Conversation
        Caller->>Server: Spoken Audio (16kHz PCM Frames)
        Server->>Gemini: realtimeInput.audio (16kHz PCM Base64)
        Gemini-->>Server: serverContent.modelTurn (24kHz Audio)
        Server->>Server: Downsample 24k -> 16k & Apply Voice Mastering DSP
        Server->>Caller: Transmit 60ms RTP Frames (PT=122 MLow)
    end

    opt Caller Interrupts Agent (Barge-In)
        Caller->>Server: Caller begins speaking mid-sentence (>300ms)
        Server->>Gemini: Caller audio streamed
        Gemini-->>Server: serverContent.interrupted = true
        Server->>Server: CallManager.FlushCapturedPCM() (Instantly cuts agent audio)
    end
```

### Critical Latency & Reliability Optimizations:

#### 1. Zero-Latency Pre-Warming at T=0
Standard voice agents wait until the call connects before sending the prompt to the AI, creating an awkward 1.5–2.5 second silence.
The Calling Agent connects to Gemini Live and requests greeting audio **immediately when the phone starts ringing**. By the time the 1.2-second ring delay elapses and the call answers, the entire greeting is already decoded in memory and begins streaming to the caller at millisecond zero.

#### 2. Barge-In Debouncing (`bargeInMinSpeechMs = 300`)
To prevent random ambient noise, caller throat-clearing, or quick backchannel affirmations ("mm-hmm", "okay") from abruptly cutting off the AI, the setup frame configures:
```json
"realtimeInputConfig": {
  "automaticActivityDetection": {
    "prefixPaddingMs": 300
  }
}
```
Gemini only issues an `interrupted` event after 300ms of sustained speech.

#### 3. Real-Time DTX Silence Keepalive
WhatsApp voice uses Discontinuous Transmission (DTX), sending zero audio packets during silence.
Previously, injecting silent frames broke words in half because WhatsApp frames are 60ms (960 samples) while tickers ran at 40ms.
The current architecture in `writeAudioLoop()` uses an intelligent DTX silence detector:
- **Speech Mode**: Incoming caller audio frames are forwarded immediately and contiguously to Gemini without alteration.
- **DTX Silence Mode**: If no audio packet has arrived for $\ge 75\text{ ms}$, the ticker injects 40ms silence frames to keep Gemini's internal audio clock advancing in real time.

#### 4. Session Resumption & Reconnection
Gemini Live connections automatically receive `sessionResumptionUpdate` tokens containing a `newHandle`. If Gemini sends a `goAway` frame, the agent reconnects using the cached handle, resuming the ongoing sales conversation without losing context.

---

## 7. Voice Mastering DSP & Resampling Engine

Synthetic voice models often sound unnatural or harsh over telephone speakers. The audio pipeline in `internal/voip/media/dsp.go` processes all outbound AI audio through a studio-grade mastering chain before transmission:

```mermaid
flowchart LR
    In["24kHz Gemini Audio"] --> Resample["3:2 Fractional Downsampler<br/>(24kHz -> 16kHz)"]
    Resample --> LowShelf["Low-Shelf Filter<br/>(+2.5dB @ 200Hz, Q=0.707)"]
    LowShelf --> HighShelf["High-Shelf De-Esser<br/>(-3.0dB @ 6500Hz, Q=0.707)"]
    HighShelf --> Comp["Soft-Knee Dynamic Compressor<br/>(Threshold -14dB, Ratio 2.2:1)"]
    Comp --> Limiter["Soft-Clipping Tanh Limiter<br/>(Ceiling 0.95)"]
    Limiter --> Out["16kHz Clean Audio -> Codec"]
```

1. **Fractional Downsampler (3:2)**:
   Converts Gemini's native 24,000 Hz output down to WhatsApp's 16,000 Hz VoIP rate. It groups samples into triplets ($s_0, s_1, s_2$) and maps them to pairs ($s_0, \frac{s_1 + s_2}{2}$), preserving phase alignment with zero drift.
2. **Chest Resonance Low-Shelf**:
   Adds $+2.5\text{ dB}$ at $200\text{ Hz}$ using a Direct Form II Transposed Biquad IIR filter, giving the voice a warm, authoritative telephone presence.
3. **High-Shelf De-Esser**:
   Attenuates harsh synthetic frequencies by $-3.0\text{ dB}$ at $6500\text{ Hz}$, eliminating sharp sibilance ("s", "sh", "ch") that distorts under WhatsApp's lossy compression.
4. **Soft-Knee Compressor & Tanh Peak Limiter**:
   Evens out loudness variations with a $2.2:1$ compression ratio above $-14\text{ dBFS}$. It rounds peaks through a hyperbolic tangent curve:
   $$\text{out} = 0.95 + 0.05 \cdot \tanh\left(\frac{x - 0.95}{0.05}\right)$$
   preventing harsh digital clipping.
5. **Acoustic Gating**:
   A 350ms cooldown window in `CallManager.IsPlayingAudio()` suppresses incoming audio while the agent is speaking to absorb speaker-to-microphone physical acoustic echo from the caller's mobile device.

---

## 8. Data Architecture & Storage

All persistent data is stored in PostgreSQL under the dedicated `whatsapp_infra` schema, with call audio recordings archived in MinIO S3 object storage.

```mermaid
erDiagram
    whatsapp_sessions ||--o{ call_logs : "records"
    agent_configs ||--o{ whatsapp_sessions : "configures"
    scheduled_calls }o--|| whatsapp_sessions : "dispatches"

    whatsapp_sessions {
        text session_id PK
        text business_id
        text jid
        timestamp created_at
        timestamp updated_at
    }

    agent_configs {
        text business_id PK
        text business_prompt
        text greeting_message
        text fallback_message
        text voice_model
    }

    call_logs {
        text call_id PK
        text session_id FK
        text business_id
        text caller_number
        text direction
        timestamp started_at
        timestamp connected_at
        timestamp ended_at
        int duration
        text status
        text recording_url
        jsonb transcript_json
    }

    scheduled_calls {
        uuid id PK
        uuid business_id
        text phone_number
        text status
        timestamp scheduled_time
    }

    whatsmeow_lid_map {
        text lid PK
        text pn
    }
```

### Stereo Audio Recording (`recorder.go`)
- **Non-blocking Architecture**: Incoming and outgoing PCM chunks are written to bounded channels (`callerCh`, `agentCh`) with preallocated buffers. The recorder worker runs on an independent goroutine, ensuring disk/network latency never stalls VoIP RTP loops.
- **Stereo Channel Separation**:
  - **Left Channel (Channel 1)**: Pure caller microphone audio.
  - **Right Channel (Channel 2)**: Pure AI voice consultant audio.
- **Time-Alignment Gap Filling**: Silent intervals are automatically padded with zeros relative to `time.Since(startTime)`, ensuring the resulting stereo WAV matches the exact timeline of the live conversation.
- **S3 Archival**: Upon call termination, the WAV is encoded, uploaded to MinIO (`https://storage.buildstart.io/call-recordings/<business_id>/<call_id>.wav`), and linked to the `call_logs` table.

---

## 9. Autonomous Lead Intelligence & Callback System

```mermaid
flowchart TD
    End["Call Terminates"] --> Check{"Was call answered<br/>and >= 2 turns?"}
    
    Check -- Yes --> Dossier["Generate Lead Dossier<br/>(Gemini 2.5 Flash)"]
    Dossier --> Extract["Extract JSON:<br/>• Business Name & Role<br/>• Pain Points & Plan<br/>• Deal Temperature & Score<br/>• Next Steps"]
    Extract --> SaveDossier["Save to Broker & DB Log"]
    Extract --> Followup["Send Personalized Sinhala<br/>WhatsApp Follow-Up Text (3s delay)"]

    Check -- No (Missed/Hung Up) --> Unanswered{"Inbound Call?"}
    Unanswered -- Yes --> Notify["Send Apology WhatsApp Text:<br/>'We saw you called...'"]
    Notify --> SchedCallback["Schedule Instant Auto-Callback<br/>(Debounced 2m, Delay 4s)"]
    SchedCallback --> DialOut["Dial Outbound Call via AI Consultant"]

    Unanswered -- No --> Done["Log Call End"]
```

### 1. Gemini Lead Dossier Extraction
When a conversation concludes, `GenerateAndSendLeadDossier` runs asynchronously in `cmd/server/crm_lead.go`. It sends the full conversation transcript to `gemini-2.5-flash` with a strict JSON schema:
- `business_name` & `caller_role`
- `identified_pain_points`
- `recommended_plan` (e.g. Starter LKR 6,990 / Business LKR 12,990)
- `deal_temperature` (🔥 Hot, 🟡 Warm, ❄️ Cold) & `lead_score`
- `whatsapp_summary_sinhala`: A polite, human-sounding 2–3 sentence message in natural Sinhala thanking the customer and summarizing what was discussed.
- `next_action`: Clear next steps for the human sales team.

### 2. Autonomous Instant Callbacks
- **Missed Inbound Calls**: If a caller hangs up before the call connects, the system sends an immediate WhatsApp notification and places an autonomous outbound voice call 4 seconds later (after the customer's phone returns to idle).
- **Chat Keyword Triggers**: If a customer texts keywords like `"call me"`, `"callback"`, or `"කතා කරන්න"` in a WhatsApp chat, `handleIncomingMessage()` triggers an immediate outbound call from the AI Voice Consultant.
- **Scheduled Calls Worker**: `cmd/server/scheduler.go` runs every 60 seconds, querying `whatsapp_infra.scheduled_calls` using `FOR UPDATE SKIP LOCKED` for concurrent safe dialing across multiple workers.

---

## 10. Operator Live Bridge & Browser WebRTC

Operators can monitor and manage calls from the React dashboard via two interfaces:

### 1. Server-Sent Events (SSE Broker)
Clients connect to `GET /api/events` to receive real-time streams of:
- `session-list` & `session-qr`: Real-time pairing QR codes and session state changes.
- `call-status` & `incoming`: Instant alerts when calls ring or connect.
- `agent-transcript`: Live streaming caller and agent speech transcripts.
- `call-event`: Diagnostic logs (codec changes, relay transitions, RTT).

### 2. Browser Audio Bridge (`bridge.go` & `webrtc.ts`)
- The browser opens an `RTCPeerConnection` with a data channel named **`pcm`**.
- It uses the Web Audio API with dedicated `AudioWorklet` nodes:
  - `capture-processor.js`: Captures microphone audio at 16 kHz Float32 and sends it over the data channel.
  - `playback-processor.js`: Buffers incoming 16 kHz Float32 PCM from the caller and plays it through the operator's headphones.
- If an operator answers or talks via the browser, the AI agent is muted (`ac.geminiLive.SetEnabled(false)`), allowing seamless human handover.

---

## 11. State Machine & Call Lifecycle

The call lifecycle is governed by a strict state machine implemented in `internal/voip/call/callstate.go`:

```mermaid
stateDiagram-v2
    [*] --> Initiating: StartCall() / HandleCallOffer()
    Initiating --> Ringing: TransitionOfferSent (Outbound)
    Initiating --> IncomingRinging: TransitionOfferReceived (Inbound)
    
    IncomingRinging --> Connecting: TransitionLocalAccepted (AcceptCall)
    Ringing --> Connecting: TransitionPeerAccepted (HandleCallAccept)
    
    Connecting --> Active: TransitionMediaConnected (Relay Connected)
    
    Active --> OnHold: TransitionHold
    OnHold --> Active: TransitionResume
    
    Initiating --> Ended: TransitionTerminated
    Ringing --> Ended: TransitionTerminated / Declined
    IncomingRinging --> Ended: TransitionTerminated / Declined
    Connecting --> Ended: TransitionTerminated / Error
    Active --> Ended: TransitionTerminated (EndCall / Hangup)
    OnHold --> Ended: TransitionTerminated
    
    Ended --> [*]
```

### Valid Transition Matrix

| Current State | Allowed Next States | Triggering Event / Transition |
|---|---|---|
| `initiating` | `ringing`, `incoming_ringing`, `ended` | Offer sent/received or early error |
| `incoming_ringing` | `connecting`, `ended` | Local operator/auto-answer accepts or declines |
| `ringing` | `connecting`, `ended` | Remote peer answers or rejects/times out |
| `connecting` | `active`, `ended` | WhatsApp SCTP data channel opens and SRTP keys derived |
| `active` | `on_hold`, `ended` | Normal conversation, call hold, or hangup |
| `on_hold` | `active`, `ended` | Unhold or hangup |
| `ended` | *Terminal State* | Final cleanup, recording upload, dossier generation |

---

## 12. Deployment, Production Configuration & Operations

### Deployment Architecture
- **Host VPS**: `178.104.127.220` (Ubuntu Linux)
- **Directory**: `/opt/buildstart-calling-agent`
- **Systemd Service**: `buildstart-calling-agent.service`
- **Build Container**: `golang:1.26.4` Docker image (ensures reproducible, hermetic builds without polluting the host environment)

### Systemd Service Specification (`/etc/systemd/system/buildstart-calling-agent.service`)
```ini
[Unit]
Description=BuildStart WhatsApp Calling Agent
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/buildstart-calling-agent
EnvironmentFile=/opt/buildstart-calling-agent/.env
ExecStart=/opt/buildstart-calling-agent/buildstart-calling-server -addr :8080 -static /opt/buildstart-calling-agent/client/dist
Restart=always
RestartSec=5s
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

### Critical Environment Variables (`.env`)

| Variable | Description | Example / Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string with `whatsapp_infra` schema | `postgres://user:pass@178.104.127.220:5432/postgres?search_path=whatsapp_infra` |
| `GEMINI_API_KEY` | Google AI Studio API key with access to Gemini Live API | `AIzaSy...` |
| `MINIO_ENDPOINT` | Local or remote MinIO / S3 endpoint | `127.0.0.1:9000` |
| `MINIO_ACCESS_KEY` | MinIO storage access credentials | `admin` |
| `MINIO_SECRET_KEY` | MinIO storage secret key | `...` |
| `MINIO_BUCKET` | S3 bucket for audio recordings | `call-recordings` |
| `MINIO_PUBLIC_URL` | CDN / public reverse proxy URL for audio playback | `https://storage.buildstart.io/call-recordings` |

### Production Deployment Commands
```bash
# 1. Pull latest code from GitHub
cd /opt/buildstart-calling-agent
git pull origin development

# 2. Compile the binary inside hermetic Go container
docker run --rm -v "$PWD":/usr/src/app -w /usr/src/app golang:1.26.4 \
  go build -o buildstart-calling-server.new ./cmd/server

# 3. Atomically replace binary and restart daemon
mv buildstart-calling-server.new buildstart-calling-server
chmod +x buildstart-calling-server
systemctl restart buildstart-calling-agent.service

# 4. Verify live service health
systemctl status buildstart-calling-agent.service --no-pager
journalctl -u buildstart-calling-agent.service -n 30 --no-pager
```

---

## 13. Summary & Technical Highlights

The **BuildStart Calling Agent** represents a state-of-the-art implementation of real-time conversational AI:
- **Zero Third-Party Call APIs**: Directly controls WhatsApp VoIP via native protocol emulation, avoiding expensive telephony aggregators.
- **Pure Go Media Engine**: Incorporates pure Go MLow and Opus decoding, AES-128 SRTP encryption, and Biquad DSP mastering for high performance and low CPU overhead.
- **Zero Perceived Turnaround Latency**: Achieved through ring-phase greeting pre-synthesis, 3:2 downsampling, and Gemini Live 3.1 Flash streaming.
- **Automated Sales Loop**: Transcribes every call, generates structured CRM Lead Dossiers, and dispatches follow-ups and callbacks autonomously.

