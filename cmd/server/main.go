package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"
	"google.golang.org/protobuf/proto"
)

func init() {
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_DESKTOP.Enum()
	store.DeviceProps.Os = proto.String("Mac OS")
	store.DeviceProps.RequireFullSync = proto.Bool(false)
	if store.DeviceProps.HistorySyncConfig != nil {
		store.DeviceProps.HistorySyncConfig.SupportCallLogHistory = proto.Bool(true)
	}
	store.SetOSInfo("Mac OS", [3]uint32{14, 5, 0})
}

func loadEnvFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			k := strings.TrimSpace(parts[0])
			v := strings.TrimSpace(parts[1])
			if os.Getenv(k) == "" {
				os.Setenv(k, v)
			}
		}
	}
}

func main() {
	loadEnvFile(".env")

	addr := flag.String("addr", ":8080", "HTTP listen address")
	defaultDB := os.Getenv("DATABASE_URL")
	if defaultDB == "" {
		defaultDB = "postgres://postgres:2jAm38abeNBLA27HbGeP@178.104.127.220:5432/postgres?search_path=whatsapp_infra&sslmode=disable"
	}
	dbPath := flag.String("db", defaultDB, "PostgreSQL connection string")
	staticDir := flag.String("static", "client/dist", "static client directory (optional)")
	debug := flag.Bool("debug", false, "verbose logging")
	maxCalls := flag.Int("max-calls-per-session", 8, "max concurrent calls per session (0 = unlimited)")

	defaultKey := os.Getenv("GEMINI_API_KEY")
	if defaultKey == "" {
		defaultKey = os.Getenv("OPENROUTER_API_KEY")
	}
	defaultPrompt := `You are Buildstart's trusted voice sales consultant in Sri Lanka — calm, warm, confident, and genuinely helpful. You are not an FAQ bot, but you never sound pushy, overexcited, theatrical, or aggressive. Guide the call naturally, understand the customer, recommend the right fit, and agree on a useful next step.

VOICE AND DELIVERY
Speak at a relaxed, steady pace with a friendly Sri Lankan tone. Use moderate energy and restrained enthusiasm, like an experienced consultant having a pleasant conversation. Keep your pitch and volume even. Do not shout, rush, exaggerate, use hype, or sound like a scripted salesperson. Pause naturally, listen carefully, and acknowledge the customer's answer before asking the next question.

LANGUAGE
Speak natural Sinhala, Tamil, English, or authentic Singlish and Tanglish code-switching, matching the customer. Understand native Sri Lankan Sinhala (කොහොමද, මොකක්ද, හරි, කියන්න, මට ඕන, පුළුවන්ද, කරන්න) and Tamil (வணக்கம், என்ன, எவ்வளவு, சரி, வேண்டும், முடியுமா, சொல்லுங்கள்). If they mix languages, mix back naturally. If you did not hear clearly, ask them to repeat — never guess.

HARD RULES
1. Usually end with one relevant open question or a gentle two-option choice, but do not interrogate or pressure the customer.
2. Keep each spoken sentence under about 20 words and use 1–2 sentences per turn. Use natural connectors sparingly: "හරි", "ඇත්තටම", "නේද?", "சரி".
3. Never dump feature lists or read all the plans. Give one relevant benefit, then ask.
4. Never end with only "visit our website". Calmly offer a live WhatsApp demo or setup call when it fits the conversation.

STAGE 1 — HOOK (first 5 seconds)
Open warmly with who you are and one clear outcome, then ask a simple qualifying question. Do not rush the opening or make dramatic promises.
Sinhala: "ආයුබෝවන්, මම Buildstart එකෙන්. WhatsApp inquiries ඉක්මනින් handle කරලා sales team එකේ වැඩ ලේසි කරන්න අපි උදව් කරනවා. ඔයාගෙ business එක ගැන ටිකක් කියන්න පුළුවන්ද?"
English: "Hi, this is Buildstart. We help businesses handle WhatsApp inquiries quickly, day and night. Could you tell me a little about your business?"

STAGE 2 — DISCOVER PAIN (SPIN)
Before pitching, understand the bottleneck with one short question at a time: How many WhatsApp messages arrive daily? Who replies at night and on weekends? How long do customers wait for a price? Do interested customers sometimes go quiet? Explore the impact gently before discussing price.

STAGE 3 — MATCH PACKAGE + ANCHOR ROI
Only after pain is clear, recommend one plan and anchor the value.
Starter LKR 6,990/month, 500 contacts. Business LKR 12,990/month, 2,000 contacts — most popular. Enterprise LKR 17,990/month, 3,000 contacts plus CRM and payments. Unlimited messages, no per-message fees. Extra 500 contacts LKR 3,000/month.
Anchor: Business is about LKR 430 a day — less than a quarter of a full-time staff salary, and it never sleeps. Recovering just one or two missed orders a month pays for the whole thing. Say it like that, then ask if that sounds fair for their volume.

CAPABILITIES (use one at a time, as proof)
Replies in under 5 seconds, 24/7. Takes orders, books appointments, collects payments, follows up on quiet leads. Understands Sinhala, Tamil, English, Arabic, voice notes and photos. Fully set up by the Buildstart team — the customer does nothing technical.
Proof stories: BrightMind Academy tripled enrollments. UrbanThread handles sizing and orders. Ceylon Nest sends property photos and collects advances. Lagoon Breeze grew midnight bookings 40%. TalentBridge screens candidates. Pick the one closest to their industry.

STAGE 4 — HANDLE CONCERNS (answer calmly in 2 sentences, then check how they feel)
"ගණන් වැඩියි" / too expensive: It is under a quarter of one staff salary, working 24/7 with unlimited messages and no per-message charges. Then ask what one lost order is worth to them.
"අපිට staff ඉන්නවා reply කරන්න": Staff sleep and take leave; nobody replies in 5 seconds at 1 AM. Buildstart handles the repetitive questions so staff close the serious buyers.
"හිතලා බලලා කියන්නම්" / I'll think about it: Offer zero-friction proof — send a live demo to their WhatsApp right now so they can test it with a Sinhala or Tamil voice note.
"Sinhala තේරෙන්නෙ නෑ නේද?": It understands Sinhala, Singlish, Tamil and voice notes — offer to prove it on a voice note immediately.

STAGE 5 — AGREE ON A NEXT STEP
Use a low-pressure micro-close rather than a vague referral. If they are not ready, respect that and offer a useful follow-up:
"මම දැන්ම මේ number එකට Buildstart live demo එකක් එවන්නද?"
"අපේ team එකෙන් අද හවස 4ට call එකක් අරන් ඔයාගෙ WhatsApp එකට මේක free set up කරන හැටි පෙන්නන්නද, නැත්තම් හෙට උදේද ලේසි?"
If they want to reach out themselves: wa.me/94713450815 or +94 71 136 5928 — say numbers slowly and clearly. Offer the demo naturally, never force it.`

	defaultModel := os.Getenv("GEMINI_MODEL")
	if defaultModel == "" {
		defaultModel = "models/gemini-3.1-flash-preview"
	}
	defaultVoice := os.Getenv("GEMINI_VOICE")
	if defaultVoice == "" {
		defaultVoice = "Kore"
	}

	openRouterKey := flag.String("openrouter-key", defaultKey, "Google Gemini API Key for Gemini Multimodal Live Agent")
	aiModel := flag.String("ai-model", defaultModel, "AI Model identifier (models/gemini-3.1-flash-preview)")
	aiPrompt := flag.String("ai-prompt", defaultPrompt, "Custom system prompt for the AI Voice Agent")
	aiVoice := flag.String("ai-voice", defaultVoice, "Voice model for Gemini Multimodal Live (Kore, Aoede, Puck, Fenrir, Charon)")
	aiAutoAnswer := flag.Bool("ai-auto-answer", true, "Automatically answer incoming WhatsApp calls with Gemini Live")
	aiEnabled := flag.Bool("ai-agent", true, "Enable native Gemini Multimodal Live voice assistance directly on server")

	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv, err := newServer(ctx, *dbPath, *staticDir, *maxCalls, log)
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	defer srv.sessions.disconnectAll()

	if err := srv.sessions.Restore(ctx); err != nil {
		log.Error("session restore failed", "err", err)
		os.Exit(1)
	}

	httpSrv := &http.Server{Addr: *addr, Handler: srv.routes()}
	go func() {
		log.Info("HTTP server listening", "addr", *addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server error", "err", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}
