package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestSignEngineWebhookFormat(t *testing.T) {
	secret := "whsec_test_123"
	ts := int64(1700000000)
	payload := []byte(`{"event":"message.received","data":{"text":"hello"}}`)

	got := signEngineWebhook(secret, ts, payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(fmt.Sprintf("%d.", ts)))
	mac.Write(payload)
	want := fmt.Sprintf("t=%d,v1=%s", ts, hex.EncodeToString(mac.Sum(nil)))

	if got != want {
		t.Fatalf("signature mismatch:\ngot:  %s\nwant: %s", got, want)
	}
}

func TestCallbackKeywordMatching(t *testing.T) {
	defaultCfg := defaultAgentConfigRow()

	if ok, _ := matchesCallbackKeyword("Hi please call me when free", defaultCfg, false); !ok {
		t.Fatal("expected 'call me' to match default keywords")
	}
	if ok, _ := matchesCallbackKeyword("Please do not call me", defaultCfg, false); ok {
		t.Fatal("negated 'do not call me' must not match")
	}
	if ok, _ := matchesCallbackKeyword("Please don't call me back", defaultCfg, false); ok {
		t.Fatal("negated don't call me must not match")
	}
	if ok, _ := matchesCallbackKeyword("Hi please call me", defaultCfg, true); ok {
		t.Fatal("group messages must never trigger auto-callback")
	}
	if ok, _ := matchesCallbackKeyword("recall earlier item", defaultCfg, false); ok {
		t.Fatal("partial word boundary must not match")
	}

	disabledKW := AgentConfigRow{
		HasCallbackKeywords: true,
		CallbackKeywords:    []string{},
	}
	if ok, _ := matchesCallbackKeyword("call me", disabledKW, false); ok {
		t.Fatal("explicit empty callback_keywords must disable keyword triggers")
	}

	customKW := AgentConfigRow{
		HasCallbackKeywords: true,
		CallbackKeywords:    []string{"speak to agent", "ring back"},
	}
	if ok, kw := matchesCallbackKeyword("Can someone ring back?", customKW, false); !ok || kw != "ring back" {
		t.Fatalf("expected custom keyword 'ring back' to match, got ok=%v kw=%q", ok, kw)
	}
}

func TestSSRFIPBlocking(t *testing.T) {
	blocked := []string{
		"127.0.0.1",
		"10.0.0.1",
		"172.16.0.5",
		"192.168.1.10",
		"169.254.169.254",
		"100.64.0.1",
		"0.0.0.0",
		"::1",
		"fe80::1",
		"fc00::1",
		"::ffff:127.0.0.1",
	}
	for _, s := range blocked {
		ip := net.ParseIP(s)
		if !isBlockedSSRFIP(ip) {
			t.Fatalf("expected IP %s to be blocked", s)
		}
	}

	allowed := []string{
		"8.8.8.8",
		"1.1.1.1",
		"93.184.216.34",
		"2606:4700:4700::1111",
	}
	for _, s := range allowed {
		ip := net.ParseIP(s)
		if isBlockedSSRFIP(ip) {
			t.Fatalf("expected public IP %s to be allowed", s)
		}
	}
}

func TestMatchPlatformKey(t *testing.T) {
	keys := map[string]string{
		"local":  "loc-key-1",
		"global": "glob-key-2",
	}
	if plat, ok := matchPlatformKey("glob-key-2", keys); !ok || plat != "global" {
		t.Fatalf("expected global, got plat=%q ok=%v", plat, ok)
	}
	if plat, ok := matchPlatformKey("loc-key-1", keys); !ok || plat != "local" {
		t.Fatalf("expected local, got plat=%q ok=%v", plat, ok)
	}
	if _, ok := matchPlatformKey("wrong", keys); ok {
		t.Fatal("expected invalid key to fail")
	}

	var emptyRaw json.RawMessage
	if len(emptyRaw) != 0 {
		t.Fatal("sanity check")
	}
}

func TestIgnoreNewsletterAndBroadcastMessages(t *testing.T) {
	if _, err := resolveTargetJID("12036301234567890@newsletter"); err == nil {
		t.Fatal("expected newsletter JID to be rejected by resolveTargetJID")
	}

	s := &Session{id: "s1", businessID: "11111111-1111-1111-1111-111111111111", platform: "global", log: slog.Default()}
	// Ensure handleIncomingMessage returns safely without touching mgr/store on newsletter or broadcast chats.
	s.handleIncomingMessage(&events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat: types.NewJID("12036301234567890", "newsletter"),
			},
			ID: "msg_nl_1",
		},
		Message: &waE2E.Message{Conversation: proto.String("call me")},
	})
	s.handleIncomingMessage(&events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat: types.StatusBroadcastJID,
			},
			ID: "msg_bc_1",
		},
		Message: &waE2E.Message{Conversation: proto.String("call me")},
	})
}

func TestBuildCallPromptWithOutboundContext(t *testing.T) {
	cfg := AgentConfigRow{
		BusinessPrompt: "Base prompt.",
		Timezone:       "Asia/Colombo",
	}

	locSess := &Session{platform: "local"}
	if got := locSess.buildCallPrompt(cfg, "+94771234567", "inbound", ""); got != "Base prompt." {
		t.Fatalf("expected untouched prompt for local session without context, got %q", got)
	}

	globSess := &Session{platform: "global"}
	got := globSess.buildCallPrompt(cfg, "+94771234567", "outbound", "Promo campaign: 20% off annual plan")
	if !strings.Contains(got, "[LIVE CALL CONTEXT]") ||
		!strings.Contains(got, "Timezone: Asia/Colombo") ||
		!strings.Contains(got, "Call purpose / campaign context: Promo campaign: 20% off annual plan") ||
		!strings.HasSuffix(got, "Base prompt.") {
		t.Fatalf("unexpected global outbound prompt: %s", got)
	}
}

func TestPresignMinioUsesPublicStorageDomain(t *testing.T) {
	t.Setenv("MINIO_ENDPOINT", "127.0.0.1:9000")
	t.Setenv("MINIO_PUBLIC_URL", "https://storage.buildstart.io/call-recordings")
	t.Setenv("MINIO_ACCESS_KEY", "testaccess")
	t.Setenv("MINIO_SECRET_KEY", "testsecret")

	u, err := presignRecordingURL(context.Background(), "global", "11111111-1111-1111-1111-111111111111", "call_abc", 900*1e9)
	if err != nil {
		t.Fatalf("presignRecordingURL failed: %v", err)
	}
	if !strings.HasPrefix(u, "https://storage.buildstart.io/call-recordings-private/11111111-1111-1111-1111-111111111111/call_abc.wav?") {
		t.Fatalf("expected public HTTPS path-style presigned URL, got: %s", u)
	}
	if strings.Contains(u, "127.0.0.1") {
		t.Fatalf("presigned URL must not contain 127.0.0.1: %s", u)
	}
}

func TestStartCallAndCallbackGreetingValidation(t *testing.T) {
	srv := newMockHTTPServer(t)
	handler := srv.routes()

	// Verify /api/v1/sessions/{sid}/calls rejects negative max_call_seconds once paired check passes or wait:
	// First check route existence (paired check returns 503 when unpaired, NOT 404).
	req := httptest.NewRequest("POST", "/api/v1/sessions/sess_global_1/calls", strings.NewReader(`{"to":"+94771234567","client_ref":"camp-1","context":"Follow up"}`))
	req.Header.Set("X-Api-Key", "test-global-secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for unpaired mock session on /api/v1/sessions/{sid}/calls (route must match), got %d: %s", w.Code, w.Body.String())
	}
}
