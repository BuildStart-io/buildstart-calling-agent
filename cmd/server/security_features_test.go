package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"testing"
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
