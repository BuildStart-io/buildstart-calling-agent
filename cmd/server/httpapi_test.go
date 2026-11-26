package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	waLog "go.mau.fi/whatsmeow/util/log"
)

func newMockHTTPServer(t *testing.T) *server {
	t.Helper()
	broker := NewBroker()
	mgr := newSessionManager(context.Background(), nil, broker, nil, nil, waLog.Noop, slog.Default(), 2)
	addMockSession(mgr, "sess_local_1", "11111111-1111-1111-1111-111111111111", "local")
	addMockSession(mgr, "sess_global_1", "22222222-2222-2222-2222-222222222222", "global")

	broker.upsertCall(CallRecord{
		SessionID:  "sess_local_1",
		BusinessID: "11111111-1111-1111-1111-111111111111",
		Platform:   "local",
		CallID:     "call_local_1",
		Status:     StatusRinging,
	})
	broker.upsertCall(CallRecord{
		SessionID:  "sess_global_1",
		BusinessID: "22222222-2222-2222-2222-222222222222",
		Platform:   "global",
		CallID:     "call_global_1",
		Status:     StatusRinging,
	})

	return &server{
		broker:   broker,
		sessions: mgr,
		apiKeys: map[string]string{
			"local":  "test-local-secret",
			"global": "test-global-secret",
		},
		log:       slog.Default(),
		staticDir: "./client/dist",
	}
}

func TestPlatformAuthAndHealth(t *testing.T) {
	srv := newMockHTTPServer(t)
	handler := srv.routes()

	// Unauthenticated /api/sessions must return 401
	req := httptest.NewRequest("GET", "/api/sessions", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated /api/sessions, got %d", w.Code)
	}

	// Public /health succeeds without key and does NOT leak phones
	for _, ep := range []string{"/health", "/api/health", "/api/v1/health"} {
		req = httptest.NewRequest("GET", ep, nil)
		w = httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 for %s, got %d", ep, w.Code)
		}
		var resp map[string]any
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("decode %s failed: %v", ep, err)
		}
		if resp["status"] != "online" {
			t.Fatalf("expected online status on %s, got %v", ep, resp["status"])
		}
		if _, hasPhones := resp["phones"]; hasPhones {
			t.Fatalf("public %s must not expose phone JIDs", ep)
		}
	}

	// Static console disabled by default
	req = httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Api-Key", "test-local-secret")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on / when SERVE_STATIC_CONSOLE unset, got %d", w.Code)
	}
}

func TestCrossPlatformIsolationHTTP(t *testing.T) {
	srv := newMockHTTPServer(t)
	handler := srv.routes()

	// Global key only sees global sessions and calls
	req := httptest.NewRequest("GET", "/api/sessions", nil)
	req.Header.Set("Authorization", "Bearer test-global-secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var sResp struct {
		Sessions []SessionInfo `json:"sessions"`
	}
	_ = json.NewDecoder(w.Body).Decode(&sResp)
	if len(sResp.Sessions) != 1 || sResp.Sessions[0].ID != "sess_global_1" {
		t.Fatalf("global key leaked wrong sessions: %+v", sResp.Sessions)
	}

	// Global key trying to use local session or read local call gets 404
	for _, tc := range []struct {
		method string
		path   string
	}{
		{"DELETE", "/api/sessions/sess_local_1"},
		{"POST", "/api/sessions/sess_local_1/messages/send"},
		{"GET", "/api/calls/call_local_1"},
	} {
		req = httptest.NewRequest(tc.method, tc.path, bytes.NewBufferString(`{"to":"123","text":"hi"}`))
		req.Header.Set("X-Api-Key", "test-global-secret")
		w = httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusNotFound {
			t.Fatalf("%s %s with global key expected 404, got %d", tc.method, tc.path, w.Code)
		}
	}
}

func TestDirectDialRequiresSessionID(t *testing.T) {
	srv := newMockHTTPServer(t)
	handler := srv.routes()

	payload, _ := json.Marshal(map[string]any{"to": "+94771234567"})
	req := httptest.NewRequest("POST", "/api/calls/dial", bytes.NewBuffer(payload))
	req.Header.Set("X-Api-Key", "test-local-secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when session_id missing on /api/calls/dial, got %d: %s", w.Code, w.Body.String())
	}

	// Non-existent session or wrong-platform session returns 404
	payload2, _ := json.Marshal(map[string]any{"to": "+94771234567", "session_id": "sess_local_1"})
	req = httptest.NewRequest("POST", "/api/calls/dial", bytes.NewBuffer(payload2))
	req.Header.Set("X-Api-Key", "test-global-secret")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for cross-platform dial, got %d", w.Code)
	}
}
