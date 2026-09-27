package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func newTestServer(t *testing.T) *server {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "api_test.db")
	srv, err := newServer(ctx, dbPath, "", 0, nil, slog.Default())
	if err != nil {
		t.Fatalf("failed to create test server: %v", err)
	}
	return srv
}

func TestHealthEndpoints(t *testing.T) {
	srv := newTestServer(t)
	handler := srv.routes()

	endpoints := []string{"/health", "/api/health", "/status", "/api/status"}
	for _, ep := range endpoints {
		req := httptest.NewRequest("GET", ep, nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200 for %s, got %d", ep, w.Code)
		}

		var resp map[string]any
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Errorf("failed to decode response for %s: %v", ep, err)
		}
		if status, ok := resp["status"].(string); !ok || status != "online" {
			t.Errorf("expected status 'online' for %s, got %v", ep, resp["status"])
		}
	}
}

func TestSessionManagementAPI(t *testing.T) {
	srv := newTestServer(t)
	handler := srv.routes()

	// 1. Initial sessions list is empty
	req := httptest.NewRequest("GET", "/api/sessions", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Sessions []any `json:"sessions"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode sessions: %v", err)
	}
	if len(resp.Sessions) != 0 {
		t.Fatalf("expected 0 sessions, got %d", len(resp.Sessions))
	}

	// 2. Create session successfully
	reqBody := map[string]string{"name": "Test Line"}
	bodyBytes, _ := json.Marshal(reqBody)
	req = httptest.NewRequest("POST", "/api/sessions", bytes.NewBuffer(bodyBytes))
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for session create, got %d: %s", w.Code, w.Body.String())
	}

	var created map[string]any
	if err := json.NewDecoder(w.Body).Decode(&created); err != nil {
		t.Fatalf("failed to decode created session: %v", err)
	}
	sid, ok := created["id"].(string)
	if !ok || sid == "" {
		t.Fatalf("expected valid session id, got %v", created["id"])
	}

	// 3. Delete session
	req = httptest.NewRequest("DELETE", "/api/sessions/"+sid, nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for session delete, got %d", w.Code)
	}
}

func TestDirectDialEdgeCases(t *testing.T) {
	srv := newTestServer(t)
	handler := srv.routes()

	// 1. Malformed JSON
	req := httptest.NewRequest("POST", "/api/calls/dial", bytes.NewBufferString("{broken json"))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for malformed json, got %d", w.Code)
	}

	// 2. Missing to / recipient field
	emptyPayload, _ := json.Marshal(map[string]any{"to": ""})
	req = httptest.NewRequest("POST", "/api/calls/dial", bytes.NewBuffer(emptyPayload))
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty recipient, got %d", w.Code)
	}

	// 3. Invalid phone number (letters, symbols)
	invalidPayload, _ := json.Marshal(map[string]any{"to": "invalid-phone"})
	req = httptest.NewRequest("POST", "/api/calls/dial", bytes.NewBuffer(invalidPayload))
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest && w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 400 or 503 for invalid phone, got %d", w.Code)
	}
}

func TestCallLookupNotFound(t *testing.T) {
	srv := newTestServer(t)
	handler := srv.routes()

	// Non-existent call ID
	req := httptest.NewRequest("GET", "/api/calls/non_existent_call_id_12345", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for non-existent call, got %d", w.Code)
	}
}
