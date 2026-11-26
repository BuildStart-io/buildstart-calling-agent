package main

import (
	"context"
	"log/slog"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func addMockSession(m *SessionManager, id, businessID, platform string) *Session {
	dev := &store.Device{}
	client := whatsmeow.NewClient(dev, waLog.Noop)
	s := newSession(m, id, businessID, platform, client)
	m.register(s)
	return s
}

func TestSessionManagerPlatformIsolation(t *testing.T) {
	ctx := context.Background()
	broker := NewBroker()
	m := newSessionManager(ctx, nil, broker, nil, nil, waLog.Noop, slog.Default(), 0)

	if len(m.infos()) != 0 {
		t.Fatal("expected no sessions when empty")
	}

	loc := addMockSession(m, "s_local", "11111111-1111-1111-1111-111111111111", "local")
	glob := addMockSession(m, "s_global", "22222222-2222-2222-2222-222222222222", "global")

	if got := m.infosForPlatform("local", ""); len(got) != 1 || got[0].ID != "s_local" {
		t.Fatalf("local platform filter failed: %+v", got)
	}
	if got := m.infosForPlatform("global", ""); len(got) != 1 || got[0].ID != "s_global" {
		t.Fatalf("global platform filter failed: %+v", got)
	}
	if _, ok := m.GetForPlatform(loc.id, "global"); ok {
		t.Fatal("global key must not see local session")
	}
	if _, ok := m.GetForPlatform(glob.id, "local"); ok {
		t.Fatal("local key must not see global session")
	}
	if got, ok := m.GetForPlatform(glob.id, "global"); !ok || got != glob {
		t.Fatal("global key should retrieve its own session")
	}

	m.unregister(glob.id)
	if _, ok := m.Get(glob.id); ok {
		t.Fatal("global session should be gone after unregister")
	}
	if len(m.infos()) != 1 {
		t.Fatalf("expected 1 session remaining, got %d", len(m.infos()))
	}
}
