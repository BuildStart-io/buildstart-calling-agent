package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type SessionManager struct {
	appCtx    context.Context
	container *sqlstore.Container
	broker    *Broker
	store     *sessionStore
	outbox    *OutboxDispatcher
	waLogger  waLog.Logger
	log       *slog.Logger
	maxCalls  int

	mu       sync.RWMutex
	sessions map[string]*Session
	order    []string
}

func newSessionManager(ctx context.Context, container *sqlstore.Container, broker *Broker, store *sessionStore, outbox *OutboxDispatcher, waLogger waLog.Logger, log *slog.Logger, maxCalls int) *SessionManager {
	return &SessionManager{
		appCtx:    ctx,
		container: container,
		broker:    broker,
		store:     store,
		outbox:    outbox,
		waLogger:  waLogger,
		log:       log,
		maxCalls:  maxCalls,
		sessions:  map[string]*Session{},
	}
}

func (m *SessionManager) register(s *Session) {
	m.mu.Lock()
	m.sessions[s.id] = s
	m.order = append(m.order, s.id)
	m.mu.Unlock()
}

func (m *SessionManager) unregister(id string) {
	m.mu.Lock()
	delete(m.sessions, id)
	for i, x := range m.order {
		if x == id {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	m.mu.Unlock()
}

func (m *SessionManager) Get(id string) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	return s, ok
}

func (m *SessionManager) GetForPlatform(id, platform string) (*Session, bool) {
	s, ok := m.Get(id)
	if !ok || (platform != "" && s.platform != platform) {
		return nil, false
	}
	return s, true
}

func (m *SessionManager) findByBusinessLocked(businessID string) *Session {
	for _, id := range m.order {
		if s, ok := m.sessions[id]; ok && s.businessID == businessID {
			return s
		}
	}
	return nil
}

func (m *SessionManager) infosForPlatform(platform, businessID string) []SessionInfo {
	m.mu.RLock()
	ordered := make([]*Session, 0, len(m.order))
	for _, id := range m.order {
		if s, ok := m.sessions[id]; ok {
			if platform != "" && s.platform != platform {
				continue
			}
			if businessID != "" && s.businessID != businessID {
				continue
			}
			ordered = append(ordered, s)
		}
	}
	m.mu.RUnlock()
	out := make([]SessionInfo, 0, len(ordered))
	for _, s := range ordered {
		out = append(out, s.info())
	}
	return out
}

func (m *SessionManager) infos() []SessionInfo {
	return m.infosForPlatform("", "")
}

func (m *SessionManager) allSessions() []*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		out = append(out, s)
	}
	return out
}

func (m *SessionManager) snapshotEvents(platform, sessionID, businessID string) []any {
	infos := m.infosForPlatform(platform, businessID)
	if sessionID != "" {
		filtered := make([]SessionInfo, 0, 1)
		for _, info := range infos {
			if info.ID == sessionID {
				filtered = append(filtered, info)
			}
		}
		infos = filtered
	}
	events := []any{map[string]any{"type": "session-list", "sessions": infos}}
	for _, info := range infos {
		if info.QR != "" {
			events = append(events, map[string]any{"type": "session-qr", "sessionId": info.ID, "session_id": info.ID, "qr": info.QR})
		}
	}
	return events
}

func (m *SessionManager) Restore(ctx context.Context) error {
	rows, err := m.store.list(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.JID == "" {
			_ = m.store.delete(ctx, row.ID)
			continue
		}
		jid, err := types.ParseJID(row.JID)
		if err != nil {
			m.log.Warn("dropping session with unparseable jid", "session", row.ID, "jid", row.JID)
			_ = m.store.delete(ctx, row.ID)
			continue
		}
		device, err := m.container.GetDevice(ctx, jid)
		if err != nil || device == nil {
			m.log.Warn("dropping session with no stored device", "session", row.ID, "jid", row.JID, "err", err)
			_ = m.store.delete(ctx, row.ID)
			continue
		}
		client := whatsmeow.NewClient(device, m.waLogger)
		s := newSession(m, row.ID, row.BusinessID, row.Platform, client)
		m.register(s)
		if err := s.connect(ctx); err != nil {
			m.log.Error("session connect failed", "session", row.ID, "err", err)
		}
	}
	m.broker.emitSessionListForPlatform(m.infos())
	m.log.Info("sessions restored", "count", len(m.infos()))
	return nil
}

var errPlatformMismatch = fmt.Errorf("resource belongs to another platform")

// Create idempotently ensures a single WhatsApp session exists for (platform, businessID).
func (m *SessionManager) Create(platform, businessID string) (string, bool, error) {
	if platform == "" {
		platform = "local"
	}

	m.mu.Lock()
	if existing := m.findByBusinessLocked(businessID); existing != nil {
		m.mu.Unlock()
		if existing.platform != platform {
			return "", false, errPlatformMismatch
		}
		return existing.id, true, nil
	}

	if cfg, exists, err := m.store.getAgentConfigScoped(m.appCtx, businessID); err != nil {
		m.mu.Unlock()
		return "", false, err
	} else if exists && cfg.Platform != platform {
		m.mu.Unlock()
		return "", false, errPlatformMismatch
	}

	id := newSessionID()
	if err := m.store.insert(m.appCtx, id, businessID, platform); err != nil {
		m.mu.Unlock()
		return "", false, err
	}
	device := m.container.NewDevice()
	client := whatsmeow.NewClient(device, m.waLogger)
	s := newSession(m, id, businessID, platform, client)
	m.sessions[s.id] = s
	m.order = append(m.order, s.id)
	m.mu.Unlock()

	m.broker.emitSessionListForPlatform(m.infosForPlatform(platform, ""))
	if err := s.startPairing(m.appCtx); err != nil {
		m.log.Error("start pairing failed", "session", id, "err", err)
		return "", false, fmt.Errorf("start pairing: %w", err)
	}
	m.log.Info("session created", "session", id, "businessID", businessID, "platform", platform)
	return id, false, nil
}

func (m *SessionManager) Delete(ctx context.Context, id string) error {
	s, ok := m.Get(id)
	if !ok {
		m.log.Warn("delete called for unknown session; cleaning up DB anyway", "session", id)
		_ = m.store.delete(ctx, id)
		return nil
	}
	if s.client.Store.ID != nil {
		if err := s.client.Logout(ctx); err != nil {
			m.log.Warn("logout failed; deleting locally", "session", id, "err", err)
			_ = m.container.DeleteDevice(ctx, s.client.Store)
		}
	} else {
		s.client.Disconnect()
		_ = m.container.DeleteDevice(ctx, s.client.Store)
	}
	s.teardownAllCalls()
	m.unregister(id)
	_ = m.store.delete(ctx, id)
	m.broker.emitSessionListForPlatform(m.infosForPlatform(s.platform, ""))
	s.enqueueGlobalEvent(context.Background(), s.id, "session.status", map[string]any{
		"state": "deleted",
		"jid":   "",
	})
	m.log.Info("session deleted", "session", id, "platform", s.platform)
	return nil
}

func (m *SessionManager) Logout(ctx context.Context, id string) error {
	s, ok := m.Get(id)
	if !ok {
		return fmt.Errorf("no session %s", id)
	}
	if s.client.Store.ID != nil {
		if err := s.client.Logout(ctx); err != nil {
			m.log.Warn("logout failed", "session", id, "err", err)
		}
	}
	s.replaceClient(whatsmeow.NewClient(m.container.NewDevice(), m.waLogger))
	_ = m.store.setJID(ctx, id, "")
	s.setAuth(AuthSnapshot{State: "logged_out", Paired: false})
	m.log.Info("session disconnected", "session", id)
	return nil
}

func (m *SessionManager) Pair(id string) error {
	s, ok := m.Get(id)
	if !ok {
		return fmt.Errorf("no session %s", id)
	}
	if s.client.Store.ID != nil {
		return fmt.Errorf("session already paired")
	}
	s.replaceClient(whatsmeow.NewClient(m.container.NewDevice(), m.waLogger))
	if err := s.startPairing(m.appCtx); err != nil {
		return fmt.Errorf("start pairing: %w", err)
	}
	m.broker.emitSessionListForPlatform(m.infosForPlatform(s.platform, ""))
	m.log.Info("session re-pairing", "session", id)
	return nil
}

func (m *SessionManager) disconnectAll() {
	m.mu.RLock()
	all := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		all = append(all, s)
	}
	m.mu.RUnlock()
	for _, s := range all {
		s.shutdown()
	}
}
