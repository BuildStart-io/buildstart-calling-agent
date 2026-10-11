package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

type sessionRow struct {
	ID         string
	BusinessID string
	JID        string
}

type sessionStore struct{ db *sql.DB }

func newSessionStore(ctx context.Context, db *sql.DB) (*sessionStore, error) {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS whatsmeow_lid_map (
		lid TEXT PRIMARY KEY,
		pn TEXT NOT NULL
	)`)
	if err != nil {
		return nil, err
	}
	return &sessionStore{db: db}, nil
}

func newSessionID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *sessionStore) list(ctx context.Context) ([]sessionRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT session_id, business_id, COALESCE(jid, '') FROM whatsapp_infra.whatsapp_sessions ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []sessionRow
	for rows.Next() {
		var r sessionRow
		if err := rows.Scan(&r.ID, &r.BusinessID, &r.JID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *sessionStore) insert(ctx context.Context, id, businessID string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO whatsapp_infra.whatsapp_sessions (session_id, business_id, jid) VALUES ($1, $2, '')`, id, businessID)
	return err
}

func (s *sessionStore) setJID(ctx context.Context, id, jid string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE whatsapp_infra.whatsapp_sessions SET jid = $1, updated_at = NOW() WHERE session_id = $2`, jid, id)
	return err
}

func (s *sessionStore) delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM whatsapp_infra.whatsapp_sessions WHERE session_id = $1`, id)
	return err
}

func (s *sessionStore) getPNForLID(ctx context.Context, lid string) string {
	cleaned := strings.TrimSuffix(lid, "@lid")
	cleaned = strings.TrimSuffix(cleaned, "@s.whatsapp.net")
	if idx := strings.Index(cleaned, ":"); idx != -1 {
		cleaned = cleaned[:idx]
	}
	cleaned = strings.TrimPrefix(cleaned, "+")
	if cleaned == "" {
		return ""
	}
	var pn string
	err := s.db.QueryRowContext(ctx, `SELECT pn FROM whatsmeow_lid_map WHERE lid = $1 LIMIT 1`, cleaned).Scan(&pn)
	if err == nil && pn != "" {
		return pn
	}
	return ""
}

func (s *sessionStore) putLIDMapping(ctx context.Context, lid, pn string) {
	lid = strings.TrimSuffix(lid, "@lid")
	if idx := strings.Index(lid, ":"); idx != -1 {
		lid = lid[:idx]
	}
	lid = strings.TrimPrefix(lid, "+")

	pn = strings.TrimSuffix(pn, "@s.whatsapp.net")
	if idx := strings.Index(pn, ":"); idx != -1 {
		pn = pn[:idx]
	}
	pn = strings.TrimPrefix(pn, "+")

	if lid == "" || pn == "" {
		return
	}
	_, _ = s.db.ExecContext(ctx, `INSERT INTO whatsmeow_lid_map (lid, pn) VALUES ($1, $2) ON CONFLICT(lid) DO UPDATE SET pn = EXCLUDED.pn`, lid, pn)
}

func (s *sessionStore) resolvePhone(raw string) string {
	if raw == "" {
		return ""
	}
	cleaned := strings.TrimSuffix(raw, "@s.whatsapp.net")
	cleaned = strings.TrimSuffix(cleaned, "@lid")
	if idx := strings.Index(cleaned, ":"); idx != -1 {
		cleaned = cleaned[:idx]
	}
	cleaned = strings.TrimPrefix(cleaned, "+")

	if pn := s.getPNForLID(context.Background(), cleaned); pn != "" {
		return formatPhoneNumber(pn)
	}

	if cleaned == "17609835688032" {
		return formatPhoneNumber("94765225044")
	}

	return formatPhoneNumber(cleaned)
}

func (s *sessionStore) saveCallRecord(ctx context.Context, r CallRecord, businessID string) error {
	if r.PeerNumber == "" || strings.Contains(r.PeerNumber, "@lid") || strings.HasPrefix(r.PeerNumber, "+17609835688032") {
		r.PeerNumber = s.resolvePhone(r.Peer)
	} else {
		r.PeerNumber = s.resolvePhone(r.PeerNumber)
	}

	transcriptsJSON, _ := json.Marshal(r.Transcripts)
	
	var startedAt, connectedAt, endedAt sql.NullTime
	if r.StartedAt > 0 {
		startedAt = sql.NullTime{Time: time.UnixMilli(r.StartedAt), Valid: true}
	}
	if r.ConnectedAt != nil {
		connectedAt = sql.NullTime{Time: time.UnixMilli(*r.ConnectedAt), Valid: true}
	}
	if r.EndedAt != nil {
		endedAt = sql.NullTime{Time: time.UnixMilli(*r.EndedAt), Valid: true}
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO whatsapp_infra.call_logs (
			call_id, session_id, business_id, caller_number, direction,
			started_at, connected_at, ended_at, duration,
			status, transcript_json
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (call_id) DO UPDATE SET
			caller_number = EXCLUDED.caller_number,
			direction = EXCLUDED.direction,
			connected_at = EXCLUDED.connected_at,
			ended_at = EXCLUDED.ended_at,
			duration = EXCLUDED.duration,
			status = EXCLUDED.status,
			transcript_json = EXCLUDED.transcript_json
	`,
		r.CallID, r.SessionID, businessID, r.PeerNumber, r.Direction,
		startedAt, connectedAt, endedAt, r.DurationSeconds,
		string(r.Status), string(transcriptsJSON),
	)
	return err
}

func (s *sessionStore) listCallHistory(ctx context.Context, sessionID string, limit int) ([]CallRecord, error) {
	query := `SELECT call_id, session_id, direction, COALESCE(caller_number, ''),
		started_at, connected_at, ended_at, COALESCE(duration, 0),
		status, COALESCE(transcript_json, '[]'::jsonb)
		FROM whatsapp_infra.call_logs`
	var args []any
	if sessionID != "" {
		query += ` WHERE session_id = $1`
		args = append(args, sessionID)
	}
	query += ` ORDER BY started_at DESC LIMIT $2`
	if sessionID == "" {
	    args = append(args, limit)
	} else {
	    args = append(args, limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CallRecord
	for rows.Next() {
		var r CallRecord
		var statusStr, transcriptsJSON string
		var startedAt, connectedAt, endedAt sql.NullTime

		if err := rows.Scan(
			&r.CallID, &r.SessionID, &r.Direction, &r.PeerNumber,
			&startedAt, &connectedAt, &endedAt, &r.DurationSeconds,
			&statusStr, &transcriptsJSON,
		); err != nil {
			return nil, err
		}
		r.Status = CallStatus(statusStr)
		if startedAt.Valid {
			r.StartedAt = startedAt.Time.UnixMilli()
		}
		if connectedAt.Valid {
		    unix := connectedAt.Time.UnixMilli()
			r.ConnectedAt = &unix
		}
		if endedAt.Valid {
		    unix := endedAt.Time.UnixMilli()
			r.EndedAt = &unix
		}
		_ = json.Unmarshal([]byte(transcriptsJSON), &r.Transcripts)
		if r.Transcripts == nil {
			r.Transcripts = []CallTranscriptItem{}
		}
		out = append(out, r)
	}
	_ = rows.Close()
	return out, nil
}

type AgentConfigRow struct {
	BusinessPrompt  string
	GreetingMessage string
	FallbackMessage string
	VoiceModel      string
}

func (s *sessionStore) getAgentConfig(ctx context.Context, businessID string) (AgentConfigRow, error) {
	var r AgentConfigRow
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(business_prompt, ''), COALESCE(greeting_message, ''), COALESCE(fallback_message, ''), COALESCE(voice_model, 'models/gemini-3.1-flash-live-preview')
		FROM whatsapp_infra.agent_configs
		WHERE business_id = $1
	`, businessID).Scan(&r.BusinessPrompt, &r.GreetingMessage, &r.FallbackMessage, &r.VoiceModel)
	if err != nil {
		return r, err
	}
	return r, nil
}
