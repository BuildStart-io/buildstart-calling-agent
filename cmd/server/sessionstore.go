package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type sessionRow struct {
	ID         string
	BusinessID string
	JID        string
	Platform   string
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
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *sessionStore) list(ctx context.Context) ([]sessionRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT session_id, business_id::text, COALESCE(jid, ''), COALESCE(platform, 'local')
		FROM whatsapp_infra.whatsapp_sessions
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []sessionRow
	for rows.Next() {
		var r sessionRow
		if err := rows.Scan(&r.ID, &r.BusinessID, &r.JID, &r.Platform); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *sessionStore) insert(ctx context.Context, id, businessID, platform string) error {
	if platform == "" {
		platform = "local"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO whatsapp_infra.whatsapp_sessions (session_id, business_id, jid, platform)
		VALUES ($1, $2::uuid, '', $3)
	`, id, businessID, platform)
	return err
}

func (s *sessionStore) getSessionPlatformByBusiness(ctx context.Context, businessID string) (string, bool, error) {
	var plat string
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(platform, 'local')
		FROM whatsapp_infra.whatsapp_sessions
		WHERE business_id = $1::uuid
		LIMIT 1
	`, businessID).Scan(&plat)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return plat, true, nil
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

	return formatPhoneNumber(cleaned)
}

func (s *sessionStore) saveCallRecord(ctx context.Context, r CallRecord, businessID string) error {
	if r.PeerNumber == "" || strings.Contains(r.PeerNumber, "@lid") || strings.HasPrefix(r.PeerNumber, "LID:") {
		r.PeerNumber = s.resolvePhone(r.Peer)
	} else {
		r.PeerNumber = s.resolvePhone(r.PeerNumber)
	}

	transcriptsJSON, _ := json.Marshal(r.Transcripts)
	if len(transcriptsJSON) == 0 || string(transcriptsJSON) == "null" {
		transcriptsJSON = []byte("[]")
	}

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

	// Monotonic upsert: out-of-order async updates never roll 'ended' backward or shrink transcripts.
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO whatsapp_infra.call_logs (
			call_id, session_id, business_id, caller_number, direction,
			started_at, connected_at, ended_at, duration,
			status, transcript_json
		) VALUES ($1, $2, $3::uuid, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
		ON CONFLICT (call_id) DO UPDATE SET
			caller_number = COALESCE(NULLIF(EXCLUDED.caller_number, ''), whatsapp_infra.call_logs.caller_number),
			direction = COALESCE(NULLIF(EXCLUDED.direction, ''), whatsapp_infra.call_logs.direction),
			connected_at = COALESCE(whatsapp_infra.call_logs.connected_at, EXCLUDED.connected_at),
			ended_at = COALESCE(EXCLUDED.ended_at, whatsapp_infra.call_logs.ended_at),
			duration = GREATEST(COALESCE(whatsapp_infra.call_logs.duration, 0), COALESCE(EXCLUDED.duration, 0)),
			status = CASE
				WHEN whatsapp_infra.call_logs.status = 'ended' THEN 'ended'
				ELSE EXCLUDED.status
			END,
			transcript_json = CASE
				WHEN jsonb_array_length(COALESCE(EXCLUDED.transcript_json, '[]'::jsonb)) >= jsonb_array_length(COALESCE(whatsapp_infra.call_logs.transcript_json, '[]'::jsonb))
				THEN EXCLUDED.transcript_json
				ELSE whatsapp_infra.call_logs.transcript_json
			END
	`,
		r.CallID, r.SessionID, businessID, r.PeerNumber, r.Direction,
		startedAt, connectedAt, endedAt, r.DurationSeconds,
		string(r.Status), string(transcriptsJSON),
	)
	return err
}

func (s *sessionStore) updateCallRecording(ctx context.Context, callID, recordingURL string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE whatsapp_infra.call_logs
		SET recording_url = $1
		WHERE call_id = $2
	`, recordingURL, callID)
	return err
}

func buildListCallHistoryQuery(platform, sessionID string, limit int) (string, []any) {
	query := `SELECT cl.call_id, cl.session_id, cl.business_id::text, cl.direction, COALESCE(cl.caller_number, ''),
		cl.started_at, cl.connected_at, cl.ended_at, COALESCE(cl.duration, 0),
		cl.status, COALESCE(cl.transcript_json, '[]'::jsonb), COALESCE(ws.platform, 'local')
		FROM whatsapp_infra.call_logs cl
		LEFT JOIN whatsapp_infra.whatsapp_sessions ws ON ws.session_id = cl.session_id`

	var conds []string
	var args []any
	if platform != "" {
		args = append(args, platform)
		conds = append(conds, fmt.Sprintf("COALESCE(ws.platform, 'local') = $%d", len(args)))
	}
	if sessionID != "" {
		args = append(args, sessionID)
		conds = append(conds, fmt.Sprintf("cl.session_id = $%d", len(args)))
	}
	if len(conds) > 0 {
		query += " WHERE " + strings.Join(conds, " AND ")
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY cl.started_at DESC LIMIT $%d", len(args))
	return query, args
}

func (s *sessionStore) listCallHistory(ctx context.Context, platform, sessionID string, limit int) ([]CallRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	query, args := buildListCallHistoryQuery(platform, sessionID, limit)

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
			&r.CallID, &r.SessionID, &r.BusinessID, &r.Direction, &r.PeerNumber,
			&startedAt, &connectedAt, &endedAt, &r.DurationSeconds,
			&statusStr, &transcriptsJSON, &r.Platform,
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
	return out, rows.Err()
}

type AgentConfigRow struct {
	Platform            string
	BusinessPrompt      string
	GreetingMessage     string
	FallbackMessage     string
	CallbackGreeting    string
	VoiceModel          string
	VoiceEnabled        bool
	AutoAnswer          bool
	AutoCallback        bool
	HasCallbackKeywords bool
	CallbackKeywords    []string
	MaxCallSeconds      int
	Timezone            string
	ToolsJSON           []byte
	UpdatedAt           time.Time
}

func defaultAgentConfigRow() AgentConfigRow {
	return AgentConfigRow{
		Platform:     "local",
		VoiceEnabled: true,
		AutoAnswer:   true,
		AutoCallback: true,
	}
}

func (s *sessionStore) getAgentConfigScoped(ctx context.Context, businessID string) (AgentConfigRow, bool, error) {
	r := defaultAgentConfigRow()
	var kwRaw, toolsRaw []byte
	var tz sql.NullString
	var updatedAt sql.NullTime

	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(platform, 'local'),
		       COALESCE(business_prompt, ''),
		       COALESCE(greeting_message, ''),
		       COALESCE(fallback_message, ''),
		       COALESCE(callback_greeting, ''),
		       COALESCE(voice_model, 'models/gemini-3.1-flash-live-preview'),
		       COALESCE(voice_enabled, true),
		       COALESCE(auto_answer, true),
		       COALESCE(auto_callback, true),
		       COALESCE(max_call_seconds, 0),
		       timezone,
		       callback_keywords,
		       tools_json,
		       updated_at
		FROM whatsapp_infra.agent_configs
		WHERE business_id = $1::uuid
	`, businessID).Scan(
		&r.Platform, &r.BusinessPrompt, &r.GreetingMessage, &r.FallbackMessage, &r.CallbackGreeting, &r.VoiceModel,
		&r.VoiceEnabled, &r.AutoAnswer, &r.AutoCallback, &r.MaxCallSeconds, &tz, &kwRaw, &toolsRaw, &updatedAt,
	)
	if err == sql.ErrNoRows {
		return r, false, nil
	}
	if err != nil {
		return r, false, err
	}
	if tz.Valid {
		r.Timezone = tz.String
	}
	if updatedAt.Valid {
		r.UpdatedAt = updatedAt.Time
	}
	if len(kwRaw) > 0 && string(kwRaw) != "null" {
		var kws []string
		if jsonErr := json.Unmarshal(kwRaw, &kws); jsonErr == nil {
			r.HasCallbackKeywords = true
			r.CallbackKeywords = kws
		}
	}
	if len(toolsRaw) > 0 && string(toolsRaw) != "null" {
		r.ToolsJSON = toolsRaw
	}
	return r, true, nil
}

func (s *sessionStore) getAgentConfig(ctx context.Context, businessID string) (AgentConfigRow, error) {
	r, exists, err := s.getAgentConfigScoped(ctx, businessID)
	if err != nil {
		return r, err
	}
	if !exists {
		return r, sql.ErrNoRows
	}
	return r, nil
}

func (s *sessionStore) upsertAgentConfig(ctx context.Context, businessID string, cfg AgentConfigRow) (time.Time, error) {
	var kwParam any
	if cfg.HasCallbackKeywords {
		if cfg.CallbackKeywords == nil {
			kwParam = "[]"
		} else {
			b, _ := json.Marshal(cfg.CallbackKeywords)
			kwParam = string(b)
		}
	}

	var toolsParam any
	if len(cfg.ToolsJSON) > 0 && string(cfg.ToolsJSON) != "null" {
		toolsParam = string(cfg.ToolsJSON)
	}

	var updatedAt time.Time
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO whatsapp_infra.agent_configs (
			business_id, platform, business_prompt, greeting_message, fallback_message, callback_greeting, voice_model,
			voice_enabled, auto_answer, auto_callback, max_call_seconds, timezone, callback_keywords, tools_json, updated_at
		) VALUES (
			$1::uuid, $2, $3, $4, $5, $6, $7,
			$8, $9, $10, $11, NULLIF($12, ''), $13::jsonb, $14::jsonb, NOW()
		)
		ON CONFLICT (business_id) DO UPDATE SET
			platform = EXCLUDED.platform,
			business_prompt = EXCLUDED.business_prompt,
			greeting_message = EXCLUDED.greeting_message,
			fallback_message = EXCLUDED.fallback_message,
			callback_greeting = EXCLUDED.callback_greeting,
			voice_model = EXCLUDED.voice_model,
			voice_enabled = EXCLUDED.voice_enabled,
			auto_answer = EXCLUDED.auto_answer,
			auto_callback = EXCLUDED.auto_callback,
			max_call_seconds = EXCLUDED.max_call_seconds,
			timezone = EXCLUDED.timezone,
			callback_keywords = EXCLUDED.callback_keywords,
			tools_json = EXCLUDED.tools_json,
			updated_at = NOW()
		RETURNING updated_at
	`,
		businessID, cfg.Platform, cfg.BusinessPrompt, cfg.GreetingMessage, cfg.FallbackMessage, cfg.CallbackGreeting, cfg.VoiceModel,
		cfg.VoiceEnabled, cfg.AutoAnswer, cfg.AutoCallback, cfg.MaxCallSeconds, cfg.Timezone, kwParam, toolsParam,
	).Scan(&updatedAt)
	return updatedAt, err
}

func (s *sessionStore) deleteAgentConfig(ctx context.Context, businessID, platform string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM whatsapp_infra.agent_configs
		WHERE business_id = $1::uuid AND COALESCE(platform, 'local') = $2
	`, businessID, platform)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}
