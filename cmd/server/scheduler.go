package main

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/types"
)

type ScheduledCall struct {
	ID          string
	BusinessID  string
	PhoneNumber string
}

func startScheduler(ctx context.Context, db *sql.DB, mgr *SessionManager, log *slog.Logger) {
	ticker := time.NewTicker(60 * time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				processScheduledCalls(ctx, db, mgr, log)
			}
		}
	}()
}

func processScheduledCalls(ctx context.Context, db *sql.DB, mgr *SessionManager, log *slog.Logger) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Error("failed to begin scheduler transaction", "err", err)
		return
	}

	rows, err := tx.QueryContext(ctx, `
		WITH due AS (
			SELECT id
			FROM whatsapp_infra.scheduled_calls
			WHERE status = 'pending' AND scheduled_time <= NOW()
			ORDER BY scheduled_time ASC
			LIMIT 20
			FOR UPDATE SKIP LOCKED
		)
		UPDATE whatsapp_infra.scheduled_calls sc
		SET status = 'calling', updated_at = NOW()
		FROM due
		WHERE sc.id = due.id
		RETURNING sc.id::text, sc.business_id::text, sc.phone_number
	`)
	if err != nil {
		_ = tx.Rollback()
		log.Error("failed to claim scheduled calls", "err", err)
		return
	}

	var calls []ScheduledCall
	for rows.Next() {
		var c ScheduledCall
		if err := rows.Scan(&c.ID, &c.BusinessID, &c.PhoneNumber); err == nil {
			calls = append(calls, c)
		}
	}
	_ = rows.Close()
	if err := tx.Commit(); err != nil {
		log.Error("failed to commit scheduler claim transaction", "err", err)
		return
	}

	for _, c := range calls {
		// Strict tenant isolation: never fall back to another business's WhatsApp session.
		var targetSession *Session
		for _, s := range mgr.allSessions() {
			if s.businessID == c.BusinessID && s.client != nil && s.client.IsConnected() {
				targetSession = s
				break
			}
		}

		if targetSession == nil {
			log.Warn("no active session for scheduled call", "business_id", c.BusinessID, "call_id", c.ID)
			updateScheduledCallOutcome(ctx, db, c.ID, "failed_no_session", "", "no connected whatsapp session for business_id")
			continue
		}

		phone := normalizePhone(strings.TrimSpace(c.PhoneNumber))
		if phone == "" {
			updateScheduledCallOutcome(ctx, db, c.ID, "failed_invalid_number", "", "empty phone number")
			continue
		}
		peer := types.NewJID(phone, types.DefaultUserServer)

		callID, err := targetSession.startOutgoing(ctx, peer, false)
		if err != nil {
			log.Error("failed to start scheduled call", "business_id", c.BusinessID, "id", c.ID, "err", err)
			updateScheduledCallOutcome(ctx, db, c.ID, "failed_dial_error", "", err.Error())
		} else {
			log.Info("successfully dialed scheduled call", "business_id", c.BusinessID, "id", c.ID, "call_id", callID)
			updateScheduledCallOutcome(ctx, db, c.ID, "completed", callID, "")
		}
	}
}

func updateScheduledCallOutcome(ctx context.Context, db *sql.DB, id, status, callID, errMsg string) {
	_, _ = db.ExecContext(ctx, `
		UPDATE whatsapp_infra.scheduled_calls
		SET status = $1,
		    call_id = NULLIF($2, ''),
		    error_message = NULLIF($3, ''),
		    updated_at = NOW()
		WHERE id = $4::uuid
	`, status, callID, errMsg, id)
}
