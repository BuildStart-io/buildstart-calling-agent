package main

import (
	"context"
	"database/sql"
	"log/slog"
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
	rows, err := db.QueryContext(ctx, `
		SELECT id, business_id::text, phone_number 
		FROM whatsapp_infra.scheduled_calls 
		WHERE status = 'pending' AND scheduled_time <= NOW()
		FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		log.Error("failed to query scheduled calls", "err", err)
		return
	}
	defer rows.Close()

	var calls []ScheduledCall
	for rows.Next() {
		var c ScheduledCall
		if err := rows.Scan(&c.ID, &c.BusinessID, &c.PhoneNumber); err == nil {
			calls = append(calls, c)
		}
	}
	rows.Close()

	for _, c := range calls {
		// Find session for this business_id
		var targetSession *Session
		for _, s := range mgr.allSessions() {
			if s.businessID == c.BusinessID {
				targetSession = s
				break
			}
		}

		if targetSession == nil {
			log.Warn("no active session for scheduled call", "business_id", c.BusinessID, "call_id", c.ID)
			updateScheduledCallStatus(ctx, db, c.ID, "failed_no_session")
			continue
		}

		// Prepare JID
		phone := c.PhoneNumber
		peer := types.NewJID(phone, types.DefaultUserServer)

		// Start outbound call
		_, err := targetSession.startOutgoing(ctx, peer, false)
		if err != nil {
			log.Error("failed to start scheduled call", "business_id", c.BusinessID, "call_id", c.ID, "err", err)
			updateScheduledCallStatus(ctx, db, c.ID, "failed_dial_error")
		} else {
			log.Info("successfully dialed scheduled call", "business_id", c.BusinessID, "call_id", c.ID)
			updateScheduledCallStatus(ctx, db, c.ID, "completed")
		}
	}
}

func updateScheduledCallStatus(ctx context.Context, db *sql.DB, id, status string) {
	_, _ = db.ExecContext(ctx, "UPDATE whatsapp_infra.scheduled_calls SET status = $1, updated_at = NOW() WHERE id = $2", status, id)
}
