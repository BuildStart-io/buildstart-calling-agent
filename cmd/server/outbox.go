package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	outboxWorkers     = 16
	outboxMaxAttempts = 6
)

var outboxBackoffs = []time.Duration{
	2 * time.Second,
	6 * time.Second,
	18 * time.Second,
	45 * time.Second,
	120 * time.Second,
}

type GlobalWebhookEnvelope struct {
	EventID    string          `json:"event_id"`
	Event      string          `json:"event"`
	OccurredAt string          `json:"occurred_at"`
	Platform   string          `json:"platform"`
	SessionID  string          `json:"session_id"`
	BusinessID string          `json:"business_id"`
	Data       json.RawMessage `json:"data"`
}

type outboxRow struct {
	Seq          int64
	EventID      string
	Platform     string
	SessionID    string
	BusinessID   string
	PartitionKey string
	EventType    string
	Payload      []byte
	Attempts     int
}

// OutboxDispatcher persists Global webhook events in PostgreSQL and delivers them
// with strict per-chat FIFO ordering across 16 sharded workers.
type OutboxDispatcher struct {
	db         *sql.DB
	log        *slog.Logger
	httpClient *http.Client
	notifyChs  [outboxWorkers]chan struct{}
	stopOnce   sync.Once
}

func newEventID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "evt_" + hex.EncodeToString(b)
}

func signEngineWebhook(secret string, ts int64, body []byte) string {
	tsStr := strconv.FormatInt(ts, 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(tsStr))
	mac.Write([]byte("."))
	mac.Write(body)
	return fmt.Sprintf("t=%s,v1=%s", tsStr, hex.EncodeToString(mac.Sum(nil)))
}

func platformWebhookConfig(platform string) (webhookURL, secret string) {
	p := strings.ToUpper(strings.TrimSpace(platform))
	if p == "" {
		return "", ""
	}
	webhookURL = strings.TrimSpace(os.Getenv("PLATFORM_" + p + "_WEBHOOK_URL"))
	secret = strings.TrimSpace(os.Getenv("PLATFORM_" + p + "_WEBHOOK_SECRET"))
	return webhookURL, secret
}

func newOutboxDispatcher(ctx context.Context, db *sql.DB, log *slog.Logger) *OutboxDispatcher {
	d := &OutboxDispatcher{
		db:         db,
		log:        log.With("component", "outbox"),
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
	for i := range d.notifyChs {
		d.notifyChs[i] = make(chan struct{}, 1)
	}
	if db == nil {
		return d
	}

	// Recover any events left mid-flight from a prior crash/restart.
	if _, err := db.ExecContext(ctx, `UPDATE whatsapp_infra.webhook_outbox SET status = 'pending', next_attempt_at = NOW() WHERE status = 'delivering'`); err != nil {
		d.log.Warn("failed to recover stale outbox rows on startup", "err", err)
	}

	for shard := 0; shard < outboxWorkers; shard++ {
		go d.runWorker(ctx, shard)
	}
	go d.runMaintenance(ctx)
	return d
}

// Enqueue persists an event synchronously in webhook_outbox and wakes workers.
func (d *OutboxDispatcher) Enqueue(ctx context.Context, platform, sessionID, businessID, partitionKey, eventType string, data any) (string, error) {
	if d == nil || d.db == nil {
		return "", fmt.Errorf("outbox not initialized")
	}
	url, _ := platformWebhookConfig(platform)
	if url == "" {
		return "", nil
	}
	rawData, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	if partitionKey == "" {
		partitionKey = sessionID
	}
	evID := newEventID()
	_, err = d.db.ExecContext(ctx, `
		INSERT INTO whatsapp_infra.webhook_outbox (
			event_id, platform, session_id, business_id, partition_key, event_type, payload
		) VALUES ($1, $2, $3, $4::uuid, $5, $6, $7::jsonb)
	`, evID, platform, sessionID, businessID, partitionKey, eventType, string(rawData))
	if err != nil {
		return "", err
	}
	d.wakeAll()
	return evID, nil
}

func (d *OutboxDispatcher) wakeAll() {
	for i := range d.notifyChs {
		select {
		case d.notifyChs[i] <- struct{}{}:
		default:
		}
	}
}

func (d *OutboxDispatcher) runWorker(ctx context.Context, shard int) {
	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()

	for {
		for {
			if ctx.Err() != nil {
				return
			}
			ev, err := d.claimNext(ctx, shard)
			if err != nil {
				d.log.Warn("outbox claim failed", "shard", shard, "err", err)
				break
			}
			if ev == nil {
				break
			}
			d.deliver(ctx, ev)
		}

		select {
		case <-ctx.Done():
			return
		case <-d.notifyChs[shard]:
		case <-ticker.C:
		}
	}
}

func (d *OutboxDispatcher) claimNext(ctx context.Context, shard int) (*outboxRow, error) {
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	row := d.db.QueryRowContext(qCtx, `
		WITH candidate AS (
			SELECT w.event_id
			FROM whatsapp_infra.webhook_outbox w
			WHERE w.status = 'pending'
			  AND w.next_attempt_at <= NOW()
			  AND ((hashtext(w.partition_key)) & 15) = $1
			  AND NOT EXISTS (
				SELECT 1 FROM whatsapp_infra.webhook_outbox prev
				WHERE prev.partition_key = w.partition_key
				  AND prev.seq < w.seq
				  AND prev.status IN ('pending', 'delivering')
			  )
			ORDER BY w.seq ASC
			LIMIT 1
			FOR UPDATE OF w SKIP LOCKED
		)
		UPDATE whatsapp_infra.webhook_outbox o
		SET status = 'delivering',
		    next_attempt_at = NOW() + INTERVAL '2 minutes'
		FROM candidate c
		WHERE o.event_id = c.event_id
		RETURNING o.seq, o.event_id, o.platform, o.session_id, o.business_id::text,
		          o.partition_key, o.event_type, o.payload, o.attempts
	`, shard)

	var ev outboxRow
	err := row.Scan(
		&ev.Seq, &ev.EventID, &ev.Platform, &ev.SessionID, &ev.BusinessID,
		&ev.PartitionKey, &ev.EventType, &ev.Payload, &ev.Attempts,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &ev, nil
}

func (d *OutboxDispatcher) deliver(ctx context.Context, ev *outboxRow) {
	webhookURL, secret := platformWebhookConfig(ev.Platform)
	if webhookURL == "" {
		d.markDead(ctx, ev.EventID, ev.Attempts+1, "webhook URL not configured for platform")
		return
	}

	env := GlobalWebhookEnvelope{
		EventID:    ev.EventID,
		Event:      ev.EventType,
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Platform:   ev.Platform,
		SessionID:  ev.SessionID,
		BusinessID: ev.BusinessID,
		Data:       json.RawMessage(ev.Payload),
	}
	body, err := json.Marshal(env)
	if err != nil {
		d.markDead(ctx, ev.EventID, ev.Attempts+1, "marshal envelope: "+err.Error())
		return
	}

	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		d.markDead(ctx, ev.EventID, ev.Attempts+1, "build request: "+err.Error())
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if secret != "" {
		sig := signEngineWebhook(secret, time.Now().Unix(), body)
		req.Header.Set("X-Engine-Signature", sig)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		d.scheduleRetryOrDead(ctx, ev, true, err.Error())
		return
	}
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = d.db.ExecContext(context.Background(), `
			UPDATE whatsapp_infra.webhook_outbox
			SET status = 'delivered', delivered_at = NOW(), attempts = $2, last_error = NULL
			WHERE event_id = $1
		`, ev.EventID, ev.Attempts+1)
		d.wakeAll()
		return
	}

	retryable := resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
	errMsg := fmt.Sprintf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	d.scheduleRetryOrDead(ctx, ev, retryable, errMsg)
}

func (d *OutboxDispatcher) scheduleRetryOrDead(ctx context.Context, ev *outboxRow, retryable bool, errMsg string) {
	nextAttempts := ev.Attempts + 1
	if !retryable || nextAttempts >= outboxMaxAttempts {
		d.markDead(ctx, ev.EventID, nextAttempts, errMsg)
		d.wakeAll()
		return
	}
	idx := ev.Attempts
	if idx < 0 {
		idx = 0
	}
	if idx >= len(outboxBackoffs) {
		idx = len(outboxBackoffs) - 1
	}
	delay := outboxBackoffs[idx]
	d.log.Warn("outbox delivery retrying", "event_id", ev.EventID, "event", ev.EventType, "attempt", nextAttempts, "backoff", delay, "err", errMsg)
	_, _ = d.db.ExecContext(context.Background(), `
		UPDATE whatsapp_infra.webhook_outbox
		SET status = 'pending',
		    attempts = $2,
		    next_attempt_at = NOW() + ($3 * INTERVAL '1 millisecond'),
		    last_error = $4
		WHERE event_id = $1
	`, ev.EventID, nextAttempts, delay.Milliseconds(), errMsg)
}

func (d *OutboxDispatcher) markDead(ctx context.Context, eventID string, attempts int, errMsg string) {
	d.log.Error("outbox event marked dead", "event_id", eventID, "attempts", attempts, "err", errMsg)
	_, _ = d.db.ExecContext(context.Background(), `
		UPDATE whatsapp_infra.webhook_outbox
		SET status = 'dead', attempts = $2, last_error = $3
		WHERE event_id = $1
	`, eventID, attempts, errMsg)
}

func (d *OutboxDispatcher) runMaintenance(ctx context.Context) {
	staleTicker := time.NewTicker(1 * time.Minute)
	cleanupTicker := time.NewTicker(1 * time.Hour)
	defer staleTicker.Stop()
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-staleTicker.C:
			_, _ = d.db.ExecContext(ctx, `
				UPDATE whatsapp_infra.webhook_outbox
				SET status = 'pending', next_attempt_at = NOW()
				WHERE status = 'delivering' AND next_attempt_at <= NOW()
			`)
		case <-cleanupTicker.C:
			_, _ = d.db.ExecContext(ctx, `
				DELETE FROM whatsapp_infra.webhook_outbox
				WHERE status = 'delivered' AND delivered_at < NOW() - INTERVAL '7 days'
			`)
		}
	}
}
