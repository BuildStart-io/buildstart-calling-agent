package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/types"
)

type PostCallWebhookPayload struct {
	Event           string               `json:"event"`
	CallID          string               `json:"call_id"`
	SessionID       string               `json:"session_id"`
	BusinessID      string               `json:"business_id"`
	CallerNumber    string               `json:"caller_number"`
	PeerJID         string               `json:"peer_jid"`
	Direction       string               `json:"direction"`
	DurationSeconds int                  `json:"duration_seconds"`
	Transcripts     []CallTranscriptItem `json:"transcripts"`
}

// dispatchPostCallWebhook sends a generic call.completed payload to POST_CALL_WEBHOOK_URL
// when an answered call finishes, leaving all domain/CRM intelligence to the upstream platform.
func (s *Session) dispatchPostCallWebhook(
	callID string,
	peerJID types.JID,
	callerNumber string,
	direction string,
	durationSeconds int,
	transcripts []CallTranscriptItem,
) {
	webhookURL := strings.TrimSpace(os.Getenv("POST_CALL_WEBHOOK_URL"))
	if webhookURL == "" || len(transcripts) < 2 {
		return
	}

	webhookSecret := strings.TrimSpace(os.Getenv("POST_CALL_WEBHOOK_SECRET"))

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		targetJID := peerJID
		if targetJID.Server == "lid" {
			if pn := s.mgr.store.getPNForLID(ctx, targetJID.User); pn != "" {
				targetJID = types.NewJID(pn, types.DefaultUserServer)
			}
		}

		payload := PostCallWebhookPayload{
			Event:           "call.completed",
			CallID:          callID,
			SessionID:       s.id,
			BusinessID:      s.businessID,
			CallerNumber:    callerNumber,
			PeerJID:         targetJID.String(),
			Direction:       direction,
			DurationSeconds: durationSeconds,
			Transcripts:     transcripts,
		}

		body, err := json.Marshal(payload)
		if err != nil {
			s.log.Error("failed to marshal post-call webhook payload", "call_id", callID, "err", err)
			return
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
		if err != nil {
			s.log.Error("failed to create post-call webhook request", "call_id", callID, "err", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if webhookSecret != "" {
			req.Header.Set("X-Webhook-Secret", webhookSecret)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			s.log.Error("post-call webhook request failed", "call_id", callID, "err", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			s.log.Warn("post-call webhook returned non-2xx status", "call_id", callID, "status", resp.StatusCode)
			return
		}
		s.log.Info("post-call webhook dispatched successfully", "call_id", callID, "status", resp.StatusCode)
	}()
}

