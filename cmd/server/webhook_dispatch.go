package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// dispatchPostCallWebhook preserves Local CRM's exact webhook contract on POST_CALL_WEBHOOK_URL.
func (s *Session) dispatchPostCallWebhook(
	callID string,
	peerJID types.JID,
	callerNumber string,
	direction string,
	durationSeconds int,
	transcripts []CallTranscriptItem,
) {
	if s.platform != "local" {
		return
	}
	webhookURL := strings.TrimSpace(os.Getenv("POST_CALL_WEBHOOK_URL"))
	if webhookURL == "" || len(transcripts) < 2 {
		return
	}

	webhookSecret := strings.TrimSpace(os.Getenv("POST_CALL_WEBHOOK_SECRET"))

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		targetJID := s.resolvePhoneJID(ctx, peerJID)

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

// resolvePhoneJID converts a @lid JID to a phone @s.whatsapp.net JID if known.
func (s *Session) resolvePhoneJID(ctx context.Context, jid types.JID) types.JID {
	out := jid.ToNonAD()
	if out.Server == "lid" || out.Server == "" {
		if pn := s.mgr.store.getPNForLID(ctx, out.User); pn != "" {
			out = types.NewJID(pn, types.DefaultUserServer)
		}
	}
	return out
}

// enqueueGlobalEvent persists an ordered platform event in whatsapp_infra.webhook_outbox,
// retrying transient DB insert failures synchronously briefly and then asynchronously so events are not dropped.
func (s *Session) enqueueGlobalEvent(ctx context.Context, partitionKey, eventType string, data any) {
	if s.platform != "global" || s.mgr == nil || s.mgr.outbox == nil {
		return
	}
	var lastErr error
	syncDelays := []time.Duration{0, 50 * time.Millisecond, 150 * time.Millisecond}
	for _, d := range syncDelays {
		if d > 0 {
			time.Sleep(d)
		}
		attemptCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := s.mgr.outbox.Enqueue(attemptCtx, s.platform, s.id, s.businessID, partitionKey, eventType, data)
		cancel()
		if err == nil {
			return
		}
		lastErr = err
	}
	s.log.Warn("outbox insert failed synchronously; retrying asynchronously", "event", eventType, "err", lastErr)
	go func() {
		for _, delay := range []time.Duration{1 * time.Second, 2 * time.Second, 5 * time.Second, 15 * time.Second} {
			time.Sleep(delay)
			attemptCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := s.mgr.outbox.Enqueue(attemptCtx, s.platform, s.id, s.businessID, partitionKey, eventType, data)
			cancel()
			if err == nil {
				s.log.Info("outbox insert succeeded on background retry", "event", eventType)
				return
			}
			lastErr = err
		}
		s.log.Error("failed to enqueue global outbox event after retries", "event", eventType, "err", lastErr)
	}()
}

func platformToolsURL(platform string) (toolsURL, secret string) {
	p := strings.ToUpper(strings.TrimSpace(platform))
	if p == "" {
		return "", ""
	}
	toolsURL = strings.TrimSpace(os.Getenv("PLATFORM_" + p + "_TOOLS_URL"))
	if toolsURL == "" {
		toolsURL = strings.TrimSpace(os.Getenv("PLATFORM_" + p + "_WEBHOOK_URL"))
	}
	secret = strings.TrimSpace(os.Getenv("PLATFORM_" + p + "_WEBHOOK_SECRET"))
	return toolsURL, secret
}

// invokePlatformTool executes a synchronous tool.invoke call for live voice calls (timeout <= 8s).
func (s *Session) invokePlatformTool(
	ctx context.Context,
	callID string,
	callerNumber string,
	peerJID types.JID,
	clientRef string,
	invocationID string,
	name string,
	args map[string]any,
) (map[string]any, error) {
	toolsURL, secret := platformToolsURL(s.platform)
	if toolsURL == "" {
		return nil, fmt.Errorf("platform tools URL not configured")
	}

	resolvedPeer := s.resolvePhoneJID(ctx, peerJID)
	dataPayload := map[string]any{
		"invocation_id": invocationID,
		"call_id":       callID,
		"caller_number": callerNumber,
		"peer_jid":      resolvedPeer.String(),
		"name":          name,
		"args":          args,
	}
	if clientRef != "" {
		dataPayload["client_ref"] = clientRef
	}
	rawData, err := json.Marshal(dataPayload)
	if err != nil {
		return nil, err
	}

	env := GlobalWebhookEnvelope{
		EventID:    newEventID(),
		Event:      "tool.invoke",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Platform:   s.platform,
		SessionID:  s.id,
		BusinessID: s.businessID,
		Data:       json.RawMessage(rawData),
	}
	body, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, toolsURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if secret != "" {
		req.Header.Set("X-Engine-Signature", signEngineWebhook(secret, time.Now().Unix(), body))
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("tool endpoint returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBytes)))
	}

	var parsed struct {
		Result any    `json:"result"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return nil, fmt.Errorf("invalid tool response json: %w", err)
	}
	if parsed.Error != "" {
		return map[string]any{"error": parsed.Error}, nil
	}
	if m, ok := parsed.Result.(map[string]any); ok {
		return m, nil
	}
	return map[string]any{"output": parsed.Result}, nil
}
