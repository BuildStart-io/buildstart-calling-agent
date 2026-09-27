package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

type LeadDossier struct {
	BusinessName         string   `json:"business_name"`
	CallerRole           string   `json:"caller_role"`
	IdentifiedPainPoints []string `json:"identified_pain_points"`
	RecommendedPlan      string   `json:"recommended_plan"`
	ObjectionsRaised     string   `json:"objections_raised"`
	DealTemperature      string   `json:"deal_temperature"` // e.g. "🔥 Hot Lead (Score: 9/10)"
	LeadScore            string   `json:"lead_score"`
	WhatsAppSummary      string   `json:"whatsapp_summary_sinhala"`
	NextAction           string   `json:"next_action"`
}

// GenerateAndSendLeadDossier analyzes transcripts from a call using Gemini Flash,
// generates a structured CRM lead dossier, emits it to the broker, and sends a follow-up WhatsApp message.
func (s *Session) GenerateAndSendLeadDossier(callID string, peerJID types.JID, transcripts []CallTranscriptItem) {
	if len(transcripts) < 2 {
		return
	}

	apiKey := ""
	if s.mgr.agentConfig != nil {
		apiKey = s.mgr.agentConfig.RawKey()
	}
	if apiKey == "" {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var transcriptBuilder strings.Builder
		for _, t := range transcripts {
			role := "Customer"
			if t.Role == "assistant" || t.Role == "agent" {
				role = "Buildstart AI"
			}
			transcriptBuilder.WriteString(fmt.Sprintf("%s: %s\n", role, t.Text))
		}

		prompt := fmt.Sprintf(`You are the Senior Lead Intelligence AI for Buildstart (a WhatsApp CRM & Voice Automation software in Sri Lanka).
Analyze this live WhatsApp phone call transcript between our AI Voice Consultant and the caller:

TRANSCRIPT:
%s

Generate a comprehensive CRM Lead Dossier in STRICT JSON format with these exact keys:
{
  "business_name": "<Short description of business or store, e.g. Clothing Retailer in Colombo>",
  "caller_role": "<e.g. Business Owner / Manager / Customer>",
  "identified_pain_points": ["<Pain point 1>", "<Pain point 2>"],
  "recommended_plan": "<Recommended Buildstart Plan e.g. Starter (LKR 6,990) / Business (LKR 12,990) / Enterprise>",
  "objections_raised": "<Any objection, budget worry, or decision delay>",
  "deal_temperature": "<🔥 Hot Lead / 🟡 Warm Lead / ❄️ Cold Lead>",
  "lead_score": "<e.g. 8.5/10>",
  "whatsapp_summary_sinhala": "<Polite, personalized 2-3 sentence WhatsApp follow-up message in Sinhala thanking them, summarizing the package discussed, and offering a direct link or quick reply. Do NOT use markdown asterisks or robotic phrases.>",
  "next_action": "<Recommended next step for human sales team>"
}
Return ONLY valid JSON.`, transcriptBuilder.String())

		reqBody := map[string]any{
			"contents": []map[string]any{
				{
					"parts": []map[string]any{
						{"text": prompt},
					},
				},
			},
			"generationConfig": map[string]any{
				"responseMimeType": "application/json",
				"temperature":      0.2,
			},
		}

		jsonData, err := json.Marshal(reqBody)
		if err != nil {
			return
		}

		url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=%s", apiKey)
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonData))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			s.log.Error("failed to generate lead dossier from gemini", "err", err)
			return
		}
		defer resp.Body.Close()

		bodyBytes, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			s.log.Error("gemini dossier generation status error", "status", resp.StatusCode, "body", string(bodyBytes))
			return
		}

		var geminiResp struct {
			Candidates []struct {
				Content struct {
					Parts []struct {
						Text string `json:"text"`
					} `json:"parts"`
				} `json:"content"`
			} `json:"candidates"`
		}

		if err := json.Unmarshal(bodyBytes, &geminiResp); err != nil || len(geminiResp.Candidates) == 0 {
			return
		}

		rawJSON := geminiResp.Candidates[0].Content.Parts[0].Text
		var dossier LeadDossier
		if err := json.Unmarshal([]byte(rawJSON), &dossier); err != nil {
			s.log.Error("failed to unmarshal lead dossier json", "err", err, "raw", rawJSON)
			return
		}

		s.log.Info("🎯 Lead Dossier generated successfully",
			"call_id", callID,
			"business", dossier.BusinessName,
			"temperature", dossier.DealTemperature,
			"score", dossier.LeadScore,
			"recommended_plan", dossier.RecommendedPlan,
		)

		// Record in Call Broker event log
		dossierSummary := fmt.Sprintf("[%s - Score %s] %s | Plan: %s | Next: %s",
			dossier.DealTemperature, dossier.LeadScore, dossier.BusinessName, dossier.RecommendedPlan, dossier.NextAction)
		s.mgr.broker.recordCallEvent(callID, "lead_dossier", dossierSummary, rawJSON)
		s.mgr.broker.setCallOutcome(callID, fmt.Sprintf("%s (%s)", dossier.DealTemperature, dossier.RecommendedPlan), dossierSummary)

		// Send WhatsApp automated customer follow-up message
		if dossier.WhatsAppSummary != "" && !peerJID.IsEmpty() {
			targetJID := peerJID
			if targetJID.Server == "lid" {
				if pn := s.mgr.store.getPNForLID(ctx, targetJID.User); pn != "" {
					targetJID = types.NewJID(pn, types.DefaultUserServer)
				}
			}

			// Add a short delay so caller is off the phone when receiving the message
			time.Sleep(3 * time.Second)
			msg := &waProto.Message{
				Conversation: proto.String(dossier.WhatsAppSummary),
			}
			if resp, err := s.client.SendMessage(ctx, targetJID, msg); err == nil {
				s.log.Info("WhatsApp post-call lead follow-up sent to customer", "to", targetJID.String(), "msg_id", resp.ID)
				s.mgr.broker.recordCallEvent(callID, "whatsapp_followup", "Automated WhatsApp summary sent to customer", dossier.WhatsAppSummary)
			} else {
				s.log.Error("failed to send WhatsApp lead follow-up", "to", targetJID.String(), "err", err)
			}
		}
	}()
}
