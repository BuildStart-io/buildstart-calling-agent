package main

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var uuidRegex = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func isValidUUID(v string) bool {
	return uuidRegex.MatchString(strings.TrimSpace(v))
}

type agentConfigPayload struct {
	BusinessPrompt   string          `json:"business_prompt"`
	GreetingMessage  string          `json:"greeting_message"`
	FallbackMessage  string          `json:"fallback_message"`
	CallbackGreeting string          `json:"callback_greeting"`
	VoiceModel       string          `json:"voice_model"`
	VoiceEnabled     *bool           `json:"voice_enabled"`
	AutoAnswer       *bool           `json:"auto_answer"`
	AutoCallback     *bool           `json:"auto_callback"`
	CallbackKeywords []string        `json:"callback_keywords"`
	MaxCallSeconds   *int            `json:"max_call_seconds"`
	Timezone         string          `json:"timezone"`
	Tools            json.RawMessage `json:"tools"`
}

func (s *server) handleGetAgentConfig(w http.ResponseWriter, r *http.Request) {
	plat := callerPlatform(r)
	businessID := strings.TrimSpace(r.PathValue("business_id"))
	if !isValidUUID(businessID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid business_id uuid"})
		return
	}

	cfg, exists, err := s.sessions.store.getAgentConfigScoped(r.Context(), businessID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !exists || cfg.Platform != plat {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "agent config not found"})
		return
	}

	var toolsAny any
	if len(cfg.ToolsJSON) > 0 && string(cfg.ToolsJSON) != "null" {
		_ = json.Unmarshal(cfg.ToolsJSON, &toolsAny)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"business_id":       businessID,
		"platform":          cfg.Platform,
		"business_prompt":   cfg.BusinessPrompt,
		"greeting_message":  cfg.GreetingMessage,
		"fallback_message":  cfg.FallbackMessage,
		"callback_greeting": cfg.CallbackGreeting,
		"voice_model":       cfg.VoiceModel,
		"voice_enabled":     cfg.VoiceEnabled,
		"auto_answer":       cfg.AutoAnswer,
		"auto_callback":     cfg.AutoCallback,
		"callback_keywords": cfg.CallbackKeywords,
		"max_call_seconds":  cfg.MaxCallSeconds,
		"timezone":          cfg.Timezone,
		"tools":             toolsAny,
		"updated_at":        cfg.UpdatedAt.UTC().Format(time.RFC3339),
	})
}

func (s *server) handlePutAgentConfig(w http.ResponseWriter, r *http.Request) {
	plat := callerPlatform(r)
	businessID := strings.TrimSpace(r.PathValue("business_id"))
	if !isValidUUID(businessID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid business_id uuid"})
		return
	}

	var body agentConfigPayload
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
		return
	}

	// Verify ownership across existing agent_configs or whatsapp_sessions rows.
	existingCfg, cfgExists, err := s.sessions.store.getAgentConfigScoped(r.Context(), businessID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if cfgExists && existingCfg.Platform != plat {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	if sessPlat, sessExists, err := s.sessions.store.getSessionPlatformByBusiness(r.Context(), businessID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	} else if sessExists && sessPlat != plat {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}

	tz := strings.TrimSpace(body.Timezone)
	if tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid IANA timezone"})
			return
		}
	}

	maxCallSec := 0
	if body.MaxCallSeconds != nil {
		if *body.MaxCallSeconds < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max_call_seconds must be >= 0"})
			return
		}
		maxCallSec = *body.MaxCallSeconds
	}

	voiceEnabled := true
	if body.VoiceEnabled != nil {
		voiceEnabled = *body.VoiceEnabled
	}
	autoAnswer := true
	if body.AutoAnswer != nil {
		autoAnswer = *body.AutoAnswer
	}
	autoCallback := true
	if body.AutoCallback != nil {
		autoCallback = *body.AutoCallback
	}

	var toolsRaw []byte
	if len(body.Tools) > 0 && string(bytesTrimSpace(body.Tools)) != "null" {
		if bytesTrimSpace(body.Tools)[0] != '[' {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tools must be a JSON array"})
			return
		}
		toolsRaw = body.Tools
	}

	voice := strings.TrimSpace(body.VoiceModel)
	if voice == "" {
		voice = "Aoede"
	}

	row := AgentConfigRow{
		Platform:            plat,
		BusinessPrompt:      body.BusinessPrompt,
		GreetingMessage:     body.GreetingMessage,
		FallbackMessage:     body.FallbackMessage,
		CallbackGreeting:    body.CallbackGreeting,
		VoiceModel:          voice,
		VoiceEnabled:        voiceEnabled,
		AutoAnswer:          autoAnswer,
		AutoCallback:        autoCallback,
		HasCallbackKeywords: body.CallbackKeywords != nil,
		CallbackKeywords:    body.CallbackKeywords,
		MaxCallSeconds:      maxCallSec,
		Timezone:            tz,
		ToolsJSON:           toolsRaw,
	}

	updatedAt, err := s.sessions.store.upsertAgentConfig(r.Context(), businessID, row)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"business_id": businessID,
		"updated_at":  updatedAt.UTC().Format(time.RFC3339),
	})
}

func (s *server) handleDeleteAgentConfig(w http.ResponseWriter, r *http.Request) {
	plat := callerPlatform(r)
	businessID := strings.TrimSpace(r.PathValue("business_id"))
	if !isValidUUID(businessID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid business_id uuid"})
		return
	}

	deleted, err := s.sessions.store.deleteAgentConfig(r.Context(), businessID, plat)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !deleted {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "agent config not found"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func bytesTrimSpace(b []byte) []byte {
	return []byte(strings.TrimSpace(string(b)))
}
