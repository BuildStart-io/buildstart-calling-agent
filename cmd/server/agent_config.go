package main

import (
	"sync"
)

type AgentConfig struct {
	mu                sync.RWMutex
	Enabled           bool   `json:"enabled"`
	AutoAnswer        bool   `json:"autoAnswer"`
	OpenRouterKey     string `json:"openRouterKey"`
	Model             string `json:"model"`
	SystemPrompt      string `json:"systemPrompt"`
	Voice             string `json:"voice"`
	AzureSpeechKey    string `json:"azureSpeechKey"`
	AzureSpeechRegion string `json:"azureSpeechRegion"`
	GoogleCloudKey    string `json:"googleCloudKey"`
	HfToken           string `json:"hfToken"`
	CustomTtsURL      string `json:"customTtsUrl"`
	CustomTtsKey      string `json:"customTtsKey"`
}

func newAgentConfig(openRouterKey, model, systemPrompt, voice string, autoAnswer, enabled bool) *AgentConfig {
	if model == "" {
		model = "models/gemini-3.1-flash-live-preview"
	}
	if systemPrompt == "" {
		systemPrompt = "You are a helpful AI assistant." // Will be overridden dynamically per session
	}
	if voice == "" {
		voice = "dialog-nipunika"
	}
	return &AgentConfig{
		Enabled:           enabled,
		AutoAnswer:        autoAnswer,
		OpenRouterKey:     openRouterKey,
		Model:             model,
		SystemPrompt:      systemPrompt,
		Voice:             voice,
		AzureSpeechRegion: "eastus",
	}
}

func (c *AgentConfig) Get() AgentConfigSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return AgentConfigSnapshot{
		Enabled:           c.Enabled,
		AutoAnswer:        c.AutoAnswer,
		OpenRouterKey:     maskKey(c.OpenRouterKey),
		HasKey:            c.OpenRouterKey != "",
		Model:             c.Model,
		SystemPrompt:      c.SystemPrompt,
		Voice:             c.Voice,
		AzureSpeechKey:    maskKey(c.AzureSpeechKey),
		HasAzureKey:       c.AzureSpeechKey != "",
		AzureSpeechRegion: c.AzureSpeechRegion,
		GoogleCloudKey:    maskKey(c.GoogleCloudKey),
		HasGoogleKey:      c.GoogleCloudKey != "",
		HfToken:           maskKey(c.HfToken),
		HasHfToken:        c.HfToken != "",
		CustomTtsURL:      c.CustomTtsURL,
		CustomTtsKey:      maskKey(c.CustomTtsKey),
		HasCustomTtsKey:   c.CustomTtsKey != "",
	}
}

func (c *AgentConfig) Update(enabled, autoAnswer *bool, key, model, prompt, voice, azureKey, azureRegion, googleKey, hfToken, customTtsUrl, customTtsKey *string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if enabled != nil {
		c.Enabled = *enabled
	}
	if autoAnswer != nil {
		c.AutoAnswer = *autoAnswer
	}
	if key != nil && *key != "" {
		c.OpenRouterKey = *key
	}
	if model != nil && *model != "" {
		c.Model = *model
	}
	if prompt != nil && *prompt != "" {
		c.SystemPrompt = *prompt
	}
	if voice != nil && *voice != "" {
		c.Voice = *voice
	}
	if azureKey != nil && *azureKey != "" {
		c.AzureSpeechKey = *azureKey
	}
	if azureRegion != nil && *azureRegion != "" {
		c.AzureSpeechRegion = *azureRegion
	}
	if googleKey != nil && *googleKey != "" {
		c.GoogleCloudKey = *googleKey
	}
	if hfToken != nil && *hfToken != "" {
		c.HfToken = *hfToken
	}
	if customTtsUrl != nil {
		c.CustomTtsURL = *customTtsUrl
	}
	if customTtsKey != nil && *customTtsKey != "" {
		c.CustomTtsKey = *customTtsKey
	}
}

func (c *AgentConfig) RawKey() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.OpenRouterKey
}

func (c *AgentConfig) CurrentModel() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Model
}

func (c *AgentConfig) CurrentPrompt() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.SystemPrompt
}

func (c *AgentConfig) CurrentVoice() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Voice
}

func (c *AgentConfig) AzureConfig() (string, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.AzureSpeechKey, c.AzureSpeechRegion
}

func (c *AgentConfig) GoogleConfig() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.GoogleCloudKey
}

func (c *AgentConfig) HuggingFaceToken() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.HfToken
}

func (c *AgentConfig) CustomTts() (string, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.CustomTtsURL, c.CustomTtsKey
}

func (c *AgentConfig) IsAutoAnswer() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.AutoAnswer
}

func (c *AgentConfig) IsEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Enabled
}

type AgentConfigSnapshot struct {
	Enabled           bool   `json:"enabled"`
	AutoAnswer        bool   `json:"autoAnswer"`
	OpenRouterKey     string `json:"openRouterKey"`
	HasKey            bool   `json:"hasKey"`
	Model             string `json:"model"`
	SystemPrompt      string `json:"systemPrompt"`
	Voice             string `json:"voice"`
	AzureSpeechKey    string `json:"azureSpeechKey"`
	HasAzureKey       bool   `json:"hasAzureKey"`
	AzureSpeechRegion string `json:"azureSpeechRegion"`
	GoogleCloudKey    string `json:"googleCloudKey"`
	HasGoogleKey      bool   `json:"hasGoogleKey"`
	HfToken           string `json:"hfToken"`
	HasHfToken        bool   `json:"hasHfToken"`
	CustomTtsURL      string `json:"customTtsUrl"`
	CustomTtsKey      string `json:"customTtsKey"`
	HasCustomTtsKey   bool   `json:"hasCustomTtsKey"`
}

func maskKey(k string) string {
	if len(k) <= 8 {
		if k == "" {
			return ""
		}
		return "****"
	}
	return k[:6] + "..." + k[len(k)-4:]
}
