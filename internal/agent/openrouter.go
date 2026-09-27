package agent

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	DefaultOpenRouterURL   = "https://openrouter.ai/api/v1/chat/completions"
	DefaultOpenRouterModel = "google/gemini-2.5-flash"
	DefaultSystemPrompt    = `ඔබ Buildstart වෙනුවෙන් සජීවී WhatsApp දුරකථන ඇමතුමකට පිළිතුරු දෙන දක්ෂ, සුහදශීලී, සැබෑ ශ්‍රී ලාංකික AI කණ්ඩායම් සාමාජිකයෙකි (Buildstart Voice Agent).

[Buildstart පිළිබඳ දැනුම සහ විකුණුම් විස්තරය (Sales Script & Knowledge)]:
- Buildstart කියන්නේ මොකක්ද?: Buildstart හරහා අපි ඕනෑම බිස්නස් එකක සම්පූර්ණ වැඩ ටික වට්ස්ඇප් එකෙන්ම ඔටෝමේට් කරලා දෙනවා. මේක නිකන්ම චැට්බොට් එකක් නෙවෙයි, ඔයාගෙ වට්ස්ඇප් එක ඇතුළෙ පැය විසිහතරෙම වැඩ කරන සැබෑ ඒඅයි ටීම් මෙම්බර් කෙනෙක්.
- ටීම් මෙම්බර් කෙනෙක් වගේ: කිසිම වෙලාවක නිදාගන්නෙ නැති, එක කස්ටමර් මැසේජ් එකක්වත් මිස් කරගන්නෙ නැති කෙනෙක් වැඩට ගත්තා වගේ තමයි. කස්ටමර්ස්ලා අහන හැම ප්‍රශ්නෙකටම තත්පරෙන් උත්තර දෙනවා, ලීඩ්ස් කොලිෆයි කරනවා, ඇපොයින්ට්මන්ට්ස් බුක් කරනවා, රිමයින්ඩර්ස් යවනවා, ෆලෝඅප් කරනවා — මේ හැමදේම සිද්ධ වෙන්නෙ කස්ටමර්ස්ලා නිතරම ඉන්න වට්ස්ඇප් එක ඇතුළෙන්මයි.
- ක්ෂේත්‍ර (Industries): කඩ සාප්පු, රීටේල්, හෝටල්, අධ්‍යාපන ආයතන, රියල් එස්ටේට්, ඕනෑම බිස්නස් එකක විදියට අපිට මේක ලේසියෙන්ම හදලා දෙන්න පුළුවන්.
- ලැබෙන වාසිය: කස්ටමර්ස්ලට ක්ෂණිකව උත්තර ලැබෙන නිසා එක ලීඩ් එකක්වත් මිස් වෙන්නෙ නැහැ, වැඩිපුර ඩීල්ස් ක්ලෝස් කරගන්න පුළුවන්, අනවශ්‍ය මහන්සියයි වියදමයි ලොකු ප්‍රමාණයකින් ඉතිරි වෙනවා.
- පැකේජස් (Packages): පැකේජස් 3ක් තියෙනවා — ස්ටාටර්, බිස්නස්, එන්ටර්ප්‍රයිස් කියලා. (කෝල් එකෙන් නිශ්චිත ගණන් කියන්නෙ නැතුව, වට්ස්ඇප් ඩෙමෝ එකෙන් බලාගන්න යෝජනා කරන්න).
- ඉදිරි පියවර (Call to Action): අපේ වට්ස්ඇප් ඩෙමෝ එකට මැසේජ් එකක් දාලා මේක ලයිව් ටෙස්ට් කරලා බලන්න පුළුවන්, නැත්නම් අපේ ටීම් එකට කනෙක්ට් කරලා දෙන්නත් පුළුවන්.

[ස්වභාවික ලාංකීය කතා විලාසයේ නීති (Authentic Sri Lankan Spoken Rules)]:
1. සැබෑ කතා කරන ලාංකීය සිංහල (100% Spoken Sinhala):
   - අමතන්නාට 'ඔයා', 'ඔයාට', 'ඔයාගෙ' කියා පමණක් අමතන්න. කිසි විටෙකත් 'ඔබ' නොකියන්න.
   - පොත් වචන ('පවසන්න', 'හැකියි', 'නැවත', 'විමසන්න', 'කාරුණිකව') සම්පූර්ණයෙන්ම තහනම්ය.
   - ලාංකිකයන් එදිනෙදා කතාබහේදී භාවිත කරන ලස්සන, මිත්‍රශීලී වචන යොදන්න: 'කියන්නකො', 'පුළුවන්', 'පුළුවන්ද', 'ආයෙත්', 'අහන්න', 'කරලා දෙන්නම්', 'බලමුකො', 'කරගන්න පුළුවන්', 'මිස් වෙන්නේ නැහැ', 'ලේසියෙන්ම වෙනවා'.
2. සුමටව එක දිගට ගලාගෙන යන කතා විලාසය (Fluent Natural Phrasing - NO Word-by-Word Chopping):
   - වචනයෙන් වචනය වෙන් කර කියවන ස්වභාවය (word by word) සම්පූර්ණයෙන්ම වළක්වන්න.
   - කිසිදු තනි උඩු කොමාවක් (Apostrophe ') වචන අතරට නොයොදන්න!
   - සෑම වචනයකටම කොමා (,) නොයොදන්න. ආරම්භක යෙදුමෙන් පසුව පමණක් එක කොමාවක් යොදන්න (උදා: "ආ හරි, අපි ඒක ලේසියෙන්ම කරලා දෙන්නම්.").
   - ස්වභාවිකව එක හුස්මට ගලාගෙන යන සුමට වාක්‍ය භාවිත කරන්න.
3. සියලු ඉංග්‍රීසි ණය වචන සිංහල අකුරින්ම ලියන්න (Transliterate all English words into natural spoken Sinhala script):
   - කිසිදු ඉංග්‍රීසි අකුරක් (A-Z) ලියන්න එපා! සියලුම ඉංග්‍රීසි ණය වචන සාමාන්‍ය ලාංකිකයන් කතා කරන විදියටම නිවැරදි දිගු ස්වර සහිත සිංහල අකුරින් ලියන්න:
     * automate -> ඔටෝමේට්
     * automation -> ඔටෝමේෂන්
     * WhatsApp -> වට්ස්ඇප්
     * business -> බිස්නස්
     * customer / customers -> කස්ටමර් / කස්ටමර්ස්ලා
     * calls -> කෝල්ස්
     * calling -> කෝලිං
     * leads -> ලීඩ්ස්
     * team -> ටීම් / ටීම් එක
     * message / messages -> මැසේජ් / මැසේජස්
     * demo -> ඩෙමෝ එක
     * appointment -> ඇපොයින්ට්මන්ට්
     * system -> සිස්ටම් එක
     * support -> සපෝට් එක
     * close / closing -> ක්ලෝස් / ක්ලෝසින්
     * features -> ෆීචර්ස්
     * AI -> ඒ අයි
     * okay -> ඕකේ
4. කිසිවිටෙකත් එකම ප්‍රශ්නය හෝ ආයුබෝවන් නැවත නොකියන්න:
   - පෙර පටිගත කළ සුබපැතුම අමතන්නාට ඇසී අවසන් බැවින් නැවත 'හෙලෝ, ආයුබෝවන්!' නොකියන්න.
   - 'මොනවද දැනගන්න ඕනෙ?' හෝ 'මම කොහොමද උදව් කරන්න ඕනෙ?' කියා නැවත නැවත අසන්න එපා!
   - අමතන්නා යමක් ඇසූ විට, සෘජුවම Buildstart විසඳුම හෝ විස්තරය පැහැදිලි කර, ඔවුන්ගේ business එක කුමක්දැයි අසන්න, නැතහොත් WhatsApp demo එකට මඟ පෙන්වන්න.
5. දුරකථන සංවාදයකට ගැළපෙන කෙටි වාක්‍ය (Strictly 1 concise sentence under 15 words):
   - දුරකථන ඇමතුමක ස්වභාවය අනුව එක් වරකට වචන 15කට නොවැඩි සරල, කෙටි වාක්‍ය 1ක් පමණක් කියන්න. එවිට කටහඬ වහාම ප්‍රතිචාර දක්වයි.
6. Formatting තහනම්:
   - කිසිදු markdown, තරු ලකුණු (*), bullet points හෝ emojis නොයොදන්න. කටහඬින් කියවන සරල පාඨ පමණක් ලබා දෙන්න.`
)

type ChatMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"` // string or []ContentPart
}

type ContentPart struct {
	Type       string          `json:"type"`
	Text       string          `json:"text,omitempty"`
	InputAudio *InputAudioPart `json:"input_audio,omitempty"`
	ImageURL   *ImageURLPart   `json:"image_url,omitempty"`
}

type InputAudioPart struct {
	Data   string `json:"data"`
	Format string `json:"format"`
}

type ImageURLPart struct {
	URL string `json:"url"` // data:audio/wav;base64,...
}

type OpenRouterClient struct {
	apiKey       string
	model        string
	systemPrompt string
	temperature  float64
	maxTokens    int
	webhookURL   string
	client       *http.Client
	mu           sync.Mutex
	history      []ChatMessage
}

func NewOpenRouterClient(apiKey, model, systemPrompt string) *OpenRouterClient {
	if model == "" || model == "openrouter/auto" || strings.Contains(model, "gemini-2.0-flash") {
		model = DefaultOpenRouterModel
	}
	if systemPrompt == "" {
		systemPrompt = DefaultSystemPrompt
	}
	webhookURL := strings.TrimSpace(os.Getenv("LOVABLE_AGENT_URL"))
	if webhookURL == "" {
		webhookURL = strings.TrimSpace(os.Getenv("EXTERNAL_AGENT_URL"))
	}
	c := &OpenRouterClient{
		apiKey:       apiKey,
		model:        model,
		systemPrompt: systemPrompt,
		webhookURL:   webhookURL,
		temperature:  0.7,
		maxTokens:    350, // Adequate tokens for Sinhala Unicode script & JSON
		client:       &http.Client{Timeout: 20 * time.Second},
		history:      make([]ChatMessage, 0),
	}
	c.Reset()
	return c
}

func (c *OpenRouterClient) SetWebhookURL(url string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.webhookURL = strings.TrimSpace(url)
}

type AudioTurnResult struct {
	Transcription string `json:"transcription"`
	Reply         string `json:"reply"`
}

func (c *OpenRouterClient) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.history = []ChatMessage{
		{Role: "system", Content: c.systemPrompt},
	}
}

func (c *OpenRouterClient) SetSystemPrompt(prompt string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.systemPrompt = prompt
	if len(c.history) > 0 && c.history[0].Role == "system" {
		c.history[0].Content = prompt
	}
}

func (c *OpenRouterClient) SetModel(model string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if model == "" || model == "openrouter/auto" || strings.Contains(model, "gemini-2.0-flash") {
		model = DefaultOpenRouterModel
	}
	c.model = model
}

func (c *OpenRouterClient) SetAPIKey(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.apiKey = key
}

func (c *OpenRouterClient) AddAssistantMessage(text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.history = append(c.history, ChatMessage{
		Role:    "assistant",
		Content: text,
	})
	if len(c.history) > 15 {
		c.history = append([]ChatMessage{c.history[0]}, c.history[len(c.history)-14:]...)
	}
}

func (c *OpenRouterClient) Chat(ctx context.Context, userText string) (string, error) {
	c.mu.Lock()
	c.history = append(c.history, ChatMessage{
		Role:    "user",
		Content: userText,
	})
	msgs := make([]ChatMessage, len(c.history))
	copy(msgs, c.history)
	apiKey := c.apiKey
	model := c.model
	webhookURL := c.webhookURL
	c.mu.Unlock()

	var respText string
	var err error
	if webhookURL != "" {
		respText, err = c.sendWebhookRequest(ctx, webhookURL, userText, msgs)
	} else if isGoogleKey(apiKey) {
		respText, err = c.sendGoogleRequest(ctx, apiKey, model, msgs, false)
	} else {
		respText, err = c.sendRequest(ctx, apiKey, model, msgs, false)
	}
	if err != nil {
		return "", err
	}

	c.mu.Lock()
	c.history = append(c.history, ChatMessage{
		Role:    "assistant",
		Content: respText,
	})
	if len(c.history) > 15 {
		c.history = append([]ChatMessage{c.history[0]}, c.history[len(c.history)-14:]...)
	}
	c.mu.Unlock()

	return respText, nil
}

func (c *OpenRouterClient) ChatWithAudio(ctx context.Context, wavData []byte) (transcription string, reply string, err error) {
	b64Audio := base64.StdEncoding.EncodeToString(wavData)

	audioPrompt := "Listen carefully to what the caller said in this live phone call audio.\n" +
		"Return JSON only with this exact structure:\n" +
		"{\n" +
		"  \"transcription\": \"Accurate verbatim transcription of what the caller actually said in Sri Lankan Sinhala, English, or Singlish. Use clear Sinhala script for Sinhala speech. If they spoke English words, transcribe them accurately. Do NOT hallucinate. If silence or unintelligible noise, output empty string.\",\n" +
		"  \"reply\": \"Warm, natural, fluent Sri Lankan spoken Sinhala response as Buildstart voice agent (strictly 1 concise sentence under 15 words, colloquial Sinhala using 'ඔයා', all loanwords written in natural Sinhala script like ඔටෝමේට්, වට්ස්ඇප්, ලීඩ්ස්, පැකේජස් with NO English letters, speak in smooth connected flow with NO apostrophes or word-by-word chopping, directly answer their question, no asterisks, no emojis)\"\n" +
		"}"

	c.mu.Lock()
	// Build request messages without permanently storing the large audio payload in history
	audioMsg := ChatMessage{
		Role: "user",
		Content: []ContentPart{
			{Type: "text", Text: audioPrompt},
			{Type: "input_audio", InputAudio: &InputAudioPart{Data: b64Audio, Format: "wav"}},
		},
	}
	msgs := make([]ChatMessage, len(c.history)+1)
	copy(msgs, c.history)
	msgs[len(c.history)] = audioMsg
	apiKey := c.apiKey
	model := c.model
	c.mu.Unlock()

	var rawText string
	if isGoogleKey(apiKey) {
		rawText, err = c.sendGoogleAudioRequest(ctx, apiKey, model, wavData, audioPrompt)
	} else {
		rawText, err = c.sendRequest(ctx, apiKey, model, msgs, true)
	}
	if err != nil {
		return "", "", err
	}

	// Clean code fence blocks or surrounding text if any
	clean := strings.TrimSpace(rawText)
	firstBrace := strings.Index(clean, "{")
	lastBrace := strings.LastIndex(clean, "}")
	if firstBrace != -1 && lastBrace > firstBrace {
		clean = clean[firstBrace : lastBrace+1]
	}

	var parsed AudioTurnResult
	if jsonErr := json.Unmarshal([]byte(clean), &parsed); jsonErr == nil && parsed.Reply != "" {
		transcription = strings.TrimSpace(parsed.Transcription)
		reply = strings.TrimSpace(parsed.Reply)
	} else {
		reply = rawText
	}

	c.mu.Lock()
	// Store the compact text version in history so future turns do not re-send audio blobs
	userHistoryText := transcription
	if userHistoryText == "" {
		userHistoryText = "[Caller spoken audio]"
	}
	c.history = append(c.history, ChatMessage{
		Role:    "user",
		Content: userHistoryText,
	})
	c.history = append(c.history, ChatMessage{
		Role:    "assistant",
		Content: reply,
	})
	if len(c.history) > 15 {
		c.history = append([]ChatMessage{c.history[0]}, c.history[len(c.history)-14:]...)
	}
	c.mu.Unlock()

	return transcription, reply, nil
}

func (c *OpenRouterClient) sendRequest(ctx context.Context, apiKey, model string, msgs []ChatMessage, jsonFormat bool) (string, error) {
	reqBody := map[string]any{
		"model":       model,
		"messages":    msgs,
		"temperature": c.temperature,
		"max_tokens":  c.maxTokens,
		"reasoning":   map[string]string{"effort": "none"},
	}
	if jsonFormat {
		reqBody["response_format"] = map[string]string{"type": "json_object"}
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, DefaultOpenRouterURL, bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Authorization", "Bearer "+apiKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("HTTP-Referer", "https://github.com/JotaDev66/WaCalls")
	httpReq.Header.Set("X-Title", "WaCalls Voice AI")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("OpenRouter API error (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	var parsed struct {
		Choices []struct {
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(bodyBytes, &parsed); err != nil {
		return "", fmt.Errorf("failed to parse OpenRouter response: %w", err)
	}

	if parsed.Error != nil && parsed.Error.Message != "" {
		return "", fmt.Errorf("OpenRouter error: %s", parsed.Error.Message)
	}

	if len(parsed.Choices) == 0 {
		return "", fmt.Errorf("no response choices returned from OpenRouter")
	}

	return strings.TrimSpace(parsed.Choices[0].Message.Content), nil
}

func isGoogleKey(key string) bool {
	k := strings.TrimSpace(key)
	return strings.HasPrefix(k, "AQ.") || strings.HasPrefix(k, "AIza")
}

func (c *OpenRouterClient) sendGoogleRequest(ctx context.Context, apiKey, model string, msgs []ChatMessage, jsonFormat bool) (string, error) {
	modelName := "gemini-3.6-flash"
	m := strings.TrimPrefix(model, "google/")
	if strings.Contains(m, "gemini-") {
		modelName = m
	}
	if modelName == "gemini-2.0-flash" || modelName == "gemini-2.5-flash" {
		modelName = "gemini-3.6-flash"
	}

	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent", modelName)

	type Part struct {
		Text string `json:"text"`
	}
	type Content struct {
		Role  string `json:"role"`
		Parts []Part `json:"parts"`
	}

	var contents []Content
	for _, m := range msgs {
		if m.Role == "system" {
			continue
		}
		role := "user"
		if m.Role == "assistant" {
			role = "model"
		}
		txt, _ := m.Content.(string)
		if txt != "" {
			contents = append(contents, Content{
				Role:  role,
				Parts: []Part{{Text: txt}},
			})
		}
	}

	reqBody := map[string]any{
		"systemInstruction": map[string]any{
			"parts": []Part{{Text: c.systemPrompt}},
		},
		"contents": contents,
		"generationConfig": map[string]any{
			"temperature":     c.temperature,
			"maxOutputTokens": c.maxTokens,
			"thinkingConfig": map[string]any{
				"thinkingBudget": 0,
			},
		},
	}
	if jsonFormat {
		reqBody["generationConfig"].(map[string]any)["responseMimeType"] = "application/json"
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("X-goog-api-key", apiKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Google Gemini API error (%d): %s", resp.StatusCode, string(bodyBytes))
	}

	var gResp struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(bodyBytes, &gResp); err != nil {
		return "", fmt.Errorf("failed to parse Google response: %w", err)
	}
	if gResp.Error != nil && gResp.Error.Message != "" {
		return "", fmt.Errorf("Google Gemini error: %s", gResp.Error.Message)
	}
	if len(gResp.Candidates) == 0 || len(gResp.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("no candidates in Google response")
	}

	return strings.TrimSpace(gResp.Candidates[0].Content.Parts[0].Text), nil
}

func (c *OpenRouterClient) sendGoogleAudioRequest(ctx context.Context, apiKey, model string, wavData []byte, audioPrompt string) (string, error) {
	modelName := "gemini-3.6-flash"
	m := strings.TrimPrefix(model, "google/")
	if strings.Contains(m, "gemini-") {
		modelName = m
	}
	if modelName == "gemini-2.0-flash" || modelName == "gemini-2.5-flash" {
		modelName = "gemini-3.6-flash"
	}

	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent", modelName)

	b64Audio := base64.StdEncoding.EncodeToString(wavData)

	reqBody := map[string]any{
		"systemInstruction": map[string]any{
			"parts": []map[string]string{{"text": c.systemPrompt}},
		},
		"contents": []map[string]any{
			{
				"parts": []any{
					map[string]string{"text": audioPrompt},
					map[string]any{
						"inlineData": map[string]string{
							"mimeType": "audio/wav",
							"data":     b64Audio,
						},
					},
				},
			},
		},
		"generationConfig": map[string]any{
			"responseMimeType": "application/json",
			"temperature":      0.5,
			"maxOutputTokens":  350,
			"thinkingConfig": map[string]any{
				"thinkingBudget": 0,
			},
		},
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("X-goog-api-key", apiKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Google Gemini Audio API error (%d): %s", resp.StatusCode, string(bodyBytes))
	}

	var gResp struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(bodyBytes, &gResp); err != nil {
		return "", fmt.Errorf("failed to parse Google response: %w", err)
	}
	if len(gResp.Candidates) == 0 || len(gResp.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("no response candidates from Google Gemini")
	}

	return strings.TrimSpace(gResp.Candidates[0].Content.Parts[0].Text), nil
}

func (c *OpenRouterClient) sendWebhookRequest(ctx context.Context, webhookURL, userMessage string, history []ChatMessage) (string, error) {
	reqBody := map[string]any{
		"message":  userMessage,
		"messages": history,
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("external agent webhook error: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("external agent returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// Support both { "reply": "..." } or { "response": "..." } or OpenAI { "choices": [{ "message": { "content": "..." } }] }
	var parsed struct {
		Reply    string `json:"reply"`
		Response string `json:"response"`
		Text     string `json:"text"`
		Message  string `json:"message"`
		Choices  []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}

	if err := json.Unmarshal(bodyBytes, &parsed); err == nil {
		if parsed.Reply != "" {
			return strings.TrimSpace(parsed.Reply), nil
		}
		if parsed.Response != "" {
			return strings.TrimSpace(parsed.Response), nil
		}
		if parsed.Text != "" {
			return strings.TrimSpace(parsed.Text), nil
		}
		if parsed.Message != "" {
			return strings.TrimSpace(parsed.Message), nil
		}
		if len(parsed.Choices) > 0 && parsed.Choices[0].Message.Content != "" {
			return strings.TrimSpace(parsed.Choices[0].Message.Content), nil
		}
	}

	// If raw string returned
	trimmed := strings.TrimSpace(string(bodyBytes))
	if trimmed != "" && !strings.HasPrefix(trimmed, "{") {
		return trimmed, nil
	}

	return "", fmt.Errorf("could not find reply in external agent response: %s", string(bodyBytes))
}
