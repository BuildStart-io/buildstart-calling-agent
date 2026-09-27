package agent

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/hajimehoshi/go-mp3"
	"github.com/mewkiz/flac"
)

type TTSClient interface {
	Synthesize(ctx context.Context, text string) ([]float32, error)
	GetVoice() string
	SetVoice(v string)
	SetAzureConfig(key, region string)
	SetGoogleCloudConfig(key string)
	SetHuggingFaceToken(token string)
	SetOpenAIConfig(url, key string)
}

type MultiTTS struct {
	voice          string
	azureKey       string
	azureRegion    string
	googleCloudKey string
	hfToken        string
	openAIKey      string
	openAIURL      string
	httpClient     *http.Client
}

func NewMultiTTS(voice string) *MultiTTS {
	if voice == "" {
		voice = "dialog-nipunika" // Default to Dialog Axiata & University of Moratuwa Studio Voice
	}
	return &MultiTTS{
		voice:      voice,
		httpClient: &http.Client{Timeout: 8 * time.Second},
	}
}

func (t *MultiTTS) GetVoice() string {
	if t.voice == "" {
		return "dialog-nipunika"
	}
	return t.voice
}

func (t *MultiTTS) SetVoice(v string) {
	if v != "" {
		t.voice = v
	}
}

func (t *MultiTTS) SetAzureConfig(key, region string) {
	t.azureKey = key
	t.azureRegion = region
}

func (t *MultiTTS) SetGoogleCloudConfig(key string) {
	t.googleCloudKey = key
}

func (t *MultiTTS) SetHuggingFaceToken(token string) {
	t.hfToken = token
}

func (t *MultiTTS) SetOpenAIConfig(url, key string) {
	t.openAIURL = url
	t.openAIKey = key
}

// Synthesize converts text into 16 kHz Mono float32 PCM samples.
func (t *MultiTTS) Synthesize(ctx context.Context, text string) ([]float32, error) {
	text = cleanSpeechText(text)
	if text == "" {
		return nil, nil
	}

	// 1. Local Sinhala Neural VITS/Piper Server (localhost:5050 - Studio Quality, Zero Latency)
	if pcm, err := t.synthesizeLocal(ctx, text); err == nil && len(pcm) > 0 {
		fmt.Printf("🎙️ [TTS] Synthesized using Local Neural TTS engine (%d samples)\n", len(pcm))
		return pcm, nil
	} else if err != nil {
		fmt.Printf("⚠️ [TTS] Local engine error: %v\n", err)
	}

	// 2. Official Azure Cognitive Services Speech (Studio-Grade Human Sinhala Neural Voice)
	if t.azureKey != "" {
		pcm, err := t.synthesizeAzure(ctx, text)
		if err == nil && len(pcm) > 0 {
			fmt.Printf("🎙️ [TTS] Synthesized using Azure Speech Neural (%d samples)\n", len(pcm))
			return pcm, nil
		}
	}

	// 3. Google Cloud Text-to-Speech API (High Definition WaveNet / Neural2)
	if t.googleCloudKey != "" {
		pcm, err := t.synthesizeGoogleCloud(ctx, text)
		if err == nil && len(pcm) > 0 {
			fmt.Printf("🎙️ [TTS] Synthesized using Google Cloud TTS (%d samples)\n", len(pcm))
			return pcm, nil
		}
	}

	// 4. Hugging Face Serverless Meta MMS Sinhala Neural VITS
	if t.hfToken != "" {
		pcm, err := t.synthesizeHuggingFace(ctx, text)
		if err == nil && len(pcm) > 0 {
			fmt.Printf("🎙️ [TTS] Synthesized using Hugging Face TTS (%d samples)\n", len(pcm))
			return pcm, nil
		}
	}

	// 5. OpenAI-compatible TTS (e.g. Kokoro / Custom Neural TTS server)
	if t.openAIKey != "" && t.openAIURL != "" {
		pcm, err := t.synthesizeOpenAI(ctx, text)
		if err == nil && len(pcm) > 0 {
			fmt.Printf("🎙️ [TTS] Synthesized using Custom OpenAI TTS (%d samples)\n", len(pcm))
			return pcm, nil
		}
	}

	// 6. Microsoft Edge ReadAloud Neural TTS
	pcm, err := t.synthesizeEdge(ctx, text)
	if err == nil && len(pcm) > 0 {
		fmt.Printf("🎙️ [TTS] Synthesized using Microsoft Edge TTS (%d samples)\n", len(pcm))
		return pcm, nil
	}

	// 7. Google Translate TTS Fallback
	pcm, err = t.synthesizeGoogle(ctx, text)
	if err == nil && len(pcm) > 0 {
		fmt.Printf("⚠️ [TTS] Fallback to Google Translate TTS (%d samples)\n", len(pcm))
		return pcm, nil
	}

	// 8. Fallback Tone
	return synthesizeFallbackTone(text), nil
}

func (t *MultiTTS) synthesizeLocal(ctx context.Context, text string) ([]float32, error) {
	reqURL := "http://127.0.0.1:5050/synthesize"
	reqBody := map[string]any{"input": text, "voice": t.voice}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("local tts status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if bytes.HasPrefix(data, []byte("RIFF")) {
		return ParseWAV(data)
	}
	return decodeMP3To16kFloat32(data)
}

func (t *MultiTTS) synthesizeAzure(ctx context.Context, text string) ([]float32, error) {
	region := t.azureRegion
	if region == "" {
		region = "eastus"
	}
	endpoint := fmt.Sprintf("https://%s.tts.speech.microsoft.com/cognitiveservices/v1", region)

	voice := t.voice
	if voice == "" {
		voice = "si-LK-ThiliniNeural"
	}
	lang := "si-LK"
	if parts := strings.Split(voice, "-"); len(parts) >= 2 {
		lang = parts[0] + "-" + parts[1]
	}

	ssml := fmt.Sprintf("<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' xml:lang='%s'><voice name='%s'>%s</voice></speak>", lang, voice, escapeXML(text))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(ssml))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Ocp-Apim-Subscription-Key", t.azureKey)
	req.Header.Set("Content-Type", "application/ssml+xml")
	req.Header.Set("X-Microsoft-OutputFormat", "raw-16khz-16bit-mono-pcm")
	req.Header.Set("User-Agent", "WaCalls-VoiceAI")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("azure speech error (status %d): %s", resp.StatusCode, string(body))
	}

	rawBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return resamplePCMInt16To16kFloat32(rawBytes, 16000)
}

func (t *MultiTTS) synthesizeGoogleCloud(ctx context.Context, text string) ([]float32, error) {
	endpoint := fmt.Sprintf("https://texttospeech.googleapis.com/v1/text:synthesize?key=%s", url.QueryEscape(t.googleCloudKey))

	voiceName := "si-LK-Standard-A"
	if strings.Contains(t.voice, "si-LK") {
		voiceName = t.voice
	}

	reqBody := map[string]any{
		"input": map[string]string{"text": text},
		"voice": map[string]string{"languageCode": "si-LK", "name": voiceName},
		"audioConfig": map[string]any{
			"audioEncoding":   "LINEAR16",
			"sampleRateHertz": 16000,
		},
	}
	payload, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("google cloud tts error %d: %s", resp.StatusCode, string(body))
	}

	var res struct {
		AudioContent string `json:"audioContent"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	rawBytes, err := base64.StdEncoding.DecodeString(res.AudioContent)
	if err != nil {
		return nil, err
	}
	return resamplePCMInt16To16kFloat32(rawBytes, 16000)
}

func (t *MultiTTS) synthesizeHuggingFace(ctx context.Context, text string) ([]float32, error) {
	endpoint := "https://router.huggingface.co/hf-inference/models/facebook/mms-tts-sin"
	reqBody := map[string]any{
		"inputs": text,
	}
	payload, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	if t.hfToken != "" {
		token := t.hfToken
		if !strings.HasPrefix(token, "Bearer ") {
			token = "Bearer " + token
		}
		req.Header.Set("Authorization", token)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-wait-for-model", "true")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("huggingface tts error %d: %s", resp.StatusCode, string(body))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if bytes.HasPrefix(data, []byte("fLaC")) {
		return decodeFLACTo16kFloat32(data)
	}
	if bytes.HasPrefix(data, []byte("RIFF")) {
		return ParseWAV(data)
	}
	return decodeMP3To16kFloat32(data)
}

func decodeFLACTo16kFloat32(flacData []byte) ([]float32, error) {
	stream, err := flac.New(bytes.NewReader(flacData))
	if err != nil {
		return nil, fmt.Errorf("flac open error: %w", err)
	}
	defer stream.Close()

	sampleRate := int(stream.Info.SampleRate)
	channels := int(stream.Info.NChannels)
	var rawSamples []float32

	for {
		frame, err := stream.ParseNext()
		if err != nil {
			if err == io.EOF {
				break
			}
			break
		}
		if len(frame.Subframes) == 0 {
			continue
		}
		n := len(frame.Subframes[0].Samples)
		for i := 0; i < n; i++ {
			var sampleVal float32
			if channels == 1 {
				sampleVal = float32(frame.Subframes[0].Samples[i]) / 32768.0
			} else if len(frame.Subframes) >= 2 {
				left := float32(frame.Subframes[0].Samples[i]) / 32768.0
				right := float32(frame.Subframes[1].Samples[i]) / 32768.0
				sampleVal = (left + right) / 2.0
			}
			rawSamples = append(rawSamples, sampleVal)
		}
	}

	if len(rawSamples) == 0 {
		return nil, fmt.Errorf("empty flac audio decoded")
	}

	if sampleRate != 16000 && sampleRate > 0 {
		return resampleFloat32(rawSamples, sampleRate, 16000), nil
	}
	return rawSamples, nil
}

func cleanSpeechText(s string) string {
	s = strings.ReplaceAll(s, "*", "")
	s = strings.ReplaceAll(s, "#", "")
	s = strings.ReplaceAll(s, "`", "")
	s = strings.ReplaceAll(s, "\n", " ")

	// Normalize formal/bookish words to authentic spoken colloquial Sinhala
	replacements := [][2]string{
		{"ඔබගේ", "ඔයාගෙ"},
		{"ඔබට", "ඔයාට"},
		{"ඔබව", "ඔයාව"},
		{"ඔබෙන්", "ඔයාගෙන්"},
		{"ඔබ", "ඔයා"},
		{"උදවු", "උදව්"},
		{"පවසන්න", "කියන්න"},
		{"පවසන්නකො", "කියන්නකො"},
		{"හැකියි", "පුළුවන්"},
		{"හැකිය", "පුළුවන්"},
		{"හැකිද", "පුළුවන්ද"},
		{"නැවත", "ආයෙත්"},
		{"පැමිණෙන්න", "එන්න"},
		{"විමසන්න", "අහන්න"},
		{"ලබාගන්න", "ගන්න"},
		{"සැපයිය හැකිය", "දෙන්න පුළුවන්"},
		{"හලෝ", "හෙලෝ"},
	}
	for _, r := range replacements {
		s = strings.ReplaceAll(s, r[0], r[1])
	}

	return strings.TrimSpace(s)
}

func (t *MultiTTS) synthesizeEdge(ctx context.Context, text string) ([]float32, error) {
	connID := randomHex(16)
	wsURL := fmt.Sprintf("wss://speech.platform.bing.com/consumer/speech/synthesize/readaloud/edge/v1?TrustedClientToken=6A5AA1D4EAFF4E9FB37E23D68491D6F4&ConnectionId=%s", connID)

	opts := &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Origin":     []string{"chrome-extension://jdiccldimpdaibmpdkjnbmckianbfold"},
			"User-Agent": []string{"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"},
		},
	}

	dialCtx, dialCancel := context.WithTimeout(ctx, 6*time.Second)
	defer dialCancel()

	ws, _, err := websocket.Dial(dialCtx, wsURL, opts)
	if err != nil {
		return nil, err
	}
	defer ws.Close(websocket.StatusNormalClosure, "")

	cfg := "Content-Type:application/json; charset=utf-8\r\nPath:speech.config\r\n\r\n{\"context\":{\"synthesis\":{\"audio\":{\"metadataoptions\":{\"sentenceBoundaryEnabled\":\"false\",\"wordBoundaryEnabled\":\"false\"},\"outputFormat\":\"audio-24khz-48kbitrate-mono-mp3\"}}}}"
	if err := ws.Write(ctx, websocket.MessageText, []byte(cfg)); err != nil {
		return nil, err
	}

	reqID := randomHex(16)
	voice := t.voice
	if voice == "" {
		voice = "si-LK-ThiliniNeural"
	}
	lang := "si-LK"
	if parts := strings.Split(voice, "-"); len(parts) >= 2 {
		lang = parts[0] + "-" + parts[1]
	}

	ssml := fmt.Sprintf("<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' xml:lang='%s'><voice name='%s'><prosody pitch='+0Hz' rate='+0%%'>%s</prosody></voice></speak>", lang, voice, escapeXML(text))
	msg := fmt.Sprintf("X-RequestId:%s\r\nContent-Type:application/ssml+xml\r\nPath:ssml\r\n\r\n%s", reqID, ssml)

	if err := ws.Write(ctx, websocket.MessageText, []byte(msg)); err != nil {
		return nil, err
	}

	var audioData bytes.Buffer
	for {
		msgType, data, err := ws.Read(ctx)
		if err != nil {
			break
		}
		if msgType == websocket.MessageBinary && len(data) > 2 {
			headerLen := int(binary.BigEndian.Uint16(data[:2]))
			if len(data) >= 2+headerLen {
				header := string(data[2 : 2+headerLen])
				if strings.Contains(header, "Path:audio") {
					audioData.Write(data[2+headerLen:])
				}
			}
		} else if msgType == websocket.MessageText {
			if strings.Contains(string(data), "Path:turn.end") {
				break
			}
		}
	}

	if audioData.Len() == 0 {
		return nil, fmt.Errorf("no audio received from edge tts")
	}

	return decodeMP3To16kFloat32(audioData.Bytes())
}

func (t *MultiTTS) synthesizeGoogle(ctx context.Context, text string) ([]float32, error) {
	q := url.QueryEscape(text)
	tl := "si"
	if strings.HasPrefix(t.voice, "en-") {
		tl = "en"
	}
	reqURL := fmt.Sprintf("https://translate.google.com/translate_tts?ie=UTF-8&tl=%s&client=tw-ob&q=%s", tl, q)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("google tts status: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if bytes.HasPrefix(data, []byte("RIFF")) {
		return ParseWAV(data)
	}

	return decodeMP3To16kFloat32(data)
}

func decodeMP3To16kFloat32(mp3Data []byte) ([]float32, error) {
	dec, err := mp3.NewDecoder(bytes.NewReader(mp3Data))
	if err != nil {
		return nil, fmt.Errorf("mp3 decode error: %w", err)
	}

	pcmBytes, err := io.ReadAll(dec)
	if err != nil {
		return nil, fmt.Errorf("mp3 read error: %w", err)
	}

	sampleRate := dec.SampleRate()
	numSamples := len(pcmBytes) / 4 // 16-bit stereo = 4 bytes per sample frame
	if numSamples == 0 {
		return nil, fmt.Errorf("empty pcm decoded from mp3")
	}

	// Convert 16-bit stereo PCM to float32 mono
	monoFloats := make([]float32, numSamples)
	for i := 0; i < numSamples; i++ {
		left := int16(binary.LittleEndian.Uint16(pcmBytes[i*4 : i*4+2]))
		right := int16(binary.LittleEndian.Uint16(pcmBytes[i*4+2 : i*4+4]))
		monoVal := (float32(left) + float32(right)) / (2.0 * 32768.0)
		monoFloats[i] = monoVal
	}

	// Resample to 16 kHz
	if sampleRate != 16000 && sampleRate > 0 {
		return resampleFloat32(monoFloats, sampleRate, 16000), nil
	}
	return monoFloats, nil
}

func (t *MultiTTS) synthesizeOpenAI(ctx context.Context, text string) ([]float32, error) {
	reqBody := map[string]any{
		"model":           "tts-1",
		"input":           text,
		"voice":           "alloy",
		"response_format": "pcm",
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.openAIURL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+t.openAIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("openai tts error %d", resp.StatusCode)
	}

	rawBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return resamplePCMInt16To16kFloat32(rawBytes, 24000)
}

func ParseWAV(wavData []byte) ([]float32, error) {
	if len(wavData) < 44 || string(wavData[:4]) != "RIFF" || string(wavData[8:12]) != "WAVE" {
		return nil, fmt.Errorf("invalid WAV header")
	}

	channels := int(binary.LittleEndian.Uint16(wavData[22:24]))
	sampleRate := int(binary.LittleEndian.Uint32(wavData[24:28]))
	bitsPerSample := int(binary.LittleEndian.Uint16(wavData[34:36]))

	dataOffset := 36
	for dataOffset+8 <= len(wavData) {
		tag := string(wavData[dataOffset : dataOffset+4])
		chunkSize := int(binary.LittleEndian.Uint32(wavData[dataOffset+4 : dataOffset+8]))
		if tag == "data" {
			dataOffset += 8
			break
		}
		dataOffset += 8 + chunkSize
	}

	if dataOffset >= len(wavData) {
		dataOffset = 44
	}

	audioBytes := wavData[dataOffset:]
	if bitsPerSample != 16 {
		return nil, fmt.Errorf("unsupported bits per sample: %d", bitsPerSample)
	}

	numSamples := len(audioBytes) / 2
	rawFloats := make([]float32, 0, numSamples/channels)
	for i := 0; i < len(audioBytes)-1; i += 2 * channels {
		val := int16(binary.LittleEndian.Uint16(audioBytes[i : i+2]))
		rawFloats = append(rawFloats, float32(val)/32768.0)
	}

	if sampleRate != 16000 && sampleRate > 0 {
		return resampleFloat32(rawFloats, sampleRate, 16000), nil
	}
	return rawFloats, nil
}

func resamplePCMInt16To16kFloat32(b []byte, srcRate int) ([]float32, error) {
	numSamples := len(b) / 2
	floats := make([]float32, numSamples)
	for i := 0; i < numSamples; i++ {
		val := int16(binary.LittleEndian.Uint16(b[i*2 : i*2+2]))
		floats[i] = float32(val) / 32768.0
	}
	if srcRate != 16000 {
		return resampleFloat32(floats, srcRate, 16000), nil
	}
	return floats, nil
}

func resampleFloat32(in []float32, srcRate, targetRate int) []float32 {
	if srcRate == targetRate || len(in) == 0 {
		return in
	}
	ratio := float64(targetRate) / float64(srcRate)
	outLen := int(float64(len(in)) * ratio)
	out := make([]float32, outLen)
	for i := 0; i < outLen; i++ {
		srcIdx := float64(i) / ratio
		idx0 := int(srcIdx)
		idx1 := min(idx0+1, len(in)-1)
		frac := float32(srcIdx - float64(idx0))
		out[i] = in[idx0]*(1.0-frac) + in[idx1]*frac
	}
	return out
}

func synthesizeFallbackTone(text string) []float32 {
	duration := 0.8
	samples := int(16000 * duration)
	pcm := make([]float32, samples)
	for i := 0; i < samples; i++ {
		t := float64(i) / 16000.0
		freq := 520.0
		if t > 0.4 {
			freq = 659.25
		}
		envelope := math.Sin(math.Pi * (t / duration))
		pcm[i] = float32(0.3 * envelope * math.Sin(2.0*math.Pi*freq*t))
	}
	return pcm
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func escapeXML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	return s
}
