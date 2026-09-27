
package agent

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
	"time"
)

// VADConfig holds tuning parameters for voice activity detection.
type VADConfig struct {
	SampleRate        int           // 16000
	EnergyThreshold   float32       // RMS threshold to qualify as speech (e.g. 0.032)
	MinSpeechFrames   int           // Minimum consecutive speech frames to trigger speech start
	SilenceDuration   time.Duration // Duration of silence to declare end of speech (e.g. 650ms)
	MaxSpeechDuration time.Duration // Maximum speech buffer before forcing end (e.g. 12s)
	MinSpeechSamples  int           // Minimum collected samples to qualify as speech (~600ms = 9600)
}

func DefaultVADConfig() VADConfig {
	return VADConfig{
		SampleRate:        16000,
		EnergyThreshold:   0.016, // sensitive to normal mobile phone speaking volume
		MinSpeechFrames:   2,     // 120ms to detect speech onset
		SilenceDuration:   750 * time.Millisecond,
		MaxSpeechDuration: 15 * time.Second,
		MinSpeechSamples:  6400, // 400ms minimum speech buffer
	}
}

// VAD detects speech segments in 16 kHz Float32 PCM audio streams.
type VAD struct {
	config        VADConfig
	mu            sync.Mutex
	isSpeaking    bool
	consecSpeech  int
	lastSpeechAt  time.Time
	speechStarted time.Time
	audioBuffer   []float32

	OnSpeechStart func()
	OnSpeechEnd   func(pcm []float32, wav []byte)
}

func NewVAD(cfg VADConfig) *VAD {
	if cfg.SampleRate == 0 {
		cfg = DefaultVADConfig()
	}
	return &VAD{
		config:      cfg,
		audioBuffer: make([]float32, 0, 16000*5),
	}
}

// ProcessFrame inspects one frame of float32 PCM samples and updates VAD state.
func (v *VAD) ProcessFrame(frame []float32) {
	if len(frame) == 0 {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()

	var sumSq float64
	for _, s := range frame {
		sumSq += float64(s * s)
	}
	rms := float32(math.Sqrt(sumSq / float64(len(frame))))
	now := time.Now()

	if rms >= v.config.EnergyThreshold {
		v.consecSpeech++
		v.lastSpeechAt = now
		if !v.isSpeaking && v.consecSpeech >= v.config.MinSpeechFrames {
			v.isSpeaking = true
			v.speechStarted = now
			v.audioBuffer = v.audioBuffer[:0]
			if v.OnSpeechStart != nil {
				go v.OnSpeechStart()
			}
		}
	} else {
		v.consecSpeech = 0
	}

	if v.isSpeaking {
		v.audioBuffer = append(v.audioBuffer, frame...)

		// Check for end of speech due to silence or timeout
		silenceExceeded := now.Sub(v.lastSpeechAt) >= v.config.SilenceDuration
		durationExceeded := now.Sub(v.speechStarted) >= v.config.MaxSpeechDuration

		if silenceExceeded || durationExceeded {
			v.isSpeaking = false
			totalSamples := len(v.audioBuffer)
			// Only emit if at least MinSpeechSamples (600ms) was collected
			minSamples := v.config.MinSpeechSamples
			if minSamples == 0 {
				minSamples = 9600
			}
			if totalSamples >= minSamples {
				pcmCopy := make([]float32, totalSamples)
				copy(pcmCopy, v.audioBuffer)
				wavBytes := EncodeWAV(pcmCopy, v.config.SampleRate)

				if v.OnSpeechEnd != nil {
					go v.OnSpeechEnd(pcmCopy, wavBytes)
				}
			}
			v.audioBuffer = v.audioBuffer[:0]
		}
	}
}

// Reset clears any active buffered speech.
func (v *VAD) Reset() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.isSpeaking = false
	v.consecSpeech = 0
	v.audioBuffer = v.audioBuffer[:0]
}

// IsSpeaking returns whether the caller is currently speaking.
func (v *VAD) IsSpeaking() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.isSpeaking
}

// EncodeWAV converts 16 kHz Mono float32 PCM into a standard 16-bit PCM WAV byte array.
func EncodeWAV(pcm []float32, sampleRate int) []byte {
	var buf bytes.Buffer
	numSamples := len(pcm)
	numChannels := uint16(1)
	bitsPerSample := uint16(16)
	byteRate := uint32(sampleRate) * uint32(numChannels) * uint32(bitsPerSample/8)
	blockAlign := numChannels * (bitsPerSample / 8)
	dataSize := uint32(numSamples * 2)

	// RIFF Header
	buf.WriteString("RIFF")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(36+dataSize))
	buf.WriteString("WAVE")

	// fmt chunk
	buf.WriteString("fmt ")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(16)) // Subchunk1Size (16 for PCM)
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))  // AudioFormat (1 = PCM)
	_ = binary.Write(&buf, binary.LittleEndian, numChannels)
	_ = binary.Write(&buf, binary.LittleEndian, uint32(sampleRate))
	_ = binary.Write(&buf, binary.LittleEndian, byteRate)
	_ = binary.Write(&buf, binary.LittleEndian, blockAlign)
	_ = binary.Write(&buf, binary.LittleEndian, bitsPerSample)

	// data chunk
	buf.WriteString("data")
	_ = binary.Write(&buf, binary.LittleEndian, dataSize)

	for _, sample := range pcm {
		if sample > 1.0 {
			sample = 1.0
		} else if sample < -1.0 {
			sample = -1.0
		}
		val := int16(sample * 32767.0)
		_ = binary.Write(&buf, binary.LittleEndian, val)
	}

	return buf.Bytes()
}
