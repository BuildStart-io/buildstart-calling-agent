package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// CallAudioRecorder records both caller (Left channel) and agent (Right channel)
// 16kHz PCM audio asynchronously without blocking any real-time VoIP threads.
type CallAudioRecorder struct {
	callID     string
	businessID string
	startTime  time.Time
	sampleRate int

	callerCh chan []float32
	agentCh  chan []float32
	stopCh   chan struct{}
	doneCh   chan struct{}

	callerSamples []int16
	agentSamples  []int16

	mu     sync.Mutex
	closed bool
	log    *slog.Logger
}

func newCallAudioRecorder(callID, businessID string, log *slog.Logger) *CallAudioRecorder {
	r := &CallAudioRecorder{
		callID:        callID,
		businessID:    businessID,
		startTime:     time.Now(),
		sampleRate:    16000,
		callerCh:      make(chan []float32, 100),
		agentCh:       make(chan []float32, 100),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		callerSamples: make([]int16, 0, 16000*30), // preallocate 30s buffer
		agentSamples:  make([]int16, 0, 16000*30),
		log:           log,
	}
	go r.worker()
	return r
}

// RecordCallerPCM writes inbound caller audio (Channel 1 / Left channel).
// It is strictly non-blocking: if the channel is full, the chunk is dropped to protect the VoIP loop.
func (r *CallAudioRecorder) RecordCallerPCM(pcm []float32) {
	if r == nil || len(pcm) == 0 {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	cp := make([]float32, len(pcm))
	copy(cp, pcm)

	select {
	case r.callerCh <- cp:
	default:
		// Never block WebRTC thread
	}
}

// RecordAgentPCM writes outbound AI agent audio (Channel 2 / Right channel).
// It is strictly non-blocking: if the channel is full, the chunk is dropped to protect the VoIP loop.
func (r *CallAudioRecorder) RecordAgentPCM(pcm []float32) {
	if r == nil || len(pcm) == 0 {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	cp := make([]float32, len(pcm))
	copy(cp, pcm)

	select {
	case r.agentCh <- cp:
	default:
		// Never block WebRTC thread
	}
}

func (r *CallAudioRecorder) worker() {
	defer close(r.doneCh)
	for {
		select {
		case <-r.stopCh:
			// Drain remaining chunks
			for {
				select {
				case c := <-r.callerCh:
					r.appendCaller(c)
				case a := <-r.agentCh:
					r.appendAgent(a)
				default:
					return
				}
			}
		case c := <-r.callerCh:
			r.appendCaller(c)
		case a := <-r.agentCh:
			r.appendAgent(a)
		}
	}
}

func (r *CallAudioRecorder) appendCaller(pcm []float32) {
	elapsed := time.Since(r.startTime)
	targetIdx := int(elapsed.Seconds() * float64(r.sampleRate))

	// Fill any gap/silence up to targetIdx (max 10s per gap)
	if gap := targetIdx - len(r.callerSamples); gap > 0 {
		if gap > r.sampleRate*10 {
			gap = r.sampleRate * 10
		}
		r.callerSamples = append(r.callerSamples, make([]int16, gap)...)
	}

	for _, s := range pcm {
		if s > 1.0 {
			s = 1.0
		} else if s < -1.0 {
			s = -1.0
		}
		r.callerSamples = append(r.callerSamples, int16(s*32767.0))
	}
}

func (r *CallAudioRecorder) appendAgent(pcm []float32) {
	elapsed := time.Since(r.startTime)
	targetIdx := int(elapsed.Seconds() * float64(r.sampleRate))

	// Fill any gap/silence up to targetIdx
	if gap := targetIdx - len(r.agentSamples); gap > 0 {
		if gap > r.sampleRate*10 {
			gap = r.sampleRate * 10
		}
		r.agentSamples = append(r.agentSamples, make([]int16, gap)...)
	}

	for _, s := range pcm {
		if s > 1.0 {
			s = 1.0
		} else if s < -1.0 {
			s = -1.0
		}
		r.agentSamples = append(r.agentSamples, int16(s*32767.0))
	}
}

// Stop finalizes the audio recording and returns the stereo 16-bit PCM WAV bytes.
func (r *CallAudioRecorder) Stop() []byte {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()

	close(r.stopCh)
	<-r.doneCh

	maxLen := len(r.callerSamples)
	if len(r.agentSamples) > maxLen {
		maxLen = len(r.agentSamples)
	}

	// Must have at least 0.5s of audio to produce a valid recording
	if maxLen < r.sampleRate/2 {
		return nil
	}

	// Pad shorter channel with silence so both channels match exact duration
	if len(r.callerSamples) < maxLen {
		r.callerSamples = append(r.callerSamples, make([]int16, maxLen-len(r.callerSamples))...)
	}
	if len(r.agentSamples) < maxLen {
		r.agentSamples = append(r.agentSamples, make([]int16, maxLen-len(r.agentSamples))...)
	}

	return encodeStereoWAV(r.callerSamples, r.agentSamples, r.sampleRate)
}

func encodeStereoWAV(left, right []int16, sampleRate int) []byte {
	numFrames := len(left)
	numChannels := 2
	bytesPerSample := 2
	dataSize := numFrames * numChannels * bytesPerSample

	var buf bytes.Buffer
	buf.Grow(44 + dataSize)

	// RIFF Header
	buf.WriteString("RIFF")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(36+dataSize))
	buf.WriteString("WAVE")

	// fmt subchunk
	buf.WriteString("fmt ")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(16))                        // Subchunk1Size
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))                         // AudioFormat (PCM = 1)
	_ = binary.Write(&buf, binary.LittleEndian, uint16(numChannels))               // 2 channels (Stereo)
	_ = binary.Write(&buf, binary.LittleEndian, uint32(sampleRate))                // SampleRate
	_ = binary.Write(&buf, binary.LittleEndian, uint32(sampleRate*numChannels*2))  // ByteRate
	_ = binary.Write(&buf, binary.LittleEndian, uint16(numChannels*bytesPerSample)) // BlockAlign
	_ = binary.Write(&buf, binary.LittleEndian, uint16(16))                        // BitsPerSample

	// data subchunk
	buf.WriteString("data")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(dataSize))

	// Interleave: Left (Caller), Right (Agent)
	for i := 0; i < numFrames; i++ {
		_ = binary.Write(&buf, binary.LittleEndian, left[i])
		_ = binary.Write(&buf, binary.LittleEndian, right[i])
	}

	return buf.Bytes()
}

func uploadCallRecording(ctx context.Context, rec *CallAudioRecorder, store *sessionStore, log *slog.Logger) {
	if rec == nil {
		return
	}
	wavData := rec.Stop()
	if len(wavData) == 0 {
		return
	}

	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "127.0.0.1:9000"
	}
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "admin"
	}
	secretKey := os.Getenv("MINIO_SECRET_KEY")
	if secretKey == "" {
		secretKey = "ChangeThisPassword123!@SGeeCDG"
	}
	bucket := os.Getenv("MINIO_BUCKET")
	if bucket == "" {
		bucket = "call-recordings"
	}
	publicBase := os.Getenv("MINIO_PUBLIC_URL")
	if publicBase == "" {
		publicBase = "https://storage.buildstart.io/call-recordings"
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Error("failed to create MinIO client for recording upload", "err", err)
		return
	}

	objectName := fmt.Sprintf("%s/%s.wav", rec.businessID, rec.callID)
	reader := bytes.NewReader(wavData)

	uploadCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = client.PutObject(uploadCtx, bucket, objectName, reader, int64(len(wavData)), minio.PutObjectOptions{
		ContentType: "audio/wav",
	})
	if err != nil {
		log.Error("failed to upload call recording to MinIO", "object", objectName, "err", err)
		return
	}

	publicURL := fmt.Sprintf("%s/%s", strings.TrimRight(publicBase, "/"), objectName)
	log.Info("call recording uploaded successfully", "call_id", rec.callID, "url", publicURL, "bytes", len(wavData))

	if store != nil {
		if err := store.updateCallRecording(uploadCtx, rec.callID, publicURL); err != nil {
			log.Error("failed to update recording_url in call_logs", "call_id", rec.callID, "err", err)
		}
	}
}
