package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// CallAudioRecorder records and synchronizes both caller (inbound) and agent (outbound)
// 16kHz PCM audio streams, aligning them relative to the call connection timestamp.
type CallAudioRecorder struct {
	callID     string
	businessID string
	startTime  time.Time
	sampleRate int

	callerSamples []int16
	agentSamples  []int16

	mu     sync.Mutex
	closed bool
	log    *slog.Logger
}

func newCallAudioRecorder(callID, businessID string, log *slog.Logger) *CallAudioRecorder {
	return &CallAudioRecorder{
		callID:        callID,
		businessID:    businessID,
		startTime:     time.Now(),
		sampleRate:    16000,
		callerSamples: make([]int16, 0, 16000*30), // preallocate 30s buffer
		agentSamples:  make([]int16, 0, 16000*30),
		log:           log,
	}
}

// RecordCallerPCM writes inbound caller audio (Channel 1 / Left channel).
func (r *CallAudioRecorder) RecordCallerPCM(pcm []float32) {
	if len(pcm) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}

	elapsed := time.Since(r.startTime)
	targetIdx := int(elapsed.Seconds() * float64(r.sampleRate))

	// Fill any gap/silence up to targetIdx (clamped to max 10s per burst to prevent memory spikes)
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

// RecordAgentPCM writes outbound AI agent audio (Channel 2 / Right channel).
func (r *CallAudioRecorder) RecordAgentPCM(pcm []float32) {
	if len(pcm) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}

	elapsed := time.Since(r.startTime)
	targetIdx := int(elapsed.Seconds() * float64(r.sampleRate))

	// Fill any gap/silence up to targetIdx so agent responses align with caller timeline
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

// Finalize closes the recorder and builds a 16kHz 16-bit 2-channel stereo WAV file.
func (r *CallAudioRecorder) Finalize() ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true

	maxLen := len(r.callerSamples)
	if len(r.agentSamples) > maxLen {
		maxLen = len(r.agentSamples)
	}

	// If total audio is less than 1 second (16000 samples), skip recording
	if maxLen < r.sampleRate {
		return nil, fmt.Errorf("call audio duration too short (%d samples), skipping", maxLen)
	}

	// Equalize channel lengths with silence
	if len(r.callerSamples) < maxLen {
		r.callerSamples = append(r.callerSamples, make([]int16, maxLen-len(r.callerSamples))...)
	}
	if len(r.agentSamples) < maxLen {
		r.agentSamples = append(r.agentSamples, make([]int16, maxLen-len(r.agentSamples))...)
	}

	// 2 channels * 2 bytes per sample = 4 bytes per stereo frame
	dataSize := uint32(maxLen * 4)
	buf := bytes.NewBuffer(make([]byte, 0, 44+dataSize))

	// RIFF header
	buf.WriteString("RIFF")
	binary.Write(buf, binary.LittleEndian, uint32(36+dataSize))
	buf.WriteString("WAVE")

	// Subchunk 1: fmt
	buf.WriteString("fmt ")
	binary.Write(buf, binary.LittleEndian, uint32(16))      // Subchunk1Size (16 for PCM)
	binary.Write(buf, binary.LittleEndian, uint16(1))       // AudioFormat (1 for PCM)
	binary.Write(buf, binary.LittleEndian, uint16(2))       // NumChannels (2 for Stereo)
	binary.Write(buf, binary.LittleEndian, uint32(16000))   // SampleRate (16000)
	binary.Write(buf, binary.LittleEndian, uint32(64000))   // ByteRate (16000 * 2 * 2 = 64000)
	binary.Write(buf, binary.LittleEndian, uint16(4))       // BlockAlign (2 channels * 2 bytes)
	binary.Write(buf, binary.LittleEndian, uint16(16))      // BitsPerSample (16)

	// Subchunk 2: data
	buf.WriteString("data")
	binary.Write(buf, binary.LittleEndian, dataSize)

	// Interleave Left (Caller) and Right (Agent)
	for i := 0; i < maxLen; i++ {
		binary.Write(buf, binary.LittleEndian, r.callerSamples[i])
		binary.Write(buf, binary.LittleEndian, r.agentSamples[i])
	}

	return buf.Bytes(), nil
}

// uploadCallRecording finalizes the audio into WAV, uploads to MinIO, and updates database.
func uploadCallRecording(ctx context.Context, rec *CallAudioRecorder, store *sessionStore, log *slog.Logger) {
	if rec == nil {
		return
	}

	wavBytes, err := rec.Finalize()
	if err != nil {
		log.Debug("skipping call recording", "callID", rec.callID, "reason", err.Error())
		return
	}

	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:9000"
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
	publicBaseURL := os.Getenv("MINIO_PUBLIC_BASE_URL")
	if publicBaseURL == "" {
		publicBaseURL = "https://storage.buildstart.io"
	}

	useSSL := false
	host := endpoint
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
		host = u.Host
		if u.Scheme == "https" {
			useSSL = true
		}
	}

	minioClient, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Error("failed to create minio client for call recording", "callID", rec.callID, "err", err)
		return
	}

	objectName := fmt.Sprintf("%s.wav", rec.callID)
	if rec.businessID != "" {
		objectName = fmt.Sprintf("%s/%s.wav", rec.businessID, rec.callID)
	}

	uploadCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, err = minioClient.PutObject(uploadCtx, bucket, objectName, bytes.NewReader(wavBytes), int64(len(wavBytes)), minio.PutObjectOptions{
		ContentType: "audio/wav",
	})
	if err != nil {
		log.Error("failed to upload call recording to minio", "callID", rec.callID, "object", objectName, "err", err)
		return
	}

	publicURL := fmt.Sprintf("%s/%s/%s", strings.TrimRight(publicBaseURL, "/"), bucket, objectName)
	log.Info("call recording uploaded successfully", "callID", rec.callID, "url", publicURL, "sizeBytes", len(wavBytes))

	if store != nil {
		if dbErr := store.updateCallRecording(uploadCtx, rec.callID, publicURL); dbErr != nil {
			log.Error("failed to save recording_url to call_logs", "callID", rec.callID, "err", dbErr)
		} else {
			log.Info("call recording url updated in database", "callID", rec.callID)
		}
	}
}
