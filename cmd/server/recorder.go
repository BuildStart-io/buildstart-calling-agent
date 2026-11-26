package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net/url"
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
	_ = binary.Write(&buf, binary.LittleEndian, uint32(16))                         // Subchunk1Size
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))                          // AudioFormat (PCM = 1)
	_ = binary.Write(&buf, binary.LittleEndian, uint16(numChannels))                // 2 channels (Stereo)
	_ = binary.Write(&buf, binary.LittleEndian, uint32(sampleRate))                 // SampleRate
	_ = binary.Write(&buf, binary.LittleEndian, uint32(sampleRate*numChannels*2))   // ByteRate
	_ = binary.Write(&buf, binary.LittleEndian, uint16(numChannels*bytesPerSample)) // BlockAlign
	_ = binary.Write(&buf, binary.LittleEndian, uint16(16))                         // BitsPerSample

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

func minioRegion() string {
	if r := strings.TrimSpace(os.Getenv("MINIO_REGION")); r != "" {
		return r
	}
	return "us-east-1"
}

func newMinioClientFromEnv() (*minio.Client, error) {
	endpoint := strings.TrimSpace(os.Getenv("MINIO_ENDPOINT"))
	accessKey := strings.TrimSpace(os.Getenv("MINIO_ACCESS_KEY"))
	secretKey := strings.TrimSpace(os.Getenv("MINIO_SECRET_KEY"))
	if endpoint == "" || accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("MINIO_ENDPOINT, MINIO_ACCESS_KEY, or MINIO_SECRET_KEY not set")
	}
	secure := strings.EqualFold(strings.TrimSpace(os.Getenv("MINIO_USE_SSL")), "true")
	return minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:       secure,
		Region:       minioRegion(),
		BucketLookup: minio.BucketLookupPath,
	})
}

// newPresignMinioClientFromEnv creates an offline presigner anchored to the public storage domain
// (e.g. storage.buildstart.io over HTTPS) so AWS SigV4 Host matches external clients through Nginx.
func newPresignMinioClientFromEnv() (*minio.Client, error) {
	accessKey := strings.TrimSpace(os.Getenv("MINIO_ACCESS_KEY"))
	secretKey := strings.TrimSpace(os.Getenv("MINIO_SECRET_KEY"))
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("MINIO_ACCESS_KEY or MINIO_SECRET_KEY not set")
	}

	endpoint := strings.TrimSpace(os.Getenv("MINIO_PRESIGN_ENDPOINT"))
	secure := true
	if endpoint == "" {
		if pub := strings.TrimSpace(os.Getenv("MINIO_PUBLIC_URL")); pub != "" {
			if u, err := url.Parse(pub); err == nil && u.Host != "" {
				endpoint = u.Host
				secure = strings.EqualFold(u.Scheme, "https")
			}
		}
	}
	if endpoint == "" {
		return newMinioClientFromEnv()
	}

	return minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:       secure,
		Region:       minioRegion(),
		BucketLookup: minio.BucketLookupPath,
	})
}

func minioBucketForPlatform(platform string) string {
	if platform == "global" {
		if b := strings.TrimSpace(os.Getenv("MINIO_GLOBAL_BUCKET")); b != "" {
			return b
		}
		return "call-recordings-private"
	}
	if b := strings.TrimSpace(os.Getenv("MINIO_BUCKET")); b != "" {
		return b
	}
	return "call-recordings"
}

func uploadCallRecording(ctx context.Context, rec *CallAudioRecorder, platform, sessionID string, store *sessionStore, outbox *OutboxDispatcher, log *slog.Logger) {
	if rec == nil {
		return
	}
	wavData := rec.Stop()
	if len(wavData) == 0 {
		return
	}

	client, err := newMinioClientFromEnv()
	if err != nil {
		log.Warn("call recording upload skipped", "call_id", rec.callID, "reason", err.Error())
		return
	}

	bucket := minioBucketForPlatform(platform)
	objectName := fmt.Sprintf("%s/%s.wav", rec.businessID, rec.callID)
	reader := bytes.NewReader(wavData)

	uploadCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Ensure private bucket exists for global recordings without changing bucket policy to public.
	if platform == "global" {
		if exists, exErr := client.BucketExists(uploadCtx, bucket); exErr == nil && !exists {
			_ = client.MakeBucket(uploadCtx, bucket, minio.MakeBucketOptions{})
		}
	}

	_, err = client.PutObject(uploadCtx, bucket, objectName, reader, int64(len(wavData)), minio.PutObjectOptions{
		ContentType: "audio/wav",
	})
	if err != nil {
		log.Error("failed to upload call recording to MinIO", "bucket", bucket, "object", objectName, "err", err)
		return
	}

	var storedRef string
	if platform == "global" {
		// Private object key — never expose an unauthenticated public URL for Global recordings.
		storedRef = "global-private:" + objectName
		if outbox != nil {
			_, _ = outbox.Enqueue(uploadCtx, platform, sessionID, rec.businessID, sessionID, "recording.ready", map[string]any{
				"call_id":       rec.callID,
				"recording_key": objectName,
			})
		}
	} else {
		publicBase := strings.TrimSpace(os.Getenv("MINIO_PUBLIC_URL"))
		if publicBase == "" {
			log.Warn("call recording uploaded to private local bucket because MINIO_PUBLIC_URL is unset", "call_id", rec.callID)
			storedRef = objectName
		} else {
			storedRef = fmt.Sprintf("%s/%s", strings.TrimRight(publicBase, "/"), objectName)
		}
	}

	log.Info("call recording uploaded successfully", "call_id", rec.callID, "platform", platform, "bucket", bucket, "bytes", len(wavData))

	if store != nil {
		if err := store.updateCallRecording(uploadCtx, rec.callID, storedRef); err != nil {
			log.Error("failed to update recording_url in call_logs", "call_id", rec.callID, "err", err)
		}
	}
}

func presignRecordingURL(ctx context.Context, platform, businessID, callID string, expiry time.Duration) (string, error) {
	client, err := newPresignMinioClientFromEnv()
	if err != nil {
		return "", err
	}
	bucket := minioBucketForPlatform(platform)
	objectName := fmt.Sprintf("%s/%s.wav", businessID, callID)
	reqParams := make(url.Values)
	u, err := client.PresignedGetObject(ctx, bucket, objectName, expiry, reqParams)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func openRecordingObject(ctx context.Context, platform, businessID, callID string) (io.ReadSeekCloser, minio.ObjectInfo, error) {
	client, err := newMinioClientFromEnv()
	if err != nil {
		return nil, minio.ObjectInfo{}, err
	}
	bucket := minioBucketForPlatform(platform)
	objectName := fmt.Sprintf("%s/%s.wav", businessID, callID)
	obj, err := client.GetObject(ctx, bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, minio.ObjectInfo{}, err
	}
	info, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		return nil, minio.ObjectInfo{}, err
	}
	return obj, info, nil
}
