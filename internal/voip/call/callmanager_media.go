package call

import (
	"time"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

func (m *CallManager) initCodec() {
	if m.codec != nil {
		return
	}
	codec, err := media.NewMLowCodec(media.DefaultCodecOptions)
	if err != nil {
		m.log.Warn("MLow codec unavailable — call will run signaling-only (no audio)", "err", err)
		return
	}
	m.codec = codec
}

func (m *CallManager) FeedCapturedPCM(data []float32) {
	if len(data) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.playQueue = append(m.playQueue, data...)
	m.lastCaptureAt = time.Now()
	m.lastPlayingAt = time.Now()
}

func (m *CallManager) FlushCapturedPCM() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.playQueue = nil
	m.wasPlaying = false
	m.wasSilent = true
	m.lastPlayingAt = time.Time{}
}

// IsPlayingAudio returns true if the call is actively transmitting agent speech to the peer,
// or has finished transmitting within a 350ms cooldown window to absorb room and phone speaker acoustic echo.
func (m *CallManager) IsPlayingAudio() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.wasPlaying || len(m.playQueue) > 0 || (!m.lastPlayingAt.IsZero() && time.Since(m.lastPlayingAt) < 350*time.Millisecond)
}

func (m *CallManager) sendOpusFrameLocked(opus []byte) {
	if m.rtpSession == nil || m.srtpSession == nil {
		return
	}
	marker := !m.firstPacketSent
	pkt := m.rtpSession.CreatePacketWithDuration(opus, m.codec.FrameSize(), marker)
	if m.debeEnabled {
		pkt.Header.Extension = true
		pkt.Header.ExtensionProfile = 0xbede
		pkt.Header.ExtensionData = nil
	}
	m.firstPacketSent = true

	srtp, err := m.srtpSession.Protect(pkt)
	if err != nil {
		m.log.Debug("srtp protect error", "err", err)
		return
	}
	m.outPacketCount++
	if m.outPacketCount%50 == 1 {
		m.log.Info("transmitting audio packets to WhatsApp relay", "seq", pkt.Header.SequenceNumber, "total", m.outPacketCount, "bytes", len(srtp))
	}
	m.relay.Broadcast(srtp)
}

func (m *CallManager) startSilenceKeepaliveLocked() {
	if m.keepaliveStop != nil || m.codec == nil {
		return
	}
	stop := make(chan struct{})
	m.keepaliveStop = stop
	frameSize := m.codec.FrameSize()
	silence := make([]float32, frameSize)
	frame := make([]float32, frameSize)

	go func() {
		// Strict isochronous RTP ticker: 60ms frame size = 960 samples @ 16kHz
		ticker := time.NewTicker(60 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				m.mu.Lock()
				m.dispatchAudioFrameLocked(frame, silence, frameSize)
				m.mu.Unlock()
			}
		}
	}()
}

func (m *CallManager) dispatchAudioFrameLocked(frame, silence []float32, frameSize int) {
	if m.codec == nil || m.rtpSession == nil || m.srtpSession == nil || !m.relay.HasConnection() {
		return
	}

	// Instant zero-latency pre-buffer (1 frame = 960 samples @ 16kHz = 60ms)
	prebufferSamples := frameSize

	if !m.wasPlaying {
		if len(m.playQueue) >= prebufferSamples {
			m.wasPlaying = true
			m.lastPlayingAt = time.Now()
			copy(frame, m.playQueue[:frameSize])
			m.playQueue = m.playQueue[frameSize:]
			m.wasSilent = false
			if opus, err := m.codec.Encode(frame); err == nil {
				m.sendOpusFrameLocked(opus)
			}
		} else {
			// In idle / pre-buffering state: send silence keepalive to maintain RTP heartbeat
			m.wasSilent = true
			if opus, err := m.codec.Encode(silence); err == nil {
				m.sendOpusFrameLocked(opus)
			}
		}
		return
	}

	// While actively playing:
	if len(m.playQueue) >= frameSize {
		m.lastPlayingAt = time.Now()
		copy(frame, m.playQueue[:frameSize])
		m.playQueue = m.playQueue[frameSize:]
		m.wasSilent = false
		if opus, err := m.codec.Encode(frame); err == nil {
			m.sendOpusFrameLocked(opus)
		}
	} else if time.Since(m.lastCaptureAt) > 400*time.Millisecond {
		// Gemini turn has finished: flush trailing samples and return to idle
		m.lastPlayingAt = time.Now()
		if len(m.playQueue) > 0 {
			n := len(m.playQueue)
			copy(frame, m.playQueue)
			for i := n; i < frameSize; i++ {
				frame[i] = 0
			}
			m.playQueue = nil
			m.wasPlaying = false
			m.wasSilent = true
			if opus, err := m.codec.Encode(frame); err == nil {
				m.sendOpusFrameLocked(opus)
			}
		} else {
			m.wasPlaying = false
			m.wasSilent = true
			if opus, err := m.codec.Encode(silence); err == nil {
				m.sendOpusFrameLocked(opus)
			}
		}
	} else {
		// Temporary jitter pause while Gemini is still streaming:
		// PRESERVE playQueue so partial syllables are NEVER destroyed!
		m.lastPlayingAt = time.Now()
		if opus, err := m.codec.Encode(silence); err == nil {
			m.sendOpusFrameLocked(opus)
		}
	}
}

func (m *CallManager) onRelayData(data []byte) {
	if transport.IsStunPacket(data) {
		return
	}
	if !transport.IsRtpPacket(data) {
		return
	}
	if len(data) < 12 {
		return
	}
	pt := data[1] & 0x7f
	if pt != core.PayloadTypeWhatsAppOpus {
		return
	}

	m.mu.Lock()
	if m.srtpSession == nil || m.codec == nil {
		m.mu.Unlock()
		return
	}
	ssrc := uint32(data[8])<<24 | uint32(data[9])<<16 | uint32(data[10])<<8 | uint32(data[11])
	if ssrc == m.selfSsrc {
		m.mu.Unlock()
		return
	}
	if !m.actualPeerSet {
		m.actualPeerSet = true
		if !containsSsrc(m.peerSsrcs, ssrc) {
			m.log.Info("detected peer ssrc from incoming media", "ssrc", ssrc)
			m.peerSsrcs = []uint32{ssrc}
			m.relay.SetSubscriptionSsrc(ssrc)
			go m.relay.ResendSubscriptions()
		}
	}
	srtp := m.srtpSession
	codec := m.codec
	m.mu.Unlock()

	pkt, err := srtp.Unprotect(data)
	if err != nil {
		m.log.Debug("srtp unprotect error", "err", err, "ssrc", ssrc, "len", len(data))
		return
	}
	if len(pkt.Payload) == 0 {
		return
	}
	pcm, err := codec.Decode(pkt.Payload)
	if err != nil {
		m.log.Debug("opus decode error", "err", err)
		return
	}
	pcm = media.NormalizeFrame(pcm, codec.FrameSize())
	if m.OnPeerAudio != nil {
		m.OnPeerAudio(pcm)
	}
}
