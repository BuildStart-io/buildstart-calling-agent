package call

import (
	"context"
	"log/slog"
	"sync"
	"time"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/signaling"
	"wacalls/internal/voip/transport"
	"wacalls/internal/voip/wanode"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

type CallManager struct {
	sock core.VoipSocket
	log  *slog.Logger

	mu          sync.Mutex
	currentCall *CallInfo

	rtpSession  *media.RtpSession
	srtpSession *media.SrtpSession
	codec       media.Codec
	relay       RelayTransport

	selfSsrc      uint32
	peerSsrcs     []uint32
	actualPeerSet bool

	firstPacketSent       bool
	initialTransportSent  bool
	outgoingPreacceptSent bool
	acceptedByJid         string
	debeEnabled           bool

	encodeBuf    []float32
	encodeBufPos int

	playQueue   []float32
	playTrigger chan struct{}
	wasSilent   bool
	wasPlaying  bool

	lastCaptureAt  time.Time
	keepaliveStop  chan struct{}
	outPacketCount int

	OnStateChange func(*CallInfo)
	OnIncoming    func(*CallInfo)
	OnEnded       func(*CallInfo)
	OnPeerAudio   func([]float32)
}

func NewCallManager(sock core.VoipSocket, log *slog.Logger) *CallManager {
	if log == nil {
		log = slog.Default()
	}
	m := &CallManager{
		sock:        sock,
		log:         log,
		debeEnabled: false,
		playTrigger: make(chan struct{}, 1),
		wasSilent:   true,
	}
	relay := transport.NewSctpRelayManager(log)
	relay.SetOnConnected(func(ip string, port int) { m.onRelayConnected() })
	relay.SetOnReceive(func(data []byte) { m.onRelayData(data) })
	m.relay = relay
	return m
}

func (m *CallManager) CurrentCall() *CallInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.currentCall
}

func (m *CallManager) emitState() {
	if m.OnStateChange != nil && m.currentCall != nil {
		m.OnStateChange(m.currentCall)
	}
}

func (m *CallManager) StartCall(ctx context.Context, callID string, peerJid types.JID, isVideo bool) error {
	m.mu.Lock()
	if m.currentCall != nil && !m.currentCall.IsEnded() {
		m.mu.Unlock()
		return &CallError{"a call is already in progress"}
	}

	mediaType := core.CallMediaTypeAudio
	if isVideo {
		mediaType = core.CallMediaTypeVideo
	}
	creator := m.sock.OwnLID()
	if creator.IsEmpty() {
		creator = m.sock.OwnPN()
	}
	targetPN := peerJid
	if targetPN.Server == "lid" {
		if pn := m.sock.ResolvePNForLID(ctx, targetPN); !pn.IsEmpty() {
			targetPN = pn
		}
	}
	resolved := m.sock.ResolveLIDForPN(ctx, targetPN)
	if resolved.IsEmpty() {
		resolved = targetPN
	}

	call := NewOutgoingCall(callID, resolved.String(), creator.String(), mediaType)
	callKey := media.GenerateCallKey()
	call.EncryptionKey = callKey
	m.currentCall = call
	m.initialTransportSent = false
	m.outgoingPreacceptSent = false

	selfJid := creator.String()
	m.selfSsrc = media.GenerateSecureSsrc(callID, selfJid, 0)
	m.rtpSession = media.NewWhatsAppOpusSession(m.selfSsrc)
	m.peerSsrcs = []uint32{media.GenerateSecureSsrc(callID, resolved.String(), 0)}
	m.initCodec()
	m.mu.Unlock()

	offer, err := signaling.BuildOfferStanza(ctx, m.sock, callID, callKey, resolved, isVideo)
	if err != nil {
		return err
	}
	ackNode, err := m.sock.Query(ctx, offer)
	if err != nil {
		return err
	}

	m.mu.Lock()
	_ = m.currentCall.ApplyTransition(Transition{Type: TransitionOfferSent})
	m.emitState()
	m.mu.Unlock()

	if ackNode != nil {
		go m.HandleCallAck(context.Background(), ackNode)
	}

	m.log.Info("call offer sent", "call_id", callID, "peer", resolved.String())
	return nil
}

func (m *CallManager) AcceptCall(ctx context.Context, callID string) error {
	m.mu.Lock()
	call := m.currentCall
	if call == nil || call.CallID != callID {
		m.mu.Unlock()
		return &CallError{"no incoming call with id " + callID}
	}
	if call.IsEnded() {
		m.mu.Unlock()
		return &CallError{"call already ended"}
	}
	if call.StateData.State == core.CallStateActive || call.StateData.State == core.CallStateConnecting {
		m.mu.Unlock()
		return nil
	}
	if !call.CanAccept() {
		m.mu.Unlock()
		return &CallError{"call cannot be accepted in state " + string(call.StateData.State)}
	}
	_ = call.ApplyTransition(Transition{Type: TransitionLocalAccepted})
	m.emitState()
	key := call.EncryptionKey
	peer := wanode.MustJID(call.PeerJid)
	creator := wanode.MustJID(call.CallCreator)
	targetPeer := peer
	if targetPeer.Device == 0 && creator.Device != 0 {
		targetPeer = creator
	}
	isVideo := call.MediaType == core.CallMediaTypeVideo
	relayData := call.RelayData
	m.mu.Unlock()

	if key != nil {
		acceptNode, err := signaling.BuildAcceptStanza(ctx, m.sock, callID, key, targetPeer, creator, isVideo)
		if err != nil {
			m.log.Error("build accept failed", "err", err)
		} else {
			m.log.Info("accept raw node", "xml", acceptNode.String())
			if err := m.sock.SendNode(ctx, acceptNode); err != nil {
				m.log.Error("accept send error", "err", err)
			}
		}

		cleanPeer := wanode.MustJID(wanode.CleanJID(peer.String()))
		transport := waBinary.Node{
			Tag:   "call",
			Attrs: waBinary.Attrs{"to": cleanPeer, "id": signaling.GenerateCallStanzaID()},
			Content: []waBinary.Node{{
				Tag: "transport",
				Attrs: waBinary.Attrs{
					"call-id": callID, "call-creator": creator,
					"transport-message-type": "1", "p2p-cand-round": "1",
				},
				Content: []waBinary.Node{{Tag: "net", Attrs: waBinary.Attrs{"medium": "2", "protocol": "0"}}},
			}},
		}
		_ = m.sock.SendNode(ctx, transport)
		_ = m.sock.SendNode(ctx, signaling.BuildMuteV2Stanza(cleanPeer, callID, creator, 0))

		if relayData != nil && len(relayData.Endpoints) > 0 {
			var entries []signaling.RelayLatencyEntry
			for _, ep := range relayData.Endpoints {
				rtt := 70
				if ep.C2RRtt != nil {
					rtt = *ep.C2RRtt
				}
				entries = append(entries, signaling.RelayLatencyEntry{
					RelayName:    ep.RelayName,
					Latency:      rtt,
					AddressBytes: ep.AddressBytes,
				})
			}
			_ = m.sock.SendNode(ctx, signaling.BuildRelayLatencyStanza(cleanPeer, callID, creator, entries, nil))
		}
	}

	if relayData != nil {
		m.setupIncomingMedia(call, relayData)
	}

	if m.relay.HasConnection() {
		m.mu.Lock()
		if err := call.ApplyTransition(Transition{Type: TransitionMediaConnected}); err == nil {
			m.emitState()
			m.startSilenceKeepaliveLocked()
			m.log.Info("call ACTIVE (media path established)", "call_id", callID, "audio", m.codec != nil)
		}
		m.mu.Unlock()
		m.relay.ResendSubscriptions()
	} else if relayData != nil {
		m.connectRelays(relayData.Endpoints)
	} else {
		m.log.Warn("call accepted but no relay endpoints yet; media path waits for a transport message", "call_id", callID)
	}
	m.log.Info("call accepted", "call_id", callID)
	return nil
}

func (m *CallManager) setupIncomingMedia(call *CallInfo, relayData *core.RelayData) {
	m.mu.Lock()
	defer m.mu.Unlock()
	selfPid := 0
	peerPid := 0
	if relayData != nil {
		if relayData.SelfPid != nil {
			selfPid = *relayData.SelfPid
		}
		if relayData.PeerPid != nil {
			peerPid = *relayData.PeerPid
		}
		if len(relayData.ParticipantJids) > 0 {
			ourBase := wanode.CleanJID(m.ownCredJid())
			ourDeviceJid := ensureDeviceJid(findOurDevice(relayData.ParticipantJids, ourBase, m.ownCredJid()))
			if newSelf := media.GenerateSecureSsrc(call.CallID, ourDeviceJid, 0); newSelf != m.selfSsrc {
				m.selfSsrc = newSelf
				m.rtpSession = media.NewWhatsAppOpusSession(newSelf)
			}
			if peer := firstPeerDevice(relayData.ParticipantJids, ourBase); peer != "" {
				m.peerSsrcs = []uint32{media.GenerateSecureSsrc(call.CallID, ensureDeviceJid(peer), 0)}
				m.actualPeerSet = true
			}
		}
	}
	m.relay.SetPids(selfPid, peerPid)
	m.relay.SetSsrc(m.selfSsrc)
	m.relay.SetSubscriptionSsrc(firstSsrc(m.peerSsrcs))
	m.initSrtpKeysLocked()
}

func (m *CallManager) RejectCall(ctx context.Context, callID string, reason core.EndCallReason) error {
	m.mu.Lock()
	call := m.currentCall
	if call == nil || call.CallID != callID {
		m.mu.Unlock()
		return &CallError{"no call with id " + callID}
	}
	_ = call.ApplyTransition(Transition{Type: TransitionLocalRejected, Reason: reason})
	node := signaling.BuildRejectStanza(wanode.MustJID(call.PeerJid), call.CallID, wanode.MustJID(call.CallCreator))
	m.currentCall = nil
	m.emitState()
	m.mu.Unlock()

	go func() { _, _ = m.sock.Query(ctx, node) }()
	m.cleanupMedia()
	return nil
}

func (m *CallManager) EndCall(ctx context.Context, reason core.EndCallReason) error {
	m.mu.Lock()
	call := m.currentCall
	if call == nil || call.IsEnded() {
		m.mu.Unlock()
		return nil
	}
	_ = call.ApplyTransition(Transition{Type: TransitionTerminated, Reason: reason})
	node := signaling.BuildTerminateStanza(wanode.MustJID(call.PeerJid), call.CallID, wanode.MustJID(call.CallCreator))
	ended := call
	m.currentCall = nil
	m.emitState()
	m.mu.Unlock()

	go func() { _, _ = m.sock.Query(ctx, node) }()
	if m.OnEnded != nil {
		m.OnEnded(ended)
	}
	m.cleanupMedia()
	return nil
}

func (m *CallManager) ownCredJid() string {
	lid := m.sock.OwnLID()
	if !lid.IsEmpty() {
		return lid.String()
	}
	return m.sock.OwnPN().String()
}

type CallError struct{ Msg string }

func (e *CallError) Error() string { return e.Msg }
