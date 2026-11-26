package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

const (
	apiSentMsgTTL     = 10 * time.Minute
	inboundMediaTTL   = 30 * time.Minute
	inboundMediaMax   = 500
	maxOutboundMediaB = 20 << 20 // 20 MB
)

var defaultCallbackKeywords = []string{"call me", "call back", "callback"}

type cachedInboundMedia struct {
	MsgID        string
	ChatJID      types.JID
	MediaType    string
	MimeType     string
	Filename     string
	FileSize     int64
	Caption      string
	Downloadable whatsmeow.DownloadableMessage
	AddedAt      time.Time
}

func (s *Session) currentClient() *whatsmeow.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client
}

func (s *Session) trackAPISentMsgID(id string) {
	if id == "" {
		return
	}
	now := time.Now()
	s.apiSentMu.Lock()
	if s.apiSentMsgIDs == nil {
		s.apiSentMsgIDs = make(map[string]time.Time)
	}
	for k, exp := range s.apiSentMsgIDs {
		if now.After(exp) {
			delete(s.apiSentMsgIDs, k)
		}
	}
	s.apiSentMsgIDs[id] = now.Add(apiSentMsgTTL)
	s.apiSentMu.Unlock()
}

func (s *Session) untrackAPISentMsgID(id string) {
	if id == "" {
		return
	}
	s.apiSentMu.Lock()
	delete(s.apiSentMsgIDs, id)
	s.apiSentMu.Unlock()
}

func (s *Session) isAPISentMsgID(id string) bool {
	if id == "" {
		return false
	}
	now := time.Now()
	s.apiSentMu.Lock()
	defer s.apiSentMu.Unlock()
	exp, ok := s.apiSentMsgIDs[id]
	if !ok {
		return false
	}
	if now.After(exp) {
		delete(s.apiSentMsgIDs, id)
		return false
	}
	return true
}

func (s *Session) cacheInboundMedia(item cachedInboundMedia) {
	if item.MsgID == "" || item.Downloadable == nil {
		return
	}
	now := time.Now()
	item.AddedAt = now
	s.mediaMu.Lock()
	defer s.mediaMu.Unlock()

	if s.mediaCache == nil {
		s.mediaCache = make(map[string]cachedInboundMedia)
	}
	// Evict expired entries from the front of mediaOrder.
	cutoff := now.Add(-inboundMediaTTL)
	for len(s.mediaOrder) > 0 {
		headID := s.mediaOrder[0]
		head, ok := s.mediaCache[headID]
		if !ok || head.AddedAt.Before(cutoff) {
			delete(s.mediaCache, headID)
			s.mediaOrder = s.mediaOrder[1:]
			continue
		}
		break
	}
	for len(s.mediaCache) >= inboundMediaMax && len(s.mediaOrder) > 0 {
		oldest := s.mediaOrder[0]
		s.mediaOrder = s.mediaOrder[1:]
		delete(s.mediaCache, oldest)
	}
	if _, exists := s.mediaCache[item.MsgID]; !exists {
		s.mediaOrder = append(s.mediaOrder, item.MsgID)
	}
	s.mediaCache[item.MsgID] = item
}

func (s *Session) lookupInboundMedia(msgID string) (cachedInboundMedia, bool) {
	if msgID == "" {
		return cachedInboundMedia{}, false
	}
	now := time.Now()
	s.mediaMu.Lock()
	defer s.mediaMu.Unlock()
	item, ok := s.mediaCache[msgID]
	if !ok {
		return cachedInboundMedia{}, false
	}
	if now.Sub(item.AddedAt) > inboundMediaTTL {
		delete(s.mediaCache, msgID)
		return cachedInboundMedia{}, false
	}
	return item, true
}

func resolveTargetJID(raw string) (types.JID, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return types.JID{}, fmt.Errorf("recipient 'to' is required")
	}
	if strings.Contains(trimmed, "@") {
		jid, err := types.ParseJID(trimmed)
		if err != nil {
			return types.JID{}, fmt.Errorf("invalid JID %q: %w", raw, err)
		}
		if jid.Server == "broadcast" || jid.Server == "newsletter" {
			return types.JID{}, fmt.Errorf("%s JIDs are not supported", jid.Server)
		}
		if jid.Server != types.GroupServer {
			jid = jid.ToNonAD()
		}
		return jid, nil
	}
	digits := strings.TrimPrefix(trimmed, "+")
	if digits == "" {
		return types.JID{}, fmt.Errorf("invalid recipient %q", raw)
	}
	return types.NewJID(digits, types.DefaultUserServer), nil
}

// sendProtoMessage sends any WhatsApp protobuf message and pre-registers its generated ID
// before SendMessage returns so the subsequent IsFromMe reflection is not mistaken for an owner phone reply.
func (s *Session) sendProtoMessage(ctx context.Context, to types.JID, msg *waE2E.Message) (types.MessageID, error) {
	cli := s.currentClient()
	if cli == nil {
		return "", fmt.Errorf("whatsapp client is nil")
	}
	if !cli.IsConnected() {
		return "", fmt.Errorf("whatsapp client is not connected")
	}
	if msg == nil {
		return "", fmt.Errorf("message payload is nil")
	}
	target := to
	if target.Server == "" {
		target.Server = types.DefaultUserServer
	}
	if target.Server != types.GroupServer {
		target = target.ToNonAD()
	}

	msgID := cli.GenerateMessageID()
	s.trackAPISentMsgID(string(msgID))

	resp, err := cli.SendMessage(ctx, target, msg, whatsmeow.SendRequestExtra{ID: msgID})
	if err != nil {
		s.untrackAPISentMsgID(string(msgID))
		return "", err
	}
	if resp.ID != "" {
		s.trackAPISentMsgID(string(resp.ID))
		return resp.ID, nil
	}
	return msgID, nil
}

func (s *Session) sendTextSync(ctx context.Context, to types.JID, text string) (string, error) {
	if text == "" {
		return "", fmt.Errorf("message text is empty")
	}
	id, err := s.sendProtoMessage(ctx, to, &waE2E.Message{
		Conversation: proto.String(text),
	})
	return string(id), err
}

func (s *Session) sendWhatsAppMessage(to types.JID, text string) {
	if s.currentClient() == nil || text == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := s.sendTextSync(ctx, to, text); err != nil {
			s.log.Error("failed to send WhatsApp auto-message", "to", to.ToNonAD().String(), "err", err)
		} else {
			s.log.Info("WhatsApp auto-message sent to caller", "to", to.ToNonAD().String(), "text", text)
		}
	}()
}

// isBlockedSSRFIP returns true for loopback, private, link-local, CGNAT, multicast, or unspecified IPs.
func isBlockedSSRFIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		// Carrier-grade NAT 100.64.0.0/10
		if v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
			return true
		}
		// 0.0.0.0/8 ("this host")
		if v4[0] == 0 {
			return true
		}
	}
	return false
}

func validateSafeDialIP(ctx context.Context, resolver *net.Resolver, host string) (net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		if isBlockedSSRFIP(ip) {
			return nil, fmt.Errorf("blocked private or loopback IP address %s", ip.String())
		}
		return ip, nil
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	ips, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("DNS lookup failed for %q: %w", host, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no IP addresses resolved for %q", host)
	}
	var chosen net.IP
	for _, entry := range ips {
		if isBlockedSSRFIP(entry.IP) {
			return nil, fmt.Errorf("blocked non-public resolved IP %s for host %q", entry.IP.String(), host)
		}
		if chosen == nil {
			chosen = entry.IP
		}
	}
	return chosen, nil
}

func downloadPublicMediaBytes(ctx context.Context, rawURL string, maxBytes int64) ([]byte, string, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, "", fmt.Errorf("invalid media_url: %w", err)
	}
	if !strings.EqualFold(parsed.Scheme, "https") {
		return nil, "", fmt.Errorf("media_url must use https")
	}
	if parsed.Hostname() == "" {
		return nil, "", fmt.Errorf("media_url host is required")
	}

	dialer := &net.Dialer{Timeout: 10 * time.Second}
	transport := &http.Transport{
		DialContext: func(dctx context.Context, network, addr string) (net.Conn, error) {
			host, port, splitErr := net.SplitHostPort(addr)
			if splitErr != nil {
				return nil, splitErr
			}
			safeIP, checkErr := validateSafeDialIP(dctx, net.DefaultResolver, host)
			if checkErr != nil {
				return nil, checkErr
			}
			return dialer.DialContext(dctx, network, net.JoinHostPort(safeIP.String(), port))
		},
		ForceAttemptHTTP2: true,
	}

	client := &http.Client{
		Timeout:   25 * time.Second,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return errors.New("too many redirects")
			}
			if !strings.EqualFold(req.URL.Scheme, "https") {
				return errors.New("redirect to non-https scheme blocked")
			}
			if _, err := validateSafeDialIP(req.Context(), net.DefaultResolver, req.URL.Hostname()); err != nil {
				return err
			}
			return nil
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("media download returned HTTP %d", resp.StatusCode)
	}
	if maxBytes <= 0 {
		maxBytes = maxOutboundMediaB
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("failed reading media stream: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return nil, "", fmt.Errorf("media exceeds %d byte limit", maxBytes)
	}

	mimeType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if idx := strings.Index(mimeType, ";"); idx != -1 {
		mimeType = strings.TrimSpace(mimeType[:idx])
	}
	if mimeType == "" || mimeType == "application/octet-stream" {
		mimeType = http.DetectContentType(data)
	}
	return data, mimeType, nil
}

func (s *Session) sendMediaURLSync(ctx context.Context, to types.JID, mediaURL, mediaType, caption, filename string) (string, error) {
	cli := s.currentClient()
	if cli == nil || !cli.IsConnected() {
		return "", fmt.Errorf("whatsapp client is not connected")
	}

	kind := strings.ToLower(strings.TrimSpace(mediaType))
	if kind == "" {
		kind = "document"
	}

	var waMediaType whatsmeow.MediaType
	switch kind {
	case "image":
		waMediaType = whatsmeow.MediaImage
	case "document":
		waMediaType = whatsmeow.MediaDocument
	case "audio":
		waMediaType = whatsmeow.MediaAudio
	case "video":
		waMediaType = whatsmeow.MediaVideo
	default:
		return "", fmt.Errorf("unsupported media_type %q (allowed: image, document, audio, video)", mediaType)
	}

	data, mimeType, err := downloadPublicMediaBytes(ctx, mediaURL, maxOutboundMediaB)
	if err != nil {
		return "", err
	}

	up, err := cli.Upload(ctx, data, waMediaType)
	if err != nil {
		return "", fmt.Errorf("whatsapp media upload failed: %w", err)
	}

	fileLen := uint64(len(data))
	var msg *waE2E.Message

	switch kind {
	case "image":
		im := &waE2E.ImageMessage{
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			Mimetype:      proto.String(mimeType),
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(fileLen),
		}
		if caption != "" {
			im.Caption = proto.String(caption)
		}
		msg = &waE2E.Message{ImageMessage: im}
	case "document":
		fname := strings.TrimSpace(filename)
		if fname == "" {
			fname = "document"
		}
		dm := &waE2E.DocumentMessage{
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			Mimetype:      proto.String(mimeType),
			FileName:      proto.String(fname),
			Title:         proto.String(fname),
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(fileLen),
		}
		if caption != "" {
			dm.Caption = proto.String(caption)
		}
		msg = &waE2E.Message{DocumentMessage: dm}
	case "audio":
		ptt := strings.Contains(strings.ToLower(mimeType), "ogg")
		msg = &waE2E.Message{
			AudioMessage: &waE2E.AudioMessage{
				URL:           proto.String(up.URL),
				DirectPath:    proto.String(up.DirectPath),
				MediaKey:      up.MediaKey,
				Mimetype:      proto.String(mimeType),
				PTT:           proto.Bool(ptt),
				FileEncSHA256: up.FileEncSHA256,
				FileSHA256:    up.FileSHA256,
				FileLength:    proto.Uint64(fileLen),
			},
		}
	case "video":
		vm := &waE2E.VideoMessage{
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			Mimetype:      proto.String(mimeType),
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(fileLen),
		}
		if caption != "" {
			vm.Caption = proto.String(caption)
		}
		msg = &waE2E.Message{VideoMessage: vm}
	}

	id, err := s.sendProtoMessage(ctx, to, msg)
	return string(id), err
}

func matchesCallbackKeyword(text string, cfg AgentConfigRow, isGroup bool) (bool, string) {
	if isGroup {
		return false, ""
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return false, ""
	}
	keywords := defaultCallbackKeywords
	if cfg.HasCallbackKeywords {
		if len(cfg.CallbackKeywords) == 0 {
			return false, ""
		}
		keywords = cfg.CallbackKeywords
	}

	normalized := strings.Join(strings.Fields(strings.ToLower(trimmed)), " ")
	for _, rawKW := range keywords {
		kw := strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(rawKW))), " ")
		if kw == "" {
			continue
		}
		pattern := `(?i)(?:^|[^\p{L}\p{N}])` + regexp.QuoteMeta(kw) + `(?:$|[^\p{L}\p{N}])`
		re, err := regexp.Compile(pattern)
		if err != nil {
			continue
		}
		loc := re.FindStringIndex(normalized)
		if loc == nil {
			continue
		}
		// Guard against obvious immediate negations like "don't call me" / "do not call back".
		prefixStart := loc[0] - 28
		if prefixStart < 0 {
			prefixStart = 0
		}
		prefix := normalized[prefixStart:loc[0]]
		if strings.Contains(prefix, "don't") || strings.Contains(prefix, "dont") ||
			strings.Contains(prefix, "do not") || strings.Contains(prefix, "never") ||
			strings.HasSuffix(strings.TrimSpace(prefix), "no") ||
			strings.HasSuffix(strings.TrimSpace(prefix), "not") {
			continue
		}
		return true, kw
	}
	return false, ""
}

type extractedMessageContent struct {
	Text            string
	MediaType       string
	MimeType        string
	Filename        string
	FileSize        int64
	Caption         string
	QuotedMessageID string
	Downloadable    whatsmeow.DownloadableMessage
}

func extractMessageContent(msg *waE2E.Message) extractedMessageContent {
	var out extractedMessageContent
	if msg == nil {
		return out
	}
	if txt := msg.GetConversation(); txt != "" {
		out.Text = txt
	}
	if ext := msg.GetExtendedTextMessage(); ext != nil {
		if out.Text == "" {
			out.Text = ext.GetText()
		}
		if ctxInfo := ext.GetContextInfo(); ctxInfo != nil {
			out.QuotedMessageID = ctxInfo.GetStanzaID()
		}
	}
	if img := msg.GetImageMessage(); img != nil {
		out.MediaType = "image"
		out.MimeType = img.GetMimetype()
		out.Caption = img.GetCaption()
		out.FileSize = int64(img.GetFileLength())
		out.Downloadable = img
		if out.Text == "" {
			out.Text = out.Caption
		}
		if out.QuotedMessageID == "" && img.GetContextInfo() != nil {
			out.QuotedMessageID = img.GetContextInfo().GetStanzaID()
		}
	} else if doc := msg.GetDocumentMessage(); doc != nil {
		out.MediaType = "document"
		out.MimeType = doc.GetMimetype()
		out.Filename = doc.GetFileName()
		if out.Filename == "" {
			out.Filename = doc.GetTitle()
		}
		out.Caption = doc.GetCaption()
		out.FileSize = int64(doc.GetFileLength())
		out.Downloadable = doc
		if out.Text == "" {
			out.Text = out.Caption
		}
		if out.QuotedMessageID == "" && doc.GetContextInfo() != nil {
			out.QuotedMessageID = doc.GetContextInfo().GetStanzaID()
		}
	} else if aud := msg.GetAudioMessage(); aud != nil {
		out.MediaType = "audio"
		out.MimeType = aud.GetMimetype()
		out.FileSize = int64(aud.GetFileLength())
		out.Downloadable = aud
		if out.QuotedMessageID == "" && aud.GetContextInfo() != nil {
			out.QuotedMessageID = aud.GetContextInfo().GetStanzaID()
		}
	} else if vid := msg.GetVideoMessage(); vid != nil {
		out.MediaType = "video"
		out.MimeType = vid.GetMimetype()
		out.Caption = vid.GetCaption()
		out.FileSize = int64(vid.GetFileLength())
		out.Downloadable = vid
		if out.Text == "" {
			out.Text = out.Caption
		}
		if out.QuotedMessageID == "" && vid.GetContextInfo() != nil {
			out.QuotedMessageID = vid.GetContextInfo().GetStanzaID()
		}
	}
	return out
}

func (s *Session) learnMessageLIDMappings(ctx context.Context, info types.MessageInfo) {
	if s.mgr == nil || s.mgr.store == nil {
		return
	}
	if !info.SenderAlt.IsEmpty() {
		if info.Sender.Server == "lid" && info.SenderAlt.Server == types.DefaultUserServer {
			s.mgr.store.putLIDMapping(ctx, info.Sender.User, info.SenderAlt.User)
		} else if info.SenderAlt.Server == "lid" && info.Sender.Server == types.DefaultUserServer {
			s.mgr.store.putLIDMapping(ctx, info.SenderAlt.User, info.Sender.User)
		}
	}
	if !info.RecipientAlt.IsEmpty() {
		if info.Chat.Server == "lid" && info.RecipientAlt.Server == types.DefaultUserServer {
			s.mgr.store.putLIDMapping(ctx, info.Chat.User, info.RecipientAlt.User)
		} else if info.RecipientAlt.Server == "lid" && info.Chat.Server == types.DefaultUserServer {
			s.mgr.store.putLIDMapping(ctx, info.RecipientAlt.User, info.Chat.User)
		}
	}
}

func (s *Session) handleIncomingMessage(evt *events.Message) {
	if evt == nil || evt.Message == nil {
		return
	}
	if evt.Info.Chat == types.StatusBroadcastJID || evt.Info.Chat.Server == "broadcast" || evt.Info.Chat.Server == "newsletter" {
		return
	}
	// Historical backfill from WhatsApp HistorySync must not trigger replies or auto-callbacks.
	if evt.SourceWebMsg != nil {
		return
	}

	ctx := context.Background()
	s.learnMessageLIDMappings(ctx, evt.Info)

	isOffline := s.inOfflineSync.Load()
	extracted := extractMessageContent(evt.Message)
	msgID := string(evt.Info.ID)

	resolvedChat := evt.Info.Chat.ToNonAD()
	if !evt.Info.IsGroup {
		resolvedChat = s.resolvePhoneJID(ctx, evt.Info.Chat)
	}
	resolvedSender := s.resolvePhoneJID(ctx, evt.Info.Sender)

	if evt.Info.IsFromMe {
		// Suppress our own API-initiated outbound messages.
		if s.isAPISentMsgID(msgID) {
			return
		}
		if s.platform == "global" {
			s.enqueueGlobalEvent(ctx, s.id+":"+resolvedChat.String(), "message.owner_sent", map[string]any{
				"message_id": msgID,
				"chat_jid":   resolvedChat.String(),
				"peer_jid":   resolvedChat.String(),
				"text":       extracted.Text,
				"media_type": extracted.MediaType,
				"caption":    extracted.Caption,
				"is_group":   evt.Info.IsGroup,
				"is_offline": isOffline,
				"timestamp":  evt.Info.Timestamp.UnixMilli(),
			})
		}
		return
	}

	// Cache inbound media handle for 30 minutes so Global can fetch via /messages/{id}/media.
	if extracted.Downloadable != nil {
		s.cacheInboundMedia(cachedInboundMedia{
			MsgID:        msgID,
			ChatJID:      resolvedChat,
			MediaType:    extracted.MediaType,
			MimeType:     extracted.MimeType,
			Filename:     extracted.Filename,
			FileSize:     extracted.FileSize,
			Caption:      extracted.Caption,
			Downloadable: extracted.Downloadable,
		})
	}

	if s.platform == "global" {
		s.enqueueGlobalEvent(ctx, s.id+":"+resolvedChat.String(), "message.received", map[string]any{
			"message_id":        msgID,
			"chat_jid":          resolvedChat.String(),
			"sender_jid":        resolvedSender.String(),
			"sender_name":       evt.Info.PushName,
			"is_group":          evt.Info.IsGroup,
			"is_offline":        isOffline,
			"text":              extracted.Text,
			"media_type":        extracted.MediaType,
			"mime_type":         extracted.MimeType,
			"filename":          extracted.Filename,
			"file_size":         extracted.FileSize,
			"caption":           extracted.Caption,
			"has_media":         extracted.Downloadable != nil,
			"quoted_message_id": extracted.QuotedMessageID,
			"timestamp":         evt.Info.Timestamp.UnixMilli(),
		})
	}

	// Skip auto-callback for offline-queued messages or groups.
	if isOffline || evt.Info.IsGroup || strings.TrimSpace(extracted.Text) == "" {
		return
	}

	cfg := defaultAgentConfigRow()
	if loaded, err := s.mgr.store.getAgentConfig(ctx, s.businessID); err == nil {
		cfg = loaded
	}
	if !cfg.VoiceEnabled || !cfg.AutoCallback {
		return
	}
	if matched, kw := matchesCallbackKeyword(extracted.Text, cfg, evt.Info.IsGroup); matched {
		callbackTarget := resolvedSender
		if callbackTarget.IsEmpty() {
			callbackTarget = resolvedChat
		}
		s.log.Info("call request keyword received in chat", "sender", callbackTarget.String(), "keyword", kw)
		s.scheduleAutoCallback(callbackTarget, "text_trigger: "+kw)
	}
}

func (s *server) handleSessionDownloadMedia(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r, r.PathValue("sid"))
	if sess == nil {
		return
	}
	msgID := strings.TrimSpace(r.PathValue("id"))
	if msgID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "message id required"})
		return
	}
	item, found := sess.lookupInboundMedia(msgID)
	if !found {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "media expired or not found"})
		return
	}
	cli := sess.currentClient()
	if cli == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "whatsapp client unavailable"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	data, err := cli.Download(ctx, item.Downloadable)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to download media: " + err.Error()})
		return
	}
	mimeType := item.MimeType
	if mimeType == "" {
		mimeType = http.DetectContentType(data)
	}
	w.Header().Set("Content-Type", mimeType)
	if item.Filename != "" {
		safeName := strings.ReplaceAll(item.Filename, `"`, "")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename="%s"`, safeName))
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *server) handleSessionPresence(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r, r.PathValue("sid"))
	if sess == nil {
		return
	}
	var body struct {
		To    string `json:"to"`
		State string `json:"state"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	target, err := resolveTargetJID(body.To)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	cli := sess.currentClient()
	if cli == nil || !cli.IsConnected() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "whatsapp session not connected"})
		return
	}

	var cp types.ChatPresence
	var media types.ChatPresenceMedia
	switch strings.ToLower(strings.TrimSpace(body.State)) {
	case "composing", "typing":
		cp = types.ChatPresenceComposing
		media = types.ChatPresenceMediaText
	case "recording":
		cp = types.ChatPresenceComposing
		media = types.ChatPresenceMediaAudio
	case "paused":
		cp = types.ChatPresencePaused
		media = types.ChatPresenceMediaText
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid presence state (allowed: composing, paused, recording)"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := cli.SendChatPresence(ctx, target, cp, media); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to send presence: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) handleSessionMarkRead(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r, r.PathValue("sid"))
	if sess == nil {
		return
	}
	msgID := strings.TrimSpace(r.PathValue("id"))
	if msgID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "message id required"})
		return
	}
	var body struct {
		Chat   string `json:"chat"`
		Sender string `json:"sender"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	chatJID, err := resolveTargetJID(body.Chat)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid chat JID: " + err.Error()})
		return
	}
	var senderJID types.JID
	if strings.TrimSpace(body.Sender) != "" {
		senderJID, err = resolveTargetJID(body.Sender)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid sender JID: " + err.Error()})
			return
		}
	}
	cli := sess.currentClient()
	if cli == nil || !cli.IsConnected() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "whatsapp session not connected"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := cli.MarkRead(ctx, []types.MessageID{types.MessageID(msgID)}, time.Now(), chatJID, senderJID); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to mark message read: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) handleSessionGetRecording(w http.ResponseWriter, r *http.Request) {
	sess := s.sessionByID(w, r, r.PathValue("sid"))
	if sess == nil {
		return
	}
	callID := strings.TrimSpace(r.PathValue("id"))
	if callID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "call id required"})
		return
	}
	if r.URL.Query().Get("presign") == "1" {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		presigned, err := presignRecordingURL(ctx, sess.platform, sess.businessID, callID, 15*time.Minute)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "recording unavailable: " + err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"url": presigned, "expires_in_seconds": 900})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	obj, info, err := openRecordingObject(ctx, sess.platform, sess.businessID, callID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "recording not found"})
		return
	}
	defer obj.Close()

	ct := info.ContentType
	if ct == "" {
		ct = "audio/wav"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename="%s.wav"`, callID))
	http.ServeContent(w, r, callID+".wav", info.LastModified, obj)
}
