package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

var (
	waVersionMu          sync.Mutex
	lastWAVersionRefresh time.Time
)

func refreshWAVersion(ctx context.Context, log *slog.Logger, force bool) {
	waVersionMu.Lock()
	defer waVersionMu.Unlock()

	if !force && !lastWAVersionRefresh.IsZero() && time.Since(lastWAVersionRefresh) < time.Hour {
		return
	}

	fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ver, err := whatsmeow.GetLatestVersion(fetchCtx, nil)
	if err != nil {
		if log != nil {
			log.Warn("failed to fetch latest WhatsApp web version, keeping current", "current", store.GetWAVersion().String(), "err", err)
		}
		return
	}
	if ver != nil && !ver.IsZero() {
		old := store.GetWAVersion()
		store.SetWAVersion(*ver)
		lastWAVersionRefresh = time.Now()
		if log != nil {
			log.Info("synchronized WhatsApp web client version", "old", old.String(), "new", ver.String())
		}
	}
}

func wrapCall(from types.JID, inner *waBinary.Node) *waBinary.Node {
	content := []waBinary.Node{}
	if inner != nil {
		content = append(content, *inner)
	}
	return &waBinary.Node{
		Tag:     "call",
		Attrs:   waBinary.Attrs{"from": from},
		Content: content,
	}
}
