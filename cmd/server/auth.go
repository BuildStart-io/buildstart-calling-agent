package main

import (
	"context"
	"crypto/subtle"
	"net/http"
	"os"
	"strings"
)

type authCtxKey string

const ctxKeyPlatform authCtxKey = "platform"

// loadPlatformAPIKeys builds the key -> platform mapping from environment variables.
// GO_API_KEY maps to "local"; GO_API_KEY_GLOBAL maps to "global".
// Optional PLATFORM_API_KEYS supports comma-separated "platform:key" entries.
func loadPlatformAPIKeys() map[string]string {
	out := make(map[string]string)
	if k := strings.TrimSpace(os.Getenv("GO_API_KEY")); k != "" {
		out["local"] = k
	}
	if k := strings.TrimSpace(os.Getenv("GO_API_KEY_GLOBAL")); k != "" {
		out["global"] = k
	}
	if extra := strings.TrimSpace(os.Getenv("PLATFORM_API_KEYS")); extra != "" {
		for _, pair := range strings.Split(extra, ",") {
			parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
			if len(parts) != 2 {
				continue
			}
			plat := strings.ToLower(strings.TrimSpace(parts[0]))
			key := strings.TrimSpace(parts[1])
			if plat != "" && key != "" {
				out[plat] = key
			}
		}
	}
	return out
}

func extractAPIKey(r *http.Request) string {
	if k := strings.TrimSpace(r.Header.Get("X-Api-Key")); k != "" {
		return k
	}
	authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(authHeader) > 7 && strings.EqualFold(authHeader[:7], "bearer ") {
		return strings.TrimSpace(authHeader[7:])
	}
	return ""
}

func matchPlatformKey(provided string, keys map[string]string) (string, bool) {
	if provided == "" || len(keys) == 0 {
		return "", false
	}
	var matchedPlatform string
	var matched bool
	// Compare against every configured key so platform count timing stays uniform.
	for plat, expected := range keys {
		if subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) == 1 {
			matchedPlatform = plat
			matched = true
		}
	}
	return matchedPlatform, matched
}

func callerPlatform(r *http.Request) string {
	if p, ok := r.Context().Value(ctxKeyPlatform).(string); ok && p != "" {
		return p
	}
	return ""
}

func isPublicHealthPath(path string) bool {
	switch path {
	case "/health", "/api/health", "/api/v1/health":
		return true
	default:
		return false
	}
}

// withPlatformAuth protects all non-public routes with platform-scoped API keys.
func (s *server) withPlatformAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}

		keys := s.apiKeys
		if len(keys) == 0 {
			keys = loadPlatformAPIKeys()
		}

		provided := extractAPIKey(r)
		plat, ok := matchPlatformKey(provided, keys)

		if isPublicHealthPath(r.URL.Path) {
			if ok {
				r = r.WithContext(context.WithValue(r.Context(), ctxKeyPlatform, plat))
			}
			next.ServeHTTP(w, r)
			return
		}

		if len(keys) == 0 {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "server API keys not configured"})
			return
		}
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}

		r = r.WithContext(context.WithValue(r.Context(), ctxKeyPlatform, plat))
		next.ServeHTTP(w, r)
	})
}
