package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type authCtxKey string

const (
	ctxKeyPlatform    authCtxKey = "platform"
	consoleCookieName            = "wacalls_console"
)

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

func isConsoleEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("SERVE_STATIC_CONSOLE")), "true")
}

func signConsoleCookie(secret, plat string, exp int64) string {
	data := fmt.Sprintf("%s.%d", plat, exp)
	mac := hmac.New(sha256.New, []byte("console:"+secret))
	mac.Write([]byte(data))
	return data + "." + hex.EncodeToString(mac.Sum(nil))
}

func verifyConsoleCookie(secret, token string) (string, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", false
	}
	plat := parts[0]
	if plat != "local" && plat != "global" {
		return "", false
	}
	exp, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || time.Now().Unix() > exp {
		return "", false
	}
	expected := signConsoleCookie(secret, plat, exp)
	if subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
		return "", false
	}
	return plat, true
}

func (s *server) tryConsoleAuth(w http.ResponseWriter, r *http.Request) (string, bool) {
	if !isConsoleEnabled() {
		return "", false
	}
	secret := strings.TrimSpace(os.Getenv("CONSOLE_PASSWORD"))
	if secret == "" {
		return "", false
	}

	if c, err := r.Cookie(consoleCookieName); err == nil {
		if plat, ok := verifyConsoleCookie(secret, c.Value); ok {
			return plat, true
		}
	}

	if user, pass, ok := r.BasicAuth(); ok && subtle.ConstantTimeCompare([]byte(pass), []byte(secret)) == 1 {
		plat := "local"
		if strings.EqualFold(strings.TrimSpace(user), "global") {
			plat = "global"
		}
		exp := time.Now().Add(12 * time.Hour).Unix()
		http.SetCookie(w, &http.Cookie{
			Name:     consoleCookieName,
			Value:    signConsoleCookie(secret, plat, exp),
			Path:     "/",
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
			Secure:   r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https"),
			MaxAge:   43200,
		})
		return plat, true
	}
	return "", false
}

// withPlatformAuth protects all non-public routes with platform-scoped API keys
// (and optional Basic/Cookie console auth when SERVE_STATIC_CONSOLE=true + CONSOLE_PASSWORD are set).
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
			if cPlat, cOK := s.tryConsoleAuth(w, r); cOK {
				plat = cPlat
				ok = true
			}
		}

		if !ok {
			if isConsoleEnabled() && strings.TrimSpace(os.Getenv("CONSOLE_PASSWORD")) != "" &&
				!strings.HasPrefix(r.URL.Path, "/api/") && !strings.HasPrefix(r.URL.Path, "/ws/") {
				w.Header().Set("WWW-Authenticate", `Basic realm="BuildStart Calling Console"`)
				http.Error(w, "Authentication required", http.StatusUnauthorized)
				return
			}
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}

		r = r.WithContext(context.WithValue(r.Context(), ctxKeyPlatform, plat))
		next.ServeHTTP(w, r)
	})
}
