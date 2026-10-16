// Package restweb serves the embedded, named-operator browser control plane.
package restweb

import (
	"crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/json"
	"io/fs"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/leehosanganson/factory/internal/restapi"
	"github.com/leehosanganson/factory/internal/restserver"
)

//go:embed assets/*
var embedded embed.FS

const (
	cookieName      = "factory_operator_session"
	sessionTTL      = 8 * time.Hour
	maxLiveSessions = 1000
)

type session struct {
	principal restserver.Principal
	csrf      string
	expires   time.Time
}

// Handler provides the UI and a narrow authenticated same-origin REST proxy.
type Handler struct {
	api        *restapi.Handler
	operatorID string
	mu         sync.Mutex
	sessions   map[string]session
	now        func() time.Time
}

func New(api *restapi.Handler, operatorID string) http.Handler {
	return &Handler{api: api, operatorID: operatorID, sessions: make(map[string]session), now: time.Now}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	secureHeaders(w)
	if r.URL.RawQuery != "" && !(strings.HasPrefix(r.URL.Path, "/ui/api/") && r.Method == http.MethodGet) {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if r.URL.Path == "/ui/login" && r.Method == http.MethodPost {
		h.login(w, r)
		return
	}
	if r.URL.Path == "/ui/logout" && r.Method == http.MethodPost {
		h.logout(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/ui/api/") {
		h.proxy(w, r)
		return
	}
	if r.URL.Path == "/ui/login" || r.URL.Path == "/ui/logout" {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	if r.URL.Path == "/" || r.URL.Path == "/ui" || r.URL.Path == "/ui/" {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		h.asset(w, "assets/index.html")
		return
	}
	if strings.HasPrefix(r.URL.Path, "/ui/assets/") {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		h.asset(w, "assets/"+strings.TrimPrefix(r.URL.Path, "/ui/assets/"))
		return
	}
	if r.URL.Path == "/ui/session" {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		if current, ok := h.getSession(r); ok {
			writeJSON(w, http.StatusOK, map[string]any{"authenticated": true, "csrf": current.csrf, "operator": current.principal.ID})
		} else {
			writeJSON(w, http.StatusOK, map[string]any{"authenticated": false})
		}
		return
	}
	http.NotFound(w, r)
}

func secureHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	w.Header().Set("Cache-Control", "no-store")
}

func (h *Handler) asset(w http.ResponseWriter, name string) {
	file, err := fs.ReadFile(embedded, name)
	if err != nil {
		http.NotFound(w, nil)
		return
	}
	if strings.HasSuffix(name, ".css") {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	} else if strings.HasSuffix(name, ".js") {
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	} else {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	}
	_, _ = w.Write(file)
}

func (h *Handler) login(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" || !sameOrigin(r) || r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := r.ParseForm(); err != nil || len(r.Form) != 1 || r.Form.Get("token") == "" {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}
	if !h.api.AuthenticateDashboardOperator(r.Form.Get("token")) {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}
	id, err := randomValue()
	if err != nil {
		http.Error(w, "session unavailable", http.StatusInternalServerError)
		return
	}
	csrf, err := randomValue()
	if err != nil {
		http.Error(w, "session unavailable", http.StatusInternalServerError)
		return
	}
	h.mu.Lock()
	h.purgeExpiredLocked()
	if len(h.sessions) >= maxLiveSessions {
		h.mu.Unlock()
		http.Error(w, "session capacity full", http.StatusServiceUnavailable)
		return
	}
	principal := restserver.Principal{ID: h.operatorID, Role: "operator"}
	h.sessions[id] = session{principal: principal, csrf: csrf, expires: h.now().Add(sessionTTL)}
	h.mu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: id, Path: "/", HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode, Expires: h.now().Add(sessionTTL), MaxAge: int(sessionTTL.Seconds())})
	writeJSON(w, http.StatusOK, map[string]any{"authenticated": true, "csrf": csrf, "operator": principal.ID})
}

func (h *Handler) logout(w http.ResponseWriter, r *http.Request) {
	current, ok := h.getSession(r)
	if !ok || !validCSRF(r, current) || !sameOrigin(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	cookie, _ := r.Cookie(cookieName)
	h.mu.Lock()
	delete(h.sessions, cookie.Value)
	h.mu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: "", Path: "/", HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode, MaxAge: -1})
	writeJSON(w, http.StatusOK, map[string]bool{"authenticated": false})
}

func (h *Handler) proxy(w http.ResponseWriter, r *http.Request) {
	if r.URL.EscapedPath() != r.URL.Path || r.URL.RawQuery != "" && !(r.URL.Path == "/ui/api/v1/jobs" && r.Method == http.MethodGet) {
		http.NotFound(w, r)
		return
	}
	current, ok := h.getSession(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	path := "/" + strings.TrimPrefix(r.URL.Path, "/ui/api/")
	if r.Method != http.MethodGet {
		if !validCSRF(r, current) || !sameOrigin(r) {
			http.Error(w, "request rejected", http.StatusForbidden)
			return
		}
	}
	r.URL.Path = path
	r.URL.RawPath = ""
	h.api.ServeAsDashboardOperator(w, r)
}

func (h *Handler) getSession(r *http.Request) (session, bool) {
	cookie, err := r.Cookie(cookieName)
	if err != nil || cookie.Value == "" {
		return session{}, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.purgeExpiredLocked()
	current, ok := h.sessions[cookie.Value]
	return current, ok
}

func (h *Handler) purgeExpiredLocked() {
	now := h.now()
	for id, current := range h.sessions {
		if !now.Before(current.expires) {
			delete(h.sessions, id)
		}
	}
}

func validCSRF(r *http.Request, current session) bool {
	return r.Header.Get("X-CSRF-Token") != "" && r.Header.Get("X-CSRF-Token") == current.csrf
}

func sameOrigin(r *http.Request) bool {
	return r.Header.Get("Origin") == "https://"+r.Host
}

func randomValue() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func methodNotAllowed(w http.ResponseWriter, method string) {
	w.Header().Set("Allow", method)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
