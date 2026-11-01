package restserver

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/leehosanganson/factory/internal/rest/jobs"
)

const maxWorkerSessionRequestBytes = 1024

type WorkerListenerHandler struct {
	store      restjobs.WorkerSessionStore
	principals map[string]string
	ready      func() bool
	logger     *log.Logger
}

func NewWorkerListenerHandler(config WorkerListenerConfig, store restjobs.WorkerSessionStore, ready func() bool, loggers ...*log.Logger) (*WorkerListenerHandler, error) {
	if store == nil || ready == nil || len(config.Certificates) == 0 {
		return nil, errors.New("worker listener dependencies are unavailable")
	}
	principals := make(map[string]string, len(config.Certificates))
	for _, item := range config.Certificates {
		if item.Enabled {
			principals[item.SHA256] = item.PrincipalID
		}
	}
	var logger *log.Logger
	if len(loggers) > 0 {
		logger = loggers[0]
	}
	return &WorkerListenerHandler{store: store, principals: principals, ready: ready, logger: logger}, nil
}

func (h *WorkerListenerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost && r.Method != http.MethodDelete {
		writeWorkerJSON(w, http.StatusNotFound, map[string]string{"code": "not_found"})
		return
	}
	if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
		writeWorkerJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	if r.URL.Path == "/readyz" && r.Method == http.MethodGet {
		if !h.ready() {
			writeWorkerJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
			return
		}
		writeWorkerJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		return
	}
	principal, ok := h.certificatePrincipal(r)
	if !ok {
		h.logAuthentication(false)
		writeWorkerJSON(w, http.StatusUnauthorized, map[string]string{"code": "unauthenticated"})
		return
	}
	h.logAuthentication(true)
	if r.URL.Path == "/v1/worker/session" && r.Method == http.MethodPost {
		h.register(w, r, principal)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/worker/session/") && r.Method == http.MethodDelete {
		sessionID := strings.TrimPrefix(r.URL.Path, "/v1/worker/session/")
		if sessionID == "" || strings.Contains(sessionID, "/") {
			writeWorkerJSON(w, http.StatusNotFound, map[string]string{"code": "not_found"})
			return
		}
		bootID, valid := decodeWorkerBootID(w, r)
		if !valid {
			return
		}
		session, err := h.store.RetireWorkerSession(r.Context(), principal, bootID, sessionID)
		if err != nil {
			writeWorkerError(w, err)
			return
		}
		writeWorkerJSON(w, http.StatusOK, session)
		return
	}
	writeWorkerJSON(w, http.StatusNotFound, map[string]string{"code": "not_found"})
}

func (h *WorkerListenerHandler) register(w http.ResponseWriter, r *http.Request, principal string) {
	if !h.ready() {
		writeWorkerJSON(w, http.StatusServiceUnavailable, map[string]string{"code": "not_ready"})
		return
	}
	bootID, valid := decodeWorkerBootID(w, r)
	if !valid {
		return
	}
	session, replay, err := h.store.RegisterWorkerSession(r.Context(), principal, bootID)
	if errors.Is(err, restjobs.ErrWorkerSessionConflict) {
		writeWorkerJSON(w, http.StatusConflict, map[string]string{"code": "session_conflict"})
		return
	}
	if errors.Is(err, restjobs.ErrWorkerSessionBusy) {
		writeWorkerJSON(w, http.StatusServiceUnavailable, map[string]string{"code": "session_unavailable"})
		return
	}
	if err != nil {
		writeWorkerJSON(w, http.StatusServiceUnavailable, map[string]string{"code": "session_unavailable"})
		return
	}
	status := http.StatusCreated
	if replay {
		status = http.StatusOK
	}
	writeWorkerJSON(w, status, session)
}

func decodeWorkerBootID(w http.ResponseWriter, r *http.Request) (string, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxWorkerSessionRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request struct {
		BootID string `json:"boot_id"`
	}
	if decoder.Decode(&request) != nil || request.BootID == "" || decoder.Decode(new(any)) != io.EOF {
		writeWorkerJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_request"})
		return "", false
	}
	return request.BootID, true
}

func (h *WorkerListenerHandler) logAuthentication(succeeded bool) {
	if h.logger == nil {
		return
	}
	outcome := "failed"
	if succeeded {
		outcome = "succeeded"
	}
	h.logger.Printf("worker identity authentication %s", outcome)
}

func (h *WorkerListenerHandler) certificatePrincipal(r *http.Request) (string, bool) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return "", false
	}
	fingerprint := sha256.Sum256(r.TLS.PeerCertificates[0].Raw)
	principal, ok := h.principals[hex.EncodeToString(fingerprint[:])]
	return principal, ok
}

func NewWorkerListenerTLS(config WorkerListenerConfig) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(config.ServerCertFile, config.ServerKeyFile)
	if err != nil {
		return nil, errors.New("load worker listener TLS certificate")
	}
	caPEM, err := os.ReadFile(config.ClientCAFile)
	if err != nil {
		return nil, errors.New("load worker listener client CA")
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("worker listener client CA is invalid")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}, ClientCAs: clientCAs, ClientAuth: tls.RequireAndVerifyClientCert}, nil
}

func writeWorkerError(w http.ResponseWriter, err error) {
	if errors.Is(err, restjobs.ErrNotFound) {
		writeWorkerJSON(w, http.StatusNotFound, map[string]string{"code": "not_found"})
		return
	}
	if errors.Is(err, restjobs.ErrInvalidInput) {
		writeWorkerJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_request"})
		return
	}
	writeWorkerJSON(w, http.StatusConflict, map[string]string{"code": "session_conflict"})
}

func writeWorkerJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
