// Package restapi implements the authenticated, executor-independent REST API.
package restapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restserver"
)

// Config contains request bounds and the configured repository aliases exposed
// to callers. Ready must report whether the server is accepting requests; a nil
// callback is not ready.
const (
	maxRequestBodyBytes = 2 << 20
	maxTaskBytes        = 256 << 10
)

// Config bounds requests and exposes only operator-approved repository aliases.
type Config struct {
	MaxRequestBodyBytes int
	MaxTaskBytes        int
	RepositoryAliases   map[string]struct{}
	Ready               func() bool
}

// Handler serves the REST API using the supplied manager and immutable API key.
type Handler struct {
	manager restjobs.Manager
	key     restserver.APIKey
	config  Config
}

var _ http.Handler = (*Handler)(nil)

// New validates the injectable handler configuration and constructs a handler.
func New(manager restjobs.Manager, key restserver.APIKey, config Config) (*Handler, error) {
	if manager == nil || config.MaxRequestBodyBytes < 1 || config.MaxRequestBodyBytes > maxRequestBodyBytes || config.MaxTaskBytes < 1 || config.MaxTaskBytes > maxTaskBytes || config.MaxTaskBytes > config.MaxRequestBodyBytes || len(config.RepositoryAliases) == 0 {
		return nil, errors.New("invalid REST API handler configuration")
	}
	aliases := make(map[string]struct{}, len(config.RepositoryAliases))
	for alias := range config.RepositoryAliases {
		if strings.TrimSpace(alias) != alias || alias == "" {
			return nil, errors.New("invalid REST API handler configuration")
		}
		aliases[alias] = struct{}{}
	}
	config.RepositoryAliases = aliases
	return &Handler{manager: manager, key: key, config: config}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" && r.URL.EscapedPath() == "/healthz" {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	if r.URL.Path == "/readyz" && r.URL.EscapedPath() == "/readyz" {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if h.config.Ready != nil && h.config.Ready() {
			writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		} else {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		}
		return
	}
	if h.config.Ready != nil && !h.config.Ready() {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "The server is not accepting requests.")
		return
	}

	if !isJobsPath(r.URL.Path) {
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		return
	}
	if !h.authorized(r) {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "Authentication is required.")
		return
	}
	if r.URL.EscapedPath() != r.URL.Path {
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		return
	}

	path := r.URL.Path
	switch {
	case path == "/v1/jobs":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		h.createJob(w, r)
	case strings.HasPrefix(path, "/v1/jobs/"):
		tail := strings.TrimPrefix(path, "/v1/jobs/")
		parts := strings.Split(tail, "/")
		if len(parts) == 1 && parts[0] != "" {
			if r.Method != http.MethodGet {
				methodNotAllowed(w, http.MethodGet)
				return
			}
			h.getJob(w, parts[0])
			return
		}
		if len(parts) == 2 && parts[0] != "" && parts[1] == "history" {
			if r.Method != http.MethodGet {
				methodNotAllowed(w, http.MethodGet)
				return
			}
			h.getHistory(w, parts[0])
			return
		}
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
	default:
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
	}
}

func isJobsPath(path string) bool {
	return path == "/v1/jobs" || strings.HasPrefix(path, "/v1/jobs/")
}

func (h *Handler) authorized(r *http.Request) bool {
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return false
	}
	fields := strings.Fields(values[0])
	return len(fields) == 2 && strings.EqualFold(fields[0], "Bearer") && h.key.Authenticate(fields[1])
}

func (h *Handler) createJob(w http.ResponseWriter, r *http.Request) {
	keyValues := r.Header.Values("Idempotency-Key")
	if len(keyValues) != 1 || strings.TrimSpace(keyValues[0]) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	contentTypes := r.Header.Values("Content-Type")
	mediaType := ""
	var err error
	if len(contentTypes) == 1 {
		mediaType, _, err = mime.ParseMediaType(contentTypes[0])
	} else {
		err = errors.New("invalid Content-Type")
	}
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json.")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(h.config.MaxRequestBodyBytes)))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", "Request body is too large.")
		} else {
			writeError(w, http.StatusBadRequest, "malformed_json", "Request body is invalid.")
		}
		return
	}
	if !utf8.Valid(body) {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	if err := validateJSONShape(body); err != nil {
		writeError(w, http.StatusBadRequest, "malformed_json", "Request body is invalid.")
		return
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		writeError(w, http.StatusBadRequest, "malformed_json", "Request body is invalid.")
		return
	}
	for name := range fields {
		if name != "repository" && name != "task" && name != "issue" {
			writeError(w, http.StatusBadRequest, "unknown_field", "Request contains an unsupported field.")
			return
		}
	}
	var request restjobs.Request
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	request.Repository = strings.TrimSpace(request.Repository)
	if len(request.Task) > h.config.MaxTaskBytes {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	if _, ok := h.config.RepositoryAliases[request.Repository]; !ok {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	if rawIssue, ok := fields["issue"]; ok {
		var issue int
		if err := json.Unmarshal(rawIssue, &issue); err != nil || issue <= 0 {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
	}
	snapshot, replayed, err := h.manager.Admit(keyValues[0], request)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	id := snapshot.ID
	writeJSON(w, http.StatusAccepted, struct {
		Job      restjobs.Snapshot `json:"job"`
		Replayed bool              `json:"replayed"`
		Links    linksDTO          `json:"links"`
	}{snapshot, replayed, linksDTO{Self: "/v1/jobs/" + id, History: "/v1/jobs/" + id + "/history"}})
}

type linksDTO struct {
	Self    string `json:"self"`
	History string `json:"history"`
}

type errorDTO struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (h *Handler) getJob(w http.ResponseWriter, id string) {
	snapshot, err := h.manager.Get(id)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (h *Handler) getHistory(w http.ResponseWriter, id string) {
	history, err := h.manager.History(id)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	safeEvents := make([]restjobs.Event, 0, len(history.Events))
	for _, event := range history.Events {
		if !safeEventType(event.Type) {
			continue
		}
		if !safeEventMessage(event.Type, event.Message) {
			event.Message = ""
		}
		safeEvents = append(safeEvents, event)
	}
	history.Events = safeEvents
	writeJSON(w, http.StatusOK, history)
}

func safeEventType(value string) bool {
	switch value {
	case "queued", "running", "succeeded", "failed", "canceled":
		return true
	default:
		return false
	}
}

func safeEventMessage(eventType, message string) bool {
	return message == "" || (eventType == "queued" && message == "Job admitted") || (eventType == "running" && message == "Job started") || ((eventType == "succeeded" || eventType == "failed" || eventType == "canceled") && message == "Job finished")
}

func (h *Handler) writeManagerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, restjobs.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
	case errors.Is(err, restjobs.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict", "Idempotency key conflicts with an existing request.")
	case errors.Is(err, restjobs.ErrQueueFull):
		writeError(w, http.StatusServiceUnavailable, "queue_full", "Job queue is full.")
	case errors.Is(err, restjobs.ErrRegistryFull):
		writeError(w, http.StatusServiceUnavailable, "registry_full", "Job registry is full.")
	case errors.Is(err, restjobs.ErrManagerClosed):
		writeError(w, http.StatusServiceUnavailable, "server_shutting_down", "The server is shutting down.")
	case errors.Is(err, restjobs.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "An internal error occurred.")
	}
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed.")
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]errorDTO{"error": {Code: code, Message: message}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// validateJSONShape rejects duplicate keys (including nested object keys),
// malformed input, and trailing JSON values before typed decoding.
func validateJSONShape(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("expected one JSON value")
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("invalid JSON object key")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate JSON object key")
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("invalid JSON delimiter")
	}
}
