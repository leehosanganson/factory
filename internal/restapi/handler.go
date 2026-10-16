// Package restapi implements the authenticated, executor-independent REST API.
package restapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restserver"
)

// Config contains request bounds and the configured repository aliases exposed
// to callers. Ready must report whether the server is accepting requests; a nil
// callback is not ready.
const (
	maxRequestBodyBytes       = 2 << 20
	maxTaskBytes              = 256 << 10
	readinessConcurrencyLimit = 4
	eventStreamConcurrency    = 16
	eventStreamPollInterval   = 250 * time.Millisecond
	eventStreamHeartbeat      = 15 * time.Second
	eventStreamMaxLifetime    = 25 * time.Second
	eventStreamWriteTimeout   = 2 * time.Second
	submitterCursorTTL        = 15 * time.Minute
	submitterCursorCapacity   = 10000
)

// Config bounds requests and exposes only operator-approved repository aliases.
type Config struct {
	MaxRequestBodyBytes int
	MaxTaskBytes        int
	RepositoryAliases   map[string]struct{}
	Ready               func() bool
	ReadyError          func(context.Context) error
	ReconcileProvider   func(context.Context, string) error
	ResolveInterrupted  func(context.Context, string, restjobs.InterruptedDisposition) error
	CancelJob           func(context.Context, string) (restjobs.Snapshot, error)
}

// Handler serves the REST API using the supplied manager and immutable API key.
type Handler struct {
	manager         restjobs.Manager
	key             restserver.APIKey
	config          Config
	readinessChecks chan struct{}
	eventStreams    chan struct{}
	cursorMu        sync.Mutex
	cursors         map[string]submitterCursor
	cursorKeys      map[submitterCursorKey]string
	now             func() time.Time
}

type submitterCursorKey struct {
	principalID string
	after       uint64
	snapshot    uint64
}

type submitterCursor struct {
	principalID string
	after       uint64
	snapshot    uint64
	expiresAt   time.Time
}

type submitterJobPage struct {
	Jobs       []restjobs.JobSummary `json:"jobs"`
	HasMore    bool                  `json:"has_more"`
	NextCursor string                `json:"next_cursor"`
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
	return &Handler{manager: manager, key: key, config: config, readinessChecks: make(chan struct{}, readinessConcurrencyLimit), eventStreams: make(chan struct{}, eventStreamConcurrency), cursors: make(map[string]submitterCursor), cursorKeys: make(map[submitterCursorKey]string), now: time.Now}, nil
}

func (h *Handler) ready() bool {
	return h.config.Ready != nil && h.config.Ready()
}

func (h *Handler) readinessError(ctx context.Context) error {
	if h.config.Ready == nil {
		return nil
	}
	if !h.config.Ready() {
		return errors.New("server not ready")
	}
	select {
	case h.readinessChecks <- struct{}{}:
		defer func() { <-h.readinessChecks }()
	default:
		return errors.New("readiness capacity exhausted")
	}
	if h.config.ReadyError != nil {
		return h.config.ReadyError(ctx)
	}
	return nil
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
		if h.config.Ready == nil || h.readinessError(r.Context()) != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		} else {
			writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		}
		return
	}
	if h.readinessError(r.Context()) != nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "The server is not accepting requests.")
		return
	}

	if !isJobsPath(r.URL.Path) && r.URL.Path != "/v1/operations" {
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		return
	}
	caller, ok := h.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "Authentication is required.")
		return
	}
	if r.URL.EscapedPath() != r.URL.Path {
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		return
	}

	path := r.URL.Path
	if caller.Role == "submitter" && !submitterRoute(path, r.Method) {
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		return
	}
	switch {
	case path == "/v1/operations":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		h.getOperations(w, r)
	case path == "/v1/jobs":
		if r.Method == http.MethodGet {
			if caller.Role == "submitter" {
				h.listJobsForPrincipal(w, r, caller.ID)
			} else {
				h.listJobs(w, r)
			}
			return
		}
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodGet+", "+http.MethodPost)
			return
		}
		h.createJob(w, r, caller)
	case strings.HasPrefix(path, "/v1/jobs/"):
		tail := strings.TrimPrefix(path, "/v1/jobs/")
		parts := strings.Split(tail, "/")
		if len(parts) == 1 && parts[0] != "" {
			if r.Method != http.MethodGet {
				methodNotAllowed(w, http.MethodGet)
				return
			}
			if caller.Role == "submitter" {
				h.getJobForPrincipal(w, parts[0], caller.ID)
			} else {
				h.getJob(w, parts[0])
			}
			return
		}
		if len(parts) == 2 && parts[0] != "" && parts[1] == "issue-provenance" {
			if r.Method != http.MethodGet {
				methodNotAllowed(w, http.MethodGet)
				return
			}
			if caller.Role == "submitter" {
				h.getIssueProvenanceForPrincipal(w, parts[0], caller.ID)
			} else {
				h.getIssueProvenance(w, parts[0])
			}
			return
		}
		if len(parts) == 2 && parts[0] != "" && parts[1] == "history" {
			if r.Method != http.MethodGet {
				methodNotAllowed(w, http.MethodGet)
				return
			}
			if caller.Role == "submitter" {
				h.getHistoryForPrincipal(w, parts[0], caller.ID)
			} else {
				h.getHistory(w, parts[0])
			}
			return
		}
		if len(parts) == 2 && parts[0] != "" && parts[1] == "events" {
			if r.Method != http.MethodGet {
				methodNotAllowed(w, http.MethodGet)
				return
			}
			if caller.Role == "submitter" {
				h.streamEventsForPrincipal(w, r, parts[0], caller.ID)
			} else {
				h.streamEvents(w, r, parts[0])
			}
			return
		}
		if len(parts) == 2 && parts[0] != "" && parts[1] == "cancel" {
			if r.Method != http.MethodPost {
				methodNotAllowed(w, http.MethodPost)
				return
			}
			h.cancelJob(w, r, parts[0])
			return
		}
		if len(parts) == 2 && parts[0] != "" && parts[1] == "reconcile" {
			if r.Method != http.MethodPost {
				methodNotAllowed(w, http.MethodPost)
				return
			}
			h.reconcileProvider(w, r, parts[0])
			return
		}
		if len(parts) == 3 && parts[0] != "" && parts[1] == "disposition" {
			if r.Method != http.MethodPost {
				methodNotAllowed(w, http.MethodPost)
				return
			}
			h.resolveInterrupted(w, r, parts[0], parts[2])
			return
		}
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
	default:
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
	}
}

func submitterRoute(path, method string) bool {
	if path == "/v1/jobs" {
		return method == http.MethodGet || method == http.MethodPost
	}
	if !strings.HasPrefix(path, "/v1/jobs/") || method != http.MethodGet {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(path, "/v1/jobs/"), "/")
	return len(parts) == 1 && parts[0] != "" || len(parts) == 2 && parts[0] != "" && (parts[1] == "history" || parts[1] == "events" || parts[1] == "issue-provenance")
}

func (h *Handler) listJobs(w http.ResponseWriter, r *http.Request) {
	query, err := parseJobListQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	page, err := h.manager.ListJobs(ctx, query.after, query.snapshot, query.limit)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

type jobListQuery struct {
	after    uint64
	snapshot uint64
	limit    int
}

func (h *Handler) listJobsForPrincipal(w http.ResponseWriter, r *http.Request, principalID string) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	for key, entries := range values {
		if (key != "limit" && key != "cursor") || len(entries) != 1 {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
	}
	limit := restjobs.DefaultJobListLimit
	if value, exists := values["limit"]; exists {
		parsed, parseErr := strconv.Atoi(value[0])
		if parseErr != nil || parsed < 1 || parsed > restjobs.MaxJobListLimit {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
		limit = parsed
	}
	var after, snapshot uint64
	if value, exists := values["cursor"]; exists {
		state, ok := h.lookupSubmitterCursor(value[0], principalID)
		if !ok {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
		after, snapshot = state.after, state.snapshot
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	page, err := h.manager.ListForPrincipal(ctx, principalID, after, snapshot, limit)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	response := submitterJobPage{Jobs: page.Jobs, HasMore: page.HasMore}
	if page.HasMore {
		cursor, ok := h.storeSubmitterCursor(principalID, page.NextSequence, page.SnapshotSequence)
		if !ok {
			writeError(w, http.StatusServiceUnavailable, "cursor_capacity", "Pagination capacity is full.")
			return
		}
		response.NextCursor = cursor
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) lookupSubmitterCursor(cursor, principalID string) (submitterCursor, bool) {
	now := h.now()
	h.cursorMu.Lock()
	defer h.cursorMu.Unlock()
	h.purgeSubmitterCursorsLocked(now)
	state, ok := h.cursors[cursor]
	if !ok || state.principalID != principalID {
		return submitterCursor{}, false
	}
	return state, true
}

func (h *Handler) storeSubmitterCursor(principalID string, after, snapshot uint64) (string, bool) {
	now := h.now()
	h.cursorMu.Lock()
	defer h.cursorMu.Unlock()
	h.purgeSubmitterCursorsLocked(now)
	key := submitterCursorKey{principalID: principalID, after: after, snapshot: snapshot}
	if cursor, exists := h.cursorKeys[key]; exists {
		if state, ok := h.cursors[cursor]; ok && state.principalID == principalID && state.after == after && state.snapshot == snapshot {
			return cursor, true
		}
		delete(h.cursorKeys, key)
	}
	if len(h.cursors) >= submitterCursorCapacity {
		return "", false
	}
	for range 3 {
		var raw [32]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return "", false
		}
		cursor := base64.RawURLEncoding.EncodeToString(raw[:])
		if _, exists := h.cursors[cursor]; exists {
			continue
		}
		h.cursors[cursor] = submitterCursor{principalID: principalID, after: after, snapshot: snapshot, expiresAt: now.Add(submitterCursorTTL)}
		h.cursorKeys[key] = cursor
		return cursor, true
	}
	return "", false
}

func (h *Handler) purgeSubmitterCursorsLocked(now time.Time) {
	for cursor, state := range h.cursors {
		if !now.Before(state.expiresAt) {
			delete(h.cursors, cursor)
			key := submitterCursorKey{principalID: state.principalID, after: state.after, snapshot: state.snapshot}
			if h.cursorKeys[key] == cursor {
				delete(h.cursorKeys, key)
			}
		}
	}
}

func parseJobListQuery(r *http.Request) (jobListQuery, error) {
	query := jobListQuery{limit: restjobs.DefaultJobListLimit}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return jobListQuery{}, restjobs.ErrInvalidInput
	}
	for key, entries := range values {
		if (key != "limit" && key != "after" && key != "snapshot") || len(entries) != 1 {
			return jobListQuery{}, restjobs.ErrInvalidInput
		}
	}
	if value, exists := values["limit"]; exists {
		parsed, parseErr := strconv.Atoi(value[0])
		if parseErr != nil || parsed < 1 || parsed > restjobs.MaxJobListLimit {
			return jobListQuery{}, restjobs.ErrInvalidInput
		}
		query.limit = parsed
	}
	for key, target := range map[string]*uint64{"after": &query.after, "snapshot": &query.snapshot} {
		if value, exists := values[key]; exists {
			parsed, parseErr := strconv.ParseUint(value[0], 10, 64)
			if parseErr != nil || parsed == 0 {
				return jobListQuery{}, restjobs.ErrInvalidInput
			}
			*target = parsed
		}
	}
	_, hasAfter := values["after"]
	_, hasSnapshot := values["snapshot"]
	if hasAfter != hasSnapshot || (hasAfter && query.after > query.snapshot) {
		return jobListQuery{}, restjobs.ErrInvalidInput
	}
	return query, nil
}

func (h *Handler) getOperations(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	summary, err := h.manager.OperationalSummary(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "An internal error occurred.")
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func isJobsPath(path string) bool {
	return path == "/v1/jobs" || strings.HasPrefix(path, "/v1/jobs/")
}

func (h *Handler) authenticate(r *http.Request) (restserver.Principal, bool) {
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return restserver.Principal{}, false
	}
	fields := strings.Fields(values[0])
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
		return restserver.Principal{}, false
	}
	resolved, ok := h.key.Resolve(fields[1])
	return resolved, ok
}

func (h *Handler) createJob(w http.ResponseWriter, r *http.Request, caller restserver.Principal) {
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
		if name != "repository" && name != "task" && name != "issue" && name != "issue_provenance" {
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
	if rawProvenance, ok := fields["issue_provenance"]; ok {
		if !validProvenanceShape(rawProvenance) {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
	}
	snapshot, replayed, err := h.manager.AdmitAs(keyValues[0], caller.ID, request)
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

func (h *Handler) resolveInterrupted(w http.ResponseWriter, r *http.Request, id, rawDisposition string) {
	if !bodyless(r) {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	var disposition restjobs.InterruptedDisposition
	switch rawDisposition {
	case string(restjobs.InterruptedDispositionFailed):
		disposition = restjobs.InterruptedDispositionFailed
	case string(restjobs.InterruptedDispositionCanceled):
		disposition = restjobs.InterruptedDispositionCanceled
	default:
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	if h.config.ResolveInterrupted == nil {
		writeError(w, http.StatusConflict, "reconciliation_not_allowed", "Job is not eligible for operator disposition.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	if err := h.config.ResolveInterrupted(ctx, id, disposition); err != nil {
		h.writeManagerError(w, err)
		return
	}
	snapshot, err := h.manager.Get(id)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (h *Handler) cancelJob(w http.ResponseWriter, r *http.Request, id string) {
	if !bodyless(r) {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	var snapshot restjobs.Snapshot
	var err error
	if h.config.CancelJob != nil {
		snapshot, err = h.config.CancelJob(r.Context(), id)
	} else {
		snapshot, err = h.manager.Cancel(id)
	}
	if err != nil {
		if errors.Is(err, restjobs.ErrInvalidTransition) {
			writeError(w, http.StatusConflict, "job_not_cancelable", "Job is not eligible for cancellation.")
		} else {
			h.writeManagerError(w, err)
		}
		return
	}
	status := http.StatusAccepted
	if snapshot.Status == restjobs.StatusCanceled {
		status = http.StatusOK
	}
	writeJSON(w, status, snapshot)
}

func (h *Handler) reconcileProvider(w http.ResponseWriter, r *http.Request, id string) {
	if !bodyless(r) {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	if h.config.ReconcileProvider == nil {
		writeError(w, http.StatusConflict, "reconciliation_unavailable", "Provider reconciliation is unavailable.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	if err := h.config.ReconcileProvider(ctx, id); err != nil {
		h.writeManagerError(w, err)
		return
	}
	snapshot, err := h.manager.Get(id)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func bodyless(r *http.Request) bool {
	if r.Body == nil || r.Body == http.NoBody {
		return true
	}
	if r.ContentLength > 0 {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1))
	return err == nil && len(body) == 0
}

func (h *Handler) getJobForPrincipal(w http.ResponseWriter, id, principalID string) {
	snapshot, err := h.manager.GetForPrincipal(id, principalID)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

type issueProvenanceResponse struct {
	JobID      string                   `json:"job_id"`
	Provenance restjobs.IssueProvenance `json:"issue_provenance"`
}

func (h *Handler) getIssueProvenanceForPrincipal(w http.ResponseWriter, id, principalID string) {
	snapshot, err := h.manager.GetForPrincipal(id, principalID)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	writeIssueProvenance(w, snapshot)
}

func (h *Handler) getIssueProvenance(w http.ResponseWriter, id string) {
	snapshot, err := h.manager.Get(id)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	writeIssueProvenance(w, snapshot)
}

func writeIssueProvenance(w http.ResponseWriter, snapshot restjobs.Snapshot) {
	if snapshot.Request.IssueProvenance == nil {
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		return
	}
	writeJSON(w, http.StatusOK, issueProvenanceResponse{JobID: snapshot.ID, Provenance: *snapshot.Request.IssueProvenance})
}

func (h *Handler) getHistoryForPrincipal(w http.ResponseWriter, id, principalID string) {
	if _, err := h.manager.GetForPrincipal(id, principalID); err != nil {
		h.writeManagerError(w, err)
		return
	}
	h.getHistory(w, id)
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

func (h *Handler) streamEventsForPrincipal(w http.ResponseWriter, r *http.Request, id, principalID string) {
	if _, err := h.manager.GetForPrincipal(id, principalID); err != nil {
		h.writeManagerError(w, err)
		return
	}
	h.streamEvents(w, r, id)
}

func (h *Handler) streamEvents(w http.ResponseWriter, r *http.Request, id string) {
	if r.URL.RawQuery != "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	values := r.Header.Values("Last-Event-ID")
	if len(values) > 1 {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	var cursor uint64
	hasCursor := len(values) == 1
	if hasCursor {
		parsed, err := strconv.ParseUint(values[0], 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
		cursor = parsed
	}
	select {
	case h.eventStreams <- struct{}{}:
		defer func() { <-h.eventStreams }()
	default:
		writeError(w, http.StatusServiceUnavailable, "stream_capacity", "Event stream capacity is full.")
		return
	}
	readCtx, cancel := context.WithTimeout(r.Context(), eventStreamWriteTimeout)
	initial, err := h.readStreamState(readCtx, id)
	cancel()
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	if (hasCursor && staleEventCursor(initial.History, cursor) != "") || (!hasCursor && initial.History.Truncated && !initial.Snapshot.Status.Terminal()) {
		writeError(w, http.StatusConflict, "cursor_expired", "Event history is incomplete; fetch job status and history before reconnecting.")
		return
	}
	if hasCursor && cursor > initial.History.LatestSequence {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	controller := http.NewResponseController(w)
	streamDeadline := time.Now().Add(eventStreamMaxLifetime)
	deadline := time.NewTimer(time.Until(streamDeadline))
	defer deadline.Stop()
	if err := setStreamWriteDeadline(controller, streamDeadline); err != nil {
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if err := controller.Flush(); err != nil {
		return
	}
	if err := clearStreamWriteDeadline(controller); err != nil {
		return
	}
	ticker := time.NewTicker(eventStreamPollInterval)
	defer ticker.Stop()
	heartbeat := time.NewTicker(eventStreamHeartbeat)
	defer heartbeat.Stop()
	sentTerminalEvent := false
	for {
		state := initial
		initial = streamState{}
		for _, event := range state.History.Events {
			if event.Sequence <= cursor {
				continue
			}
			if safeEventType(event.Type) {
				if err := writeStreamEvent(w, controller, streamDeadline, event); err != nil {
					return
				}
				if event.Type == string(state.Snapshot.Status) {
					sentTerminalEvent = true
				}
			}
			cursor = event.Sequence
		}
		if state.Snapshot.Status.Terminal() {
			if !sentTerminalEvent {
				data := fmt.Sprintf(`{"status":%q}`, state.Snapshot.Status)
				if err := writeStreamControl(w, controller, streamDeadline, "terminal_snapshot", data); err != nil {
					return
				}
			}
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-deadline.C:
			return
		case <-heartbeat.C:
			if err := writeStreamHeartbeat(w, controller, streamDeadline); err != nil {
				return
			}
		case <-ticker.C:
			pollCtx, cancel := context.WithTimeout(r.Context(), eventStreamWriteTimeout)
			state, err = h.readStreamState(pollCtx, id)
			cancel()
			if err != nil {
				return
			}
			if (cursor != 0 && staleEventCursor(state.History, cursor) != "") || (cursor == 0 && state.History.Truncated && !state.Snapshot.Status.Terminal()) {
				if writeStreamControl(w, controller, streamDeadline, "reset", `{"reason":"cursor_expired","fallback":"GET status and history"}`) != nil {
					return
				}
				return
			}
			initial = state
			if state.Snapshot.Status.Terminal() {
				for _, event := range state.History.Events {
					if event.Sequence <= cursor {
						continue
					}
					if safeEventType(event.Type) {
						if err := writeStreamEvent(w, controller, streamDeadline, event); err != nil {
							return
						}
						if event.Type == string(state.Snapshot.Status) {
							sentTerminalEvent = true
						}
					}
					cursor = event.Sequence
				}
				if !sentTerminalEvent {
					data := fmt.Sprintf(`{"status":%q}`, state.Snapshot.Status)
					if err := writeStreamControl(w, controller, streamDeadline, "terminal_snapshot", data); err != nil {
						return
					}
				}
				return
			}
		}
	}
}

type streamState struct {
	Snapshot restjobs.Snapshot
	History  restjobs.History
}

func (h *Handler) readStreamState(ctx context.Context, id string) (streamState, error) {
	readCtx, cancel := context.WithTimeout(ctx, eventStreamWriteTimeout)
	defer cancel()
	snapshot, history, err := h.manager.EventStreamState(readCtx, id)
	if err != nil {
		return streamState{}, err
	}
	return streamState{Snapshot: snapshot, History: history}, nil
}

func staleEventCursor(history restjobs.History, cursor uint64) string {
	if len(history.Events) > 0 && cursor < history.Events[0].Sequence-1 {
		return "expired"
	}
	if history.Truncated && len(history.Events) == 0 && cursor < history.LatestSequence {
		return "expired"
	}
	return ""
}

func writeStreamEvent(w http.ResponseWriter, controller *http.ResponseController, streamDeadline time.Time, event restjobs.Event) error {
	return writeStreamFrame(w, controller, streamDeadline, fmt.Sprintf("id: %d\nevent: %s\ndata: {\"at\":%q,\"type\":%q}\n\n", event.Sequence, event.Type, event.At.UTC().Format(time.RFC3339Nano), event.Type))
}

func writeStreamHeartbeat(w http.ResponseWriter, controller *http.ResponseController, streamDeadline time.Time) error {
	return writeStreamFrame(w, controller, streamDeadline, ": heartbeat\n\n")
}

func writeStreamControl(w http.ResponseWriter, controller *http.ResponseController, streamDeadline time.Time, event, data string) error {
	return writeStreamFrame(w, controller, streamDeadline, fmt.Sprintf("event: %s\ndata: %s\n\n", event, data))
}

func setStreamWriteDeadline(controller *http.ResponseController, streamDeadline time.Time) error {
	writeDeadline := time.Now().Add(eventStreamWriteTimeout)
	if streamDeadline.Before(writeDeadline) {
		writeDeadline = streamDeadline
	}
	if !time.Now().Before(writeDeadline) {
		return context.DeadlineExceeded
	}
	if err := controller.SetWriteDeadline(writeDeadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

func writeStreamFrame(w http.ResponseWriter, controller *http.ResponseController, streamDeadline time.Time, frame string) error {
	if err := setStreamWriteDeadline(controller, streamDeadline); err != nil {
		return err
	}
	_, writeErr := io.WriteString(w, frame)
	if writeErr == nil {
		writeErr = controller.Flush()
	}
	return errors.Join(writeErr, clearStreamWriteDeadline(controller))
}

func clearStreamWriteDeadline(controller *http.ResponseController) error {
	if err := controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

func safeEventType(value string) bool {
	switch value {
	case "queued", "running", "succeeded", "failed", "canceled", "cancel_requested", "provider_reconciled", "operator_disposition":
		return true
	default:
		return false
	}
}

func safeEventMessage(eventType, message string) bool {
	return message == "" || (eventType == "queued" && message == "Job admitted") || (eventType == "running" && message == "Job started") || (eventType == "cancel_requested" && message == "Cancellation requested") || (eventType == "canceled" && message == "Job canceled before start") || (eventType == "provider_reconciled" && message == "Operator confirmed provider outcome") || (eventType == "operator_disposition" && (message == "failed" || message == "canceled")) || ((eventType == "succeeded" || eventType == "failed" || eventType == "canceled") && message == "Job finished")
}

func validProvenanceShape(raw json.RawMessage) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil || len(fields) != 6 {
		return false
	}
	for _, name := range []string{"tracker", "source_repository", "issue_number", "version", "url", "title"} {
		if _, ok := fields[name]; !ok {
			return false
		}
	}
	var typed struct {
		Tracker          *string `json:"tracker"`
		SourceRepository *string `json:"source_repository"`
		IssueNumber      *int    `json:"issue_number"`
		Version          *string `json:"version"`
		URL              *string `json:"url"`
		Title            *string `json:"title"`
	}
	if json.Unmarshal(raw, &typed) != nil {
		return false
	}
	return typed.Tracker != nil && typed.SourceRepository != nil && typed.IssueNumber != nil && typed.Version != nil && typed.URL != nil && typed.Title != nil
}

func (h *Handler) writeManagerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, restjobs.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
	case errors.Is(err, restjobs.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict", "Idempotency key conflicts with an existing request.")
	case errors.Is(err, restjobs.ErrProvenanceConflict):
		writeError(w, http.StatusConflict, "issue_provenance_conflict", "Issue provenance conflicts with an existing request.")
	case errors.Is(err, restjobs.ErrQueueFull):
		writeError(w, http.StatusServiceUnavailable, "queue_full", "Job queue is full.")
	case errors.Is(err, restjobs.ErrRegistryFull):
		writeError(w, http.StatusServiceUnavailable, "registry_full", "Job registry is full.")
	case errors.Is(err, restjobs.ErrManagerClosed):
		writeError(w, http.StatusServiceUnavailable, "server_shutting_down", "The server is shutting down.")
	case errors.Is(err, restjobs.ErrInvalidTransition):
		writeError(w, http.StatusConflict, "reconciliation_not_allowed", "Job is not eligible for provider reconciliation.")
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
