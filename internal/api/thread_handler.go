package restapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/leehosanganson/factory/internal/jobs"
	"github.com/leehosanganson/factory/internal/server"
)

func (h *Handler) listNotifications(w http.ResponseWriter, r *http.Request, principalID string) {
	query := r.URL.Query()
	for key, values := range query {
		if (key != "limit" && key != "before") || len(values) != 1 {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
	}
	limit := restjobs.DefaultNotificationListLimit
	if raw, exists := query["limit"]; exists {
		parsed, err := strconv.Atoi(raw[0])
		if err != nil || parsed < 1 || parsed > restjobs.MaxNotificationListLimit {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
		limit = parsed
	}
	var before int64
	if raw, exists := query["before"]; exists {
		parsed, err := strconv.ParseInt(raw[0], 10, 64)
		if err != nil || parsed < 1 {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
		before = parsed
	}
	store, ok := h.manager.(restjobs.NotificationStore)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "notifications_unavailable", "Notifications are unavailable.")
		return
	}
	page, err := store.ListNotificationsForPrincipal(r.Context(), principalID, before, limit)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *Handler) listThreads(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Has("cursor") {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	query, err := parseThreadListQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	threadStore, ok := h.manager.(restjobs.ThreadStore)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "thread_unavailable", "Thread operations are unavailable.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	page, err := threadStore.ListThreads(ctx, query.before, query.snapshot, query.limit)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	if isDashboardOperator(r) {
		writeJSON(w, http.StatusOK, dashboardThreadPage{Threads: projectDashboardThreadSummaries(page.Threads), SnapshotSequence: page.SnapshotSequence, NextSequence: page.NextSequence, HasMore: page.HasMore})
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *Handler) listThreadsForPrincipal(w http.ResponseWriter, r *http.Request, principalID string) {
	if _, hasBefore := r.URL.Query()["before_sequence"]; hasBefore {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	if _, hasSnapshot := r.URL.Query()["snapshot_sequence"]; hasSnapshot {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	for key := range r.URL.Query() {
		if key != "limit" && key != "cursor" {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
	}
	query, err := parseThreadListQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	if raw, exists := r.URL.Query()["cursor"]; exists {
		state, ok := h.lookupSubmitterCursorKind(raw[0], principalID, "thread")
		if !ok {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
		query.before, query.snapshot = state.after, state.snapshot
	}
	threadStore, ok := h.manager.(restjobs.ThreadStore)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "thread_unavailable", "Thread operations are unavailable.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	page, err := threadStore.ListThreadsForPrincipal(ctx, principalID, query.before, query.snapshot, query.limit)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	cursor := ""
	if page.HasMore {
		cursor, ok = h.storeSubmitterCursorKind(principalID, "thread", page.NextSequence, page.SnapshotSequence)
		if !ok {
			writeError(w, http.StatusServiceUnavailable, "cursor_capacity", "Pagination capacity is full.")
			return
		}
	}
	writeJSON(w, http.StatusOK, submitterThreadPage{Threads: page.Threads, HasMore: page.HasMore, NextCursor: cursor})
}

type threadListQuery struct {
	before   uint64
	snapshot uint64
	limit    int
}

func parseThreadListQuery(r *http.Request) (threadListQuery, error) {
	query := threadListQuery{limit: restjobs.DefaultThreadListLimit}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return threadListQuery{}, restjobs.ErrInvalidInput
	}
	for key, entries := range values {
		if (key != "limit" && key != "before_sequence" && key != "snapshot_sequence" && key != "cursor") || len(entries) != 1 || entries[0] == "" {
			return threadListQuery{}, restjobs.ErrInvalidInput
		}
	}
	if value, exists := values["limit"]; exists {
		parsed, err := strconv.Atoi(value[0])
		if err != nil || parsed < 1 || parsed > restjobs.MaxThreadListLimit {
			return threadListQuery{}, restjobs.ErrInvalidInput
		}
		query.limit = parsed
	}
	if value, exists := values["cursor"]; exists {
		// Cursor resolution is handled by the caller because it is principal-bound.
		if value[0] == "" {
			return threadListQuery{}, restjobs.ErrInvalidInput
		}
	}
	for key, target := range map[string]*uint64{"before_sequence": &query.before, "snapshot_sequence": &query.snapshot} {
		if value, exists := values[key]; exists {
			parsed, err := strconv.ParseUint(value[0], 10, 64)
			if err != nil || parsed == 0 {
				return threadListQuery{}, restjobs.ErrInvalidInput
			}
			*target = parsed
		}
	}
	_, hasBefore := values["before_sequence"]
	_, hasSnapshot := values["snapshot_sequence"]
	if hasBefore != hasSnapshot || hasBefore && query.before > query.snapshot || values.Has("cursor") && hasBefore {
		return threadListQuery{}, restjobs.ErrInvalidInput
	}
	return query, nil
}

func (h *Handler) completeThread(w http.ResponseWriter, r *http.Request, threadID string, caller restserver.Principal) {
	if !bodyless(r) {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	threadStore, ok := h.manager.(restjobs.ThreadStore)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "thread_unavailable", "Thread operations are unavailable.")
		return
	}
	snapshot, err := threadStore.CompleteThreadAs(threadID, caller.ID)
	if err != nil {
		if errors.Is(err, restjobs.ErrThreadClosed) {
			writeError(w, http.StatusConflict, "thread_closed", "Thread is already completed.")
		} else {
			h.writeManagerError(w, err)
		}
		return
	}
	if isDashboardOperator(r) {
		writeJSON(w, http.StatusOK, dashboardThreadResponse{ID: snapshot.Thread.ID, Status: snapshot.Thread.Status, CreatedAt: snapshot.Thread.CreatedAt, UpdatedAt: snapshot.Thread.UpdatedAt, Completed: snapshot.Thread.CompletedAt})
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

type addThreadJobResponse struct {
	Thread   restjobs.Thread   `json:"thread"`
	Job      restjobs.Snapshot `json:"job"`
	Replayed bool              `json:"replayed"`
}

func (h *Handler) addThreadJob(w http.ResponseWriter, r *http.Request, threadID string, caller restserver.Principal) {
	threadStore, ok := h.manager.(restjobs.ThreadStore)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "thread_unavailable", "Thread operations are unavailable.")
		return
	}
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || strings.TrimSpace(keys[0]) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	contentTypes := r.Header.Values("Content-Type")
	if len(contentTypes) != 1 {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json.")
		return
	}
	mediaType, _, err := mime.ParseMediaType(contentTypes[0])
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
	if !utf8.Valid(body) || validateJSONShape(body) != nil {
		writeError(w, http.StatusBadRequest, "malformed_json", "Request body is invalid.")
		return
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		writeError(w, http.StatusBadRequest, "malformed_json", "Request body is invalid.")
		return
	}
	for name := range fields {
		if name != "repository" && name != "task" {
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
	principalID := caller.ID
	snapshot, job, replayed, err := threadStore.AdmitThreadJobAs(threadID, keys[0], principalID, request)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	if isDashboardOperator(r) {
		writeJSON(w, http.StatusAccepted, dashboardThreadAppendResponse{Thread: projectDashboardThread(snapshot.Thread), Job: projectDashboardThreadJob(job), Replayed: replayed})
		return
	}
	writeJSON(w, http.StatusAccepted, addThreadJobResponse{Thread: snapshot.Thread, Job: job, Replayed: replayed})
}
