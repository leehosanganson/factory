package restapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restserver"
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
	writeJSON(w, http.StatusAccepted, addThreadJobResponse{Thread: snapshot.Thread, Job: job, Replayed: replayed})
}
