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
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
)

type scheduleResponse struct {
	ID                string                               `json:"id"`
	Owner             string                               `json:"owner"`
	Cron              string                               `json:"cron"`
	Repository        string                               `json:"repository"`
	Task              string                               `json:"task"`
	Status            restjobs.ScheduleStatus              `json:"status"`
	Revision          int64                                `json:"revision"`
	CreatedAt         time.Time                            `json:"created_at"`
	UpdatedAt         time.Time                            `json:"updated_at"`
	PendingOccurrence *time.Time                           `json:"pending_occurrence_at,omitempty"`
	Skipped           *restjobs.SkippedScheduleOccurrences `json:"skipped_occurrences,omitempty"`
}

func scheduleView(schedule restjobs.Schedule) scheduleResponse {
	return scheduleResponse{
		ID: schedule.ID, Owner: schedule.Owner, Cron: schedule.Cron, Repository: schedule.Repository,
		Task: schedule.Task, Status: schedule.Status, Revision: schedule.Revision,
		CreatedAt: schedule.CreatedAt, UpdatedAt: schedule.UpdatedAt,
		PendingOccurrence: schedule.PendingOccurrence(), Skipped: schedule.SkippedOccurrences(),
	}
}

func (h *Handler) scheduleStore(w http.ResponseWriter) (restjobs.ScheduleStore, bool) {
	store, ok := h.manager.(restjobs.ScheduleStore)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "durable_store_required", "Schedule operations require SQLite or PostgreSQL persistence.")
		return nil, false
	}
	return store, true
}
func (h *Handler) handleSchedules(w http.ResponseWriter, r *http.Request, caller restserver.Principal) {
	if caller.Role != "submitter" {
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		return
	}
	store, ok := h.scheduleStore(w)
	if !ok {
		return
	}
	switch r.Method {
	case http.MethodGet:
		schedules, err := store.ListSchedules(caller.ID)
		if err != nil {
			h.writeManagerError(w, err)
			return
		}
		response := make([]scheduleResponse, 0, len(schedules))
		for _, schedule := range schedules {
			response = append(response, scheduleView(schedule))
		}
		writeJSON(w, http.StatusOK, struct {
			Schedules []scheduleResponse `json:"schedules"`
		}{response})
	case http.MethodPost:
		h.createSchedule(w, r, caller, store)
	default:
		methodNotAllowed(w, http.MethodGet+", "+http.MethodPost)
	}
}
func (h *Handler) handleSchedule(w http.ResponseWriter, r *http.Request, id string, caller restserver.Principal) {
	if caller.Role != "submitter" || id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		return
	}
	store, ok := h.scheduleStore(w)
	if !ok {
		return
	}
	switch r.Method {
	case http.MethodGet:
		schedule, err := store.GetSchedule(id)
		if err != nil || schedule.Owner != caller.ID {
			writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
			return
		}
		setScheduleETag(w, schedule)
		writeJSON(w, http.StatusOK, scheduleView(schedule))
	case http.MethodPatch:
		h.mutateSchedule(w, r, id, caller, store)
	case http.MethodDelete:
		h.removeSchedule(w, r, id, caller, store)
	default:
		methodNotAllowed(w, http.MethodGet+", "+http.MethodPatch+", "+http.MethodDelete)
	}
}
func (h *Handler) createSchedule(w http.ResponseWriter, r *http.Request, caller restserver.Principal, store restjobs.ScheduleStore) {
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || strings.TrimSpace(keys[0]) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	var body struct {
		Cron       string `json:"cron"`
		Repository string `json:"repository"`
		Task       string `json:"task"`
	}
	if !decodeScheduleJSON(w, r, h.config.MaxRequestBodyBytes, &body) {
		return
	}
	if err := restjobs.ValidateScheduleCron(body.Cron); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_cron", "Cron must be a valid five-field UTC expression.")
		return
	}
	if _, ok := h.config.RepositoryAliases[strings.TrimSpace(body.Repository)]; !ok {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	schedule, replayed, err := store.CreateSchedule(caller.ID, keys[0], restjobs.ScheduleInput{Cron: body.Cron, Repository: body.Repository, Task: body.Task})
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	setScheduleETag(w, schedule)
	status := http.StatusCreated
	if replayed {
		status = http.StatusOK
	}
	writeJSON(w, status, struct {
		Schedule scheduleResponse `json:"schedule"`
		Replayed bool             `json:"replayed"`
	}{scheduleView(schedule), replayed})
}
func (h *Handler) mutateSchedule(w http.ResponseWriter, r *http.Request, id string, caller restserver.Principal, store restjobs.ScheduleStore) {
	revision, ok := scheduleIfMatch(w, r)
	if !ok {
		return
	}
	var body struct {
		Action     string `json:"action"`
		Cron       string `json:"cron"`
		Repository string `json:"repository"`
		Task       string `json:"task"`
	}
	if !decodeScheduleJSON(w, r, h.config.MaxRequestBodyBytes, &body) {
		return
	}
	mutation := restjobs.ScheduleMutation(body.Action)
	var input *restjobs.ScheduleInput
	switch mutation {
	case restjobs.SchedulePause, restjobs.ScheduleResume:
		if body.Cron != "" || body.Repository != "" || body.Task != "" {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
	case restjobs.ScheduleEdit:
		if err := restjobs.ValidateScheduleCron(body.Cron); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_cron", "Cron must be a valid five-field UTC expression.")
			return
		}
		if _, ok := h.config.RepositoryAliases[strings.TrimSpace(body.Repository)]; !ok {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
		input = &restjobs.ScheduleInput{Cron: body.Cron, Repository: body.Repository, Task: body.Task}
	default:
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	schedule, err := store.MutateSchedule(id, caller.ID, revision, mutation, input)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	setScheduleETag(w, schedule)
	writeJSON(w, http.StatusOK, scheduleView(schedule))
}
func (h *Handler) removeSchedule(w http.ResponseWriter, r *http.Request, id string, caller restserver.Principal, store restjobs.ScheduleStore) {
	revision, ok := scheduleIfMatch(w, r)
	if !ok {
		return
	}
	if !bodyless(r) {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	schedule, err := store.MutateSchedule(id, caller.ID, revision, restjobs.ScheduleRemove, nil)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	setScheduleETag(w, schedule)
	writeJSON(w, http.StatusOK, scheduleView(schedule))
}
func scheduleIfMatch(w http.ResponseWriter, r *http.Request) (int64, bool) {
	values := r.Header.Values("If-Match")
	if len(values) == 0 {
		writeError(w, http.StatusPreconditionRequired, "precondition_required", "A strong If-Match revision is required.")
		return 0, false
	}
	if len(values) != 1 {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return 0, false
	}
	value := values[0]
	if len(value) < 3 || value[0] != '"' || value[len(value)-1] != '"' {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return 0, false
	}
	revision, err := strconv.ParseInt(value[1:len(value)-1], 10, 64)
	if err != nil || revision < 1 || strconv.FormatInt(revision, 10) != value[1:len(value)-1] {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return 0, false
	}
	return revision, true
}
func setScheduleETag(w http.ResponseWriter, schedule restjobs.Schedule) {
	w.Header().Set("ETag", "\""+strconv.FormatInt(schedule.Revision, 10)+"\"")
}
func decodeScheduleJSON(w http.ResponseWriter, r *http.Request, max int, destination any) bool {
	contentTypes := r.Header.Values("Content-Type")
	mediaType := ""
	var err error
	if len(contentTypes) == 1 {
		mediaType, _, err = mime.ParseMediaType(contentTypes[0])
	} else {
		err = errors.New("invalid content type")
	}
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json.")
		return false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(max)))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return false
	}
	if err = validateJSONShape(body); err != nil {
		writeError(w, http.StatusBadRequest, "malformed_json", "Request body is invalid.")
		return false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		writeError(w, http.StatusBadRequest, "malformed_json", "Request body is invalid.")
		return false
	}
	allowed := map[string]bool{"cron": true, "repository": true, "task": true, "action": true}
	for name := range fields {
		if !allowed[name] {
			writeError(w, http.StatusBadRequest, "unknown_field", "Request contains an unsupported field.")
			return false
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(destination) != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return false
	}
	return true
}
