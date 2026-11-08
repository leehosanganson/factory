package restapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
)

type scheduleRunResponse struct {
	OccurrenceAt time.Time       `json:"occurrence_at"`
	JobID        string          `json:"job_id"`
	Status       restjobs.Status `json:"status"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
	JobURL       string          `json:"job_url"`
}

const dashboardScheduleLimit = 20

type dashboardScheduleResponse struct {
	ID        string                               `json:"id"`
	Cron      string                               `json:"cron"`
	Status    restjobs.ScheduleStatus              `json:"status"`
	Revision  int64                                `json:"revision"`
	Pending   *time.Time                           `json:"pending_occurrence_at,omitempty"`
	Skipped   *restjobs.SkippedScheduleOccurrences `json:"skipped_occurrences,omitempty"`
	CreatedAt time.Time                            `json:"created_at"`
	UpdatedAt time.Time                            `json:"updated_at"`
}

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

func (h *Handler) handleDashboardScheduleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	if r.URL.RawQuery != "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	store, ok := h.scheduleStore(w)
	if !ok {
		return
	}
	schedules, err := store.ListRecentSchedules(dashboardScheduleLimit)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	response := make([]dashboardScheduleResponse, 0, len(schedules))
	for _, schedule := range schedules {
		response = append(response, dashboardScheduleResponse{
			ID: schedule.ID, Cron: schedule.Cron, Status: schedule.Status, Revision: schedule.Revision,
			Pending: schedule.PendingOccurrence(), Skipped: schedule.SkippedOccurrences(),
			CreatedAt: schedule.CreatedAt, UpdatedAt: schedule.UpdatedAt,
		})
	}
	writeJSON(w, http.StatusOK, struct {
		Schedules []dashboardScheduleResponse `json:"schedules"`
	}{response})
}

func (h *Handler) handleDashboardScheduleRuns(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	query, err := parseScheduleRunsQuery(r)
	if err != nil || query.limit > 5 || query.beforeSequence != 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	store, ok := h.scheduleStore(w)
	if !ok {
		return
	}
	page, err := store.ListScheduleRuns(id, h.config.DashboardOperatorID, true, query.beforeSequence, query.limit)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	runs := make([]scheduleRunResponse, 0, len(page.Runs))
	for _, run := range page.Runs {
		runs = append(runs, scheduleRunResponse{
			OccurrenceAt: run.OccurrenceAt, JobID: run.JobID, Status: run.Status,
			CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt, JobURL: "/v1/jobs/" + run.JobID,
		})
	}
	writeJSON(w, http.StatusOK, struct {
		Runs []scheduleRunResponse `json:"runs"`
	}{runs})
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
	if caller.Role != "submitter" && caller.Role != "operator" {
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		return
	}
	if caller.Role != "submitter" && r.Method != http.MethodGet {
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		return
	}
	store, ok := h.scheduleStore(w)
	if !ok {
		return
	}
	switch r.Method {
	case http.MethodGet:
		var schedules []restjobs.Schedule
		var err error
		if caller.Role == "operator" {
			schedules, err = store.ListAllSchedules()
		} else {
			schedules, err = store.ListSchedules(caller.ID)
		}
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
		if caller.Role != "submitter" {
			writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
			return
		}
		h.createSchedule(w, r, caller, store)
	default:
		methodNotAllowed(w, http.MethodGet+", "+http.MethodPost)
	}
}
func (h *Handler) handleScheduleRuns(w http.ResponseWriter, r *http.Request, id string, caller restserver.Principal) {
	if (caller.Role != "submitter" && caller.Role != "operator") || id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		return
	}
	query, err := parseScheduleRunsQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	store, ok := h.scheduleStore(w)
	if !ok {
		return
	}
	page, err := store.ListScheduleRuns(id, caller.ID, caller.Role == "operator", query.beforeSequence, query.limit)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	runs := make([]scheduleRunResponse, 0, len(page.Runs))
	for _, run := range page.Runs {
		runs = append(runs, scheduleRunResponse{
			OccurrenceAt: run.OccurrenceAt, JobID: run.JobID, Status: run.Status,
			CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt, JobURL: "/v1/jobs/" + run.JobID,
		})
	}
	response := struct {
		Runs       []scheduleRunResponse `json:"runs"`
		HasMore    bool                  `json:"has_more"`
		NextBefore uint64                `json:"next_before_sequence,omitempty"`
	}{Runs: runs, HasMore: page.HasMore}
	if page.HasMore {
		response.NextBefore = page.NextBefore
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) handleScheduleHistory(w http.ResponseWriter, r *http.Request, id string, caller restserver.Principal) {
	if (caller.Role != "submitter" && caller.Role != "operator") || id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		return
	}
	query, err := parseScheduleHistoryQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	store, ok := h.scheduleStore(w)
	if !ok {
		return
	}
	page, err := store.ListScheduleHistory(id, caller.ID, caller.Role == "operator", query.afterRevision, query.limit)
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	response := struct {
		Events       []restjobs.ScheduleAuditEvent `json:"events"`
		HasMore      bool                          `json:"has_more"`
		NextRevision int64                         `json:"next_revision,omitempty"`
	}{Events: page.Events, HasMore: page.HasMore}
	if page.HasMore {
		response.NextRevision = page.NextRevision
	}
	writeJSON(w, http.StatusOK, response)
}

type scheduleHistoryQuery struct {
	afterRevision int64
	limit         int
}

type scheduleRunsQuery struct {
	beforeSequence uint64
	limit          int
}

func parseScheduleRunsQuery(r *http.Request) (scheduleRunsQuery, error) {
	query := scheduleRunsQuery{limit: restjobs.DefaultScheduleHistoryLimit}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return scheduleRunsQuery{}, restjobs.ErrInvalidInput
	}
	for key, entries := range values {
		if (key != "before_sequence" && key != "limit") || len(entries) != 1 {
			return scheduleRunsQuery{}, restjobs.ErrInvalidInput
		}
	}
	if value, exists := values["before_sequence"]; exists {
		parsed, parseErr := strconv.ParseUint(value[0], 10, 64)
		if parseErr != nil || parsed == 0 || parsed > 1<<63-1 || strconv.FormatUint(parsed, 10) != value[0] {
			return scheduleRunsQuery{}, restjobs.ErrInvalidInput
		}
		query.beforeSequence = parsed
	}
	if value, exists := values["limit"]; exists {
		parsed, parseErr := strconv.Atoi(value[0])
		if parseErr != nil || parsed < 1 || parsed > restjobs.MaxScheduleHistoryLimit {
			return scheduleRunsQuery{}, restjobs.ErrInvalidInput
		}
		query.limit = parsed
	}
	return query, nil
}

func parseScheduleHistoryQuery(r *http.Request) (scheduleHistoryQuery, error) {
	query := scheduleHistoryQuery{limit: restjobs.DefaultScheduleHistoryLimit}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return scheduleHistoryQuery{}, restjobs.ErrInvalidInput
	}
	for key, entries := range values {
		if (key != "after_revision" && key != "limit") || len(entries) != 1 {
			return scheduleHistoryQuery{}, restjobs.ErrInvalidInput
		}
	}
	if value, exists := values["after_revision"]; exists {
		parsed, parseErr := strconv.ParseInt(value[0], 10, 64)
		if parseErr != nil || parsed < 0 || strconv.FormatInt(parsed, 10) != value[0] {
			return scheduleHistoryQuery{}, restjobs.ErrInvalidInput
		}
		query.afterRevision = parsed
	}
	if value, exists := values["limit"]; exists {
		parsed, parseErr := strconv.Atoi(value[0])
		if parseErr != nil || parsed < 1 || parsed > restjobs.MaxScheduleHistoryLimit {
			return scheduleHistoryQuery{}, restjobs.ErrInvalidInput
		}
		query.limit = parsed
	}
	return query, nil
}

func (h *Handler) handleSchedule(w http.ResponseWriter, r *http.Request, id string, caller restserver.Principal) {
	if (caller.Role != "submitter" && caller.Role != "operator") || id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		return
	}
	store, ok := h.scheduleStore(w)
	if !ok {
		return
	}
	switch r.Method {
	case http.MethodGet:
		if strings.HasSuffix(id, "/history") {
			writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
			return
		}
		schedule, err := store.GetSchedule(id)
		if err != nil || caller.Role != "operator" && schedule.Owner != caller.ID {
			writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
			return
		}
		setScheduleETag(w, schedule)
		writeJSON(w, http.StatusOK, scheduleView(schedule))
	case http.MethodPatch:
		h.mutateSchedule(w, r, id, caller, store)
	case http.MethodDelete:
		h.removeSchedule(w, r, id, caller, store)
	case http.MethodOptions:
		methodNotAllowed(w, http.MethodGet+", "+http.MethodPatch+", "+http.MethodDelete)
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
	schedule, err := store.MutateScheduleAs(id, caller.ID, caller.Role == "operator", revision, mutation, input)
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
	schedule, err := store.MutateScheduleAs(id, caller.ID, caller.Role == "operator", revision, restjobs.ScheduleRemove, nil)
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
