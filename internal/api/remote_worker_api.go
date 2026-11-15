package restapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
)

const maxRemoteWorkerRequestBytes = 512 << 10

var remoteWorkerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var remoteRepositoryAliasPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

const maxRemoteWorkerRepositories = 16

// RemoteWorkerStore is the PostgreSQL-backed protocol surface required by these routes.
type RemoteWorkerStore interface {
	ClaimRemoteAttempt(context.Context, string, string, string, []string) (restjobs.AttemptClaim, error)
	HeartbeatRemoteAttempt(restjobs.AttemptHandle) (bool, error)
	FinishRemoteAttempt(restjobs.AttemptHandle, restjobs.Status, restjobs.VerificationEvidence) error
}

type remoteClaimRequest struct {
	ClaimID      string   `json:"claim_id"`
	WorkerID     string   `json:"worker_id"`
	BootID       string   `json:"boot_id"`
	Repositories []string `json:"repositories"`
}

type remoteAttemptRequest struct {
	Generation int64  `json:"generation"`
	OwnerToken string `json:"owner_token"`
	WorkerID   string `json:"worker_id"`
	BootID     string `json:"boot_id"`
}

type remoteClaimResponse struct {
	Job     remoteJobDTO     `json:"job"`
	Attempt remoteAttemptDTO `json:"attempt"`
}

type remoteJobDTO struct {
	ID      string           `json:"id"`
	Request remoteRequestDTO `json:"request"`
}

type remoteRequestDTO struct {
	Repository      string                    `json:"repository"`
	Task            string                    `json:"task"`
	Issue           *int                      `json:"issue,omitempty"`
	IssueProvenance *remoteIssueProvenanceDTO `json:"issue_provenance,omitempty"`
}

type remoteIssueProvenanceDTO struct {
	Tracker          string `json:"tracker"`
	SourceRepository string `json:"source_repository"`
	IssueNumber      int    `json:"issue_number"`
	Version          string `json:"version"`
	URL              string `json:"url"`
	Title            string `json:"title"`
}

type remoteAttemptDTO struct {
	JobID      string `json:"job_id"`
	Generation int64  `json:"generation"`
	OwnerToken string `json:"owner_token"`
	WorkerID   string `json:"worker_id"`
	BootID     string `json:"boot_id"`
}

type remoteHeartbeatResponse struct {
	CancellationRequested bool `json:"cancellation_requested"`
}

type remoteFinishRequest struct {
	remoteAttemptRequest
	Status   restjobs.Status    `json:"status"`
	Evidence *remoteEvidenceDTO `json:"evidence"`
}

type remoteEvidenceDTO struct {
	Checks      []remoteVerificationCheckDTO `json:"checks"`
	Limitations []string                     `json:"limitations"`
}

type remoteVerificationCheckDTO struct {
	Name    string `json:"name"`
	Outcome string `json:"outcome"`
}

func isRemoteWorkerPath(path string) bool {
	return strings.HasPrefix(path, "/v1/worker/")
}

func (h *Handler) serveRemoteWorkerHTTP(w http.ResponseWriter, r *http.Request) {
	config := h.config.RemoteWorkerAPI
	if config == nil || config.Store == nil || !authenticateRemoteWorker(r, config.Secret, config.Authenticate) {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "Worker authentication is required.")
		return
	}
	if r.URL.EscapedPath() != r.URL.Path || r.URL.RawQuery != "" {
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		return
	}
	if h.readinessError(r.Context()) != nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "The server is not accepting requests.")
		return
	}
	switch {
	case r.URL.Path == "/v1/worker/claims":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		h.remoteClaim(w, r, config.Store)
	case strings.HasPrefix(r.URL.Path, "/v1/worker/attempts/"):
		tail := strings.TrimPrefix(r.URL.Path, "/v1/worker/attempts/")
		parts := strings.Split(tail, "/")
		if len(parts) != 2 || !remoteJobIDPattern.MatchString(parts[0]) {
			writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
			return
		}
		switch parts[1] {
		case "heartbeat":
			if r.Method != http.MethodPost {
				methodNotAllowed(w, http.MethodPost)
				return
			}
			h.remoteHeartbeat(w, r, config.Store, parts[0])
		case "finish":
			if r.Method != http.MethodPost {
				methodNotAllowed(w, http.MethodPost)
				return
			}
			h.remoteFinish(w, r, config.Store, parts[0])
		default:
			writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
		}
	default:
		writeError(w, http.StatusNotFound, "not_found", "Resource not found.")
	}
}

func authenticateRemoteWorker(r *http.Request, secret []byte, authenticate func(string) bool) bool {
	values := r.Header.Values("Authorization")
	if len(values) != 1 || len(secret) == 0 && authenticate == nil {
		return false
	}
	parts := strings.Split(values[0], " ")
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" || len(parts[1]) > 4096 || strings.TrimSpace(parts[1]) != parts[1] {
		return false
	}
	if authenticate != nil {
		return authenticate(parts[1])
	}
	provided := sha256.Sum256([]byte(parts[1]))
	configured := sha256.Sum256(secret)
	return subtle.ConstantTimeCompare(provided[:], configured[:]) == 1
}

func (h *Handler) decodeRemoteWorkerJSON(w http.ResponseWriter, r *http.Request, destination any) bool {
	limit := maxRemoteWorkerRequestBytes
	if h.config.MaxRequestBodyBytes < limit {
		limit = h.config.MaxRequestBodyBytes
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(limit)))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", "Request body is too large.")
		} else {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request body is invalid.")
		}
		return false
	}
	if len(bytes.TrimSpace(body)) == 0 || !utf8.Valid(body) || validateJSONShape(body) != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request body is invalid.")
		return false
	}
	contentTypes := r.Header.Values("Content-Type")
	if len(contentTypes) != 1 || contentTypes[0] != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json.")
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(destination) != nil || decoder.Decode(new(any)) != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request body is invalid.")
		return false
	}
	return true
}

func (h *Handler) remoteClaim(w http.ResponseWriter, r *http.Request, store RemoteWorkerStore) {
	var request remoteClaimRequest
	if !h.decodeRemoteWorkerJSON(w, r, &request) {
		return
	}
	if !remoteWorkerIDPattern.MatchString(request.ClaimID) || !remoteWorkerIDPattern.MatchString(request.WorkerID) || !remoteWorkerIDPattern.MatchString(request.BootID) || !validRemoteWorkerRepositories(request.Repositories) {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	for _, alias := range request.Repositories {
		if _, configured := h.config.RepositoryAliases[alias]; !configured {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
	}
	sort.Strings(request.Repositories)
	claim, err := store.ClaimRemoteAttempt(r.Context(), request.ClaimID, request.WorkerID, request.BootID, request.Repositories)
	if err != nil {
		writeRemoteWorkerStoreError(w, err)
		return
	}
	if !remoteJobIDPattern.MatchString(claim.Job.ID) || claim.Job.Request.Repository == "" || len(claim.Job.Request.Task) > h.config.MaxTaskBytes || claim.Attempt.JobID != claim.Job.ID || claim.Attempt.Generation < 1 || !remoteWorkerTokenPattern.MatchString(claim.Attempt.OwnerToken) || claim.Attempt.WorkerID != request.WorkerID || claim.Attempt.BootID != request.BootID {
		writeError(w, http.StatusServiceUnavailable, "service_unavailable", "Worker operation is unavailable.")
		return
	}
	if _, configured := h.config.RepositoryAliases[claim.Job.Request.Repository]; !configured {
		writeError(w, http.StatusServiceUnavailable, "service_unavailable", "Worker operation is unavailable.")
		return
	}
	jobRequest := remoteRequestDTO{Repository: claim.Job.Request.Repository, Task: claim.Job.Request.Task, Issue: claim.Job.Request.Issue}
	if provenance := claim.Job.Request.IssueProvenance; provenance != nil {
		jobRequest.IssueProvenance = &remoteIssueProvenanceDTO{Tracker: provenance.Tracker, SourceRepository: provenance.SourceRepository, IssueNumber: provenance.IssueNumber, Version: provenance.Version, URL: provenance.URL, Title: provenance.Title}
	}
	writeJSON(w, http.StatusOK, remoteClaimResponse{
		Job:     remoteJobDTO{ID: claim.Job.ID, Request: jobRequest},
		Attempt: remoteAttemptDTO{JobID: claim.Attempt.JobID, Generation: claim.Attempt.Generation, OwnerToken: claim.Attempt.OwnerToken, WorkerID: claim.Attempt.WorkerID, BootID: claim.Attempt.BootID},
	})
}

func validRemoteWorkerRepositories(repositories []string) bool {
	if len(repositories) == 0 || len(repositories) > maxRemoteWorkerRepositories {
		return false
	}
	seen := make(map[string]struct{}, len(repositories))
	for _, alias := range repositories {
		if !remoteRepositoryAliasPattern.MatchString(alias) {
			return false
		}
		if _, ok := seen[alias]; ok {
			return false
		}
		seen[alias] = struct{}{}
	}
	return true
}

func (h *Handler) remoteHeartbeat(w http.ResponseWriter, r *http.Request, store RemoteWorkerStore, jobID string) {
	var request remoteAttemptRequest
	if !h.decodeRemoteWorkerJSON(w, r, &request) {
		return
	}
	attempt, ok := remoteAttemptHandle(jobID, request)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	cancellationRequested, err := store.HeartbeatRemoteAttempt(attempt)
	if err != nil {
		writeRemoteWorkerStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, remoteHeartbeatResponse{CancellationRequested: cancellationRequested})
}

func (h *Handler) remoteFinish(w http.ResponseWriter, r *http.Request, store RemoteWorkerStore, jobID string) {
	var request remoteFinishRequest
	if !h.decodeRemoteWorkerJSON(w, r, &request) {
		return
	}
	attempt, ok := remoteAttemptHandle(jobID, request.remoteAttemptRequest)
	if !ok || !request.Status.Terminal() {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	if request.Evidence == nil || request.Evidence.Checks == nil || request.Evidence.Limitations == nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	evidence := restjobs.VerificationEvidence{Limitations: request.Evidence.Limitations, Checks: make([]restjobs.VerificationCheck, len(request.Evidence.Checks))}
	for index, check := range request.Evidence.Checks {
		evidence.Checks[index] = restjobs.VerificationCheck{Name: check.Name, Outcome: check.Outcome}
	}
	if !validRemoteEvidence(evidence) {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	if err := store.FinishRemoteAttempt(attempt, request.Status, evidence); err != nil {
		writeRemoteWorkerStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "finished"})
}

func remoteAttemptHandle(jobID string, request remoteAttemptRequest) (restjobs.AttemptHandle, bool) {
	attempt := restjobs.AttemptHandle{JobID: jobID, Generation: request.Generation, OwnerToken: request.OwnerToken, WorkerID: request.WorkerID, BootID: request.BootID}
	if !remoteJobIDPattern.MatchString(jobID) || request.Generation < 1 || len(request.OwnerToken) != 64 || !remoteWorkerTokenPattern.MatchString(request.OwnerToken) || !remoteWorkerIDPattern.MatchString(request.WorkerID) || !remoteWorkerIDPattern.MatchString(request.BootID) {
		return restjobs.AttemptHandle{}, false
	}
	return attempt, true
}

var remoteJobIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
var remoteWorkerTokenPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validRemoteEvidence(evidence restjobs.VerificationEvidence) bool {
	if evidence.Checks == nil || evidence.Limitations == nil || len(evidence.Checks) > 16 || len(evidence.Limitations) > 4 {
		return false
	}
	for index, check := range evidence.Checks {
		if check.Name != fmt.Sprintf("check-%02d", index+1) || (check.Outcome != restjobs.VerificationNotCompleted && check.Outcome != restjobs.VerificationPassed && check.Outcome != restjobs.VerificationFailed && check.Outcome != restjobs.VerificationCanceled) {
			return false
		}
	}
	for _, limitation := range evidence.Limitations {
		if len(limitation) > 128 || (limitation != restjobs.LimitationNoChecksConfigured && limitation != restjobs.LimitationAgentNotVerdict) {
			return false
		}
	}
	return true
}

func writeRemoteWorkerStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, restjobs.ErrNoQueuedJobs), errors.Is(err, restjobs.ErrNoWorkerSlots):
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, restjobs.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
	case errors.Is(err, restjobs.ErrCancellationPending):
		writeError(w, http.StatusConflict, "cancellation_pending", "A cancellation request must be observed before this result can be accepted.")
	case errors.Is(err, restjobs.ErrCancellationNotRequested):
		writeError(w, http.StatusConflict, "cancellation_not_requested", "A cancellation result requires an explicit cancellation request.")
	case errors.Is(err, restjobs.ErrStaleAttempt), errors.Is(err, restjobs.ErrRemoteClaimConflict), errors.Is(err, restjobs.ErrInvalidTransition):
		writeError(w, http.StatusConflict, "attempt_conflict", "Worker attempt is no longer valid.")
	default:
		writeError(w, http.StatusServiceUnavailable, "service_unavailable", "Worker operation is unavailable.")
	}
}
