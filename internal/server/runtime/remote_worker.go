package runtime

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/leehosanganson/factory/internal/harness/pi"
	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
	restworker "github.com/leehosanganson/factory/internal/worker"
	restworkflow "github.com/leehosanganson/factory/internal/workflow"
	restworkspace "github.com/leehosanganson/factory/internal/workspace"
)

const remoteWorkerPollInterval = time.Second
const remoteWorkerHeartbeatInterval = 5 * time.Second
const remoteWorkerHTTPTimeout = 15 * time.Second
const remoteWorkerResponseLimit = 512 << 10

type remoteWorkerClient struct {
	baseURL        string
	workerID       string
	bootID         string
	repositories   []string
	threadSessions bool
	secret         string
	client         *http.Client
}

type remoteWorkerJob struct {
	ThreadID       string `json:"thread_id,omitempty"`
	ThreadPosition int    `json:"thread_position,omitempty"`
	ID             string `json:"id"`
	Request        struct {
		Repository      string `json:"repository"`
		Task            string `json:"task"`
		Issue           *int   `json:"issue,omitempty"`
		IssueProvenance *struct {
			Tracker          string `json:"tracker"`
			SourceRepository string `json:"source_repository"`
			IssueNumber      int    `json:"issue_number"`
			Version          string `json:"version"`
			URL              string `json:"url"`
			Title            string `json:"title"`
		} `json:"issue_provenance,omitempty"`
	} `json:"request"`
}
type remoteWorkerAttempt struct {
	JobID      string `json:"job_id"`
	Generation int64  `json:"generation"`
	OwnerToken string `json:"owner_token"`
	WorkerID   string `json:"worker_id"`
	BootID     string `json:"boot_id"`
}
type remoteWorkerClaim struct {
	Job     remoteWorkerJob     `json:"job"`
	Attempt remoteWorkerAttempt `json:"attempt"`
}
type remoteWorkerAttemptRequest struct {
	Generation int64  `json:"generation"`
	OwnerToken string `json:"owner_token"`
	WorkerID   string `json:"worker_id"`
	BootID     string `json:"boot_id"`
}
type remoteWorkerEvidence struct {
	Reply          string                       `json:"reply,omitempty"`
	ReplyTruncated bool                         `json:"reply_truncated,omitempty"`
	Checks         []restjobs.VerificationCheck `json:"checks"`
	Limitations    []string                     `json:"limitations"`
}
type remoteWorkerFinish struct {
	remoteWorkerAttemptRequest
	Status   restjobs.Status      `json:"status"`
	Evidence remoteWorkerEvidence `json:"evidence"`
}

type remoteWorkerRetryableError struct{ err error }

type remoteWorkerCancellationPendingError struct{}

type remoteWorkerCancellationNotRequestedError struct{}

func (e remoteWorkerRetryableError) Error() string { return e.err.Error() }
func (e remoteWorkerRetryableError) Unwrap() error { return e.err }
func (remoteWorkerCancellationPendingError) Error() string {
	return "remote Worker success conflicts with a pending cancellation"
}
func (remoteWorkerCancellationNotRequestedError) Error() string {
	return "remote Worker cancellation was not requested"
}

var remoteWorkerTokenPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var remoteWorkerJobIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func newRemoteWorkerClient(config restserver.RemoteWorkerConfig, secret []byte) (*remoteWorkerClient, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.New("remote Worker config is invalid")
	}
	if len(secret) < 32 {
		return nil, errors.New("remote Worker shared secret is unavailable")
	}
	parsed, err := url.Parse(config.ControllerBaseURL)
	if err != nil {
		return nil, errors.New("remote Worker Controller URL is invalid")
	}
	allowed := make(map[string]struct{})
	var threadSessions bool
	for _, profile := range config.HarnessProfiles {
		threadSessions = threadSessions || profile.Adapter == "pi"
		for _, alias := range profile.Repositories {
			allowed[alias] = struct{}{}
		}
	}
	repositories := make([]string, 0, len(allowed))
	for alias := range allowed {
		repositories = append(repositories, alias)
	}
	sort.Strings(repositories)
	return &remoteWorkerClient{baseURL: strings.TrimRight(parsed.String(), "/"), workerID: config.WorkerID, repositories: repositories, threadSessions: threadSessions, secret: string(secret), client: &http.Client{Timeout: remoteWorkerHTTPTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (c *remoteWorkerClient) do(ctx context.Context, method, path string, body any, response any) (int, error) {
	if ctx == nil || c == nil || c.client == nil || c.secret == "" || !strings.HasPrefix(path, "/v1/worker/") || strings.Contains(path, "?") || strings.Contains(path, "#") {
		return 0, errors.New("remote Worker request is invalid")
	}
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			return 0, errors.New("encode remote Worker request")
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return 0, errors.New("build remote Worker request")
	}
	req.Header.Set("Authorization", "Bearer "+c.secret)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.client.Do(req)
	if err != nil {
		return 0, remoteWorkerRetryableError{err: errors.New("remote Worker transport failed")}
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, remoteWorkerResponseLimit+1))
	if err != nil || len(data) > remoteWorkerResponseLimit {
		return res.StatusCode, remoteWorkerRetryableError{err: errors.New("remote Worker response transport failed")}
	}
	if res.StatusCode >= 500 || res.StatusCode == http.StatusTooManyRequests {
		return res.StatusCode, remoteWorkerRetryableError{err: errors.New("remote Worker Controller is unavailable")}
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var responseError struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if json.Unmarshal(data, &responseError) == nil {
			switch responseError.Error.Code {
			case "cancellation_pending":
				return res.StatusCode, remoteWorkerCancellationPendingError{}
			case "cancellation_not_requested":
				return res.StatusCode, remoteWorkerCancellationNotRequestedError{}
			}
		}
		return res.StatusCode, errors.New("remote Worker Controller rejected request")
	}
	if response != nil && res.StatusCode != http.StatusNoContent {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if decoder.Decode(response) != nil || decoder.Decode(new(any)) != io.EOF {
			return res.StatusCode, errors.New("remote Worker response schema is invalid")
		}
	} else if response == nil && res.StatusCode != http.StatusNoContent && len(bytes.TrimSpace(data)) == 0 {
		return res.StatusCode, errors.New("remote Worker response schema is invalid")
	}
	return res.StatusCode, nil
}

func (c *remoteWorkerClient) claim(ctx context.Context, id string) (*remoteWorkerClaim, error) {
	var claim remoteWorkerClaim
	status, err := c.do(ctx, http.MethodPost, "/v1/worker/claims", struct {
		ClaimID        string   `json:"claim_id"`
		WorkerID       string   `json:"worker_id"`
		BootID         string   `json:"boot_id"`
		Repositories   []string `json:"repositories"`
		ThreadSessions bool     `json:"thread_sessions,omitempty"`
	}{id, c.workerID, c.bootID, c.repositories, c.threadSessions}, &claim)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNoContent {
		if claim.Job.ID != "" || claim.Attempt.OwnerToken != "" {
			return nil, errors.New("remote Worker empty claim response is invalid")
		}
		return nil, nil
	}
	if !remoteWorkerJobIDPattern.MatchString(claim.Job.ID) || claim.Attempt.JobID != claim.Job.ID || claim.Attempt.Generation < 1 || claim.Attempt.WorkerID != c.workerID || claim.Attempt.BootID != c.bootID || !remoteWorkerTokenPattern.MatchString(claim.Attempt.OwnerToken) {
		return nil, errors.New("remote Worker claim is invalid")
	}
	return &claim, nil
}

func remoteAttemptPath(jobID, action string) string {
	return "/v1/worker/attempts/" + jobID + "/" + action
}
func (c *remoteWorkerClient) heartbeat(ctx context.Context, attempt remoteWorkerAttempt) (bool, error) {
	var result struct {
		CancellationRequested *bool `json:"cancellation_requested"`
	}
	_, err := c.do(ctx, http.MethodPost, remoteAttemptPath(attempt.JobID, "heartbeat"), remoteWorkerAttemptRequest{attempt.Generation, attempt.OwnerToken, attempt.WorkerID, attempt.BootID}, &result)
	if err != nil {
		return false, err
	}
	if result.CancellationRequested == nil {
		return false, errors.New("remote Worker heartbeat response is invalid")
	}
	return *result.CancellationRequested, nil
}
func (c *remoteWorkerClient) finish(ctx context.Context, attempt remoteWorkerAttempt, outcome remoteWorkerFinish) error {
	_, err := c.do(ctx, http.MethodPost, remoteAttemptPath(attempt.JobID, "finish"), outcome, nil)
	return err
}

func retryRemoteFinish(ctx context.Context, client *remoteWorkerClient, attempt remoteWorkerAttempt, outcome remoteWorkerFinish) error {
	return retryRemoteFinishAfterStop(ctx, client, attempt, outcome, false)
}

func retryRemoteFinishAfterStop(ctx context.Context, client *remoteWorkerClient, attempt remoteWorkerAttempt, outcome remoteWorkerFinish, cooperativelyStopped bool) error {
	for {
		requestCtx, cancel := context.WithTimeout(ctx, remoteWorkerHTTPTimeout)
		err := client.finish(requestCtx, attempt, outcome)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var retryable remoteWorkerRetryableError
		if errors.As(err, &retryable) {
			if !waitRemoteWorkerRetry(ctx) {
				return ctx.Err()
			}
			continue
		}
		var cancellationNotRequested remoteWorkerCancellationNotRequestedError
		var cancellationPending remoteWorkerCancellationPendingError
		if errors.As(err, &cancellationPending) && cooperativelyStopped {
			requestCtx, cancel := context.WithTimeout(ctx, remoteWorkerHTTPTimeout)
			_, heartbeatErr := client.heartbeat(requestCtx, attempt)
			cancel()
			if heartbeatErr != nil {
				return errors.New("remote Worker heartbeat failed; assigned job remains inspectable")
			}
			// Keep the worker's original successful outcome. The Controller's
			// cancellation request does not authorize rewriting it as canceled.
			return errors.New("remote Worker success conflicts with cancellation; assigned job remains inspectable")
		}
		if errors.As(err, &cancellationNotRequested) && outcome.Status == restjobs.StatusCanceled && cooperativelyStopped {
			requestCtx, cancel := context.WithTimeout(ctx, remoteWorkerHTTPTimeout)
			requested, heartbeatErr := client.heartbeat(requestCtx, attempt)
			cancel()
			if heartbeatErr != nil {
				return errors.New("remote Worker heartbeat failed; assigned job remains inspectable")
			}
			if requested {
				continue
			}
		}
		return errors.New("remote Worker finish was rejected; assigned job remains inspectable")
	}
}

// SelectRemoteHarness maps a Job to one locally configured compatible profile.
func SelectRemoteHarness(config restserver.RemoteWorkerConfig, jobID, repository string) (string, restserver.RemoteHarnessConfig, error) {
	profiles := make([]string, 0, len(config.HarnessProfiles))
	for name, profile := range config.HarnessProfiles {
		for _, alias := range profile.Repositories {
			if alias == repository {
				profiles = append(profiles, name)
				break
			}
		}
	}
	if len(profiles) == 0 {
		return "", restserver.RemoteHarnessConfig{}, errors.New("no local harness supports the assigned repository")
	}
	sort.Strings(profiles)
	digest := sha256.Sum256([]byte(jobID))
	var index uint64
	for _, value := range digest[:8] {
		index = index<<8 | uint64(value)
	}
	name := profiles[index%uint64(len(profiles))]
	return name, config.HarnessProfiles[name], nil
}

// RunRemoteWorker executes Controller-assigned jobs without local persistence.
// Each slot resolves its claim and finish retries before claiming more work.
func RunRemoteWorker(ctx context.Context, config restserver.RemoteWorkerConfig) error {
	if ctx == nil {
		return errors.New("remote Worker context must not be nil")
	}
	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid remote Worker config: %w", err)
	}
	for _, profile := range config.HarnessProfiles {
		if profile.Adapter == "pi" {
			if _, err := exec.LookPath(profile.Executable); err != nil {
				return errors.New("configured Pi executable is unavailable")
			}
			lock, err := lockPiWorker()
			if err != nil {
				return err
			}
			defer lock.Close()
		}
	}
	secret, err := restserver.LoadRemoteWorkerSecret(config.SharedSecretFile)
	if err != nil {
		return err
	}
	client, err := newRemoteWorkerClient(config, secret.Value())
	if err != nil {
		return err
	}
	defer func() {
		client.secret = ""
		if transport, ok := client.client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}()
	return runRemoteWorkerLoop(ctx, config, client)
}

func runRemoteWorkerLoop(ctx context.Context, config restserver.RemoteWorkerConfig, client *remoteWorkerClient) error {
	bootID, err := processBootIDWithReader(rand.Reader)
	if err != nil {
		return err
	}
	client.bootID = bootID
	if config.MaxConcurrentJobs <= 1 {
		return runRemoteWorkerSlot(ctx, ctx, config, client)
	}

	claimsCtx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	var slots sync.WaitGroup
	for range config.MaxConcurrentJobs {
		slots.Go(func() {
			if err := runRemoteWorkerSlot(ctx, claimsCtx, config, client); err != nil {
				stop(err)
			}
		})
	}
	slots.Wait()
	if ctx.Err() != nil {
		return nil
	}
	return context.Cause(claimsCtx)
}

func runRemoteWorkerSlot(ctx, claimsCtx context.Context, config restserver.RemoteWorkerConfig, client *remoteWorkerClient) error {
	for claimsCtx.Err() == nil {
		claimID, err := newRemoteClaimID(rand.Reader)
		if err != nil {
			return err
		}
		var claim *remoteWorkerClaim
		for claimsCtx.Err() == nil {
			// Let a sent claim resolve: the Controller may already have assigned a job.
			requestCtx, cancel := context.WithTimeout(ctx, remoteWorkerHTTPTimeout)
			claim, err = client.claim(requestCtx, claimID)
			cancel()
			if err == nil {
				break
			}
			var retryable remoteWorkerRetryableError
			if !errors.As(err, &retryable) {
				return err
			}
			if !waitRemoteWorkerRetry(claimsCtx) {
				return nil
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		if claim == nil {
			if !waitRemoteWorkerPoll(claimsCtx) {
				return nil
			}
			continue
		}
		if err := runRemoteWorkerAttempt(ctx, config, client, *claim); err != nil && ctx.Err() == nil {
			return err
		}
	}
	return nil
}

func finishRemoteWorkerFailure(ctx context.Context, cancel context.CancelFunc, heartbeat <-chan remoteHeartbeatResult, config restserver.RemoteWorkerConfig, client *remoteWorkerClient, claim remoteWorkerClaim) error {
	cancel()
	heartbeatResult := <-heartbeat
	if ctx.Err() != nil {
		return nil
	}
	if heartbeatResult.err != nil {
		return errors.New("remote Worker heartbeat failed; assigned job remains inspectable")
	}
	status := restjobs.StatusFailed
	if heartbeatResult.cancellationRequested {
		status = restjobs.StatusCanceled
	}
	evidence := restjobs.VerificationEvidence{Checks: make([]restjobs.VerificationCheck, len(config.VerificationChecks)), Limitations: []string{restjobs.LimitationAgentNotVerdict}}
	if len(evidence.Checks) == 0 {
		evidence.Limitations = append(evidence.Limitations, restjobs.LimitationNoChecksConfigured)
	}
	for index := range evidence.Checks {
		evidence.Checks[index] = restjobs.VerificationCheck{Name: fmt.Sprintf("check-%02d", index+1), Outcome: restjobs.VerificationNotCompleted}
	}
	outcome := remoteWorkerFinish{remoteWorkerAttemptRequest: remoteWorkerAttemptRequest{claim.Attempt.Generation, claim.Attempt.OwnerToken, claim.Attempt.WorkerID, claim.Attempt.BootID}, Status: status, Evidence: remoteWorkerEvidence{Checks: evidence.Checks, Limitations: evidence.Limitations}}
	if err := retryRemoteFinish(ctx, client, claim.Attempt, outcome); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

func validRemoteWorkerTask(task string) bool {
	if strings.TrimSpace(task) == "" || len(task) > 256<<10 || !utf8.ValidString(task) {
		return false
	}
	for _, r := range task {
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
			return false
		}
	}
	return true
}

func cloneRemoteEvidence(value restjobs.VerificationEvidence) restjobs.VerificationEvidence {
	checks := make([]restjobs.VerificationCheck, len(value.Checks))
	copy(checks, value.Checks)
	limitations := make([]string, len(value.Limitations))
	copy(limitations, value.Limitations)
	value.Checks, value.Limitations = checks, limitations
	return value
}

func newRemoteClaimID(reader io.Reader) (string, error) {
	var b [16]byte
	if _, err := io.ReadFull(reader, b[:]); err != nil {
		return "", errors.New("generate remote Worker claim ID")
	}
	return hex.EncodeToString(b[:]), nil
}
func waitRemoteWorkerRetry(ctx context.Context) bool {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func waitRemoteWorkerPoll(ctx context.Context) bool {
	timer := time.NewTimer(remoteWorkerPollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

type remoteHeartbeatResult struct {
	cancellationRequested bool
	err                   error
}

func monitorRemoteAttempt(ctx context.Context, cancel context.CancelFunc, client *remoteWorkerClient, attempt remoteWorkerAttempt, interval time.Duration) <-chan remoteHeartbeatResult {
	done := make(chan remoteHeartbeatResult, 1)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			requestCtx, stopRequest := context.WithTimeout(ctx, remoteWorkerHTTPTimeout)
			requested, err := client.heartbeat(requestCtx, attempt)
			stopRequest()
			if err != nil {
				if ctx.Err() != nil {
					done <- remoteHeartbeatResult{}
					return
				}
				cancel()
				done <- remoteHeartbeatResult{err: err}
				return
			}
			if requested {
				cancel()
				done <- remoteHeartbeatResult{cancellationRequested: true}
				return
			}
			select {
			case <-ctx.Done():
				done <- remoteHeartbeatResult{}
				return
			case <-ticker.C:
			}
		}
	}()
	return done
}

func runRemoteWorkerAttempt(parent context.Context, config restserver.RemoteWorkerConfig, client *remoteWorkerClient, claim remoteWorkerClaim) error {
	jobCtx, cancel := context.WithCancel(parent)
	defer cancel()
	heartbeatDone := monitorRemoteAttempt(jobCtx, cancel, client, claim.Attempt, remoteWorkerHeartbeatInterval)
	_, profile, err := SelectRemoteHarness(config, claim.Job.ID, claim.Job.Request.Repository)
	if err != nil {
		return finishRemoteWorkerFailure(parent, cancel, heartbeatDone, config, client, claim)
	}
	root, ok := config.Repositories[claim.Job.Request.Repository]
	if !ok {
		return finishRemoteWorkerFailure(parent, cancel, heartbeatDone, config, client, claim)
	}
	if !validRemoteWorkerTask(claim.Job.Request.Task) {
		return finishRemoteWorkerFailure(parent, cancel, heartbeatDone, config, client, claim)
	}
	if err := (restserver.Config{Repositories: map[string]string{claim.Job.Request.Repository: root}}).ValidateRepositoryRoots(); err != nil {
		return finishRemoteWorkerFailure(parent, cancel, heartbeatDone, config, client, claim)
	}
	resultsBase, err := serverResultsBase()
	if err != nil {
		return finishRemoteWorkerFailure(parent, cancel, heartbeatDone, config, client, claim)
	}
	if profile.Adapter == "pi" {
		return runRemoteHarnessAttempt(parent, jobCtx, cancel, heartbeatDone, config, client, claim, pi.Runner{Executable: profile.Executable}, root, resultsBase)
	}
	workspace, err := restworkspace.New(restworkspace.Config{RepositoryRoot: root, ResultsRoot: filepath.Join(resultsBase, aliasDirectory(claim.Job.Request.Repository), "results")})
	if err != nil {
		return finishRemoteWorkerFailure(parent, cancel, heartbeatDone, config, client, claim)
	}
	serverConfig := restserver.DefaultConfig()
	serverConfig.Repositories = map[string]string{claim.Job.Request.Repository: root}
	serverConfig.Harness = restserver.HarnessConfig{Executable: profile.Executable, Args: append([]string(nil), profile.Args...)}
	serverConfig.VerificationChecks = cloneVerificationChecks(config.VerificationChecks)
	serverConfig.Limits.JobTimeout = config.Limits.JobTimeout
	serverConfig.Limits.HarnessOutput = config.Limits.HarnessOutputBytes
	workflowConfig := restworkflow.DefaultConfig()
	workflowConfig.AutoPublish = false
	workflowConfig.WorktreeParent = ""
	workflowConfig.PipelineChecks = cloneVerificationChecks(config.VerificationChecks)
	var evidenceMu sync.Mutex
	evidence := restjobs.VerificationEvidence{}
	executor, err := restworker.NewFactoryExecutorContext(jobCtx, restworker.FactoryExecutorConfig{Server: serverConfig, Workflow: workflowConfig, Workspaces: map[string]restworker.WorkspaceManager{claim.Job.Request.Repository: workspace}, RecordVerificationEvidence: func(_ restjobs.AttemptHandle, value restjobs.VerificationEvidence) error {
		evidenceMu.Lock()
		defer evidenceMu.Unlock()
		evidence = cloneRemoteEvidence(value)
		return nil
	}})
	if err != nil {
		return finishRemoteWorkerFailure(parent, cancel, heartbeatDone, config, client, claim)
	}
	jobRequest := restjobs.Request{Repository: claim.Job.Request.Repository, Task: claim.Job.Request.Task, Issue: claim.Job.Request.Issue}
	if provenance := claim.Job.Request.IssueProvenance; provenance != nil {
		jobRequest.IssueProvenance = &restjobs.IssueProvenance{Tracker: provenance.Tracker, SourceRepository: provenance.SourceRepository, IssueNumber: provenance.IssueNumber, Version: provenance.Version, URL: provenance.URL, Title: provenance.Title}
	}
	job := restjobs.Snapshot{ID: claim.Job.ID, Request: jobRequest}
	attemptHandle := restjobs.AttemptHandle{JobID: claim.Attempt.JobID, Generation: claim.Attempt.Generation, OwnerToken: claim.Attempt.OwnerToken, WorkerID: claim.Attempt.WorkerID, BootID: claim.Attempt.BootID}
	result := executor.Execute(jobCtx, job, attemptHandle)
	if result.Err == nil && executor.Complete(jobCtx, job, attemptHandle, result) != nil {
		result.Err = errors.New("remote Worker completion failed")
	}
	evidenceMu.Lock()
	observed := cloneRemoteEvidence(evidence)
	evidenceMu.Unlock()
	return finishRemoteExecution(parent, cancel, heartbeatDone, config, client, claim, observed, result.Err)
}

func finishRemoteExecution(parent context.Context, cancel context.CancelFunc, heartbeatDone <-chan remoteHeartbeatResult, config restserver.RemoteWorkerConfig, client *remoteWorkerClient, claim remoteWorkerClaim, evidence restjobs.VerificationEvidence, executionErr error) error {
	cancel()
	heartbeatResult := <-heartbeatDone
	if parent.Err() != nil {
		return nil
	}
	if heartbeatResult.err != nil {
		return errors.New("remote Worker heartbeat failed; assigned job remains inspectable")
	}
	status := restjobs.StatusSucceeded
	if executionErr != nil {
		status = restjobs.StatusFailed
	}
	if heartbeatResult.cancellationRequested && errors.Is(executionErr, context.Canceled) {
		status = restjobs.StatusCanceled
	}
	if heartbeatResult.cancellationRequested && status != restjobs.StatusCanceled {
		return errors.New("remote Worker execution did not confirm cooperative cancellation; assigned job remains inspectable")
	}
	if len(evidence.Limitations) == 0 {
		evidence = restjobs.VerificationEvidence{Checks: make([]restjobs.VerificationCheck, len(config.VerificationChecks)), Limitations: []string{restjobs.LimitationAgentNotVerdict}}
		if len(evidence.Checks) == 0 {
			evidence.Limitations = append(evidence.Limitations, restjobs.LimitationNoChecksConfigured)
		}
		for i := range evidence.Checks {
			evidence.Checks[i] = restjobs.VerificationCheck{Name: fmt.Sprintf("check-%02d", i+1), Outcome: restjobs.VerificationNotCompleted}
		}
	}
	if status == restjobs.StatusCanceled {
		for i := range evidence.Checks {
			evidence.Checks[i].Outcome = restjobs.VerificationCanceled
		}
	}
	if status == restjobs.StatusFailed {
		for i := range evidence.Checks {
			if evidence.Checks[i].Outcome == restjobs.VerificationPassed {
				evidence.Checks[i].Outcome = restjobs.VerificationNotCompleted
			}
		}
	}
	if status != restjobs.StatusSucceeded {
		evidence.Reply, evidence.ReplyTruncated = "", false
	}
	outcome := remoteWorkerFinish{remoteWorkerAttemptRequest: remoteWorkerAttemptRequest{claim.Attempt.Generation, claim.Attempt.OwnerToken, claim.Attempt.WorkerID, claim.Attempt.BootID}, Status: status, Evidence: remoteWorkerEvidence{Checks: evidence.Checks, Limitations: evidence.Limitations, Reply: evidence.Reply, ReplyTruncated: evidence.ReplyTruncated}}
	if err := retryRemoteFinishAfterStop(parent, client, claim.Attempt, outcome, true); err != nil && parent.Err() == nil {
		return err
	}
	return nil
}
