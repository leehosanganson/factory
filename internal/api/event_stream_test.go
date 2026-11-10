package restapi

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
)

func TestJobEventStreamReplaysSafeOrderedHistoryAndClosesAtTerminal(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 4))
	job, _, err := manager.Admit("event-stream", restjobs.Request{Repository: "widget", Task: "private task payload"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := manager.ClaimNextAttempt()
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEventForAttempt(claim.Attempt, "running", "/workspace/private-log credential-secret"); err != nil {
		t.Fatal(err)
	}
	if err := manager.RecordProgressForAttempt(claim.Attempt, restjobs.ProgressSummary{Stage: "implement", State: "started"}); err != nil {
		t.Fatal(err)
	}
	if err := manager.FinishAttempt(claim.Attempt, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}

	handler := newTestHandler(t, manager, func() bool { return true })
	response := request(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/events", "", true)
	body := response.Body.String()
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("event stream response status=%d content-type=%q body=%q", response.Code, response.Header().Get("Content-Type"), body)
	}
	for _, event := range []string{"id: 1", "event: queued", "id: 2", "event: running", "event: workflow_progress", `"stage":"implement"`, `"state":"started"`, "id: 3", "event: succeeded"} {
		if !strings.Contains(body, event) {
			t.Errorf("event stream omitted %q: %q", event, body)
		}
	}
	if strings.Contains(body, "private task payload") || strings.Contains(body, "private-log") || strings.Contains(body, "credential-secret") || strings.Contains(body, "provider") || strings.Contains(body, "path") {
		t.Fatalf("event stream exposed private job data: %q", body)
	}
	if strings.Index(body, "event: queued") > strings.Index(body, "event: running") || strings.Index(body, "event: running") > strings.Index(body, "event: succeeded") {
		t.Fatalf("event stream history is out of order: %q", body)
	}
}

func TestJobEventStreamReconnectReplaysOnlyEventsAfterCursor(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 8))
	job, _, err := manager.Admit("stream-resume", restjobs.Request{Repository: "widget", Task: "private task"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	resume := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil)
	resume.Header.Set("Authorization", "Bearer "+testAPIKey)
	resume.Header.Set("Last-Event-ID", "1")
	ctx, cancel := context.WithCancel(resume.Context())
	resume = resume.WithContext(ctx)
	writer := newSynchronizedStreamRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(writer, resume)
		close(done)
	}()
	waitForTestCondition(t, func() bool { return len(handler.eventStreams) == 1 })
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(job.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	waitForTestCondition(t, func() bool { return strings.Contains(writer.String(), "event: succeeded") })
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream did not release after disconnect")
	}
	body := writer.String()
	if strings.Contains(body, "id: 1") || !strings.Contains(body, "id: 2") || !strings.Contains(body, "id: 3") {
		t.Fatalf("cursor replay included duplicate or omitted events: %q", body)
	}
}

func TestJobEventStreamReplaysFromCursorForSQLiteStore(t *testing.T) {
	databaseDir := t.TempDir()
	if err := os.Chmod(databaseDir, 0o700); err != nil {
		t.Fatal(err)
	}
	database := databaseDir + "/jobs.db"
	manager, err := restjobs.OpenSQLiteStore(database, managerConfig(2, 2, 8))
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	job, _, err := manager.Admit("sqlite-event-stream", restjobs.Request{Repository: "widget", Task: "private SQLite task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(job.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	history, err := manager.History(job.ID)
	if err != nil || len(history.Events) != 3 {
		t.Fatalf("SQLite event history=%+v err=%v", history, err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	resume := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil)
	resume.Header.Set("Authorization", "Bearer "+testAPIKey)
	resume.Header.Set("Last-Event-ID", "2")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, resume)
	body := recorder.Body.String()
	if recorder.Code != http.StatusOK || strings.Contains(body, "id: 2") || !strings.Contains(body, "id: 3") || strings.Contains(body, "private SQLite task") {
		t.Fatalf("SQLite cursor replay status=%d body=%q", recorder.Code, body)
	}
}

func TestJobEventStreamReadinessAndMalformedCursor(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 4))
	job, _, err := manager.Admit("stream-ready", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return false })
	notReady := request(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/events", "", true)
	assertError(t, notReady, http.StatusServiceUnavailable, "not_ready")
	handler.config.Ready = func() bool { return true }
	for _, cursor := range []string{"not-a-number", "1,2"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil)
		req.Header.Set("Authorization", "Bearer "+testAPIKey)
		req.Header.Add("Last-Event-ID", cursor)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		assertError(t, response, http.StatusBadRequest, "invalid_request")
	}
	withQuery := request(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/events?cursor=1", "", true)
	assertError(t, withQuery, http.StatusBadRequest, "invalid_request")
}

func TestJobEventStreamTerminalSnapshotWithoutHistoryCloses(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 1))
	job, _, err := manager.Admit("stream-terminal-empty", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(job.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(job.ID, "audit", "event two"); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(job.ID, "audit", "event three"); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(job.ID, "audit", "event four"); err != nil {
		t.Fatal(err)
	}
	history, err := manager.History(job.ID)
	if err != nil || !history.Truncated || len(history.Events) != 1 || history.Events[0].Sequence != 6 {
		t.Fatalf("terminal history fixture=%+v err=%v", history, err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	response := request(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/events", "", true)
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, "event: terminal_snapshot") || !strings.Contains(body, `"status":"succeeded"`) || strings.Contains(body, "event two") || strings.Contains(body, "event three") || strings.Contains(body, "event four") || len(handler.eventStreams) != 0 {
		t.Fatalf("terminal tail replay status=%d active=%d body=%q", response.Code, len(handler.eventStreams), body)
	}
}

func TestJobEventStreamTerminalClosesWhenTerminalEventExpired(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 1))
	job, _, err := manager.Admit("stream-terminal-expired", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(job.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(job.ID, "audit", "private details"); err != nil {
		t.Fatal(err)
	}
	history, err := manager.History(job.ID)
	if err != nil || history.LatestSequence != 4 || history.Events[0].Type != "audit" {
		t.Fatalf("terminal event expiration fixture history=%+v err=%v", history, err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	req.Header.Set("Last-Event-ID", "4")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK || len(handler.eventStreams) != 0 {
		t.Fatalf("terminal stream status=%d active slots=%d body=%q", response.Code, len(handler.eventStreams), response.Body.String())
	}
}

func TestJobEventStreamCursorDoesNotRepeatAfterEventEviction(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 1))
	job, _, err := manager.Admit("stream-eviction", restjobs.Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(job.ID, "audit", "one"); err != nil {
		t.Fatal(err)
	}
	history, err := manager.History(job.ID)
	if err != nil || history.LatestSequence != 2 {
		t.Fatalf("pre-eviction cursor=%+v err=%v", history, err)
	}
	if err := manager.AddEvent(job.ID, "audit", "two"); err != nil {
		t.Fatal(err)
	}
	history, err = manager.History(job.ID)
	if err != nil || history.LatestSequence != 3 || history.Events[0].Sequence != 3 {
		t.Fatalf("post-eviction cursor=%+v err=%v", history, err)
	}
}

func TestJobEventStreamResetWhenHistoryExpiresDuringSubscription(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 1))
	job, _, err := manager.Admit("stream-expire-live", restjobs.Request{Repository: "widget", Task: "private task"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })
	writer := newSynchronizedStreamRecorder()
	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil).WithContext(requestCtx)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(writer, req)
		close(done)
	}()
	waitForTestCondition(t, func() bool { return len(handler.eventStreams) == 1 })
	if err := manager.AddEvent(job.ID, "audit", "private log /workspace/token-secret"); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(job.ID, "audit", "another private event"); err != nil {
		t.Fatal(err)
	}
	waitForTestCondition(t, func() bool { return strings.Contains(writer.String(), `"reason":"cursor_expired"`) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reset stream did not terminate")
	}
	body := writer.String()
	if strings.Contains(body, "token-secret") || strings.Contains(body, "/workspace") || strings.Contains(body, "private task") || !strings.Contains(body, "GET status and history") {
		t.Fatalf("reset control leaked private data or omitted fallback: %q", body)
	}
}

func waitForTestCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not reached before timeout")
}

type synchronizedStreamRecorder struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	header http.Header
	status int
}

func newSynchronizedStreamRecorder() *synchronizedStreamRecorder {
	return &synchronizedStreamRecorder{header: make(http.Header)}
}

func (r *synchronizedStreamRecorder) Header() http.Header { return r.header }
func (r *synchronizedStreamRecorder) WriteHeader(status int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status == 0 {
		r.status = status
	}
}
func (r *synchronizedStreamRecorder) Write(data []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.buffer.Write(data)
}
func (r *synchronizedStreamRecorder) Flush() {}
func (r *synchronizedStreamRecorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buffer.String()
}

type streamTestWriter struct {
	entered  chan struct{}
	header   http.Header
	mu       sync.Mutex
	deadline time.Time
}

func (w *streamTestWriter) Header() http.Header { return w.header }

func (w *streamTestWriter) WriteHeader(int) {}

func (w *streamTestWriter) Write(data []byte) (int, error) {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	w.mu.Lock()
	deadline := w.deadline
	w.mu.Unlock()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	<-timer.C
	return 0, errors.New("simulated slow client write timeout")
}

func (w *streamTestWriter) SetWriteDeadline(time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.deadline = time.Now().Add(10 * time.Millisecond)
	return nil
}

func (w *streamTestWriter) Flush() {}

func TestJobEventStreamAuthenticationCursorAndCapacity(t *testing.T) {
	manager := newTestManager(t, managerConfig(2, 2, 3))
	job, _, err := manager.Admit("stream-auth", restjobs.Request{Repository: "widget", Task: "private"})
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestHandler(t, manager, func() bool { return true })

	unauthenticated := request(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/events", "", false)
	assertError(t, unauthenticated, http.StatusUnauthorized, "unauthenticated")
	unknown := request(handler, http.MethodGet, "/v1/jobs/unknown/events", "", true)
	assertError(t, unknown, http.StatusNotFound, "not_found")
	wrongMethod := request(handler, http.MethodPost, "/v1/jobs/"+job.ID+"/events", "", true)
	assertError(t, wrongMethod, http.StatusMethodNotAllowed, "method_not_allowed")
	if wrongMethod.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("event stream Allow=%q, want GET", wrongMethod.Header().Get("Allow"))
	}

	oldest := newTestManager(t, managerConfig(2, 2, 1))
	truncatedJob, _, err := oldest.Admit("stream-stale", restjobs.Request{Repository: "widget", Task: "private"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldest.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := oldest.Finish(truncatedJob.ID, restjobs.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	truncatedHandler := newTestHandler(t, oldest, func() bool { return true })
	staleRequest := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+truncatedJob.ID+"/events", nil)
	staleRequest.Header.Set("Authorization", "Bearer "+testAPIKey)
	staleRequest.Header.Set("Last-Event-ID", "1")
	staleResponse := httptest.NewRecorder()
	truncatedHandler.ServeHTTP(staleResponse, staleRequest)
	assertError(t, staleResponse, http.StatusConflict, "cursor_expired")
	noCursor := request(truncatedHandler, http.MethodGet, "/v1/jobs/"+truncatedJob.ID+"/events", "", true)
	if noCursor.Code != http.StatusOK || !strings.Contains(noCursor.Body.String(), "event: succeeded") {
		t.Fatalf("terminal retained-tail stream status=%d body=%q", noCursor.Code, noCursor.Body.String())
	}
	badCursor := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil)
	badCursor.Header.Set("Authorization", "Bearer "+testAPIKey)
	badCursor.Header.Set("Last-Event-ID", "2")
	badCursorResponse := httptest.NewRecorder()
	handler.ServeHTTP(badCursorResponse, badCursor)
	assertError(t, badCursorResponse, http.StatusBadRequest, "invalid_request")

	entered := make(chan struct{}, eventStreamConcurrency+1)
	writer := &streamTestWriter{entered: entered, header: make(http.Header)}
	requestCtx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil).WithContext(requestCtx)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(writer, req)
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("stream did not begin writing")
	}
	for index := 0; index < eventStreamConcurrency-1; index++ {
		handler.eventStreams <- struct{}{}
	}
	resp := request(handler, http.MethodGet, "/v1/jobs/"+job.ID+"/events", "", true)
	assertError(t, resp, http.StatusServiceUnavailable, "stream_capacity")
	select {
	case <-done:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("slow-client write deadline did not release its handler")
	}
	cancel()
	if got := len(handler.eventStreams); got != eventStreamConcurrency-1 {
		t.Fatalf("event stream capacity retained %d slots after disconnect, want only %d simulated reservations", got, eventStreamConcurrency-1)
	}
	for range eventStreamConcurrency - 1 {
		<-handler.eventStreams
	}
}
