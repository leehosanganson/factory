package restjobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func testManager(t *testing.T, config Config) *LocalManager {
	t.Helper()
	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	return manager
}

func testConfig() Config {
	return Config{QueueCapacity: 8, MaxConcurrentJobs: 2, MaxRecords: 8, MaxEventsPerJob: 8}
}

func TestNewManagerRejectsInvalidBounds(t *testing.T) {
	cases := []struct {
		name   string
		config Config
	}{
		{"zero queue", Config{MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 1}},
		{"negative queue", Config{QueueCapacity: -1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 1}},
		{"queue exceeds records", Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 1}},
		{"zero concurrency", Config{QueueCapacity: 1, MaxRecords: 1, MaxEventsPerJob: 1}},
		{"concurrency exceeds records", Config{QueueCapacity: 1, MaxConcurrentJobs: 2, MaxRecords: 1, MaxEventsPerJob: 1}},
		{"zero records", Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxEventsPerJob: 1}},
		{"zero history", Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1}},
		{"unbounded queue", Config{QueueCapacity: maxQueueLimit + 1, MaxConcurrentJobs: 1, MaxRecords: maxQueueLimit + 1, MaxEventsPerJob: 1}},
		{"unbounded records", Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: maxRecordsLimit + 1, MaxEventsPerJob: 1}},
		{"unbounded history", Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: maxEventsLimit + 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewManager(tc.config); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("NewManager() error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestAdmissionNormalizesAndReplaysPayload(t *testing.T) {
	manager := testManager(t, testConfig())
	issue := 17
	first, replay, err := manager.Admit(" key-1 ", Request{Repository: " widget ", Task: "  Fix the queue.  ", Issue: &issue})
	if err != nil || replay {
		t.Fatalf("first Admit() = (%+v, %t, %v), want new admission", first, replay, err)
	}
	issue = 99 // The record must not retain caller-owned pointers.
	second, replay, err := manager.Admit("key-1", Request{Repository: "widget", Task: "Fix the queue.", Issue: intPointer(17)})
	if err != nil || !replay || first.ID != second.ID {
		t.Fatalf("replay Admit() = (%+v, %t, %v), want same retained job", second, replay, err)
	}
	if second.Request.Issue == nil || *second.Request.Issue != 17 {
		t.Fatalf("stored issue = %v, want copied issue 17", second.Request.Issue)
	}
}

func TestAdmissionRejectsChangedPayloadForKey(t *testing.T) {
	manager := testManager(t, testConfig())
	if _, _, err := manager.Admit("same-key", Request{Repository: "widget", Task: "first"}); err != nil {
		t.Fatal(err)
	}
	_, _, err := manager.Admit("same-key", Request{Repository: "widget", Task: "second"})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed request error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestQueueFullRejectsWithoutRecordAndKeyCanBeReused(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 3, MaxEventsPerJob: 4})
	first, _, err := manager.Admit("first", Request{Repository: "widget", Task: "one"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.Admit("retry-key", Request{Repository: "widget", Task: "two"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue error = %v, want ErrQueueFull", err)
	}
	if _, err := manager.Get("retry-key"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected key has record: Get() error = %v", err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	accepted, replay, err := manager.Admit("retry-key", Request{Repository: "widget", Task: "two"})
	if err != nil || replay || accepted.ID == first.ID {
		t.Fatalf("retry Admit() = (%+v, %t, %v), want new record after capacity frees", accepted, replay, err)
	}
}

func TestRegistryEvictsOldestTerminalAndPreservesActiveJobs(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 3, MaxConcurrentJobs: 1, MaxRecords: 3, MaxEventsPerJob: 4})
	oldest, _, err := manager.Admit("oldest", Request{Repository: "widget", Task: "oldest"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(oldest.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	newerTerminal, _, err := manager.Admit("newer-terminal", Request{Repository: "widget", Task: "newer terminal"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(newerTerminal.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	active, _, err := manager.Admit("active", Request{Repository: "widget", Task: "active"})
	if err != nil {
		t.Fatal(err)
	}
	active, err = manager.ClaimNext()
	if err != nil {
		t.Fatal(err)
	}
	newest, _, err := manager.Admit("new", Request{Repository: "widget", Task: "new"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Get(oldest.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("oldest terminal record retained: Get() error = %v", err)
	}
	if got, err := manager.Get(newerTerminal.ID); err != nil || got.Status != StatusFailed {
		t.Fatalf("newer terminal record evicted: (%+v, %v)", got, err)
	}
	if got, err := manager.Get(active.ID); err != nil || got.Status != StatusRunning {
		t.Fatalf("active record evicted or changed: (%+v, %v)", got, err)
	}
	if got, err := manager.Get(newest.ID); err != nil || got.Status != StatusQueued {
		t.Fatalf("new record unavailable: (%+v, %v)", got, err)
	}
	replacement, replay, err := manager.Admit("oldest", Request{Repository: "widget", Task: "replacement"})
	if err != nil || replay || replacement.ID == oldest.ID {
		t.Fatalf("admission with evicted key = (%+v, %t, %v), want a new record", replacement, replay, err)
	}
}

func TestRegistryRejectsWhenAtCapWithNoTerminalRecord(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 2})
	first, _, err := manager.Admit("running", Request{Repository: "widget", Task: "running"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	queued, _, err := manager.Admit("queued", Request{Repository: "widget", Task: "queued"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.Admit("over-cap", Request{Repository: "widget", Task: "rejected"}); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("active/queued cap rejection error = %v, want ErrRegistryFull", err)
	}
	if _, err := manager.Get(first.ID); err != nil {
		t.Fatalf("running job was not retained: %v", err)
	}
	if _, err := manager.Get(queued.ID); err != nil {
		t.Fatalf("queued job was not retained: %v", err)
	}
	if _, err := manager.Get("over-cap"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected job has record: %v", err)
	}
}

func TestAdmissionConcurrentSameKeyCreatesOneJob(t *testing.T) {
	const callers = 64
	manager := testManager(t, Config{QueueCapacity: callers, MaxConcurrentJobs: callers, MaxRecords: callers, MaxEventsPerJob: 4})
	ids := make(chan string, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snapshot, _, err := manager.Admit("parallel-key", Request{Repository: "widget", Task: "one"})
			if err != nil {
				errs <- err
				return
			}
			ids <- snapshot.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent admission: %v", err)
	}
	var id string
	for got := range ids {
		if id == "" {
			id = got
		} else if got != id {
			t.Errorf("concurrent admissions produced IDs %q and %q", id, got)
		}
	}
	if id == "" {
		t.Fatal("no concurrent admission succeeded")
	}
	if len(manager.jobs) != 1 || len(manager.queue) != 1 {
		t.Fatalf("concurrent admission retained jobs=%d queued=%d, want one each", len(manager.jobs), len(manager.queue))
	}
}

func TestAdmissionConcurrentDistinctKeysHonorsCapacity(t *testing.T) {
	const callers = 48
	const capacity = 9
	manager := testManager(t, Config{QueueCapacity: capacity, MaxConcurrentJobs: 1, MaxRecords: capacity, MaxEventsPerJob: 2})
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, full := 0, 0
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := manager.Admit(fmt.Sprintf("key-%d", i), Request{Repository: "widget", Task: "task"})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				accepted++
			case errors.Is(err, ErrQueueFull):
				full++
			default:
				t.Errorf("Admit() error = %v", err)
			}
		}(i)
	}
	wg.Wait()
	if accepted != capacity || full != callers-capacity || len(manager.jobs) != capacity || len(manager.queue) != capacity {
		t.Fatalf("accepted=%d full=%d records=%d queued=%d, want %d/%d/%d/%d", accepted, full, len(manager.jobs), len(manager.queue), capacity, callers-capacity, capacity, capacity)
	}
}

func TestEventHistoryTruncatesOldestEvents(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 3})
	job, _, err := manager.Admit("events", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 4; i++ {
		if err := manager.AddEvent(job.ID, fmt.Sprintf("custom-%d", i), "event"); err != nil {
			t.Fatal(err)
		}
	}
	history, err := manager.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !history.Truncated || len(history.Events) != 3 {
		t.Fatalf("History() = (%+v), want 3 events and truncated", history)
	}
	if history.Events[0].Type != "custom-2" || history.Events[2].Type != "custom-4" {
		t.Fatalf("retained events = %+v, want oldest retained custom-2 through custom-4", history.Events)
	}
}

func TestSnapshotsAndHistoryAreJSONSerializable(t *testing.T) {
	manager := testManager(t, testConfig())
	job, _, err := manager.Admit("json", Request{Repository: "widget", Task: "task", Issue: intPointer(4)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := json.Marshal(job); err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	history, err := manager.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := json.Marshal(history); err != nil {
		t.Fatalf("marshal history: %v", err)
	}
}

func TestSnapshotsAndHistoryAreDefensiveCopies(t *testing.T) {
	manager := testManager(t, testConfig())
	job, _, err := manager.Admit("copy", Request{Repository: "widget", Task: "task", Issue: intPointer(3)})
	if err != nil {
		t.Fatal(err)
	}
	*job.Request.Issue = 90
	job.Request.Task = "mutated"
	stored, err := manager.Get(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Request.Task != "task" || stored.Request.Issue == nil || *stored.Request.Issue != 3 {
		t.Fatalf("snapshot mutation changed manager state: %+v", stored)
	}
	if err := manager.AddEvent(job.ID, "check", "original"); err != nil {
		t.Fatal(err)
	}
	history, err := manager.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	history.Events[0].Type = "mutated"
	history.Events[0].Message = "mutated"
	storedHistory, err := manager.History(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedHistory.Events[0].Type != "queued" || storedHistory.Events[1].Message != "original" {
		t.Fatalf("history mutation changed manager state: %+v", storedHistory)
	}
}

func TestClaimNextEnforcesConfiguredConcurrency(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 4})
	first, _, err := manager.Admit("first", Request{Repository: "widget", Task: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := manager.Admit("second", Request{Repository: "widget", Task: "second"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := manager.ClaimNext()
	if err != nil || claimed.ID != first.ID {
		t.Fatalf("first ClaimNext() = (%+v, %v)", claimed, err)
	}
	if _, err := manager.ClaimNext(); !errors.Is(err, ErrNoWorkerSlots) {
		t.Fatalf("ClaimNext() at concurrency limit = %v, want ErrNoWorkerSlots", err)
	}
	if queued, err := manager.Get(second.ID); err != nil || queued.Status != StatusQueued {
		t.Fatalf("job blocked by concurrency limit was lost: (%+v, %v)", queued, err)
	}
	if err := manager.Finish(first.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	claimed, err = manager.ClaimNext()
	if err != nil || claimed.ID != second.ID || claimed.Status != StatusRunning {
		t.Fatalf("ClaimNext() after slot released = (%+v, %v)", claimed, err)
	}
}

func TestLifecycleAndInvalidTransitions(t *testing.T) {
	manager := testManager(t, testConfig())
	job, _, err := manager.Admit("lifecycle", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(job.ID, StatusSucceeded); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Finish(queued) error = %v, want ErrInvalidTransition", err)
	}
	running, err := manager.ClaimNext()
	if err != nil || running.Status != StatusRunning {
		t.Fatalf("ClaimNext() = (%+v, %v)", running, err)
	}
	for _, status := range []Status{StatusQueued, StatusRunning, Status("unknown")} {
		if err := manager.Finish(job.ID, status); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("Finish(%q) error = %v, want ErrInvalidTransition", status, err)
		}
	}
	if err := manager.Finish(job.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	finished, err := manager.Get(job.ID)
	if err != nil || finished.Status != StatusFailed || finished.UpdatedAt.Before(running.UpdatedAt) {
		t.Fatalf("finished snapshot = (%+v, %v)", finished, err)
	}
	if _, err := manager.ClaimNext(); !errors.Is(err, ErrNoQueuedJobs) {
		t.Fatalf("empty ClaimNext() error = %v, want ErrNoQueuedJobs", err)
	}
}

func TestAdmissionAndEventInputValidation(t *testing.T) {
	manager := testManager(t, testConfig())
	invalid := []struct {
		name string
		key  string
		req  Request
	}{
		{"empty key", "", Request{Repository: "widget", Task: "task"}},
		{"oversized key", string(make([]byte, maxKeyBytes+1)), Request{Repository: "widget", Task: "task"}},
		{"blank repository", "key", Request{Repository: " \t", Task: "task"}},
		{"oversized repository", "key", Request{Repository: stringsOf('r', maxAliasBytes+1), Task: "task"}},
		{"blank task", "key", Request{Repository: "widget", Task: " \n "}},
		{"oversized task", "key", Request{Repository: "widget", Task: stringsOf('t', maxTaskBytes+1)}},
		{"invalid UTF-8", "key", Request{Repository: string([]byte{0xff}), Task: "task"}},
		{"control in alias", "key", Request{Repository: "wid\x00get", Task: "task"}},
		{"nonpositive issue", "key", Request{Repository: "widget", Task: "task", Issue: intPointer(0)}},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := manager.Admit(tc.key, tc.req); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("Admit() error = %v, want ErrInvalidInput", err)
			}
		})
	}
	job, _, err := manager.Admit("valid", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ eventType, message string }{
		{"", "message"}, {"bad\x00type", "message"}, {"type", stringsOf('m', maxEventBytes+1)}, {"type", string([]byte{0xff})},
	} {
		if err := manager.AddEvent(job.ID, tc.eventType, tc.message); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("AddEvent(%q, %q) error = %v, want ErrInvalidInput", tc.eventType, tc.message, err)
		}
	}
}

func intPointer(value int) *int { return &value }

func stringsOf(char byte, count int) string {
	return string(makeRepeated(char, count))
}

func makeRepeated(char byte, count int) []byte {
	value := make([]byte, count)
	for i := range value {
		value[i] = char
	}
	return value
}
