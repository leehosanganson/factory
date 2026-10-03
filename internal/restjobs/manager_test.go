package restjobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
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
		{"negative records", Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: -1, MaxEventsPerJob: 1}},
		{"negative history", Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: -1}},
		{"unbounded queue", Config{QueueCapacity: maxQueueLimit + 1, MaxConcurrentJobs: 1, MaxRecords: maxQueueLimit + 1, MaxEventsPerJob: 1}},
		{"unbounded records", Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: maxRecordsLimit + 1, MaxEventsPerJob: 1}},
		{"unbounded registry", Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 1, RegistryBytes: defaultRegistryBytes + 1}},
		{"negative registry", Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 1, RegistryBytes: -1}},
		{"unbounded history", Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: maxEventsLimit + 1}},
		{"zero task limit", Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 1, MaxTaskBytes: -1}},
		{"task limit exceeds maximum", Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 1, MaxTaskBytes: maxTaskBytes + 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewManager(tc.config); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("NewManager() error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestTaskByteLimitIsConfigurableAndDefaultsToMaximum(t *testing.T) {
	config := Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 2, MaxTaskBytes: 12, RegistryBytes: defaultRegistryBytes}
	manager := testManager(t, config)
	if _, _, err := manager.Admit("at-limit", Request{Repository: "widget", Task: stringsOf('t', 12)}); err != nil {
		t.Fatalf("Admit() at configured byte limit: %v", err)
	}
	if _, _, err := manager.Admit("over-limit", Request{Repository: "widget", Task: stringsOf('t', 13)}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Admit() over configured byte limit = %v, want ErrInvalidInput", err)
	}
	if _, _, err := manager.Admit("multibyte-over-limit", Request{Repository: "widget", Task: "ééééééé"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Admit() over configured byte limit with multibyte text = %v, want ErrInvalidInput", err)
	}

	defaulted := testManager(t, Config{QueueCapacity: 8, MaxConcurrentJobs: 2})
	if defaulted.config.MaxRecords != 1000 || defaulted.config.MaxEventsPerJob != 200 {
		t.Fatalf("default record/event limits mismatch REST defaults: %+v", defaulted.config)
	}
	if _, _, err := defaulted.Admit("default-limit", Request{Repository: "widget", Task: stringsOf('t', maxTaskBytes)}); err != nil {
		t.Fatalf("Admit() at default byte limit: %v", err)
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

func TestRegistryBudgetAcceptsExactAccountingBoundary(t *testing.T) {
	request := Request{Repository: "repo", Task: "boundary"}
	key := "boundary-key"
	budget := recordBytes(request, key) + eventBytes(Event{Type: "queued", Message: "Job admitted"})
	manager := testManager(t, Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 1, MaxTaskBytes: 128, RegistryBytes: int64(budget)})
	if _, _, err := manager.Admit(key, request); err != nil {
		t.Fatalf("admission at exact logical budget: %v", err)
	}
	if manager.registryBytes != budget {
		t.Fatalf("accounted bytes = %d, want exact budget %d", manager.registryBytes, budget)
	}
}

func TestRegistryBudgetRejectsWithoutStoringKeyAndAllowsRetry(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 2, MaxTaskBytes: 1000, RegistryBytes: 500})
	if _, _, err := manager.Admit("first", Request{Repository: "w", Task: "first"}); err != nil {
		t.Fatal(err)
	}
	before := manager.registryBytes
	if _, _, err := manager.Admit("retry-key", Request{Repository: "w", Task: stringsOf('t', 1100)}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("oversize admission error = %v, want ErrInvalidInput", err)
	}
	if _, _, err := manager.Admit("retry-key", Request{Repository: "w", Task: stringsOf('t', 400)}); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("budget rejection = %v, want ErrRegistryFull", err)
	}
	if _, err := manager.Get("retry-key"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("budget-rejected key was retained: %v", err)
	}
	if manager.registryBytes != before {
		t.Fatalf("budget rejection changed accounting: got %d, want %d", manager.registryBytes, before)
	}

	// Free space by evicting the oldest terminal record, then reuse the rejected key.
	firstID := manager.queue[0]
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(firstID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	accepted, replay, err := manager.Admit("retry-key", Request{Repository: "w", Task: "retry now"})
	if err != nil || replay || accepted.ID == firstID {
		t.Fatalf("retry after reclaiming budget = (%+v, %v, %v), want new job", accepted, replay, err)
	}
}

func TestVerificationEvidenceCountsAgainstRegistryByteBudget(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 4, MaxTaskBytes: 128, RegistryBytes: 1000})
	job, _, err := manager.Admit("evidence-key", Request{Repository: "repo", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	before := manager.registryBytes
	evidence := VerificationEvidence{
		Checks:      []VerificationCheck{{Name: "check-01", Outcome: VerificationPassed}},
		Limitations: []string{LimitationAgentNotVerdict},
	}
	if err := manager.RecordVerificationEvidence(job.ID, evidence); err != nil {
		t.Fatal(err)
	}
	if manager.registryBytes <= before {
		t.Fatalf("recording evidence did not count against registry budget: before=%d after=%d", before, manager.registryBytes)
	}
	got, err := manager.Get(job.ID)
	if err != nil || got.Verification == nil || got.Verification.Checks[0] != evidence.Checks[0] {
		t.Fatalf("stored evidence = %+v, %v", got.Verification, err)
	}
}

func TestRegistryBudgetAccountsForAdmissionLifecycleAndEviction(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 2, MaxTaskBytes: 128, RegistryBytes: 2000})
	first, _, err := manager.Admit("first-key", Request{Repository: "repo", Task: "first"})
	if err != nil {
		t.Fatal(err)
	}
	firstExpected := recordBytes(first.Request, "first-key") + eventBytes(Event{Type: "queued", Message: "Job admitted"})
	if manager.registryBytes != firstExpected {
		t.Fatalf("new admission charge = %d, want %d", manager.registryBytes, firstExpected)
	}
	beforeReplay := manager.registryBytes
	if _, replay, err := manager.Admit("first-key", first.Request); err != nil || !replay || manager.registryBytes != beforeReplay {
		t.Fatalf("replay altered accounting: replay=%t bytes=%d err=%v", replay, manager.registryBytes, err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	firstExpected += eventBytes(Event{Type: "running", Message: "Job started"})
	if manager.registryBytes != firstExpected {
		t.Fatalf("running transition charge = %d, want %d", manager.registryBytes, firstExpected)
	}
	if err := manager.Finish(first.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	firstExpected += eventBytes(Event{Type: "succeeded", Message: "Job finished"}) - eventBytes(Event{Type: "queued", Message: "Job admitted"})
	if manager.registryBytes != firstExpected {
		t.Fatalf("finish transition charge = %d, want %d", manager.registryBytes, firstExpected)
	}
	if err := manager.AddEvent(first.ID, "extra", "history payload"); err != nil {
		t.Fatal(err)
	}
	firstExpected += eventBytes(Event{Type: "extra", Message: "history payload"}) - eventBytes(Event{Type: "running", Message: "Job started"})
	if manager.registryBytes != firstExpected {
		t.Fatalf("truncation charge = %d, want %d", manager.registryBytes, firstExpected)
	}
	_, _, err = manager.Admit("second-key", Request{Repository: "repo", Task: "second"})
	if err != nil {
		t.Fatal(err)
	}
	beforeEvict := manager.registryBytes
	third, _, err := manager.Admit("third-key", Request{Repository: "repo", Task: "third"})
	if err != nil {
		t.Fatal(err)
	}
	thirdCharge := recordBytes(third.Request, "third-key") + eventBytes(Event{Type: "queued", Message: "Job admitted"})
	if manager.registryBytes != beforeEvict+thirdCharge-(recordBytes(first.Request, "first-key")+eventBytes(Event{Type: "succeeded", Message: "Job finished"})+eventBytes(Event{Type: "extra", Message: "history payload"})) {
		t.Fatalf("eviction charge incorrect: got %d", manager.registryBytes)
	}
	if _, err := manager.Get(first.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("terminal record not evicted: %v", err)
	}
	_, _, err = manager.Admit("first-key", Request{Repository: "repo", Task: "key reusable"})
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("reusing evicted idempotency key before queue capacity: %v", err)
	}
	firstQueuedID := manager.queue[0]
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(firstQueuedID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	thirdID := manager.queue[0]
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(thirdID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.Admit("first-key", Request{Repository: "repo", Task: "key reusable"}); err != nil {
		t.Fatalf("evicted idempotency key was not reusable after capacity freed: %v", err)
	}
}

func TestCloseMarksTruncatedWhenCancellationEventCannotFit(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 4, MaxTaskBytes: 128, RegistryBytes: 360})
	job, _, err := manager.Admit("close-key", Request{Repository: "repo", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	manager.Close()
	closed, err := manager.Get(job.ID)
	if err != nil || closed.Status != StatusCanceled {
		t.Fatalf("job state after close = (%+v, %v), want canceled", closed, err)
	}
	history, err := manager.History(job.ID)
	if err != nil || !history.Truncated {
		t.Fatalf("close did not flag omitted cancellation event: (%+v, %v)", history, err)
	}
}

func TestAddEventRejectedWhenNoHistoryBytesCanBeReclaimed(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 2, MaxTaskBytes: 128, RegistryBytes: 650})
	job, _, err := manager.Admit("key", Request{Repository: "repo", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(job.ID, "large", stringsOf('x', 100)); err != nil {
		t.Fatal(err)
	}
	before, _ := manager.History(job.ID)
	beforeBytes := manager.registryBytes
	if err := manager.AddEvent(job.ID, "larger", stringsOf('y', 300)); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("non-fitting event = %v, want ErrRegistryFull", err)
	}
	after, _ := manager.History(job.ID)
	if manager.registryBytes != beforeBytes || len(after.Events) != len(before.Events) || after.Events[0].Message != before.Events[0].Message || after.Truncated != before.Truncated {
		t.Fatalf("failed event altered existing history/accounting: before=%+v after=%+v bytes=%d/%d", before, after, beforeBytes, manager.registryBytes)
	}
}

func TestHistoryAppendEvictsOlderTerminalRecordBeforeItsOwnHistory(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 4, MaxTaskBytes: 128, RegistryBytes: 650})
	terminal, _, err := manager.Admit("terminal", Request{Repository: "repo", Task: "old"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(terminal.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	active, _, err := manager.Admit("active", Request{Repository: "repo", Task: "new"})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(active.ID, "detail", stringsOf('x', 250)); err != nil {
		t.Fatalf("history append should evict older terminal record: %v", err)
	}
	if _, err := manager.Get(terminal.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("older terminal record was not evicted: %v", err)
	}
	if manager.registryBytes > uint64(manager.config.RegistryBytes) {
		t.Fatalf("registry accounting exceeded budget: %d", manager.registryBytes)
	}
}

func TestRegistryBudgetReclaimsOldestHistoryAcrossRecords(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 4, MaxTaskBytes: 128, RegistryBytes: 850})
	first, _, err := manager.Admit("first", Request{Repository: "repo", Task: "one"})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := manager.Admit("second", Request{Repository: "repo", Task: "two"})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(first.ID, "detail", stringsOf('x', 100)); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddEvent(second.ID, "detail", stringsOf('y', 370)); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("event exceeding aggregate budget = %v, want ErrRegistryFull", err)
	}
	if manager.registryBytes > uint64(manager.config.RegistryBytes) {
		t.Fatalf("registry charge exceeded budget: %d > %d", manager.registryBytes, manager.config.RegistryBytes)
	}
	firstHistory, err := manager.History(first.ID)
	if err != nil || !firstHistory.Truncated || len(firstHistory.Events) != 1 || firstHistory.Events[0].Type != "detail" {
		t.Fatalf("oldest history was not reclaimed/truncated: (%+v, %v)", firstHistory, err)
	}
	secondHistory, err := manager.History(second.ID)
	if err != nil || len(secondHistory.Events) != 1 || secondHistory.Truncated || secondHistory.Events[0].Type != "queued" {
		t.Fatalf("rejected event was partially retained: (%+v, %v)", secondHistory, err)
	}
}

func TestLifecycleTransitionsTruncateHistoryWhenEventCannotFit(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 4, MaxTaskBytes: 128, RegistryBytes: 349})
	job, _, err := manager.Admit("key", Request{Repository: "repo", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatalf("claim using reclaimed queued event: %v", err)
	}
	if err := manager.Finish(job.ID, StatusSucceeded); err != nil {
		t.Fatalf("finish after bounded event truncation: %v", err)
	}
	finished, err := manager.Get(job.ID)
	if err != nil || finished.Status != StatusSucceeded || manager.running != 0 {
		t.Fatalf("finished state = (%+v, %v), running=%d", finished, err, manager.running)
	}
	history, err := manager.History(job.ID)
	if err != nil || !history.Truncated {
		t.Fatalf("lifecycle history was not marked truncated: (%+v, %v)", history, err)
	}
}

func TestRegistryBudgetRejectsEventsWithoutCorruptingAccounting(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 3, MaxTaskBytes: 128, RegistryBytes: 500})
	job, _, err := manager.Admit("key", Request{Repository: "repo", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	before := manager.registryBytes
	if err := manager.AddEvent(job.ID, "large", stringsOf('x', 300)); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("large event = %v, want ErrRegistryFull", err)
	}
	if manager.registryBytes != before {
		t.Fatalf("rejected event changed logical accounting: got %d want %d", manager.registryBytes, before)
	}
	if err := manager.Finish(job.ID, StatusSucceeded); err != nil {
		t.Fatalf("finish within remaining budget: %v", err)
	}
	if manager.registryBytes > uint64(manager.config.RegistryBytes) || manager.registryBytes == 0 || manager.registryBytes == before {
		t.Fatalf("lifecycle transition did not update bounded accounting: got %d before %d", manager.registryBytes, before)
	}
	finished, err := manager.Get(job.ID)
	if err != nil || finished.Status != StatusSucceeded || manager.running != 0 {
		t.Fatalf("finish state = (%+v, %v), running=%d", finished, err, manager.running)
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

func TestWaitClaimWakesAfterAdmission(t *testing.T) {
	manager := testManager(t, testConfig())
	result := make(chan struct {
		snapshot Snapshot
		err      error
	}, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		snapshot, err := manager.WaitClaim(context.Background())
		result <- struct {
			snapshot Snapshot
			err      error
		}{snapshot, err}
	}()
	<-started
	select {
	case got := <-result:
		t.Fatalf("WaitClaim returned before admission: (%+v, %v)", got.snapshot, got.err)
	case <-time.After(20 * time.Millisecond):
	}
	admitted, _, err := manager.Admit("wait", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.err != nil || got.snapshot.ID != admitted.ID || got.snapshot.Status != StatusRunning {
			t.Fatalf("WaitClaim() = (%+v, %v), want admitted running job", got.snapshot, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitClaim did not wake after admission")
	}
}

func TestWaitClaimWakesWhenFinishReleasesWorkerSlot(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 4})
	first, _, err := manager.Admit("first", Request{Repository: "widget", Task: "first"})
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
	result := make(chan struct {
		snapshot Snapshot
		err      error
	}, 1)
	go func() {
		snapshot, err := manager.WaitClaim(context.Background())
		result <- struct {
			snapshot Snapshot
			err      error
		}{snapshot, err}
	}()
	select {
	case got := <-result:
		t.Fatalf("WaitClaim bypassed full worker capacity: (%+v, %v)", got.snapshot, got.err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := manager.Finish(first.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.err != nil || got.snapshot.ID != queued.ID || got.snapshot.Status != StatusRunning {
			t.Fatalf("WaitClaim() after slot release = (%+v, %v), want queued running job", got.snapshot, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitClaim did not wake after Finish released a worker slot")
	}
}

func TestWaitClaimReturnsOnContextCancellation(t *testing.T) {
	manager := testManager(t, testConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		_, err := manager.WaitClaim(ctx)
		result <- err
	}()
	<-started
	select {
	case err := <-result:
		t.Fatalf("WaitClaim returned before cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitClaim() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitClaim did not return after context cancellation")
	}
}

func TestWaitClaimWakesOnClose(t *testing.T) {
	manager := testManager(t, testConfig())
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		_, err := manager.WaitClaim(context.Background())
		result <- err
	}()
	<-started
	select {
	case err := <-result:
		t.Fatalf("WaitClaim returned before close: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	manager.Close()
	select {
	case err := <-result:
		if !errors.Is(err, ErrManagerClosed) {
			t.Fatalf("WaitClaim() error = %v, want ErrManagerClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitClaim did not return after manager close")
	}
}

func TestCloseCancelsQueuedJobsButLeavesRunningForFinish(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 3, MaxConcurrentJobs: 1, MaxRecords: 3, MaxEventsPerJob: 4})
	active, _, err := manager.Admit("active", Request{Repository: "widget", Task: "active"})
	if err != nil {
		t.Fatal(err)
	}
	active, err = manager.ClaimNext()
	if err != nil {
		t.Fatal(err)
	}
	queued, _, err := manager.Admit("queued", Request{Repository: "widget", Task: "queued"})
	if err != nil {
		t.Fatal(err)
	}
	manager.Close()
	manager.Close()

	gotQueued, err := manager.Get(queued.ID)
	if err != nil || gotQueued.Status != StatusCanceled {
		t.Fatalf("queued job after Close() = (%+v, %v), want canceled", gotQueued, err)
	}
	history, err := manager.History(queued.ID)
	if err != nil || len(history.Events) != 2 || history.Events[1].Type != string(StatusCanceled) {
		t.Fatalf("queued job history after Close() = (%+v, %v), want canceled event", history, err)
	}
	gotActive, err := manager.Get(active.ID)
	if err != nil || gotActive.Status != StatusRunning || manager.running != 1 {
		t.Fatalf("active job after Close() = (%+v, %v), running=%d; want running count 1", gotActive, err, manager.running)
	}
	if _, err := manager.ClaimNext(); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("ClaimNext() after Close() = %v, want ErrManagerClosed", err)
	}
	if _, _, err := manager.Admit("after-close", Request{Repository: "widget", Task: "task"}); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("Admit() after Close() = %v, want ErrManagerClosed", err)
	}
	if err := manager.Finish(active.ID, StatusSucceeded); err != nil {
		t.Fatalf("Finish(active) after Close(): %v", err)
	}
	if manager.running != 0 {
		t.Fatalf("running count after Finish() = %d, want 0", manager.running)
	}
	finished, err := manager.Get(active.ID)
	if err != nil || finished.Status != StatusSucceeded {
		t.Fatalf("active job after Finish() = (%+v, %v), want succeeded", finished, err)
	}
}

func TestAdmissionRacingCloseHasDeterministicOutcome(t *testing.T) {
	const attempts = 64
	for i := 0; i < attempts; i++ {
		manager := testManager(t, testConfig())
		start := make(chan struct{})
		type admissionResult struct {
			snapshot Snapshot
			err      error
		}
		result := make(chan admissionResult, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			snapshot, _, err := manager.Admit("race", Request{Repository: "widget", Task: "task"})
			result <- admissionResult{snapshot: snapshot, err: err}
		}()
		go func() {
			defer wg.Done()
			<-start
			manager.Close()
		}()
		close(start)
		wg.Wait()
		close(result)
		outcome := <-result
		if outcome.err != nil && !errors.Is(outcome.err, ErrManagerClosed) {
			t.Fatalf("racing Admit() error = %v, want nil or ErrManagerClosed", outcome.err)
		}
		if outcome.err == nil {
			got, err := manager.Get(outcome.snapshot.ID)
			if err != nil || got.Status != StatusCanceled {
				t.Fatalf("admitted job after racing Close() = (%+v, %v), want canceled", got, err)
			}
		} else if len(manager.jobs) != 0 {
			t.Fatalf("closed manager retained %d job(s) after rejected admission", len(manager.jobs))
		}
		if len(manager.queue) != 0 {
			t.Fatalf("queue after racing Close() = %d, want empty", len(manager.queue))
		}
		if _, _, err := manager.Admit("late", Request{Repository: "widget", Task: "late"}); !errors.Is(err, ErrManagerClosed) {
			t.Fatalf("late Admit() error = %v, want ErrManagerClosed", err)
		}
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
