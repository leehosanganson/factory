package restjobs

import (
	"errors"
	"fmt"
	"regexp"
	"testing"
)

var threadIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestLocalThreadCompletionIsExplicitIdempotentAndDoesNotChangeJobs(t *testing.T) {
	manager := testManager(t, testConfig())
	created, _, err := manager.AdmitThreadAs("thread", "alice", Request{Repository: "widget", Task: "work"})
	if err != nil {
		t.Fatal(err)
	}
	before := created.Jobs[0]

	completed, err := manager.CompleteThreadAs(created.Thread.ID, "alice")
	if err != nil {
		t.Fatalf("complete open Thread: %v", err)
	}
	if completed.Thread.CompletedAt == nil || completed.Thread.CompletedBy != "alice" || completed.Thread.Status != ThreadStatusInProgress {
		t.Fatalf("completed Thread=%+v, want explicit completion independent of derived in_progress status", completed.Thread)
	}
	if len(completed.Jobs) != 1 || completed.Jobs[0].ID != before.ID || completed.Jobs[0].Status != before.Status || completed.Jobs[0].UpdatedAt != before.UpdatedAt {
		t.Fatalf("completion mutated member Job: before=%+v after=%+v", before, completed.Jobs)
	}

	replayed, err := manager.CompleteThreadAs(created.Thread.ID, "alice")
	if err != nil || replayed.Thread.CompletedAt == nil || !replayed.Thread.CompletedAt.Equal(*completed.Thread.CompletedAt) {
		t.Fatalf("repeated completion=(%+v,%v), want same completion timestamp", replayed.Thread, err)
	}
	if _, _, _, err := manager.AdmitThreadJobAs(created.Thread.ID, "after-close", "alice", Request{Repository: "widget", Task: "too late"}); !errors.Is(err, ErrThreadClosed) {
		t.Fatalf("append after completion error=%v, want Thread closed", err)
	}
	if _, err := manager.Get(before.ID); err != nil {
		t.Fatalf("original Job no longer inspectable: %v", err)
	}
	if _, err := manager.CompleteThreadAs(created.Thread.ID, "bob"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign completion error=%v, want non-disclosing not found", err)
	}
}

func TestLocalThreadCompletionSerializesAgainstMemberAdmission(t *testing.T) {
	for _, closeFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "append-first", true: "close-first"}[closeFirst], func(t *testing.T) {
			manager := testManager(t, testConfig())
			created, _, err := manager.AdmitThreadAs("thread", "alice", Request{Repository: "widget", Task: "work"})
			if err != nil {
				t.Fatal(err)
			}
			if closeFirst {
				if _, err := manager.CompleteThreadAs(created.Thread.ID, "alice"); err != nil {
					t.Fatal(err)
				}
				if _, _, _, err := manager.AdmitThreadJobAs(created.Thread.ID, "member", "alice", Request{Repository: "widget", Task: "follow-up"}); !errors.Is(err, ErrThreadClosed) {
					t.Fatalf("append after close error=%v, want Thread closed", err)
				}
				return
			}
			if _, _, _, err := manager.AdmitThreadJobAs(created.Thread.ID, "member", "alice", Request{Repository: "widget", Task: "follow-up"}); err != nil {
				t.Fatal(err)
			}
			completed, err := manager.CompleteThreadAs(created.Thread.ID, "alice")
			if err != nil || completed.Thread.CompletedAt == nil || len(completed.Jobs) != 2 {
				t.Fatalf("append-first completion=(%+v,%v), want closed Thread with both Jobs", completed, err)
			}
		})
	}
}

func TestLocalThreadCompletionRaceWithMemberAdmission(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		manager := testManager(t, testConfig())
		created, _, err := manager.AdmitThreadAs(fmt.Sprintf("thread-%d", iteration), "alice", Request{Repository: "widget", Task: "work"})
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		type outcome struct {
			completed ThreadSnapshot
			added     Snapshot
			err       error
		}
		completeResult := make(chan outcome, 1)
		addResult := make(chan outcome, 1)
		go func() {
			<-start
			snapshot, err := manager.CompleteThreadAs(created.Thread.ID, "alice")
			completeResult <- outcome{completed: snapshot, err: err}
		}()
		go func() {
			<-start
			_, added, _, err := manager.AdmitThreadJobAs(created.Thread.ID, "member", "alice", Request{Repository: "widget", Task: "follow-up"})
			addResult <- outcome{added: added, err: err}
		}()
		close(start)
		completed, added := <-completeResult, <-addResult
		if completed.err != nil || completed.completed.Thread.CompletedAt == nil {
			t.Fatalf("iteration %d completion=(%+v,%v), want completion success", iteration, completed.completed.Thread, completed.err)
		}
		if added.err != nil && !errors.Is(added.err, ErrThreadClosed) {
			t.Fatalf("iteration %d append error=%v, want success or closed", iteration, added.err)
		}
		if added.err == nil {
			final, err := manager.GetThread(created.Thread.ID)
			if err != nil || len(final.Jobs) != 2 || final.Thread.CompletedAt == nil {
				t.Fatalf("iteration %d append-first state=(%+v,%v), want closed with both jobs", iteration, final, err)
			}
		} else {
			final, err := manager.GetThread(created.Thread.ID)
			if err != nil || len(final.Jobs) != 1 || final.Thread.CompletedAt == nil {
				t.Fatalf("iteration %d close-first state=(%+v,%v), want closed with initial job only", iteration, final, err)
			}
		}
	}
}

func TestAdmitThreadJobAsAddsOrderedMembersAndReplaysWithinThread(t *testing.T) {
	manager := testManager(t, testConfig())
	created, _, err := manager.AdmitThreadAs("thread-create", "alice", Request{Repository: "widget", Task: "initial"})
	if err != nil {
		t.Fatal(err)
	}
	updated, job, replay, err := manager.AdmitThreadJobAs(created.Thread.ID, "member-key", "alice", Request{Repository: "widget", Task: "follow up"})
	if err != nil || replay {
		t.Fatalf("add member = (%+v, %+v, %t, %v), want new member", updated, job, replay, err)
	}
	if len(updated.Jobs) != 2 || len(updated.Thread.JobIDs) != 2 || updated.Thread.JobIDs[0] != created.Jobs[0].ID || updated.Thread.JobIDs[1] != job.ID || updated.Thread.Status != ThreadStatusInProgress {
		t.Fatalf("added Thread = %+v, want stable initial/follow-up membership", updated)
	}
	replayed, replayedJob, replay, err := manager.AdmitThreadJobAs(created.Thread.ID, " member-key ", "alice", Request{Repository: "widget", Task: "follow up"})
	if err != nil || !replay || replayedJob.ID != job.ID || len(replayed.Jobs) != 2 {
		t.Fatalf("member retry = (%+v, %+v, %t, %v), want same Job and no duplicate member", replayed, replayedJob, replay, err)
	}
	if _, _, _, err := manager.AdmitThreadJobAs(created.Thread.ID, "member-key", "alice", Request{Repository: "widget", Task: "changed"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed member retry error=%v, want conflict", err)
	}
	if _, _, _, err := manager.AdmitThreadJobAs(created.Thread.ID, "foreign-key", "bob", Request{Repository: "widget", Task: "foreign"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign member add error=%v, want non-disclosing not found", err)
	}
}

func TestAdmitThreadAsReplaysWithinPrincipalAndKeepsJobKeyNamespaceSeparate(t *testing.T) {
	manager := testManager(t, testConfig())
	request := Request{Repository: " widget ", Task: "  Fix the queue.  "}

	first, replay, err := manager.AdmitThreadAs("same-key", "alice", request)
	if err != nil || replay {
		t.Fatalf("first AdmitThreadAs() = (%+v, %t, %v), want new Thread", first, replay, err)
	}
	if !threadIDPattern.MatchString(first.Thread.ID) || first.Thread.Submitter != "alice" {
		t.Fatalf("Thread identity/owner = (%q, %q), want UUID and alice", first.Thread.ID, first.Thread.Submitter)
	}
	if first.Thread.CreatedAt.IsZero() || !first.Thread.CreatedAt.Equal(first.Thread.UpdatedAt) {
		t.Fatalf("new Thread timestamps = (%v, %v), want equal non-zero values", first.Thread.CreatedAt, first.Thread.UpdatedAt)
	}
	if len(first.Thread.JobIDs) != 1 || len(first.Jobs) != 1 || first.Thread.JobIDs[0] != first.Jobs[0].ID {
		t.Fatalf("initial membership is not one stable ordered Job: Thread=%+v Jobs=%+v", first.Thread, first.Jobs)
	}
	if first.Jobs[0].Request.Repository != "widget" || first.Jobs[0].Request.Task != "Fix the queue." || first.Thread.Status != ThreadStatusInProgress {
		t.Fatalf("initial Thread snapshot = %+v, want normalized queued Job and in_progress", first)
	}

	second, replay, err := manager.AdmitThreadAs(" same-key ", "alice", Request{Repository: "widget", Task: "Fix the queue."})
	if err != nil || !replay || second.Thread.ID != first.Thread.ID || second.Jobs[0].ID != first.Jobs[0].ID {
		t.Fatalf("normalized replay = (%+v, %t, %v), want same Thread/Job", second, replay, err)
	}
	otherPrincipal, replay, err := manager.AdmitThreadAs("same-key", "bob", request)
	if err != nil || replay || otherPrincipal.Thread.ID == first.Thread.ID || otherPrincipal.Jobs[0].ID == first.Jobs[0].ID {
		t.Fatalf("different principal admission = (%+v, %t, %v); want distinct Thread and Job", otherPrincipal, replay, err)
	}
	otherKey, replay, err := manager.AdmitThreadAs("another-key", "alice", request)
	if err != nil || replay || otherKey.Thread.ID == first.Thread.ID || otherKey.Jobs[0].ID == first.Jobs[0].ID {
		t.Fatalf("distinct key admission = (%+v, %t, %v); want distinct Thread and Job", otherKey, replay, err)
	}
	if _, replay, err := manager.Admit("same-key", request); err != nil || replay {
		t.Fatalf("standalone Job key namespace collided with Thread: replay %t, err %v", replay, err)
	}
	if _, _, err := manager.AdmitThreadAs("same-key", "alice", Request{Repository: "widget", Task: "different"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed Thread payload error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestThreadMemberAdmissionProtectsTargetInitialJobFromRetentionEviction(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 8})
	created, _, err := manager.AdmitThreadAs("thread", "alice", Request{Repository: "widget", Task: "initial"})
	if err != nil {
		t.Fatal(err)
	}
	finishThreadJobs(t, manager, created.Jobs[0].ID)
	other, _, err := manager.Admit("other", Request{Repository: "widget", Task: "retention candidate"})
	if err != nil {
		t.Fatal(err)
	}
	finishThreadJobs(t, manager, other.ID)

	updated, member, replay, err := manager.AdmitThreadJobAs(created.Thread.ID, "member", "alice", Request{Repository: "widget", Task: "follow-up"})
	if err != nil || replay {
		t.Fatalf("member admission=(%+v,%+v,%t,%v)", updated, member, replay, err)
	}
	if len(updated.Jobs) != 2 || updated.Jobs[0].ID != created.Jobs[0].ID || updated.Jobs[1].ID != member.ID {
		t.Fatalf("member admission lost target Thread membership: %+v", updated)
	}
	if _, err := manager.Get(other.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("oldest unrelated terminal record error=%v, want it evicted to retain target Thread", err)
	}
}

func TestThreadCreationRetryAfterInitialMemberEvictionCreatesFreshThread(t *testing.T) {
	config := Config{QueueCapacity: 3, MaxConcurrentJobs: 1, MaxRecords: 3, RegistryBytes: 64 * 1024, MaxEventsPerJob: 8}
	manager := testManager(t, config)
	created, replay, err := manager.AdmitThreadAs("create", "alice", Request{Repository: "widget", Task: "initial"})
	if err != nil || replay {
		t.Fatalf("create Thread=(%+v,%t,%v)", created, replay, err)
	}
	finish := func() string {
		t.Helper()
		claimed, err := manager.ClaimNext()
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Finish(claimed.ID, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
		return claimed.ID
	}
	if got := finish(); got != created.Jobs[0].ID {
		t.Fatalf("finished initial Job=%s, want %s", got, created.Jobs[0].ID)
	}
	if _, _, _, err := manager.AdmitThreadJobAs(created.Thread.ID, "member", "alice", Request{Repository: "widget", Task: "follow-up"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		_, _, err := manager.AdmitAs(fmt.Sprintf("fill-%d", i), "alice", Request{Repository: "widget", Task: "fill"})
		if err != nil {
			t.Fatal(err)
		}
		finish()
	}
	if _, err := manager.GetThread(created.Thread.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Thread survived initial Job eviction: %v", err)
	}
	if _, _, _, err := manager.AdmitThreadJobAs(created.Thread.ID, "new-member", "alice", Request{Repository: "widget", Task: "orphan"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("member add to evicted Thread returned %v, want not found", err)
	}
	refreshed, replay, err := manager.AdmitThreadAs("create", "alice", Request{Repository: "widget", Task: "initial"})
	if err != nil || replay || refreshed.Thread.ID == created.Thread.ID {
		t.Fatalf("retry after initial Job eviction=(%+v,%t,%v), want a fresh Thread", refreshed, replay, err)
	}
}

func TestThreadStatusTracksJobLifecycleAndOwnership(t *testing.T) {
	manager := testManager(t, testConfig())
	created, _, err := manager.AdmitThreadAs("thread-status", "alice", Request{Repository: "widget", Task: "inspect status"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.GetThreadForPrincipal(created.Thread.ID, "bob"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign principal read error = %v, want ErrNotFound", err)
	}
	if _, err := manager.GetThread("missing-thread"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing Thread read error = %v, want ErrNotFound", err)
	}

	claimed, err := manager.ClaimNext()
	if err != nil {
		t.Fatal(err)
	}
	running, err := manager.GetThreadForPrincipal(created.Thread.ID, " alice ")
	if err != nil || running.Thread.Status != ThreadStatusInProgress || !running.Thread.UpdatedAt.After(created.Thread.UpdatedAt) {
		t.Fatalf("running Thread = (%+v, %v), want updated in_progress", running, err)
	}
	if err := manager.Finish(claimed.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	done, err := manager.GetThread(created.Thread.ID)
	if err != nil || done.Thread.Status != ThreadStatusDone || done.Jobs[0].Status != StatusFailed {
		t.Fatalf("terminal failed Thread = (%+v, %v), want done", done, err)
	}
}

func TestThreadStatusTreatsAllTerminalJobOutcomesAsDone(t *testing.T) {
	for _, terminal := range []Status{StatusSucceeded, StatusFailed, StatusCanceled} {
		t.Run(string(terminal), func(t *testing.T) {
			manager := testManager(t, testConfig())
			created, _, err := manager.AdmitThreadAs("thread-"+string(terminal), "alice", Request{Repository: "widget", Task: "finish"})
			if err != nil {
				t.Fatal(err)
			}
			job, err := manager.ClaimNext()
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.Finish(job.ID, terminal); err != nil {
				t.Fatal(err)
			}
			got, err := manager.GetThread(created.Thread.ID)
			if err != nil || got.Thread.Status != ThreadStatusDone {
				t.Fatalf("terminal %s Thread = (%+v, %v), want done", terminal, got, err)
			}
		})
	}
}

func TestAdmitThreadRejectsIssueIntakeAndCapacityFailureLeavesKeyReusable(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 4})
	issue := 7
	for name, request := range map[string]Request{
		"issue":      {Repository: "widget", Task: "task", Issue: &issue},
		"provenance": provenanceRequest("task"),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := manager.AdmitThreadAs("invalid-"+name, "alice", request); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("direct Thread request with issue metadata error = %v, want ErrInvalidInput", err)
			}
		})
	}

	if _, _, err := manager.Admit("fill-queue", Request{Repository: "widget", Task: "fill"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.AdmitThreadAs("retry-thread-key", "alice", Request{Repository: "widget", Task: "thread"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue Thread admission error = %v, want ErrQueueFull", err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	accepted, replay, err := manager.AdmitThreadAs("retry-thread-key", "alice", Request{Repository: "widget", Task: "thread"})
	if err != nil || replay {
		t.Fatalf("retry after capacity frees = (%+v, %t, %v), want fresh admission", accepted, replay, err)
	}
	if _, err := manager.GetThread(accepted.Thread.ID); err != nil {
		t.Fatalf("successful retry has no retained Thread: %v", err)
	}
}

func TestThreadRegistryCapacityFailureDoesNotReservePartialAdmission(t *testing.T) {
	request := Request{Repository: "widget", Task: "thread"}
	key := "registry-retry"
	budget := recordBytes(request, key, "alice") + eventBytes(Event{Type: "queued", Message: "Job admitted"}) - 1
	manager := testManager(t, Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 1, RegistryBytes: int64(budget)})
	if _, _, err := manager.AdmitThreadAs(key, "alice", request); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("over-budget Thread admission error = %v, want ErrRegistryFull", err)
	}
	if len(manager.jobs) != 0 || len(manager.threads) != 0 || len(manager.threadIdempotency) != 0 || len(manager.queue) != 0 {
		t.Fatalf("failed admission left partial state: jobs=%d threads=%d keys=%d queued=%d", len(manager.jobs), len(manager.threads), len(manager.threadIdempotency), len(manager.queue))
	}
	// The failed Thread admission must not leave a retry mapping; even if the
	// registry policy still rejects storage, the error must be capacity-related.
	if _, _, err := manager.Admit(key, request); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("failed Thread admission reserved standalone Job key: error = %v, want ErrRegistryFull", err)
	}
}

func TestThreadAdmissionDoesNotEvictTerminalJobsWhenItCannotFit(t *testing.T) {
	config := Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 4}
	request := Request{Repository: "widget", Task: "new"}
	key := "new-thread"
	budget := recordBytes(request, key, "alice") + threadRecordOverheadBytes + eventBytes(Event{Type: "queued", Message: "Job admitted"})
	manager := testManager(t, config)
	terminal, _, err := manager.Admit("terminal", Request{Repository: "widget", Task: "old"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(terminal.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	// Leave less than the Thread+Job pair size and a terminal Job to protect.
	manager.config.RegistryBytes = int64(budget - 1)
	if _, _, err := manager.AdmitThreadAs(key, "alice", request); !errors.Is(err, ErrRegistryFull) {
		t.Fatalf("Thread admission error = %v, want ErrRegistryFull", err)
	}
	if got, err := manager.Get(terminal.ID); err != nil || got.Status != StatusSucceeded {
		t.Fatalf("failed Thread admission evicted existing terminal Job: (%+v, %v)", got, err)
	}
}

func TestThreadIsEvictedWithItsTerminalInitialJob(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 1, MaxEventsPerJob: 4})
	created, _, err := manager.AdmitThreadAs("evicted-thread-key", "alice", Request{Repository: "widget", Task: "old"})
	if err != nil {
		t.Fatal(err)
	}
	job, err := manager.ClaimNext()
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(job.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	replacement, _, err := manager.Admit("replacement-job", Request{Repository: "widget", Task: "new"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Finish(replacement.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.GetThread(created.Thread.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Thread outlived evicted Job: GetThread() error = %v, want ErrNotFound", err)
	}
	if _, replay, err := manager.AdmitThreadAs("evicted-thread-key", "alice", Request{Repository: "widget", Task: "new thread"}); err != nil || replay {
		t.Fatalf("evicted Thread key remained reserved: replay=%t err=%v", replay, err)
	}
}

func TestConcurrentThreadAdmissionCreatesOneThreadAndJob(t *testing.T) {
	manager := testManager(t, Config{QueueCapacity: 16, MaxConcurrentJobs: 2, MaxRecords: 16, MaxEventsPerJob: 4})
	const callers = 16
	start := make(chan struct{})
	type result struct {
		snapshot ThreadSnapshot
		replay   bool
		err      error
	}
	results := make(chan result, callers)
	for range callers {
		go func() {
			<-start
			snapshot, replay, err := manager.AdmitThreadAs("one-thread-key", "alice", Request{Repository: "widget", Task: "same task"})
			results <- result{snapshot: snapshot, replay: replay, err: err}
		}()
	}
	close(start)
	var first ThreadSnapshot
	newCount := 0
	for range callers {
		got := <-results
		if got.err != nil {
			t.Errorf("concurrent Thread admission: %v", got.err)
			continue
		}
		if first.Thread.ID == "" {
			first = got.snapshot
		}
		if got.snapshot.Thread.ID != first.Thread.ID || got.snapshot.Jobs[0].ID != first.Jobs[0].ID {
			t.Errorf("concurrent admission returned different pairs: first=(%s,%s) got=(%s,%s)", first.Thread.ID, first.Jobs[0].ID, got.snapshot.Thread.ID, got.snapshot.Jobs[0].ID)
		}
		if !got.replay {
			newCount++
		}
	}
	if first.Thread.ID == "" || newCount != 1 || len(manager.jobs) != 1 {
		t.Fatalf("first Thread=%q new admissions=%d retained Jobs=%d, want one pair and one new admission", first.Thread.ID, newCount, len(manager.jobs))
	}
}

func TestDeriveThreadStatusUsesRecoveryClassificationAndUnknownStatePrecedence(t *testing.T) {
	jobs := []Snapshot{{ID: "live", Status: StatusRunning}, {ID: "done", Status: StatusFailed}}
	if got := DeriveThreadStatus(jobs, func(id string) bool { return id == "live" }); got != ThreadStatusNeedAttention {
		t.Fatalf("recovery-needed status=%q, want need_attention", got)
	}
	if got := DeriveThreadStatus(jobs, func(string) bool { return false }); got != ThreadStatusInProgress {
		t.Fatalf("live-running status=%q, want in_progress", got)
	}
	unknown := []Snapshot{{ID: "unknown", Status: Status("future-status")}}
	if got := DeriveThreadStatus(unknown, nil); got != ThreadStatusNeedAttention {
		t.Fatalf("unknown lifecycle status=%q, want need_attention", got)
	}
	if got := DeriveThreadStatus(nil, nil); got != ThreadStatusNeedAttention {
		t.Fatalf("empty member set status=%q, want need_attention", got)
	}
}

func TestThreadSnapshotCopiesMembershipAndDerivesStatus(t *testing.T) {
	manager := testManager(t, testConfig())
	created, _, err := manager.AdmitThreadAs("copy-thread", "alice", Request{Repository: "widget", Task: "copy"})
	if err != nil {
		t.Fatal(err)
	}
	created.Thread.JobIDs[0] = "mutated"
	created.Jobs[0].Request.Task = "mutated"
	got, err := manager.GetThread(created.Thread.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Thread.JobIDs) != 1 || got.Thread.JobIDs[0] != got.Jobs[0].ID || got.Jobs[0].Request.Task != "copy" || got.Thread.Status != ThreadStatusInProgress {
		t.Fatalf("caller mutation changed retained Thread state: %+v", got)
	}
}
