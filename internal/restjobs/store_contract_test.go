package restjobs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLocalManagerSatisfiesStoreContract(t *testing.T) {
	runStoreContract(t, func(t *testing.T) Store {
		t.Helper()
		store, err := NewManager(testConfig())
		if err != nil {
			t.Fatal(err)
		}
		return store
	})
}

func forceAttemptLeaseExpired(t *testing.T, store Store, id string) {
	t.Helper()
	past := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	switch typed := store.(type) {
	case *LocalManager:
		typed.mu.Lock()
		typed.jobs[id].leaseUntil = time.Now().UTC().Add(-time.Minute)
		typed.mu.Unlock()
	case *SQLiteStore:
		if _, err := typed.db.Exec(`UPDATE factory_worker_ownership SET lease_until=? WHERE job_id=?`, past, id); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported test store %T", store)
	}
}

func setListTestCreatedAt(t *testing.T, store Store, ids ...string) {
	t.Helper()
	const tied = "2026-10-10T12:00:00Z"
	switch typed := store.(type) {
	case *LocalManager:
		typed.mu.Lock()
		for _, id := range ids {
			entry := typed.jobs[id]
			entry.snapshot.CreatedAt = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
		}
		typed.mu.Unlock()
	case *SQLiteStore:
		for _, id := range ids {
			if _, err := typed.db.Exec(`UPDATE factory_jobs SET created_at=? WHERE id=?`, tied, id); err != nil {
				t.Fatal(err)
			}
		}
	default:
		t.Fatalf("unsupported test store %T", store)
	}
}

func runStoreContract(t *testing.T, newStore func(*testing.T) Store) {
	t.Helper()
	t.Run("expired attempt leases block renewal and finishing without reclaiming work", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("expired-attempt", Request{Repository: "widget", Task: "expired worker cannot revive lease"})
		if err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimNextAttempt()
		if err != nil {
			t.Fatal(err)
		}
		forceAttemptLeaseExpired(t, store, job.ID)
		if err := store.HeartbeatAttempt(claim.Attempt); !errors.Is(err, ErrStaleAttempt) {
			t.Fatalf("expired heartbeat = %v, want ErrStaleAttempt", err)
		}
		if err := store.FinishAttempt(claim.Attempt, StatusFailed); !errors.Is(err, ErrStaleAttempt) {
			t.Fatalf("expired finish = %v, want ErrStaleAttempt", err)
		}
		got, err := store.Get(job.ID)
		if err != nil || got.Status != StatusRunning {
			t.Fatalf("job after expired attempt = (%+v, %v), want running", got, err)
		}
		if _, err := store.ClaimNextAttempt(); !errors.Is(err, ErrNoQueuedJobs) {
			t.Fatalf("claim after expiry = %v, want ErrNoQueuedJobs", err)
		}
	})
	t.Run("unfenced compatibility claims preserve legacy mutation APIs", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("legacy-claim", Request{Repository: "widget", Task: "single-process admin compatibility"})
		if err != nil {
			t.Fatal(err)
		}
		claimed, err := store.ClaimNext()
		if err != nil || claimed.ID != job.ID {
			t.Fatalf("legacy ClaimNext() = (%+v, %v)", claimed, err)
		}
		if err := store.RecordVerificationEvidence(job.ID, VerificationEvidence{Limitations: []string{LimitationAgentNotVerdict}}); err != nil {
			t.Fatalf("legacy evidence mutation: %v", err)
		}
		if err := store.Finish(job.ID, StatusSucceeded); err != nil {
			t.Fatalf("legacy finish mutation: %v", err)
		}
	})
	t.Run("legacy mutations cannot bypass expired attempt ownership", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("legacy-after-attempt", Request{Repository: "widget", Task: "admin compatibility cannot mutate worker claim"})
		if err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimNextAttempt()
		if err != nil {
			t.Fatal(err)
		}
		forceAttemptLeaseExpired(t, store, job.ID)
		outcome := ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 1, URL: "https://github.com/acme/widget/pull/1", Branch: "factory/job/" + job.ID, Commit: "abcdef0", State: "open"}
		evidence := VerificationEvidence{Limitations: []string{LimitationAgentNotVerdict}}
		for name, mutate := range map[string]func() error{
			"finish":                func() error { return store.Finish(job.ID, StatusFailed) },
			"provider outcome":      func() error { return store.RecordProviderOutcome(job.ID, outcome) },
			"verification evidence": func() error { return store.RecordVerificationEvidence(job.ID, evidence) },
			"event":                 func() error { return store.AddEvent(job.ID, "late", "unscoped compatibility mutation") },
		} {
			if err := mutate(); !errors.Is(err, ErrStaleAttempt) && !errors.Is(err, ErrInvalidTransition) {
				t.Errorf("unscoped %s mutation error = %v, want fenced rejection", name, err)
			}
		}
		got, err := store.Get(job.ID)
		if err != nil || got.Status != StatusRunning || got.Provider != nil || got.Verification != nil {
			t.Fatalf("job after rejected unscoped mutations = (%+v, %v)", got, err)
		}
		for name, mutate := range map[string]func() error{
			"provider outcome":      func() error { return store.RecordProviderOutcomeForAttempt(claim.Attempt, outcome) },
			"verification evidence": func() error { return store.RecordVerificationEvidenceForAttempt(claim.Attempt, evidence) },
			"event":                 func() error { return store.AddEventForAttempt(claim.Attempt, "late_attempt", "expired attempt") },
			"finish":                func() error { return store.FinishAttempt(claim.Attempt, StatusFailed) },
		} {
			if err := mutate(); !errors.Is(err, ErrStaleAttempt) {
				t.Errorf("expired attempt %s mutation = %v, want ErrStaleAttempt", name, err)
			}
		}
	})
	t.Run("attempt handles fence worker mutations after terminalization", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("fenced-attempt", Request{Repository: "widget", Task: "fence stale worker"})
		if err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimNextAttempt()
		if err != nil || claim.Job.ID != job.ID || !validAttemptHandle(claim.Attempt) {
			t.Fatalf("ClaimNextAttempt() = (%+v, %v)", claim, err)
		}
		if claim.Attempt.Generation != 1 {
			t.Fatalf("first generation = %d, want 1", claim.Attempt.Generation)
		}
		if err := store.HeartbeatAttempt(claim.Attempt); err != nil {
			t.Fatalf("heartbeat: %v", err)
		}
		if err := store.AddEventForAttempt(claim.Attempt, "worker", "owned mutation"); err != nil {
			t.Fatalf("fenced event: %v", err)
		}
		if err := store.RecordVerificationEvidenceForAttempt(claim.Attempt, VerificationEvidence{Limitations: []string{LimitationAgentNotVerdict}}); err != nil {
			t.Fatalf("fenced evidence: %v", err)
		}
		providerAttempt := ProviderAttempt{Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + job.ID, Commit: "abcdef0"}
		if err := store.RecordProviderAttemptForAttempt(claim.Attempt, providerAttempt); err != nil {
			t.Fatalf("fenced provider attempt: %v", err)
		}
		if err := store.MarkProviderAttemptUncertainForAttempt(claim.Attempt); err != nil {
			t.Fatalf("fenced uncertain attempt: %v", err)
		}
		outcome := ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 1, URL: "https://github.com/acme/widget/pull/1", Branch: providerAttempt.Branch, Commit: providerAttempt.Commit, State: "open"}
		if err := store.RecordProviderOutcomeForAttempt(claim.Attempt, outcome); err != nil {
			t.Fatalf("fenced provider outcome: %v", err)
		}
		if err := store.FinishAttempt(claim.Attempt, StatusFailed); err != nil {
			t.Fatalf("fenced finish: %v", err)
		}
		for name, mutate := range map[string]func() error{
			"heartbeat": func() error { return store.HeartbeatAttempt(claim.Attempt) },
			"event":     func() error { return store.AddEventForAttempt(claim.Attempt, "late", "stale") },
			"provider attempt": func() error {
				return store.RecordProviderAttemptForAttempt(claim.Attempt, ProviderAttempt{Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + job.ID, Commit: "abcdef0"})
			},
			"uncertain provider attempt": func() error { return store.MarkProviderAttemptUncertainForAttempt(claim.Attempt) },
			"provider outcome": func() error {
				return store.RecordProviderOutcomeForAttempt(claim.Attempt, ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 1, URL: "https://github.com/acme/widget/pull/1", Branch: "factory/job/" + job.ID, Commit: "abcdef0", State: "open"})
			},
			"verification": func() error { return store.RecordVerificationEvidenceForAttempt(claim.Attempt, VerificationEvidence{}) },
			"finish":       func() error { return store.FinishAttempt(claim.Attempt, StatusCanceled) },
		} {
			if err := mutate(); !errors.Is(err, ErrStaleAttempt) {
				t.Errorf("stale %s mutation error = %v, want ErrStaleAttempt", name, err)
			}
		}
	})
	t.Run("bounded listing has stable admission order and snapshot boundary", func(t *testing.T) {
		store := newStore(t)
		first, _, err := store.Admit("list-first", Request{Repository: "widget", Task: "private first task"})
		if err != nil {
			t.Fatal(err)
		}
		second, _, err := store.Admit("list-second", Request{Repository: "widget", Task: "private second task"})
		if err != nil {
			t.Fatal(err)
		}
		setListTestCreatedAt(t, store, first.ID, second.ID)
		if _, err := store.ClaimNext(); err != nil {
			t.Fatal(err)
		}
		if err := store.Finish(first.ID, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimNext(); err != nil {
			t.Fatal(err)
		}
		page, err := store.ListJobs(context.Background(), 0, 0, 1)
		if err != nil || len(page.Jobs) != 1 || page.Jobs[0].ID != first.ID || !page.HasMore || page.SnapshotSequence == 0 || page.NextSequence == 0 {
			t.Fatalf("first list page = (%+v, %v), want first admitted job and continuation", page, err)
		}
		startConcurrent := make(chan struct{})
		listed := make(chan struct {
			page JobPage
			err  error
		}, 1)
		concurrentAdmission := make(chan error, 1)
		go func() {
			<-startConcurrent
			result, err := store.ListJobs(context.Background(), page.NextSequence, page.SnapshotSequence, 1)
			listed <- struct {
				page JobPage
				err  error
			}{page: result, err: err}
		}()
		go func() {
			<-startConcurrent
			_, _, err := store.Admit("list-fourth", Request{Repository: "widget", Task: "concurrent admission"})
			concurrentAdmission <- err
		}()
		close(startConcurrent)
		nextResult := <-listed
		if err := <-concurrentAdmission; err != nil {
			t.Fatal(err)
		}
		next := nextResult.page
		third, _, err := store.Admit("list-fresh", Request{Repository: "widget", Task: "fresh snapshot"})
		if err != nil {
			t.Fatal(err)
		}
		if nextResult.err != nil || len(next.Jobs) != 1 || next.Jobs[0].ID != second.ID || next.Jobs[0].Status != StatusRunning || next.HasMore {
			t.Fatalf("continuation page = (%+v, %v), want second admitted job only", next, nextResult.err)
		}
		encoded, err := json.Marshal(next.Jobs[0])
		if err != nil || strings.Contains(string(encoded), "private second task") || strings.Contains(string(encoded), "request") {
			t.Fatalf("listed summary exposed request payload: %s err=%v", encoded, err)
		}
		all, err := store.ListJobs(context.Background(), 0, 0, 10)
		if err != nil || len(all.Jobs) != 4 || all.Jobs[3].ID != third.ID {
			t.Fatalf("fresh list = (%+v, %v), want concurrent admission on fresh snapshot", all, err)
		}
	})

	t.Run("stream state is atomic, context-aware, and preserves bounded history", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("stream-state", Request{Repository: "widget", Task: "private"})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, history, err := store.EventStreamState(context.Background(), job.ID)
		if err != nil || snapshot.Status != StatusQueued || history.JobID != job.ID || len(history.Events) != 1 || history.LatestSequence != 1 {
			t.Fatalf("initial stream state=(%+v,%+v,%v)", snapshot, history, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := store.EventStreamState(ctx, job.ID); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled stream state error=%v, want context.Canceled", err)
		}
	})

	t.Run("history cursor is monotonic across bounded retention", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("event-cursor", Request{Repository: "widget", Task: "private task"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimNext(); err != nil {
			t.Fatal(err)
		}
		if err := store.Finish(job.ID, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
		history, err := store.History(job.ID)
		if err != nil || len(history.Events) == 0 {
			t.Fatalf("retained history = (%+v, %v)", history, err)
		}
		encoded, err := json.Marshal(history)
		if err != nil || strings.Contains(string(encoded), "latest_sequence") || strings.Contains(string(encoded), `"sequence"`) {
			t.Fatalf("internal cursors changed the public history JSON: %s err=%v", encoded, err)
		}
		for index, event := range history.Events {
			if event.Sequence == 0 || (index > 0 && event.Sequence <= history.Events[index-1].Sequence) {
				t.Fatalf("event sequence at index %d = %d, history=%+v", index, event.Sequence, history.Events)
			}
		}
		if history.LatestSequence < history.Events[len(history.Events)-1].Sequence {
			t.Fatalf("latest event cursor %d precedes retained events %+v", history.LatestSequence, history.Events)
		}
	})

	t.Run("empty listing and terminal retention", func(t *testing.T) {
		store := newStore(t)
		empty, err := store.ListJobs(context.Background(), 0, 0, 2)
		if err != nil || len(empty.Jobs) != 0 || empty.HasMore || empty.SnapshotSequence != 0 {
			t.Fatalf("empty list = (%+v, %v), want an empty page", empty, err)
		}
		job, _, err := store.Admit("list-terminal", Request{Repository: "widget", Task: "terminal"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimNext(); err != nil {
			t.Fatal(err)
		}
		if err := store.Finish(job.ID, StatusFailed); err != nil {
			t.Fatal(err)
		}
		listed, err := store.ListJobs(context.Background(), 0, 0, 2)
		if err != nil || len(listed.Jobs) != 1 || listed.Jobs[0].ID != job.ID || listed.Jobs[0].Status != StatusFailed {
			t.Fatalf("terminal list = (%+v, %v), want retained failed job", listed, err)
		}
	})

	t.Run("invalid pagination and canceled context are rejected", func(t *testing.T) {
		store := newStore(t)
		for _, tc := range []struct {
			after, snapshot uint64
			limit           int
		}{
			{limit: 0}, {limit: -1}, {after: 2, snapshot: 1, limit: 1}, {snapshot: ^uint64(0), limit: 1},
		} {
			if _, err := store.ListJobs(context.Background(), tc.after, tc.snapshot, tc.limit); !errors.Is(err, ErrInvalidInput) {
				t.Errorf("ListJobs(%d,%d,%d) error = %v, want ErrInvalidInput", tc.after, tc.snapshot, tc.limit, err)
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := store.ListJobs(ctx, 0, 0, 1); !errors.Is(err, context.Canceled) {
			t.Errorf("ListJobs(canceled) error = %v, want context.Canceled", err)
		}
	})

	t.Run("admission replay and conflict", func(t *testing.T) {
		store := newStore(t)
		first, replay, err := store.Admit("contract-key", Request{Repository: "widget", Task: "task"})
		if err != nil || replay || first.Submitter != legacySharedPrincipalID {
			t.Fatalf("initial admission = (%+v, %v, %v), want a new job", first, replay, err)
		}
		again, replay, err := store.Admit("contract-key", Request{Repository: "widget", Task: "task"})
		if err != nil || !replay || again.ID != first.ID || again.Submitter != first.Submitter {
			t.Fatalf("identical admission = (%+v, %v, %v), want same job and immutable attribution", again, replay, err)
		}
		encoded, err := json.Marshal(again)
		if err != nil || !strings.Contains(string(encoded), `"submitted_by":"legacy-shared"`) {
			t.Fatalf("snapshot omitted persisted submitter attribution: %s err=%v", encoded, err)
		}
		if _, _, err := store.Admit("contract-key", Request{Repository: "widget", Task: "different"}); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("changed payload error = %v, want ErrIdempotencyConflict", err)
		}
	})

	t.Run("principal-scoped idempotency and globally unique provenance", func(t *testing.T) {
		store := newStore(t)
		request := Request{Repository: "widget", Task: "same idempotency key from separate principals"}
		first, replay, err := store.AdmitAs("shared-key", "alice", request)
		if err != nil || replay || first.Submitter != "alice" {
			t.Fatalf("first principal admission=(%+v,%t,%v)", first, replay, err)
		}
		first.Submitter = "caller-mutated"
		again, replay, err := store.AdmitAs("shared-key", "alice", request)
		if err != nil || !replay || again.ID != first.ID || again.Submitter != "alice" {
			t.Fatalf("same-principal replay=(%+v,%t,%v)", again, replay, err)
		}
		second, replay, err := store.AdmitAs("shared-key", "bob", request)
		if err != nil || replay || second.ID == first.ID || second.Submitter != "bob" {
			t.Fatalf("cross-principal key reuse=(%+v,%t,%v), want independent admission", second, replay, err)
		}
		if _, err := store.GetForPrincipal(first.ID, "bob"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-principal get error=%v, want ErrNotFound", err)
		}
		if owned, err := store.GetForPrincipal(first.ID, "alice"); err != nil || owned.Submitter != "alice" {
			t.Fatalf("owner get=(%+v,%v)", owned, err)
		}
		provenance := provenanceRequest("identical request")
		provJob, replay, err := store.AdmitAs("prov-key-1", "alice", provenance)
		if err != nil || replay {
			t.Fatalf("first provenance admission=(%+v,%t,%v)", provJob, replay, err)
		}
		attached, replay, err := store.AdmitAs("prov-key-2", "alice", provenance)
		if err != nil || !replay || attached.ID != provJob.ID {
			t.Fatalf("same-principal provenance replay=(%+v,%t,%v)", attached, replay, err)
		}
		other := provenanceRequest("unrelated payload must not affect cross-principal conflict")
		if _, _, err := store.AdmitAs("other-principal-provenance", "charlie", other); !errors.Is(err, ErrProvenanceConflict) {
			t.Fatalf("cross-principal provenance error=%v, want ErrProvenanceConflict regardless of payload", err)
		}
	})

	t.Run("principal list filters before applying page limit", func(t *testing.T) {
		store := newStore(t)
		first, _, err := store.AdmitAs("owner-a-1", "alice", Request{Repository: "widget", Task: "first"})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.AdmitAs("owner-b", "bob", Request{Repository: "widget", Task: "hidden"}); err != nil {
			t.Fatal(err)
		}
		second, _, err := store.AdmitAs("owner-a-2", "alice", Request{Repository: "widget", Task: "second"})
		if err != nil {
			t.Fatal(err)
		}
		page, err := store.ListForPrincipal(context.Background(), "alice", 0, 0, 1)
		if err != nil || len(page.Jobs) != 1 || page.Jobs[0].ID != first.ID || !page.HasMore {
			t.Fatalf("first owner page=(%+v,%v)", page, err)
		}
		next, err := store.ListForPrincipal(context.Background(), "alice", page.NextSequence, page.SnapshotSequence, 1)
		if err != nil || len(next.Jobs) != 1 || next.Jobs[0].ID != second.ID || next.HasMore {
			t.Fatalf("second owner page=(%+v,%v)", next, err)
		}
	})

	t.Run("provenance deduplicates independently of idempotency keys", func(t *testing.T) {
		store := newStore(t)
		request := provenanceRequest("same complete task")
		first, replay, err := store.Admit("provenance-key-one", request)
		if err != nil || replay {
			t.Fatalf("first provenance admission=(%+v,%t,%v)", first, replay, err)
		}
		second, replay, err := store.Admit("provenance-key-two", request)
		if err != nil || !replay || second.ID != first.ID {
			t.Fatalf("second key for same provenance=(%+v,%t,%v), want same job", second, replay, err)
		}
		changed := request
		changed.Task = "changed task"
		if _, _, err := store.Admit("provenance-key-three", changed); !errors.Is(err, ErrProvenanceConflict) {
			t.Fatalf("different task on same provenance error=%v, want provenance conflict", err)
		}
		without := Request{Repository: request.Repository, Task: request.Task, Issue: request.Issue}
		if _, _, err := store.Admit("provenance-key-one", without); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("omitted provenance reusing provenance idempotency key error=%v", err)
		}
		if _, _, err := store.Admit("ordinary-key", without); err != nil {
			t.Fatalf("legacy request without provenance was rejected: %v", err)
		}
		changedVersion := request
		provenance := *request.IssueProvenance
		provenance.Version = strings.Repeat("b", 64)
		changedVersion.IssueProvenance = &provenance
		third, replay, err := store.Admit("provenance-version-two", changedVersion)
		if err != nil || replay || third.ID == first.ID {
			t.Fatalf("new provenance version=(%+v,%t,%v), want new job", third, replay, err)
		}
	})

	t.Run("concurrent identical admissions share one job", func(t *testing.T) {
		store := newStore(t)
		const callers = 16
		type result struct {
			job    Snapshot
			replay bool
			err    error
		}
		start := make(chan struct{})
		results := make(chan result, callers)
		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				job, replay, err := store.Admit("shared-key", Request{Repository: "widget", Task: "same task"})
				results <- result{job: job, replay: replay, err: err}
			}()
		}
		close(start)
		wg.Wait()
		close(results)

		var jobID string
		newAdmissions := 0
		for result := range results {
			if result.err != nil {
				t.Errorf("concurrent Admit() error: %v", result.err)
				continue
			}
			if jobID == "" {
				jobID = result.job.ID
			}
			if result.job.ID != jobID {
				t.Errorf("concurrent admissions returned IDs %q and %q", jobID, result.job.ID)
			}
			if !result.replay {
				newAdmissions++
			}
		}
		if jobID == "" {
			t.Fatal("no concurrent admission returned a job")
		}
		if newAdmissions != 1 {
			t.Errorf("new admissions = %d, want exactly one", newAdmissions)
		}
		if _, _, err := store.Admit("shared-key", Request{Repository: "widget", Task: "different"}); !errors.Is(err, ErrIdempotencyConflict) {
			t.Errorf("changed-payload replay error = %v, want conflict", err)
		}
	})

	t.Run("lifecycle and history", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("lifecycle-key", Request{Repository: "widget", Task: "task"})
		if err != nil {
			t.Fatal(err)
		}
		claimed, err := store.ClaimNext()
		if err != nil || claimed.ID != job.ID || claimed.Status != StatusRunning {
			t.Fatalf("claim = (%+v, %v), want running job %q", claimed, err, job.ID)
		}
		if err := store.AddEvent(job.ID, "verification", "tests passed"); err != nil {
			t.Fatal(err)
		}
		verification := VerificationEvidence{
			Checks:      []VerificationCheck{{Name: "check-01", Outcome: "passed"}},
			Limitations: []string{LimitationAgentNotVerdict},
		}
		if err := store.RecordVerificationEvidence(job.ID, verification); err != nil {
			t.Fatal(err)
		}
		if err := store.Finish(job.ID, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
		got, err := store.Get(job.ID)
		if err != nil || got.Status != StatusSucceeded || got.Verification == nil || len(got.Verification.Checks) != 1 || got.Verification.Checks[0] != verification.Checks[0] || len(got.Verification.Limitations) != 1 || got.Verification.Limitations[0] != LimitationAgentNotVerdict {
			t.Fatalf("Get() = (%+v, %v), want succeeded job with durable verification evidence", got, err)
		}
		history, err := store.History(job.ID)
		if err != nil || len(history.Events) != 4 || history.Events[2].Type != "verification" || history.Events[3].Type != string(StatusSucceeded) {
			t.Fatalf("History() = (%+v, %v), want queued/running/verification/succeeded events", history, err)
		}
		report, err := store.Recover(context.Background())
		if err != nil || len(report.Terminal) != 1 || report.Terminal[0].ID != job.ID || len(report.Resume) != 0 || len(report.NeedsOperator) != 0 {
			t.Fatalf("Recover() = (%+v, %v), want terminal job retained without replay", report, err)
		}
	})

	t.Run("cancel queued work without claiming it", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("cancel-queued", Request{Repository: "widget", Task: "task"})
		if err != nil {
			t.Fatal(err)
		}
		canceled, err := store.Cancel(job.ID)
		if err != nil || canceled.Status != StatusCanceled {
			t.Fatalf("Cancel() = (%+v, %v), want canceled queued job", canceled, err)
		}
		if _, err := store.ClaimNext(); !errors.Is(err, ErrNoQueuedJobs) {
			t.Fatalf("ClaimNext() after queued cancellation = %v, want ErrNoQueuedJobs", err)
		}
		history, err := store.History(job.ID)
		if err != nil || len(history.Events) != 2 || history.Events[1].Type != string(StatusCanceled) {
			t.Fatalf("canceled queued history = (%+v, %v), want queued/canceled", history, err)
		}
		if _, err := store.Cancel(job.ID); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("repeated terminal Cancel() = %v, want ErrInvalidTransition", err)
		}
	})

	t.Run("request running cancellation without terminalizing early", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("cancel-running", Request{Repository: "widget", Task: "task"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimNext(); err != nil {
			t.Fatal(err)
		}
		requested, err := store.Cancel(job.ID)
		if err != nil || requested.Status != StatusRunning || !requested.CancellationRequested {
			t.Fatalf("Cancel() = (%+v, %v), want running with cancellation requested", requested, err)
		}
		repeated, err := store.Cancel(job.ID)
		if err != nil || repeated.Status != StatusRunning || !repeated.CancellationRequested {
			t.Fatalf("repeated Cancel() = (%+v, %v), want same pending request", repeated, err)
		}
		history, err := store.History(job.ID)
		if err != nil || len(history.Events) != 3 || history.Events[2].Type != "cancel_requested" {
			t.Fatalf("pending cancellation history = (%+v, %v), want one cancel_requested event", history, err)
		}
		if err := store.Finish(job.ID, StatusSucceeded); err != nil {
			t.Fatal(err)
		}
		finished, err := store.Get(job.ID)
		if err != nil || finished.Status != StatusSucceeded || finished.CancellationRequested {
			t.Fatalf("finished cancellation = (%+v, %v), want executor-selected terminal state", finished, err)
		}
		history, err = store.History(job.ID)
		if err != nil || len(history.Events) != 4 || history.Events[3].Type != string(StatusSucceeded) {
			t.Fatalf("finished cancellation history = (%+v, %v), want terminal event after request", history, err)
		}
	})

	t.Run("close cancels queued work", func(t *testing.T) {
		store := newStore(t)
		job, _, err := store.Admit("queued-key", Request{Repository: "widget", Task: "task"})
		if err != nil {
			t.Fatal(err)
		}
		store.Close()
		got, err := store.Get(job.ID)
		if err != nil || got.Status != StatusCanceled {
			t.Fatalf("Get() after Close() = (%+v, %v), want canceled", got, err)
		}
		if _, err := store.WaitClaim(context.Background()); !errors.Is(err, ErrManagerClosed) {
			t.Fatalf("WaitClaim() after Close() = %v, want ErrManagerClosed", err)
		}
	})
}

func TestRecoveryDispositionRequiresOperatorForRunningJobs(t *testing.T) {
	tests := []struct {
		status Status
		want   RecoveryDisposition
	}{
		{StatusQueued, RecoveryResume},
		{StatusRunning, RecoveryNeedsOperator},
		{StatusSucceeded, RecoveryRetainTerminal},
		{StatusFailed, RecoveryRetainTerminal},
		{StatusCanceled, RecoveryRetainTerminal},
		{Status("unknown"), RecoveryNeedsOperator},
	}
	for _, test := range tests {
		t.Run(string(test.status), func(t *testing.T) {
			if got := RecoveryDispositionFor(test.status); got != test.want {
				t.Fatalf("RecoveryDispositionFor(%q) = %q, want %q", test.status, got, test.want)
			}
		})
	}
}

func TestLocalStoreRecoveryReportsQueuedAsResumableAndRunningAsUncertain(t *testing.T) {
	store, err := NewManager(Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 4})
	if err != nil {
		t.Fatal(err)
	}
	queued, _, err := store.Admit("queued", Request{Repository: "widget", Task: "queued"})
	if err != nil {
		t.Fatal(err)
	}
	running, _, err := store.Admit("running", Request{Repository: "widget", Task: "running"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimNext()
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != queued.ID {
		t.Fatalf("first claimed job = %q, want FIFO queued job %q", claimed.ID, queued.ID)
	}
	report, err := store.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Resume) != 1 || report.Resume[0].ID != running.ID {
		t.Fatalf("resumable jobs = %+v, want queued job %q", report.Resume, running.ID)
	}
	if len(report.NeedsOperator) != 1 || report.NeedsOperator[0].ID != claimed.ID {
		t.Fatalf("operator-recovery jobs = %+v, want claimed job %q", report.NeedsOperator, claimed.ID)
	}
}

func TestLocalManagerOperationalSummaryIsBoundedAndAggregated(t *testing.T) {
	store, err := NewManager(Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 3, MaxEventsPerJob: 4})
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := store.Admit("summary-1", Request{Repository: "widget", Task: "secret task one"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	second, _, err := store.Admit("summary-2", Request{Repository: "widget", Task: "secret task two"})
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.OperationalSummary(context.Background())
	if err != nil || before.RetainedRecords != 2 || before.RecordLimit != 3 || before.QueueCapacity != 1 || before.Queued != 1 || before.Running != 1 || !before.QueueSaturated {
		t.Fatalf("summary with active and queued jobs = (%+v, %v)", before, err)
	}
	if _, err := store.OperationalSummary(nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("OperationalSummary(nil) error = %v, want ErrInvalidInput", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.OperationalSummary(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("OperationalSummary(canceled context) error = %v, want context.Canceled", err)
	}
	if err := store.Finish(first.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(second.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	third, _, err := store.Admit("summary-3", Request{Repository: "widget", Task: "secret task three"})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := store.OperationalSummary(context.Background())
	if err != nil || summary.RetainedRecords != 3 || summary.Queued != 1 || summary.Running != 0 || summary.Succeeded != 1 || summary.Failed != 1 || summary.Canceled != 0 || !summary.QueueSaturated {
		t.Fatalf("terminal/queued summary = (%+v, %v)", summary, err)
	}
	store.Close()
	afterClose, err := store.OperationalSummary(context.Background())
	if err != nil || afterClose.Canceled != 1 || afterClose.QueueSaturated {
		t.Fatalf("summary after queued cancellation = (%+v, %v), third=%s", afterClose, err, third.ID)
	}
}

func TestStoreRecoveryHonorsCanceledContext(t *testing.T) {
	store, err := NewManager(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Admit("cancel-recovery", Request{Repository: "widget", Task: "task"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Recover(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Recover() error = %v, want context.Canceled", err)
	}
}

func TestWaitClaimUsesWorkerOwnershipLimit(t *testing.T) {
	store, err := NewManager(Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 4})
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := store.Admit("first", Request{Repository: "widget", Task: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := store.Admit("second", Request{Repository: "widget", Task: "second"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.WaitClaim(context.Background())
	if err != nil || claimed.ID != first.ID {
		t.Fatalf("first claim = (%+v, %v), want %q", claimed, err, first.ID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := store.WaitClaim(ctx)
		result <- err
	}()
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("claim while worker is owned = %v, want context cancellation", err)
	}
	if err := store.Finish(first.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.WaitClaim(context.Background())
	if err != nil || claimed.ID != second.ID {
		t.Fatalf("claim after ownership released = (%+v, %v), want %q", claimed, err, second.ID)
	}
}
