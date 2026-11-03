package restjobs

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestSQLiteStoresRaceForOneQueuedAttempt(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "race.db")
	first, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseStore()
	second, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer second.CloseStore()
	job, _, err := first.Admit("race-claim", Request{Repository: "widget", Task: "one claim only"})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan struct {
		claim AttemptClaim
		err   error
	}, 2)
	var wg sync.WaitGroup
	for _, store := range []*SQLiteStore{first, second} {
		wg.Add(1)
		go func(store *SQLiteStore) {
			defer wg.Done()
			<-start
			claim, err := store.ClaimNextAttempt()
			results <- struct {
				claim AttemptClaim
				err   error
			}{claim, err}
		}(store)
	}
	close(start)
	wg.Wait()
	close(results)
	claims := 0
	for result := range results {
		if result.err == nil {
			claims++
			if result.claim.Job.ID != job.ID || result.claim.Attempt.Generation != 1 || !validAttemptHandle(result.claim.Attempt) {
				t.Errorf("claim identity = %+v", result.claim)
			}
		} else if !errors.Is(result.err, ErrNoQueuedJobs) && !errors.Is(result.err, ErrNoWorkerSlots) {
			t.Errorf("competing claim error = %v", result.err)
		}
	}
	if claims != 1 {
		t.Fatalf("successful claims = %d, want exactly one", claims)
	}
}

func TestLocalStoreContractSerializesConcurrentIdempotentAdmission(t *testing.T) {
	store, err := NewManager(Config{QueueCapacity: 16, MaxConcurrentJobs: 4, MaxRecords: 16, MaxEventsPerJob: 4})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 16
	var wg sync.WaitGroup
	ids := make(chan string, callers)
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, _, err := store.Admit("shared-key", Request{Repository: "widget", Task: "same task"})
			if err != nil {
				errs <- err
				return
			}
			ids <- job.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent Admit() error: %v", err)
	}
	var first string
	for id := range ids {
		if first == "" {
			first = id
			continue
		}
		if id != first {
			t.Fatalf("concurrent admissions returned different jobs %q and %q", first, id)
		}
	}
	if first == "" {
		t.Fatal("no concurrent admission returned a job")
	}
	if _, _, err := store.Admit("shared-key", Request{Repository: "widget", Task: "different"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed payload error = %v, want %v", err, ErrIdempotencyConflict)
	}
	if _, err := store.Get(first); err != nil {
		t.Fatalf("retained concurrently admitted job: %v", err)
	}
}

func TestLocalStoreContractConcurrentProvenanceKeysAttachToOneJob(t *testing.T) {
	store, err := NewManager(Config{QueueCapacity: 16, MaxConcurrentJobs: 2, MaxRecords: 16, MaxEventsPerJob: 4})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 16
	start := make(chan struct{})
	type result struct {
		id  string
		err error
	}
	results := make(chan result, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			job, _, err := store.Admit(fmt.Sprintf("concurrent-provenance-key-%d", index), provenanceRequest("same task"))
			results <- result{id: job.ID, err: err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	var id string
	for result := range results {
		if result.err != nil {
			t.Errorf("concurrent provenance admission: %v", result.err)
			continue
		}
		if id == "" {
			id = result.id
		}
		if result.id != id {
			t.Errorf("concurrent provenance IDs %q and %q", id, result.id)
		}
	}
	if id == "" || len(store.idempotency) != callers {
		t.Fatalf("job ID=%q attached keys=%d, want %d keys on one job", id, len(store.idempotency), callers)
	}
}

func TestLocalStoreContractDoesNotReserveIdempotencyKeyWhenCapacityRejects(t *testing.T) {
	store, err := NewManager(Config{QueueCapacity: 1, MaxConcurrentJobs: 1, MaxRecords: 2, MaxEventsPerJob: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Admit("first", Request{Repository: "widget", Task: "first"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Admit("retry", Request{Repository: "widget", Task: "retry"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue error = %v, want %v", err, ErrQueueFull)
	}
	if _, err := store.Get("retry"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("capacity-rejected request was recorded: %v", err)
	}
}

var _ Store = (*LocalManager)(nil)
var _ Manager = (*LocalManager)(nil)
