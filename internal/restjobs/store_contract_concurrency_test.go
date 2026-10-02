package restjobs

import (
	"errors"
	"sync"
	"testing"
)

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
