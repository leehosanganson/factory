package restjobs

import (
	"errors"
	"testing"
)

func TestLocalManagerRecordsProviderOutcomeBeforeTerminalSuccess(t *testing.T) {
	store, err := NewManager(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.Admit("provider", Request{Repository: "widget", Task: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordProviderOutcome(job.ID, ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 5, URL: "https://github.com/acme/widget/pull/5", Branch: "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb", Commit: "abc123", State: "open"}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("record while queued = %v", err)
	}
	if _, err := store.ClaimNext(); err != nil {
		t.Fatal(err)
	}
	want := ProviderOutcome{Provider: "github", Repository: "acme/widget", Number: 5, URL: "https://github.com/acme/widget/pull/5", Branch: "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb", Commit: "abc123", State: "open"}
	if err := store.RecordProviderOutcome(job.ID, want); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(job.ID, StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(job.ID)
	if err != nil || got.Provider == nil || *got.Provider != want {
		t.Fatalf("persisted outcome = %+v, %v", got.Provider, err)
	}
}

func TestLocalManagerRejectsUnsafeProviderOutcome(t *testing.T) {
	store, _ := NewManager(testConfig())
	job, _, _ := store.Admit("provider", Request{Repository: "widget", Task: "task"})
	_, _ = store.ClaimNext()
	for _, outcome := range []ProviderOutcome{{Provider: "github", Repository: "acme/widget", Number: 5, URL: "http://github.com/acme/widget/pull/5", Branch: "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb", Commit: "sha", State: "open"}, {Provider: "github", Repository: "acme/widget", Number: 5, URL: "https://evil.test/acme/widget/pull/5", Branch: "factory/job/2c9131bb-2cde-4c9d-aaf3-bcc675e482bb", Commit: "sha", State: "open"}} {
		if err := store.RecordProviderOutcome(job.ID, outcome); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("unsafe outcome error = %v", err)
		}
	}
}
