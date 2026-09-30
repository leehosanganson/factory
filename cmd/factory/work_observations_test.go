package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/leehosanganson/factory/internal/factory"
)

func TestWorkRefreshHistoryCLIRecordsDuplicateAndLeavesQueueUnchanged(t *testing.T) {
	state, config, bin := t.TempDir(), t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	t.Setenv("XDG_CONFIG_HOME", config)
	calls := filepath.Join(t.TempDir(), "gh-calls")
	t.Setenv("FACTORY_GH_CALLS", calls)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	gh := filepath.Join(bin, "gh")
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$FACTORY_GH_CALLS\"\nprintf '%s' '{\"id\":101,\"number\":42,\"title\":\"Fix widget handling\",\"body\":\"details\",\"state\":\"open\",\"html_url\":\"https://github.com/acme/widget/issues/42\",\"updated_at\":\"2025-03-04T05:06:07Z\"}'\n"
	if err := os.WriteFile(gh, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	queue, err := factory.NewLocalWorkQueue(filepath.Join(state, "factory", "work-requests"))
	if err != nil {
		t.Fatal(err)
	}
	request := factory.WorkRequest{TrackerProvider: "github", IssueID: "42", CodeHostProvider: "github", Repository: "acme/widget", DeduplicationKey: "refresh-integration"}
	if err := queue.Enqueue(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	before, err := queue.Get(context.Background(), request.DeduplicationKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"newly recorded", "duplicate"} {
		var out, errOut bytes.Buffer
		if err := run([]string{"work", "refresh", request.DeduplicationKey}, nil, &out, &errOut); err != nil {
			t.Fatal(err)
		}
		for _, value := range []string{"Title: Fix widget handling", "State: open", "Version:", "Observation: " + want} {
			if !strings.Contains(out.String(), value) {
				t.Errorf("refresh output missing %q: %s", value, out.String())
			}
		}
	}
	callData, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.TrimSpace(string(callData)), "api --method GET repos/acme/widget/issues/42\napi --method GET repos/acme/widget/issues/42"; got != want {
		t.Fatalf("gh calls = %q; want one fetch per explicit refresh", got)
	}

	var out, errOut bytes.Buffer
	if err := run([]string{"work", "history", request.DeduplicationKey}, nil, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if strings.Count(out.String(), "Issue snapshot:") != 1 || !strings.Contains(out.String(), "Title: Fix widget handling") {
		t.Fatalf("history should list one deduplicated version: %s", out.String())
	}
	queue, err = factory.NewLocalWorkQueue(filepath.Join(state, "factory", "work-requests"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	after, err := queue.Get(context.Background(), request.DeduplicationKey)
	if err != nil || after != before {
		t.Fatalf("refresh/history changed queue item: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestWorkWatchCLIRecordsClosedSnapshotAndExits(t *testing.T) {
	state, config, bin := t.TempDir(), t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	t.Setenv("XDG_CONFIG_HOME", config)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	gh := filepath.Join(bin, "gh")
	script := "#!/bin/sh\nprintf '%s' '{\"id\":101,\"number\":42,\"title\":\"Widget fixed\",\"body\":\"details\",\"state\":\"closed\",\"html_url\":\"https://github.com/acme/widget/issues/42\",\"updated_at\":\"2025-03-04T05:06:07Z\"}'\n"
	if err := os.WriteFile(gh, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	queue, err := factory.NewLocalWorkQueue(filepath.Join(state, "factory", "work-requests"))
	if err != nil {
		t.Fatal(err)
	}
	request := factory.WorkRequest{TrackerProvider: "github", IssueID: "42", CodeHostProvider: "github", Repository: "acme/widget", DeduplicationKey: "watch-integration"}
	if err := queue.Enqueue(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	before, err := queue.Get(context.Background(), request.DeduplicationKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	if err := run([]string{"work", "watch", request.DeduplicationKey, "--interval", "1ms"}, nil, &out, &errOut); err != nil {
		t.Fatalf("watch should exit successfully for a closed issue: %v", err)
	}
	for _, want := range []string{"Title: Widget fixed", "State: closed", "Observation: newly recorded", "Lifecycle: stopped"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("watch output missing %q: %s", want, out.String())
		}
	}
	store, err := factory.NewLocalIssueObservationStore(filepath.Join(state, "factory", "work-observations"))
	if err != nil {
		t.Fatal(err)
	}
	observations, err := store.List(context.Background(), request.DeduplicationKey)
	if err != nil || len(observations) != 1 || observations[0].Snapshot.State != "closed" {
		t.Fatalf("watch history = %+v, %v; want one closed observation", observations, err)
	}
	queue, err = factory.NewLocalWorkQueue(filepath.Join(state, "factory", "work-requests"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	after, err := queue.Get(context.Background(), request.DeduplicationKey)
	if err != nil || after != before {
		t.Fatalf("watch changed queued request: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestWorkWatchCLIReconcilesChangesContinuesWhileWaitingAndStopsOnClosure(t *testing.T) {
	state, config, bin := t.TempDir(), t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	t.Setenv("XDG_CONFIG_HOME", config)
	calls := filepath.Join(t.TempDir(), "gh-calls")
	t.Setenv("FACTORY_GH_CALLS", calls)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	gh := filepath.Join(bin, "gh")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$FACTORY_GH_CALLS"
count=$(wc -l < "$FACTORY_GH_CALLS")
case "$count" in
  1) title='Initial'; state='open'; updated='2025-03-01T00:00:00Z' ;;
  2) title='Changed'; state='open'; updated='2025-03-02T00:00:00Z' ;;
  3) title='Changed again'; state='open'; updated='2025-03-03T00:00:00Z' ;;
  *) title='Closed'; state='closed'; updated='2025-03-04T00:00:00Z' ;;
esac
printf '{"id":101,"number":42,"title":"%s","body":"details","state":"%s","html_url":"https://github.com/acme/widget/issues/42","updated_at":"%s"}' "$title" "$state" "$updated"
`
	if err := os.WriteFile(gh, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	queue, err := factory.NewLocalWorkQueue(filepath.Join(state, "factory", "work-requests"))
	if err != nil {
		t.Fatal(err)
	}
	request := factory.WorkRequest{TrackerProvider: "github", IssueID: "42", CodeHostProvider: "github", Repository: "acme/widget", DeduplicationKey: "watch-reconciliation-integration"}
	if err := queue.Enqueue(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	before, err := queue.Get(context.Background(), request.DeduplicationKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	if err := run([]string{"work", "watch", request.DeduplicationKey, "--interval", "1ms"}, nil, &out, &errOut); err != nil {
		t.Fatalf("watch should stop successfully after closure: %v; stderr=%s", err, errOut.String())
	}
	for _, want := range []string{"Lifecycle: baseline", "Lifecycle: waiting_for_human", "Title: Changed again", "Lifecycle: stopped"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("watch output missing %q: %s", want, out.String())
		}
	}
	callData, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(callData)); got != "api --method GET repos/acme/widget/issues/42\napi --method GET repos/acme/widget/issues/42\napi --method GET repos/acme/widget/issues/42\napi --method GET repos/acme/widget/issues/42" {
		t.Fatalf("provider calls = %q; want four read-only GETs, continuing while waiting", got)
	}
	store, err := factory.NewLocalIssueObservationStore(filepath.Join(state, "factory", "work-observations"))
	if err != nil {
		t.Fatal(err)
	}
	observations, err := store.List(context.Background(), request.DeduplicationKey)
	if err != nil || len(observations) != 4 {
		t.Fatalf("watch history = %d observations, %v; want baseline, two changes, and closure", len(observations), err)
	}
	lifecycle, err := store.Reconcile(context.Background(), request.DeduplicationKey)
	if err != nil || lifecycle.Status != "stopped" || lifecycle.BaselineVersion == "" {
		t.Fatalf("persisted lifecycle = %+v, %v", lifecycle, err)
	}
	queue, err = factory.NewLocalWorkQueue(filepath.Join(state, "factory", "work-requests"))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	after, err := queue.Get(context.Background(), request.DeduplicationKey)
	if err != nil || after != before {
		t.Fatalf("watch changed queued request: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestWorkRefreshCLIPreflightAndFetchFailures(t *testing.T) {
	state, config, bin := t.TempDir(), t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	t.Setenv("XDG_CONFIG_HOME", config)
	calls := filepath.Join(t.TempDir(), "gh-calls")
	t.Setenv("FACTORY_GH_CALLS", calls)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	gh := filepath.Join(bin, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\nprintf called >> \"$FACTORY_GH_CALLS\"\necho unavailable >&2\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	if err := run([]string{"work", "refresh", "missing"}, nil, &out, &errOut); err == nil || !strings.Contains(err.Error(), `work request "missing" not found`) {
		t.Fatalf("missing refresh error = %v", err)
	}
	queue, err := factory.NewLocalWorkQueue(filepath.Join(state, "factory", "work-requests"))
	if err != nil {
		t.Fatal(err)
	}
	unsupported := factory.WorkRequest{TrackerProvider: "azure-boards", IssueID: "42", CodeHostProvider: "github", Repository: "acme/widget", DeduplicationKey: "unsupported-refresh"}
	if err := queue.Enqueue(context.Background(), unsupported); err != nil {
		t.Fatal(err)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := run([]string{"work", "refresh", unsupported.DeduplicationKey}, nil, &out, &errOut); err == nil || !strings.Contains(err.Error(), "only github is supported") {
		t.Fatalf("unsupported refresh error = %v", err)
	}
	if _, err := os.Stat(calls); !os.IsNotExist(err) {
		t.Fatalf("preflight errors contacted gh; calls file stat error = %v", err)
	}

	request := factory.WorkRequest{TrackerProvider: "github", IssueID: "42", CodeHostProvider: "github", Repository: "acme/widget", DeduplicationKey: "fetch-error"}
	queue, err = factory.NewLocalWorkQueue(filepath.Join(state, "factory", "work-requests"))
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Enqueue(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	_ = queue.Close()
	out.Reset()
	if err := run([]string{"work", "refresh", request.DeduplicationKey}, nil, &out, &errOut); err == nil || !strings.Contains(err.Error(), "refresh issue") {
		t.Fatalf("fetch failure error = %v", err)
	}
	callData, err := os.ReadFile(calls)
	if err != nil || strings.TrimSpace(string(callData)) != "called" {
		t.Fatalf("GitHub should be called once after valid preflight: %q, %v", callData, err)
	}
	store, err := factory.NewLocalIssueObservationStore(filepath.Join(state, "factory", "work-observations"))
	if err != nil {
		t.Fatal(err)
	}
	observations, err := store.List(context.Background(), request.DeduplicationKey)
	if err != nil || len(observations) != 0 {
		t.Fatalf("failed fetch persisted observation: %+v, %v", observations, err)
	}
}
