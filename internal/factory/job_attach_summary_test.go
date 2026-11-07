package factory

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAttachImplementationJobPrintsPersistedTerminalSummary(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      string
		publication string
		want        []string
		wantErr     string
	}{
		{
			name: "published", status: "complete", publication: "published",
			want: []string{"Workflow status: complete", "Publication status: published", "factory job get attached --details"},
		},
		{
			name: "unpublished", status: "complete", publication: "unpublished",
			want: []string{"Workflow status: complete", "Publication status: unpublished", "Publication is incomplete", "retained worktree and branch"},
		},
		{
			name: "no-op", status: "complete", publication: "no-op",
			want: []string{"Workflow status: complete", "Publication status: no-op", "No PR was created", "retained worktree"},
		},
		{
			name: "failed", status: "failed",
			want:    []string{"Workflow status: failed", "Publication status: not recorded", "factory job logs attached --session workflow"},
			wantErr: "failed",
		},
		{
			name: "interrupted", status: "interrupted",
			want:    []string{"Workflow status: interrupted", "Factory will not replay", "factory job get attached --details", "retained worktree and branch"},
			wantErr: "interrupted",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestJobStore(t)
			worktree := filepath.Join(t.TempDir(), "recovery-worktree")
			if err := os.Mkdir(worktree, 0o700); err != nil {
				t.Fatal(err)
			}
			job := JobRecord{
				ID: "attached", Type: implementationJobType, Status: tc.status,
				PublicationStatus: tc.publication, PublicationSummary: "PRIVATE_ERROR /private/path/token",
				Worktree: worktree, WorkBranch: "factory-implement-attached",
			}
			if err := store.CreateJob(job); err != nil {
				t.Fatal(err)
			}
			if _, err := store.CreateSession(job.ID, "workflow", tc.status); err != nil {
				t.Fatal(err)
			}
			if err := store.CreateJobLog(job.ID); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			err := AttachImplementationJob(context.Background(), store, job.ID, &out)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("attach returned error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("attach error = %v, want %q", err, tc.wantErr)
			}
			for _, want := range tc.want {
				if !strings.Contains(out.String(), want) {
					t.Errorf("summary omitted %q: %s", want, out.String())
				}
			}
			for _, forbidden := range []string{"PRIVATE_ERROR", "/private/path/token", "Publication summary:"} {
				if strings.Contains(out.String(), forbidden) {
					t.Errorf("summary exposed persisted detail %q: %s", forbidden, out.String())
				}
			}
		})
	}
}

func TestAttachImplementationJobCancellationOnlyDetaches(t *testing.T) {
	store := newTestJobStore(t)
	job := JobRecord{ID: "attached-running", Type: implementationJobType, Status: "running"}
	if err := store.CreateJob(job); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateJobLog(job.ID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	err := AttachImplementationJob(ctx, store, job.ID, &out)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("detaching observer returned unexpected error: %v", err)
	}
	current, err := store.GetJob(job.ID)
	if err != nil || current.Status != "running" {
		t.Fatalf("observer detach changed job status to %q: %v", current.Status, err)
	}
	if strings.Contains(out.String(), "Implementation job summary") {
		t.Fatalf("detached observer printed terminal summary: %q", out.String())
	}
}
