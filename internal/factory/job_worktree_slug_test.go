package factory

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProposeImplementationJobSlugProtocolAndFallbacks(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     string
		parent   context.Context
		wantSlug string
	}{
		{name: "valid proposal", body: "printf 'clean-worktree-name\\n'", wantSlug: "clean-worktree-name"},
		{name: "unsafe path characters are normalized", body: "printf ' ../Add/Readable:Name!! \\\n'", wantSlug: "add-readable-name"},
		{name: "unsafe proposal is bounded", body: fmt.Sprintf("printf '%%s\\n' '%s'", strings.Repeat("A", 80)), wantSlug: strings.Repeat("a", 48)},
		{name: "empty response falls back", body: "printf '  \\n'", wantSlug: "deterministic-description-slug"},
		{name: "multiple lines fall back", body: "printf 'one\\ntwo\\n'", wantSlug: "deterministic-description-slug"},
		{name: "oversized response falls back", body: fmt.Sprintf("head -c %d /dev/zero | tr '\\000' x", implementationSlugOutputLimit+1), wantSlug: "deterministic-description-slug"},
		{name: "command failure falls back", body: "exit 9", wantSlug: "deterministic-description-slug"},
		{name: "timeout falls back", body: "sleep 5; printf 'too-late\\n'", parent: canceledSlugContext(t), wantSlug: "deterministic-description-slug"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			script := writeSlugScript(t, tc.body)
			ctx := tc.parent
			if ctx == nil {
				ctx = context.Background()
			}
			logPath := filepath.Join(t.TempDir(), "slug.log")
			got := implementationJobSlugWithFallback(ctx, Runner{Config: Config{Command: script, Args: []string{"{system_prompt}", "{task}"}}}, "Deterministic description slug", t.TempDir(), logPath)
			if got != tc.wantSlug {
				t.Fatalf("slug = %q, want %q", got, tc.wantSlug)
			}
			if len(got) > 48 || strings.ContainsAny(got, "/\\") {
				t.Fatalf("unsafe slug: %q", got)
			}
		})
	}
}

func TestStartImplementationJobProposesSlugBeforeWorktreeCreation(t *testing.T) {
	repo := initTestGitRepo(t)
	state := t.TempDir()
	store, err := NewJobStore(state)
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(state, "proposal-order")
	script := writeSlugScript(t, fmt.Sprintf(`if [ "$1" = "slug" ]; then
  if [ -n "$(git -C %q branch --list 'factory-job-*')" ]; then exit 8; fi
  printf 'proposed-readable-name\n'
  printf 'called\n' >> %q
else
  printf 'PASS\\n'
fi`, repo, marker))
	worktreeParent := filepath.Join(t.TempDir(), "configured", "{repo}-worktrees")
	cfg := Config{Command: script, Args: []string{"{stage}", "{system_prompt}", "{task}"}, WorktreeParent: worktreeParent}
	id, err := startWorkflowJob(store, cfg, repo, "Task description that differs", implementationJobType)
	if err != nil {
		t.Fatalf("start implementation: %v", err)
	}
	job, err := store.GetJob(id)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := filepath.Base(job.Worktree), id[:4]+"-proposed-readable-name"; got != want {
		t.Fatalf("worktree name = %q, want %q", got, want)
	}
	if got, want := filepath.Dir(job.Worktree), canonicalTestPath(t, filepath.Join(filepath.Dir(worktreeParent), filepath.Base(repo)+"-worktrees")); got != want {
		t.Fatalf("worktree parent = %q, want configured parent %q", got, want)
	}
	if got, want := job.WorkBranch, "factory-job-proposed-readable-name-"+id; got != want {
		t.Fatalf("work branch = %q, want %q", got, want)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "called\n" {
		t.Fatalf("proposal marker = %q, err=%v", data, err)
	}
}

func TestTidyAndNonGitImplementationDoNotProposeSlug(t *testing.T) {
	for _, tc := range []struct {
		name     string
		typeName string
		target   func(*testing.T) string
	}{
		{name: "tidy Git job", typeName: tidyJobType, target: func(t *testing.T) string { return initTestGitRepo(t) }},
		{name: "non-Git implementation", typeName: implementationJobType, target: func(t *testing.T) string { return t.TempDir() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := t.TempDir()
			store, err := NewJobStore(state)
			if err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(state, "slug-called")
			script := writeSlugScript(t, fmt.Sprintf("if [ \"$1\" = slug ]; then printf called >> %q; fi\nprintf 'PASS\\n'", marker))
			cfg := Config{Command: script, Args: []string{"{stage}", "{system_prompt}", "{task}"}}
			_, err = startWorkflowJob(store, cfg, tc.target(t), "description", tc.typeName)
			if err != nil {
				t.Fatalf("start job: %v", err)
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("configured agent invoked while starting %s job (stat error %v)", tc.name, err)
			}
		})
	}
}

func canceledSlugContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}

func writeSlugScript(t *testing.T, body string) string {
	t.Helper()
	script := filepath.Join(t.TempDir(), "slug-agent")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n"+body+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return script
}
