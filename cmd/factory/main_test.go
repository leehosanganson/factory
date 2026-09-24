package main

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/leehosanganson/factory/internal/factory"
)

func TestHelpAndBabysitUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := run([]string{"-h"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "factory pipeline") || !strings.Contains(out.String(), "factory clean") || !strings.Contains(out.String(), "factory babysit") || !strings.Contains(out.String(), "Alias for factory pipeline") || !strings.Contains(out.String(), "babysit approve <id>") || !strings.Contains(out.String(), "make clean") {
		t.Fatalf("help missing babysit commands: %s", out.String())
	}
	if err := run([]string{"babysit"}, strings.NewReader(""), &out, &errOut); err == nil || !strings.Contains(err.Error(), "usage: factory babysit") {
		t.Fatalf("babysit should show usage: err=%v", err)
	}
}

func TestNonInteractivePipelineAndBareAliasAreRejected(t *testing.T) {
	var errs []error
	for _, args := range [][]string{{"pipeline"}, nil} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, errOut bytes.Buffer
			err := run(args, strings.NewReader("task\n"), &out, &errOut)
			if err == nil || !strings.Contains(err.Error(), "interactive terminal") {
				t.Fatalf("non-terminal invocation error = %v", err)
			}
			errs = append(errs, err)
		})
	}
	if len(errs) != 2 || errs[0].Error() != errs[1].Error() {
		t.Fatalf("bare invocation and explicit pipeline should use the same workflow: errors=%v", errs)
	}
}

func TestTaskEntryPromptsWithOutputGuidanceWhenInitiallyEmpty(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("implement the result\n.\n"))
	var output bytes.Buffer
	task, err := readTask(reader, &output)
	if err != nil {
		t.Fatal(err)
	}
	if task != "implement the result" {
		t.Fatalf("task = %q, want entered task", task)
	}
	if !strings.Contains(output.String(), "Tip:") || !strings.Contains(output.String(), "how it should be verified") {
		t.Fatalf("empty-entry guidance missing: %q", output.String())
	}
}

func TestCLIParsesGateOptionAcrossSubcommands(t *testing.T) {
	for _, tc := range []struct {
		args []string
		gate bool
		task string
	}{
		{args: []string{"pipeline", "--gate", "do", "work"}, gate: true, task: "do work"},
		{args: []string{"--gate", "do", "work"}, gate: true, task: "do work"},
		{args: []string{"pipeline", "do", "work"}, gate: false, task: "do work"},
	} {
		gotGate, gotTask, err := parseGate(tc.args[1:])
		if tc.args[0] == "--gate" {
			gotGate, gotTask, err = parseGate(tc.args)
		}
		if err != nil || gotGate != tc.gate || strings.Join(gotTask, " ") != tc.task {
			t.Fatalf("CLI parse %v = (%v, %v, %v)", tc.args, gotGate, gotTask, err)
		}
	}
	if gate, task, err := parseGate([]string{"--gate"}); err != nil || !gate || len(task) != 0 {
		t.Fatalf("clean gate parse = (%v, %v, %v)", gate, task, err)
	}
}

func TestGateParsingSupportsPipelineAndBareAliasForms(t *testing.T) {
	for _, args := range [][]string{{"--gate", "a", "task"}, {"a", "--gate", "task"}} {
		gate, task, err := parseGate(args)
		if err != nil || !gate || strings.Join(task, " ") != "a task" {
			t.Fatalf("parseGate(%v) = (%v, %v, %v)", args, gate, task, err)
		}
	}
	if _, _, err := parseGate([]string{"--gate", "--gate"}); err == nil {
		t.Fatal("duplicate --gate accepted")
	}
}

func TestPipelineArgumentsBecomeTaskWithoutPrompt(t *testing.T) {
	var out bytes.Buffer
	var gotTask string
	var gotInput io.Reader
	run := func(task string, input io.Reader) error {
		gotTask = task
		gotInput = input
		return nil
	}
	if err := runPipelineTask([]string{"implement", "a", "small", "feature"}, strings.NewReader("unused input"), &out, run); err != nil {
		t.Fatal(err)
	}
	if gotTask != "implement a small feature" {
		t.Fatalf("workflow task = %q, want joined arguments", gotTask)
	}
	if out.Len() != 0 {
		t.Fatalf("argument-based task unexpectedly prompted: %q", out.String())
	}
	if got, err := io.ReadAll(gotInput); err != nil || string(got) != "unused input" {
		t.Fatalf("workflow input = %q, %v; argument-based execution must preserve stdin for approval gates", got, err)
	}
}

func TestBabysitListAndDescribePersistedMetadata(t *testing.T) {
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	_ = factory.DefaultConfig()
	jobRoot := filepath.Join(state, "factory", "jobs")
	id := "20260518T120000-0123456789ab"
	dir := filepath.Join(jobRoot, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	metadata := `{"id":"` + id + `","description":"watch task","repo":"owner/repo","pr":12,"status":"running","created_at":"2026-05-18T12:00:00Z","updated_at":"2026-05-18T12:00:00Z"}`
	if err := os.WriteFile(filepath.Join(dir, "job.json"), []byte(metadata), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "actions.log"), []byte("[now] worker started\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if err := run([]string{"babysit", "list"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), id) || !strings.Contains(out.String(), "owner/repo") {
		t.Fatalf("list omitted persisted job: %s", out.String())
	}
	out.Reset()
	if err := run([]string{"babysit", "describe", id}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "watch task") || !strings.Contains(out.String(), "worker started") {
		t.Fatalf("describe omitted metadata/log: %s", out.String())
	}
	if err := run([]string{"babysit", "describe", "../escape"}, strings.NewReader(""), &out, &errOut); err == nil {
		t.Fatal("unsafe job id accepted")
	}
}
