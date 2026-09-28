package factory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigDefaultsAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.json")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Command != "pi" || strings.Join(cfg.Args, " ") != "-p --no-session --append-system-prompt {system_prompt} {task}" || len(cfg.PipelineChecks) != 0 || cfg.MonitorTimeout != "" || cfg.WorktreeParent != defaultWorktreeParent {
		t.Fatalf("unexpected default config: %#v", cfg)
	}
	if timeout, err := cfg.agentTimeout(); err != nil || timeout != 60*time.Minute {
		t.Fatalf("default agent timeout = %s, %v; want 60m", timeout, err)
	}

	for _, content := range []string{
		`{"command":"","args":[]}`,
		`{"command":"pi","args":["{task}","{system_prompt}","bad\u0000arg"]}`,
		`{"command":"pi","args":["{task}","{system_prompt}"],"unexpected":true}`,
		`{"command":"pi","args":["{task}","{system_prompt}"]} {}`,
		`{"command":"pi","args":["{task}","{system_prompt}"],"state_dir":"relative"}`,
		`{"command":"pi","args":["{task}","{system_prompt}"],"worktree_parent":"  "}`,
		`{"command":"pi","args":["{task}","{system_prompt}"],"worktree_parent":"bad\u0000path"}`,
		`{"command":"pi","args":["{task}","{system_prompt}"],"pipeline_checks":[[]]}`,
		`{"command":"pi","args":["{task}","{system_prompt}"],"pipeline_checks":[["", "arg"]]}`,
		`{"command":"pi","args":["{task}","{system_prompt}"],"pipeline_checks":[["make", "bad\u0000arg"]]}`,
		`{"command":"pi","args":["{task}","{system_prompt}"],"parallel_implementation":{"enabled":true,"max_concurrency":9}}`,
	} {
		t.Run(content, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadConfig(file); err == nil {
				t.Fatal("expected invalid config to be rejected")
			}
		})
	}
}

func TestLoadConfigPreservesPipelineCheckArgumentVectors(t *testing.T) {
	file := filepath.Join(t.TempDir(), "config.json")
	content := `{"command":"pi","args":["{task}","{system_prompt}"],"pipeline_checks":[["make","test"],["go","test","./..."]]}`
	if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(file)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"make", "test"}, {"go", "test", "./..."}}
	if len(cfg.PipelineChecks) != len(want) {
		t.Fatalf("pipeline_checks = %#v, want %#v", cfg.PipelineChecks, want)
	}
	for i := range want {
		if strings.Join(cfg.PipelineChecks[i], "\x00") != strings.Join(want[i], "\x00") {
			t.Fatalf("pipeline_checks[%d] = %#v, want %#v", i, cfg.PipelineChecks[i], want[i])
		}
	}
}

func TestLoadConfigParallelImplementationIsExplicitlyOptIn(t *testing.T) {
	file := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(file, []byte(`{"command":"pi","args":["{task}","{system_prompt}"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(file)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ParallelImplementation != nil || (Config{ParallelImplementation: &ParallelImplementationConfig{Enabled: false}}).ParallelImplementation.Enabled {
		t.Fatal("parallel implementation must remain disabled unless explicitly enabled")
	}
	if err := os.WriteFile(file, []byte(`{"command":"pi","args":["{task}","{system_prompt}"],"parallel_implementation":{"enabled":true,"max_concurrency":2}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadConfig(file)
	if err != nil || cfg.ParallelImplementation == nil || !cfg.ParallelImplementation.Enabled || cfg.ParallelImplementation.MaxConcurrency != 2 {
		t.Fatalf("enabled parallel config = %+v, %v", cfg.ParallelImplementation, err)
	}
}

func TestLoadConfigAgentTimeout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{name: "omitted defaults", value: ``, want: 60 * time.Minute},
		{name: "empty defaults", value: `,"agent_timeout":""`, want: 60 * time.Minute},
		{name: "configured duration", value: `,"agent_timeout":"2m30s"`, want: 150 * time.Second},
		{name: "invalid duration", value: `,"agent_timeout":"soon"`, wantErr: true},
		{name: "zero duration", value: `,"agent_timeout":"0s"`, wantErr: true},
		{name: "negative duration", value: `,"agent_timeout":"-1s"`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "config.json")
			content := `{"command":"pi","args":["{task}","{system_prompt}"]` + tc.value + `}`
			if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(file)
			if tc.wantErr {
				if err == nil {
					t.Fatal("invalid agent_timeout was accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := cfg.agentTimeout()
			if err != nil || got != tc.want {
				t.Fatalf("agent timeout = %s, %v; want %s", got, err, tc.want)
			}
		})
	}
}

func TestLoadConfigMonitorTimeout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		field   string
		want    time.Duration
		wantErr bool
	}{
		{name: "omitted indefinite", want: 0},
		{name: "empty indefinite", field: `,"monitor_timeout":""`, want: 0},
		{name: "configured duration", field: `,"monitor_timeout":"250ms"`, want: 250 * time.Millisecond},
		{name: "invalid duration", field: `,"monitor_timeout":"soon"`, wantErr: true},
		{name: "zero duration", field: `,"monitor_timeout":"0s"`, wantErr: true},
		{name: "negative duration", field: `,"monitor_timeout":"-1s"`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "config.json")
			content := `{"command":"pi","args":["{task}","{system_prompt}"]` + tc.field + `}`
			if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(file)
			if tc.wantErr {
				if err == nil {
					t.Fatal("invalid monitor_timeout was accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := cfg.monitorTimeout()
			if err != nil || got != tc.want {
				t.Fatalf("monitor timeout = %s, %v; want %s", got, err, tc.want)
			}
		})
	}
}

func TestConfigRequiresTaskAndSystemPromptExactlyOnce(t *testing.T) {
	for _, tc := range []struct {
		name    string
		command string
		args    []string
		wantErr string
	}{
		{name: "valid in arguments", command: "pi", args: []string{"{task}", "{system_prompt}"}},
		{name: "valid across command and arguments", command: "agent-{task}", args: []string{"{system_prompt}"}},
		{name: "optional stage and workdir", command: "pi", args: []string{"{task}", "{system_prompt}", "{stage}", "{workdir}"}},
		{name: "missing task", command: "pi", args: []string{"{system_prompt}"}, wantErr: "{task}"},
		{name: "missing system prompt", command: "pi", args: []string{"{task}"}, wantErr: "{system_prompt}"},
		{name: "duplicate task", command: "pi-{task}", args: []string{"{task}", "{system_prompt}"}, wantErr: "{task}"},
		{name: "duplicate system prompt", command: "pi", args: []string{"{task}", "{system_prompt}", "--prompt={system_prompt}"}, wantErr: "{system_prompt}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := (Config{Command: tc.command, Args: tc.args}).Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("valid placeholders rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate() error = %v, want error containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestConfigPathHonorsXDGAndRequiresAbsolute(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/tmp/factory-config")
	path, err := ConfigPath()
	if err != nil {
		t.Fatal(err)
	}
	if want := "/tmp/factory-config/factory/config.json"; path != want {
		t.Fatalf("ConfigPath() = %q, want %q", path, want)
	}
	t.Setenv("XDG_CONFIG_HOME", "relative")
	if _, err := ConfigPath(); err == nil {
		t.Fatal("relative XDG_CONFIG_HOME must be rejected")
	}
}

func TestLoadMonitorPromptExternalOverridesAndEmbeddedFallback(t *testing.T) {
	dir := t.TempDir()
	prompt, err := LoadPrompt(dir, "monitor")
	if err != nil || !strings.Contains(prompt, "FACTORY_STATUS=FIXED") || !strings.Contains(prompt, "independently deriving changed paths") || !strings.Contains(prompt, "low-risk routine fixes automatically") || !strings.Contains(prompt, "pauses after three failed automatic actions") || !strings.Contains(prompt, "live PR/check snapshot guard") {
		t.Fatalf("embedded monitor prompt = %q, %v", prompt, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "monitor.md"), []byte(" custom monitor prompt "), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadPrompt(dir, "monitor"); err != nil || got != "custom monitor prompt" {
		t.Fatalf("external monitor prompt = %q, %v", got, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "monitor.md"), []byte(" custom monitor prompt "), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadPrompt(dir, "monitor"); err != nil || got != "custom monitor prompt" {
		t.Fatalf("external monitor prompt = %q, %v", got, err)
	}
}

func TestLoadPromptExternalOverrideAndEmbeddedFallback(t *testing.T) {
	dir := t.TempDir()
	if got, err := LoadPrompt(dir, "requirements"); err != nil || !strings.Contains(got, "Do not implement") {
		t.Fatalf("embedded prompt fallback = %q, %v", got, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "requirements.md"), []byte(" custom prompt \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadPrompt(dir, "requirements"); err != nil || got != "custom prompt" {
		t.Fatalf("external prompt = %q, %v", got, err)
	}
	if _, err := LoadPrompt(dir, "../../bad"); err == nil {
		t.Fatal("unknown stage must not be used as a path")
	}
	if _, err := LoadPrompt(dir, "evaluate"); err == nil || !strings.Contains(err.Error(), `unknown prompt stage "evaluate"`) {
		t.Fatalf("obsolete evaluate override error = %v, want unknown stage", err)
	}
}
