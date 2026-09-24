package factory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigDefaultsAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.json")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Command != "pi" || len(cfg.Args) == 0 {
		t.Fatalf("unexpected default config: %#v", cfg)
	}

	for _, content := range []string{
		`{"command":"","args":[]}`,
		`{"command":"pi","args":["{task}","{system_prompt}","bad\u0000arg"]}`,
		`{"command":"pi","args":["{task}","{system_prompt}"],"unexpected":true}`,
		`{"command":"pi","args":["{task}","{system_prompt}"]} {}`,
		`{"command":"pi","args":["{task}","{system_prompt}"],"state_dir":"relative"}`,
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

func TestLoadBabysitPromptExternalOverrideAndEmbeddedFallback(t *testing.T) {
	dir := t.TempDir()
	prompt, err := LoadPrompt(dir, "babysit")
	if err != nil || !strings.Contains(prompt, "FACTORY_STATUS=FIXED") || !strings.Contains(prompt, "independent") {
		t.Fatalf("embedded babysit prompt = %q, %v", prompt, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "babysit.md"), []byte(" custom babysit prompt "), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadPrompt(dir, "babysit"); err != nil || got != "custom babysit prompt" {
		t.Fatalf("external babysit prompt = %q, %v", got, err)
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
}
