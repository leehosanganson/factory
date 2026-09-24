package factory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Agent runs a stage in a fresh process. The log includes both stdout and stderr.
type Agent interface {
	Run(stage, systemPrompt, task, workdir, logPath string) error
}

// Runner executes the configured command without invoking a shell.
type Runner struct {
	Config Config
}

// Run starts one independent agent process and records its output at logPath.
func (r Runner) Run(stage, systemPrompt, task, workdir, logPath string) error {
	return r.RunContext(context.Background(), stage, systemPrompt, task, workdir, logPath)
}

// RunContext starts one independent agent process and stops it when ctx is canceled.
func (r Runner) RunContext(ctx context.Context, stage, systemPrompt, task, workdir, logPath string) error {
	if err := r.Config.Validate(); err != nil {
		return err
	}
	command := expand(r.Config.Command, stage, systemPrompt, task, workdir)
	args := make([]string, len(r.Config.Args))
	for i, arg := range r.Config.Args {
		args[i] = expand(arg, stage, systemPrompt, task, workdir)
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open log: %w", err)
	}
	defer log.Close()
	cmd := exec.CommandContext(ctx, command, args...)
	configureProcessCancellation(cmd)
	cmd.Dir = workdir
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run %s agent: %w", stage, err)
	}
	return nil
}

func expand(value, stage, systemPrompt, task, workdir string) string {
	replacer := strings.NewReplacer("{system_prompt}", systemPrompt, "{task}", task, "{workdir}", workdir, "{stage}", stage)
	return replacer.Replace(value)
}

func readLog(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("(could not read log: %v)", err)
	}
	return string(bytes.TrimRight(data, "\n"))
}

func copyOutput(dst io.Writer, content string) {
	if content != "" {
		fmt.Fprintln(dst, content)
	}
}
