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
	"unicode"
	"unicode/utf8"
)

// Agent runs a stage in a fresh process. The log includes both stdout and stderr.
// Workflow agents must implement RunWithContext. Agents without the required
// context-aware workflow contract fail closed.
type Agent interface {
	Run(stage, systemPrompt, task, workdir, logPath string) error
}

// Runner executes the configured command without invoking a shell.
type Runner struct {
	Config          Config
	ProcessObserver func(command string, pid int, started bool)
}

const stdoutProtocolCaptureLimit = maxSubtaskPlanBytes

type protocolCapture struct {
	bytes.Buffer
	limit              int
	leadingWhitespace  []byte
	whitespaceOverflow bool
	pendingRune        []byte
	startedProtocol    bool
	truncated          bool
}

func (c *protocolCapture) Write(p []byte) (int, error) {
	written := len(p)
	if c.startedProtocol {
		c.append(p)
		return written, nil
	}
	input := append(c.pendingRune, p...)
	c.pendingRune = nil
	for len(input) > 0 {
		if !utf8.FullRune(input) {
			c.pendingRune = append(c.pendingRune, input...)
			break
		}
		r, size := utf8.DecodeRune(input)
		runeBytes := input[:size]
		input = input[size:]
		if r == '\n' {
			c.leadingWhitespace = c.leadingWhitespace[:0]
			c.whitespaceOverflow = false
			continue
		}
		if unicode.IsSpace(r) {
			if !c.whitespaceOverflow && len(c.leadingWhitespace)+size <= c.limit {
				c.leadingWhitespace = append(c.leadingWhitespace, runeBytes...)
			} else {
				c.whitespaceOverflow = true
			}
			continue
		}
		c.startedProtocol = true
		if c.whitespaceOverflow {
			c.append([]byte("!"))
		} else {
			c.append(c.leadingWhitespace)
		}
		c.leadingWhitespace = nil
		c.append(runeBytes)
		c.append(input)
		break
	}
	return written, nil
}

func (c *protocolCapture) append(p []byte) {
	remaining := c.limit - c.Len()
	if len(p) > remaining {
		p = p[:remaining]
		c.truncated = true
	}
	if len(p) > 0 {
		_, _ = c.Buffer.Write(p)
	}
}

// Run starts one independent agent process and records its output at logPath.
func (r Runner) Run(stage, systemPrompt, task, workdir, logPath string) error {
	return r.RunWithContext(context.Background(), stage, systemPrompt, task, workdir, logPath)
}

// RunWithContext starts one independent agent process, applying the configured timeout
// and stopping it when the caller's context is canceled.
func (r Runner) RunWithContext(parent context.Context, stage, systemPrompt, task, workdir, logPath string) error {
	timeout, err := r.Config.agentTimeout()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return r.RunContext(ctx, stage, systemPrompt, task, workdir, logPath)
}

// RunContext starts one independent agent process and stops it when ctx is canceled.
func (r Runner) RunContext(ctx context.Context, stage, systemPrompt, task, workdir, logPath string) error {
	return r.runContext(ctx, stage, systemPrompt, task, workdir, logPath, nil)
}

// RunWithOutputContext starts an agent and returns its stdout while recording both
// stdout and stderr in logPath. It is intended for callers with a stdout protocol.
func (r Runner) RunWithOutputContext(parent context.Context, stage, systemPrompt, task, workdir, logPath string) (string, error) {
	timeout, err := r.Config.agentTimeout()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	stdout := &protocolCapture{limit: stdoutProtocolCaptureLimit}
	err = r.runContext(ctx, stage, systemPrompt, task, workdir, logPath, stdout)
	if err == nil && stdout.truncated {
		err = fmt.Errorf("agent stdout protocol exceeds %d bytes", stdoutProtocolCaptureLimit)
	}
	return stdout.String(), err
}

func (r Runner) runContext(ctx context.Context, stage, systemPrompt, task, workdir, logPath string, response io.Writer) error {
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
	if response != nil {
		cmd.Stdout = io.MultiWriter(log, response)
	}
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("run %s agent: %w", stage, ctx.Err())
		}
		return fmt.Errorf("run %s agent: %w", stage, err)
	}
	if r.ProcessObserver != nil {
		r.ProcessObserver(command, cmd.Process.Pid, true)
		defer r.ProcessObserver(command, cmd.Process.Pid, false)
	}
	if err := cmd.Wait(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("run %s agent: %w", stage, ctx.Err())
		}
		return fmt.Errorf("run %s agent: %w", stage, err)
	}
	return nil
}

func expand(value, stage, systemPrompt, task, workdir string) string {
	replacer := strings.NewReplacer("{system_prompt}", systemPrompt, "{task}", task, "{workdir}", workdir, "{stage}", stage)
	return replacer.Replace(value)
}

func readLog(path string) string {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Sprintf("(could not read log: %v)", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return fmt.Sprintf("(could not read log: %v)", err)
	}
	if info.Size() <= evaluatorOutputLimit {
		data, err := io.ReadAll(io.LimitReader(file, evaluatorOutputLimit))
		if err != nil {
			return fmt.Sprintf("(could not read log: %v)", err)
		}
		return string(bytes.TrimRight(data, "\n"))
	}

	const marker = "… log truncated; showing recent output …\n"
	tailLimit := evaluatorOutputLimit - len(marker)
	offset := info.Size() - int64(tailLimit)
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Sprintf("(could not read log: %v)", err)
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(tailLimit)))
	if err != nil {
		return fmt.Sprintf("(could not read log: %v)", err)
	}
	if newline := bytes.IndexByte(data, '\n'); newline >= 0 {
		data = data[newline+1:]
	}
	return marker + string(bytes.TrimRight(data, "\n"))
}

func stdoutFirstLinePasses(output string) bool {
	for _, line := range strings.Split(output, "\n") {
		if strings.TrimSpace(line) != "" {
			return line == "PASS"
		}
	}
	return false
}

func copyOutput(dst io.Writer, content string) {
	if content != "" {
		fmt.Fprintln(dst, content)
	}
}
