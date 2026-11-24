package pi

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/leehosanganson/factory/internal/harness"
	"github.com/leehosanganson/factory/internal/workflow"
)

const shutdownGrace = time.Second

type Runner struct {
	Executable string
}

// Run captures Pi print-mode stdout only. The task goes through stdin so Pi
// cannot interpret its contents as an option or an @file argument.
// On cancellation Pi gets a chance to clean up tool groups it tracks. The
// fallback only targets Pi's original group, not arbitrary detached descendants.
func (r Runner) Run(ctx context.Context, task harness.Task) (harness.Reply, error) {
	args := []string{"-p", "--session-dir", task.StateDir, "--no-extensions", "--no-skills", "--no-mcp", "--no-prompt-templates", "--no-approve"}
	if task.Resume {
		matches, err := filepath.Glob(filepath.Join(task.StateDir, "*_thread.jsonl"))
		if err != nil || len(matches) != 1 {
			return harness.Reply{}, errors.New("retained Pi session is unavailable")
		}
		info, err := os.Lstat(matches[0])
		if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
			return harness.Reply{}, errors.New("retained Pi session is unavailable")
		}
		args = append(args, "--session", matches[0])
	} else {
		args = append(args, "--session-id", "thread")
	}
	cmd := exec.CommandContext(ctx, r.Executable, args...)
	cmd.Dir = task.WorkDir
	cmd.Stdin = strings.NewReader(task.Prompt)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "PI_CODING_AGENT_DIR=" + os.Getenv("PI_CODING_AGENT_DIR")}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		group := -cmd.Process.Pid
		if err := syscall.Kill(group, syscall.SIGTERM); errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		// WaitDelay can only kill the leader. Keep escalation in Cancel so it
		// still runs if Pi exits while other members of its group remain.
		time.Sleep(shutdownGrace)
		if err := syscall.Kill(group, syscall.SIGKILL); !errors.Is(err, syscall.ESRCH) {
			return err
		}
		return nil
	}
	cmd.WaitDelay = 2 * time.Second
	output := workflow.NewBoundedOutputWriter(ctx, nil, task.ReplyLimit)
	cmd.Stdout = output
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return harness.Reply{}, ctx.Err()
		}
		return harness.Reply{}, errors.New("Pi did not complete successfully")
	}
	raw := strings.ToValidUTF8(strings.TrimSuffix(output.String(), workflow.OutputTruncatedMarker), "")
	reply := strings.TrimSpace(workflow.TerminalSafeText(raw))
	return harness.Reply{Text: reply, Truncated: reply != "" && output.Truncated()}, nil
}
