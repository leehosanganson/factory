//go:build linux

package factory

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

func launchJobWorker(id, root string) (int, error) {
	executable, err := os.Executable()
	if err != nil {
		return 0, err
	}
	store, err := NewJobStore(root)
	if err != nil {
		return 0, err
	}
	logPath, err := store.JobLogPath(id)
	if err != nil {
		return 0, err
	}
	log, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, err
	}
	cmd := exec.Command(executable, "__job-worker", id, store.Root())
	cmd.Stdin = nil
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		return 0, fmt.Errorf("start detached job worker: %w", err)
	}
	_ = log.Close()
	pid := cmd.Process.Pid
	if err := cmd.Process.Release(); err != nil {
		return 0, err
	}
	return pid, nil
}
