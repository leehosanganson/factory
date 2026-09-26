//go:build darwin

package factory

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

func launchBabysitWorker(id, stateRoot string) (int, error) {
	executable, err := os.Executable()
	if err != nil {
		return 0, err
	}
	jobDir := stateRoot + string(os.PathSeparator) + id
	log, err := os.OpenFile(jobDir+string(os.PathSeparator)+"supervisor.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, err
	}
	cmd := exec.Command(executable, "__monitor-worker", id)
	cmd.Stdin = nil
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		return 0, fmt.Errorf("start detached worker: %w", err)
	}
	_ = log.Close()
	pid := cmd.Process.Pid
	if err := cmd.Process.Release(); err != nil {
		return 0, err
	}
	return pid, nil
}
