package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const managedHeartbeatInterval = 5 * time.Second
const managedStopPollInterval = 200 * time.Millisecond

type managedRun struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func startManagedRun(parent context.Context, runDir string) (*managedRun, error) {
	now := time.Now().UTC()
	owner := managedOwner{PID: os.Getpid(), StartedAt: now, HeartbeatAt: now}
	if err := writeManagedOwner(runDir, owner); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	managed := &managedRun{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(managed.done)
		heartbeat := time.NewTicker(managedHeartbeatInterval)
		poll := time.NewTicker(managedStopPollInterval)
		defer heartbeat.Stop()
		defer poll.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-poll.C:
				if managedStopRequested(runDir) {
					cancel()
					return
				}
			case <-heartbeat.C:
				owner.HeartbeatAt = time.Now().UTC()
				_ = writeManagedOwner(runDir, owner)
			}
		}
	}()
	return managed, nil
}

func (m *managedRun) close() {
	m.cancel()
	<-m.done
}

type managedOwner struct {
	PID         int       `json:"pid"`
	StartedAt   time.Time `json:"started_at"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
}

func writeManagedOwner(runDir string, owner managedOwner) error {
	now := time.Now().UTC()
	if owner.PID <= 0 {
		return fmt.Errorf("owner PID must be positive")
	}
	if owner.StartedAt.IsZero() {
		owner.StartedAt = now
	}
	if owner.HeartbeatAt.IsZero() {
		owner.HeartbeatAt = now
	}
	data, err := json.MarshalIndent(owner, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(runDir, ".owner-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, filepath.Join(runDir, "owner.json"))
}

func managedStopRequested(runDir string) bool {
	info, err := os.Lstat(filepath.Join(runDir, "stop.request"))
	return err == nil && info.Mode().IsRegular()
}
