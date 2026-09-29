package factory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

const managedHeartbeatInterval = 5 * time.Second
const managedStopPollInterval = 200 * time.Millisecond
const managedOwnerFreshness = 30 * time.Second

var managedRunIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

type managedOwner struct {
	PID         int       `json:"pid"`
	StartedAt   time.Time `json:"started_at"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
}

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

func readManagedOwner(runDir string) (managedOwner, error) {
	var owner managedOwner
	path := filepath.Join(runDir, "owner.json")
	if err := ensureRegularIfExists(path); err != nil {
		return owner, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return owner, err
	}
	if err := json.Unmarshal(data, &owner); err != nil {
		return owner, err
	}
	if owner.PID <= 0 || owner.StartedAt.IsZero() || owner.HeartbeatAt.IsZero() {
		return managedOwner{}, fmt.Errorf("invalid run owner record")
	}
	return owner, nil
}

func managedStopRequested(runDir string) bool {
	info, err := os.Lstat(filepath.Join(runDir, "stop.request"))
	return err == nil && info.Mode().IsRegular()
}

func requestManagedStop(runDir string) error {
	return ensureCancelFile(filepath.Join(runDir, "stop.request"))
}

func managedLiveness(owner managedOwner, now time.Time) string {
	if owner.PID <= 0 || owner.StartedAt.IsZero() || owner.HeartbeatAt.IsZero() || owner.HeartbeatAt.After(now) {
		return "unknown"
	}
	if now.Sub(owner.HeartbeatAt) <= managedOwnerFreshness {
		return "heartbeat_fresh"
	}
	return "heartbeat_stale"
}

func managedRunRoot(override string) (string, error) {
	root, err := StateRoot(override)
	if err != nil {
		return "", err
	}
	return resolvedPath(root)
}

// RunCommand controls only gated, foreground pipeline records.
func RunCommand(ctx context.Context, args []string, cfg Config, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: factory run list | get <id> [--details] | events <id> [--follow] | stop <id>")
	}
	root, err := managedRunRoot(cfg.StateDir)
	if err != nil {
		return err
	}
	switch args[0] {
	case "list":
		if len(args) != 1 {
			return fmt.Errorf("usage: factory run list")
		}
		entries, err := os.ReadDir(root)
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(out, "No gated runs.")
			return nil
		}
		if err != nil {
			return err
		}
		type runRow struct {
			updatedAt time.Time
			id        string
			text      string
		}
		var rows []runRow
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			dir := filepath.Join(root, entry.Name())
			state, err := readManagedState(dir)
			if errors.Is(err, os.ErrNotExist) || err == nil && !state.Managed {
				continue
			}
			if err != nil {
				return fmt.Errorf("read run %s: %w", entry.Name(), err)
			}
			life := "terminal"
			if !isTerminalStatus(state.Status) {
				life, err = managedRunLiveness(dir)
				if err != nil {
					return err
				}
			}
			rows = append(rows, runRow{
				updatedAt: state.UpdatedAt,
				id:        state.ID,
				text:      fmt.Sprintf("%-32s %-16s %-20s %s", state.ID, state.Status, life, state.Stage),
			})
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].updatedAt.Equal(rows[j].updatedAt) {
				return rows[i].id < rows[j].id
			}
			return rows[i].updatedAt.After(rows[j].updatedAt)
		})
		if len(rows) == 0 {
			fmt.Fprintln(out, "No gated runs.")
			return nil
		}
		fmt.Fprintf(out, "%-32s %-16s %-20s %s\n", "ID", "STATUS", "LIVENESS", "STAGE")
		for _, row := range rows {
			fmt.Fprintln(out, row.text)
		}
		return nil
	case "get":
		id, details, err := parseDetailsID("factory run get <id> [--details]", args[1:])
		if err != nil {
			return err
		}
		dir, state, err := managedRunByID(root, id)
		if err != nil {
			return err
		}
		life := "terminal"
		if !isTerminalStatus(state.Status) {
			life, err = managedRunLiveness(dir)
			if err != nil {
				return err
			}
		}
		fmt.Fprintf(out, "ID: %s\nStatus: %s\nLiveness: %s\nStage: %s\n", state.ID, state.Status, life, state.Stage)
		if !details {
			return nil
		}
		fmt.Fprintf(out, "Target: %s\nUpdated: %s\nDescription: %s\n", state.Workdir, state.UpdatedAt.Format(time.RFC3339), state.Task)
		for _, name := range []string{"task.txt", "workflow-events.jsonl"} {
			path := filepath.Join(dir, name)
			fmt.Fprintf(out, "%s path: %s\n", name, path)
			data, err := os.ReadFile(path)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			if len(data) > 0 {
				fmt.Fprintf(out, "%s:\n%s", name, data)
				if data[len(data)-1] != '\n' {
					fmt.Fprintln(out)
				}
			}
		}
		return nil
	case "events":
		follow := false
		if len(args) == 3 && args[2] == "--follow" {
			follow = true
		} else if len(args) != 2 {
			return fmt.Errorf("usage: factory run events <id> [--follow]")
		}
		dir, _, err := managedRunByID(root, args[1])
		if err != nil {
			return err
		}
		path := filepath.Join(dir, "workflow-events.jsonl")
		if err := ensureRegularIfExists(path); err != nil {
			return err
		}
		if !follow {
			data, err := os.ReadFile(path)
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			_, err = out.Write(data)
			return err
		}
		return followLog(ctx, path, func() (bool, error) {
			current, err := readManagedState(dir)
			if err != nil {
				return false, err
			}
			return isTerminalStatus(current.Status), nil
		}, out)
	case "stop":
		if len(args) != 2 {
			return fmt.Errorf("usage: factory run stop <id>")
		}
		dir, state, err := managedRunByID(root, args[1])
		if err != nil {
			return err
		}
		if isTerminalStatus(state.Status) {
			return fmt.Errorf("run %s is already %s", state.ID, state.Status)
		}
		if err := requestManagedStop(dir); err != nil {
			return err
		}
		fmt.Fprintf(out, "Stop requested for gated run %s.\n", state.ID)
		return nil
	default:
		return fmt.Errorf("unknown run command %q", args[0])
	}
}

func managedRunLiveness(dir string) (string, error) {
	owner, err := readManagedOwner(dir)
	if errors.Is(err, os.ErrNotExist) {
		return "unknown", nil
	}
	if err != nil {
		return "unknown", fmt.Errorf("read run owner: %w", err)
	}
	return managedLiveness(owner, time.Now()), nil
}

func managedRunByID(root, id string) (string, State, error) {
	var zero State
	if !managedRunIDPattern.MatchString(id) || id == "." || id == ".." {
		return "", zero, fmt.Errorf("invalid run ID %q", id)
	}
	dir := filepath.Join(root, id)
	if err := ensureRealDirectory(root, dir); err != nil {
		return "", zero, err
	}
	state, err := readManagedState(dir)
	if err != nil {
		return "", zero, err
	}
	if !state.Managed {
		return "", zero, fmt.Errorf("run %s is not a gated, manageable run", id)
	}
	return dir, state, nil
}

func readManagedState(dir string) (State, error) {
	var state State
	if err := readJSONRegular(filepath.Join(dir, "state.json"), &state); err != nil {
		return state, err
	}
	if state.ID != filepath.Base(dir) || state.Managed && state.StageHistoryVersion != 1 {
		return State{}, fmt.Errorf("invalid or unsupported run record")
	}
	return state, nil
}
