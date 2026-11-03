package workflow

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// State records progress without placing workflow data in the target repository.
type State struct {
	ID                  string          `json:"id"`
	Task                string          `json:"task"`
	Workdir             string          `json:"workdir"`
	Stage               string          `json:"stage,omitempty"`
	Status              string          `json:"status"`
	Managed             bool            `json:"managed,omitempty"`
	StageHistoryVersion int             `json:"stage_history_version"`
	Stages              []StageRecord   `json:"stages"`
	Checks              []CheckResult   `json:"checks,omitempty"`
	Subtasks            []SubtaskRecord `json:"subtasks,omitempty"`
	SubtaskPlanStatus   string          `json:"subtask_plan_status,omitempty"`
	SubtaskPlanLog      string          `json:"subtask_plan_log,omitempty"`
	OutputTruncated     bool            `json:"output_truncated,omitempty"`
	UpdatedAt           time.Time       `json:"updated_at"`
}

// StageRecord records one stage in the order it ran.
type StageRecord struct {
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at,omitempty"`
}

// CheckResult records one configured pipeline check and its durable log.
type CheckResult struct {
	Command   []string  `json:"command"`
	Log       string    `json:"log"`
	ExitCode  int       `json:"exit_code"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
}

func StateRoot(override string) (string, error) {
	if override != "" {
		if !filepath.IsAbs(override) {
			return "", fmt.Errorf("state directory must be absolute")
		}
		return filepath.Join(override, "runs"), nil
	}
	base := os.Getenv("XDG_STATE_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("find home directory: %w", err)
		}
		base = filepath.Join(home, ".local", "state")
	}
	if !filepath.IsAbs(base) {
		return "", fmt.Errorf("XDG_STATE_HOME must be absolute")
	}
	return filepath.Join(base, "factory", "runs"), nil
}

func ResolvedPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	abs = filepath.Clean(abs)
	current := abs
	var suffix []string
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return filepath.Clean(resolved), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
}

func IsWithin(parent, path string) bool {
	rel, err := filepath.Rel(parent, path)
	return err == nil && (rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))))
}

func CreateRun(root, workdir, task string) (string, *State, error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", nil, fmt.Errorf("create state directory: %w", err)
	}
	id, err := newUUIDv4()
	if err != nil {
		return "", nil, fmt.Errorf("create run ID: %w", err)
	}
	dir := filepath.Join(root, id)
	if err := os.Mkdir(dir, 0o700); err != nil {
		return "", nil, fmt.Errorf("create run directory: %w", err)
	}
	state := &State{
		ID: filepath.Base(dir), Task: task, Workdir: workdir, Status: "running",
		StageHistoryVersion: 1, Stages: []StageRecord{}, UpdatedAt: time.Now().UTC(),
	}
	if err := WriteState(dir, state); err != nil {
		return "", nil, err
	}
	if task != "" {
		if err := os.WriteFile(filepath.Join(dir, "task.txt"), []byte(task+"\n"), 0o600); err != nil {
			return "", nil, fmt.Errorf("write task file: %w", err)
		}
	}
	return dir, state, nil
}

func WriteState(dir string, state *State) error {
	state.UpdatedAt = time.Now().UTC()
	return writeJSONAtomic(dir, "state.json", state)
}

func writeJSONAtomic(dir, name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".record-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
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
	if err := os.Rename(tmpPath, filepath.Join(dir, name)); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
