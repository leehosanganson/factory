package factory

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
	ID        string    `json:"id"`
	Task      string    `json:"task"`
	Workdir   string    `json:"workdir"`
	Stage     string    `json:"stage,omitempty"`
	Status    string    `json:"status"`
	UpdatedAt time.Time `json:"updated_at"`
}

// StateRoot returns the state directory that contains run records.
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

func canonicalPath(path string) (string, error) {
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

func isWithin(parent, path string) bool {
	rel, err := filepath.Rel(parent, path)
	return err == nil && (rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))))
}

func createRun(root, workdir, task string) (string, *State, error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", nil, fmt.Errorf("create state directory: %w", err)
	}
	id := time.Now().UTC().Format("20060102T150405.000000000")
	dir, err := os.MkdirTemp(root, id+"-")
	if err != nil {
		return "", nil, fmt.Errorf("create run directory: %w", err)
	}
	state := &State{ID: filepath.Base(dir), Task: task, Workdir: workdir, Status: "running", UpdatedAt: time.Now().UTC()}
	if err := writeState(dir, state); err != nil {
		return "", nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "task.txt"), []byte(task+"\n"), 0o600); err != nil {
		return "", nil, fmt.Errorf("write task file: %w", err)
	}
	return dir, state, nil
}

func writeState(dir string, state *State) error {
	state.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "state.json"), append(data, '\n'), 0o600)
}
