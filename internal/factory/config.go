package factory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Config describes the agent process and prompt overrides used for each run.
type Config struct {
	Command                string                        `json:"command"`
	Args                   []string                      `json:"args"`
	PipelineChecks         [][]string                    `json:"pipeline_checks,omitempty"`
	ParallelImplementation *ParallelImplementationConfig `json:"parallel_implementation,omitempty"`
	PromptDir              string                        `json:"prompt_dir,omitempty"`
	StateDir               string                        `json:"state_dir,omitempty"`
	AgentTimeout           string                        `json:"agent_timeout,omitempty"`
	MonitorTimeout         string                        `json:"monitor_timeout,omitempty"`
}

// DefaultConfig returns a copy of the built-in pi command adapter.
func DefaultConfig() Config {
	return Config{
		Command:      "pi",
		Args:         []string{"-p", "--no-session", "--append-system-prompt", "{system_prompt}", "{task}"},
		AgentTimeout: "60m",
	}
}

// ConfigPath returns the XDG config path, defaulting to ~/.config when unset.
func ConfigPath() (string, error) {
	base := os.Getenv("XDG_CONFIG_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("find home directory: %w", err)
		}
		base = filepath.Join(home, ".config")
	}
	if !filepath.IsAbs(base) {
		return "", fmt.Errorf("XDG_CONFIG_HOME must be absolute")
	}
	return filepath.Join(base, "factory", "config.json"), nil
}

// LoadConfig reads and validates the factory config, falling back to defaults if absent.
func LoadConfig(path string) (Config, error) {
	if path == "" {
		var err error
		path, err = ConfigPath()
		if err != nil {
			return Config{}, err
		}
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		cfg := DefaultConfig()
		return cfg, nil
	}
	if err != nil {
		return Config{}, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Config
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %s: %w", path, err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Config{}, fmt.Errorf("parse config %s: expected one JSON value", path)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return cfg, nil
}

// Validate checks config fields before they can influence a child process or file path.
func (c Config) Validate() error {
	if _, err := c.agentTimeout(); err != nil {
		return err
	}
	if _, err := c.monitorTimeout(); err != nil {
		return err
	}
	if strings.TrimSpace(c.Command) == "" {
		return fmt.Errorf("command must not be empty")
	}
	if strings.ContainsRune(c.Command, 0) {
		return fmt.Errorf("command contains NUL")
	}
	var taskCount, systemPromptCount int
	for i, arg := range c.Args {
		if strings.ContainsRune(arg, 0) {
			return fmt.Errorf("argument %d contains NUL", i)
		}
		taskCount += strings.Count(arg, "{task}")
		systemPromptCount += strings.Count(arg, "{system_prompt}")
	}
	taskCount += strings.Count(c.Command, "{task}")
	systemPromptCount += strings.Count(c.Command, "{system_prompt}")
	if taskCount != 1 {
		return fmt.Errorf("command and arguments must contain {task} exactly once (found %d)", taskCount)
	}
	if systemPromptCount != 1 {
		return fmt.Errorf("command and arguments must contain {system_prompt} exactly once (found %d)", systemPromptCount)
	}
	if err := validatePipelineChecks(c.PipelineChecks); err != nil {
		return err
	}
	if c.ParallelImplementation != nil {
		if c.ParallelImplementation.MaxConcurrency < 0 || c.ParallelImplementation.MaxConcurrency > maxParallelSubtasks {
			return fmt.Errorf("parallel_implementation.max_concurrency must be between 0 and %d", maxParallelSubtasks)
		}
	}
	if c.PromptDir != "" && strings.ContainsRune(c.PromptDir, 0) {
		return fmt.Errorf("prompt_dir contains NUL")
	}
	if c.StateDir != "" && !filepath.IsAbs(c.StateDir) {
		return fmt.Errorf("state_dir must be absolute")
	}
	return nil
}

func validatePipelineChecks(checks [][]string) error {
	for i, check := range checks {
		if len(check) == 0 {
			return fmt.Errorf("pipeline check %d must not be empty", i)
		}
		if strings.TrimSpace(check[0]) == "" {
			return fmt.Errorf("pipeline check %d executable must not be empty", i)
		}
		for j, arg := range check {
			if strings.ContainsRune(arg, 0) {
				return fmt.Errorf("pipeline check %d argument %d contains NUL", i, j)
			}
		}
	}
	return nil
}

func (c Config) monitorTimeout() (time.Duration, error) {
	if c.MonitorTimeout == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(c.MonitorTimeout)
	if err != nil {
		return 0, fmt.Errorf("monitor_timeout must be a valid duration: %w", err)
	}
	if duration <= 0 {
		return 0, fmt.Errorf("monitor_timeout must be positive")
	}
	return duration, nil
}

func (c Config) agentTimeout() (time.Duration, error) {
	if c.AgentTimeout == "" {
		return 60 * time.Minute, nil
	}
	duration, err := time.ParseDuration(c.AgentTimeout)
	if err != nil {
		return 0, fmt.Errorf("agent_timeout must be a valid duration: %w", err)
	}
	if duration <= 0 {
		return 0, fmt.Errorf("agent_timeout must be positive")
	}
	return duration, nil
}
