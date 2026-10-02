package restserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const configFileLimit = 1 << 20

const (
	PersistenceBackendMemory = "memory"
	PersistenceBackendSQLite = "sqlite"
)

const (
	defaultListenAddress    = "127.0.0.1:8080"
	defaultRequestBodyBytes = 512 << 10
	defaultTaskBytes        = 256 << 10
	defaultQueueCapacity    = 32
	defaultWorkers          = 2
	defaultMaxRecords       = 1000
	defaultMaxEventsPerJob  = 200
	defaultRegistryBytes    = 256 << 20
	defaultJobTimeout       = 30 * time.Minute
	defaultHarnessOutput    = 1 << 20

	maxRequestBodyBytes   = 2 << 20
	maxTaskBytes          = 256 << 10
	maxQueueCapacity      = 1024
	maxWorkers            = 64
	maxRecords            = 1000
	maxEventsPerJob       = 200
	maxRegistryBytes      = 256 << 20
	maxJobTimeout         = 24 * time.Hour
	maxHarnessOutput      = 16 << 20
	maxVerificationChecks = 16
	maxVerificationArgs   = 32
	maxVerificationBytes  = 16 << 10
)

var aliasPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

// Config contains server-only settings. It is independent of the CLI config.
type Config struct {
	Mode               string            `json:"mode"`
	ListenAddress      string            `json:"listen_address"`
	Repositories       map[string]string `json:"repositories"`
	Harness            HarnessConfig     `json:"harness"`
	VerificationChecks [][]string        `json:"verification_checks,omitempty"`
	APIKeyFile         string            `json:"api_key_file"`
	Persistence        PersistenceConfig `json:"persistence"`
	Provider           ProviderConfig    `json:"provider"`
	Limits             Limits            `json:"limits"`
}

// HarnessConfig is a fixed executable and argument vector selected by the operator.
type HarnessConfig struct {
	Executable string   `json:"executable"`
	Args       []string `json:"args"`
}

// Limits bounds incoming work and local-process execution.
type Limits struct {
	RequestBodyBytes int    `json:"request_body_bytes"`
	TaskBytes        int    `json:"task_bytes"`
	QueueCapacity    int    `json:"queue_capacity"`
	Workers          int    `json:"workers"`
	MaxRecords       int    `json:"max_records"`
	MaxEventsPerJob  int    `json:"max_events_per_job"`
	RegistryBytes    int64  `json:"registry_bytes"`
	JobTimeout       string `json:"job_timeout"`
	HarnessOutput    int    `json:"harness_output_bytes"`
}

// PersistenceConfig selects volatile memory or an explicitly configured
// file-backed SQLite store.
type PersistenceConfig struct {
	Backend string `json:"backend"`
	Path    string `json:"path,omitempty"`
}

type ProviderConfig struct {
	Backend      string            `json:"backend,omitempty"`
	TokenFile    string            `json:"token_file,omitempty"`
	BaseBranch   string            `json:"base_branch,omitempty"`
	Repositories map[string]string `json:"repositories,omitempty"`
}

// DefaultConfig returns conservative resource limits and a loopback-only listener.
// Aliases, harness, and key path remain deployment-specific.
func DefaultConfig() Config {
	return Config{
		Mode:          "local_process",
		ListenAddress: defaultListenAddress,
		Persistence:   PersistenceConfig{Backend: PersistenceBackendMemory},
		Limits: Limits{
			RequestBodyBytes: defaultRequestBodyBytes,
			TaskBytes:        defaultTaskBytes,
			QueueCapacity:    defaultQueueCapacity,
			Workers:          defaultWorkers,
			MaxRecords:       defaultMaxRecords,
			MaxEventsPerJob:  defaultMaxEventsPerJob,
			RegistryBytes:    defaultRegistryBytes,
			JobTimeout:       defaultJobTimeout.String(),
			HarnessOutput:    defaultHarnessOutput,
		},
	}
}

// LoadConfig reads a strict JSON config. Omitted resource limits receive defaults.
func LoadConfig(path string) (Config, error) {
	if path == "" {
		return Config{}, errors.New("REST server config path must not be empty")
	}
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open REST server config: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, configFileLimit+1))
	if err != nil {
		return Config{}, fmt.Errorf("read REST server config: %w", err)
	}
	if len(data) > configFileLimit {
		return Config{}, fmt.Errorf("REST server config exceeds %d bytes", configFileLimit)
	}
	if err := validateConfigJSON(data); err != nil {
		return Config{}, fmt.Errorf("parse REST server config: invalid JSON schema")
	}
	if err := validateRequiredConfigFields(data); err != nil {
		return Config{}, fmt.Errorf("parse REST server config: %w", err)
	}

	config := DefaultConfig()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("parse REST server config: invalid JSON schema")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return Config{}, errors.New("parse REST server config: expected one JSON value")
	}
	if err := config.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid REST server config: %w", err)
	}
	return config, nil
}

// Validate checks server-only configuration without consulting CLI settings.
func (c Config) Validate() error {
	if c.Mode != "local_process" {
		return errors.New("mode must be local_process")
	}
	switch c.Persistence.Backend {
	case PersistenceBackendMemory:
		if c.Persistence.Path != "" {
			return errors.New("persistence.path is only valid with sqlite backend")
		}
	case PersistenceBackendSQLite:
		if !filepath.IsAbs(c.Persistence.Path) || strings.TrimSpace(c.Persistence.Path) == "" || strings.ContainsRune(c.Persistence.Path, 0) {
			return errors.New("persistence.path must be an absolute SQLite database file path")
		}
	default:
		return errors.New("persistence.backend must be memory or sqlite")
	}
	if c.Provider.Backend != "" && c.Provider.Backend != "github" {
		return errors.New("provider.backend must be github when configured")
	}
	if c.Provider.Backend == "github" {
		if !filepath.IsAbs(c.Provider.TokenFile) || strings.TrimSpace(c.Provider.TokenFile) == "" || strings.ContainsRune(c.Provider.TokenFile, 0) {
			return errors.New("provider.token_file must be an absolute path")
		}
		if !validBranchName(c.Provider.BaseBranch) {
			return errors.New("provider.base_branch must be a valid branch name")
		}
		if len(c.Provider.Repositories) != len(c.Repositories) {
			return errors.New("provider.repositories must map every configured repository alias")
		}
		for alias, repo := range c.Provider.Repositories {
			if _, ok := c.Repositories[alias]; !ok || !validGitHubRepository(repo) {
				return errors.New("provider.repositories must map configured aliases to valid owner/repository names")
			}
		}
	} else if c.Provider.TokenFile != "" || c.Provider.BaseBranch != "" || len(c.Provider.Repositories) != 0 {
		return errors.New("provider settings require provider.backend github")
	}
	if err := validateListenAddress(c.ListenAddress); err != nil {
		return err
	}
	if len(c.Repositories) == 0 {
		return errors.New("repositories must contain at least one alias")
	}
	for alias, root := range c.Repositories {
		if !aliasPattern.MatchString(alias) {
			return fmt.Errorf("repository alias %q is invalid", alias)
		}
		if !filepath.IsAbs(root) || strings.TrimSpace(root) == "" || strings.ContainsRune(root, 0) {
			return fmt.Errorf("repository %q root must be an absolute path", alias)
		}
	}
	if strings.TrimSpace(c.Harness.Executable) == "" || strings.ContainsRune(c.Harness.Executable, 0) || strings.ContainsAny(c.Harness.Executable, "{}") {
		return errors.New("harness executable must be nonempty and contain no placeholders")
	}
	if err := validateHarnessArgs(c.Harness.Args); err != nil {
		return err
	}
	if err := validateVerificationChecks(c.VerificationChecks); err != nil {
		return err
	}
	if !filepath.IsAbs(c.APIKeyFile) || strings.TrimSpace(c.APIKeyFile) == "" || strings.ContainsRune(c.APIKeyFile, 0) {
		return errors.New("api_key_file must be an absolute path")
	}
	if err := c.Limits.validate(); err != nil {
		return err
	}
	return nil
}

func validateListenAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(host) == "" || strings.ContainsAny(host, " \t\r\n") {
		return errors.New("listen_address must be a host:port address")
	}
	var portNumber int
	if _, err := fmt.Sscanf(port, "%d", &portNumber); err != nil || portNumber < 1 || portNumber > 65535 || fmt.Sprintf("%d", portNumber) != port {
		return errors.New("listen_address port must be between 1 and 65535")
	}
	return nil
}

var providerBranchPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$`)
var providerRepositoryPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)

func validBranchName(value string) bool {
	return providerBranchPattern.MatchString(value) && !strings.Contains(value, "..") && !strings.Contains(value, "//")
}
func validGitHubRepository(value string) bool {
	return providerRepositoryPattern.MatchString(value) && !strings.Contains(value, "..")
}

func validateVerificationChecks(checks [][]string) error {
	if len(checks) > maxVerificationChecks {
		return fmt.Errorf("verification_checks must contain at most %d commands", maxVerificationChecks)
	}
	for index, check := range checks {
		if len(check) == 0 || len(check) > maxVerificationArgs || strings.TrimSpace(check[0]) == "" {
			return fmt.Errorf("verification check %d must contain an executable and at most %d arguments", index, maxVerificationArgs)
		}
		totalBytes := 0
		for _, arg := range check {
			if strings.TrimSpace(arg) == "" || strings.ContainsRune(arg, 0) {
				return fmt.Errorf("verification check %d contains an empty or invalid argument", index)
			}
			totalBytes += len(arg)
		}
		if totalBytes > maxVerificationBytes {
			return fmt.Errorf("verification check %d exceeds %d bytes", index, maxVerificationBytes)
		}
	}
	return nil
}

func validateHarnessArgs(args []string) error {
	if len(args) == 0 {
		return errors.New("harness args must not be empty")
	}
	counts := map[string]int{"{task}": 0, "{system_prompt}": 0}
	for index, arg := range args {
		if strings.ContainsRune(arg, 0) {
			return fmt.Errorf("harness argument %d contains NUL", index)
		}
		remaining := arg
		for _, placeholder := range []string{"{task}", "{system_prompt}"} {
			counts[placeholder] += strings.Count(remaining, placeholder)
			remaining = strings.ReplaceAll(remaining, placeholder, "")
		}
		if strings.ContainsAny(remaining, "{}") {
			return fmt.Errorf("harness argument %d contains an unsupported placeholder", index)
		}
	}
	if counts["{task}"] != 1 || counts["{system_prompt}"] != 1 {
		return errors.New("harness args must contain {task} and {system_prompt} exactly once each")
	}
	return nil
}

func (l Limits) validate() error {
	if l.RequestBodyBytes < 1 || l.RequestBodyBytes > maxRequestBodyBytes {
		return fmt.Errorf("limits.request_body_bytes must be between 1 and %d", maxRequestBodyBytes)
	}
	if l.TaskBytes < 1 || l.TaskBytes > maxTaskBytes || l.TaskBytes > l.RequestBodyBytes {
		return fmt.Errorf("limits.task_bytes must be positive, at most %d, and no greater than request_body_bytes", maxTaskBytes)
	}
	if l.QueueCapacity < 1 || l.QueueCapacity > maxQueueCapacity {
		return fmt.Errorf("limits.queue_capacity must be between 1 and %d", maxQueueCapacity)
	}
	if l.Workers < 1 || l.Workers > maxWorkers {
		return fmt.Errorf("limits.workers must be between 1 and %d", maxWorkers)
	}
	timeout, err := time.ParseDuration(l.JobTimeout)
	if err != nil || timeout <= 0 || timeout > maxJobTimeout {
		return fmt.Errorf("limits.job_timeout must be positive and no greater than %s", maxJobTimeout)
	}
	if l.MaxRecords < 1 || l.MaxRecords > maxRecords {
		return fmt.Errorf("limits.max_records must be between 1 and %d", maxRecords)
	}
	if l.MaxEventsPerJob < 1 || l.MaxEventsPerJob > maxEventsPerJob {
		return fmt.Errorf("limits.max_events_per_job must be between 1 and %d", maxEventsPerJob)
	}
	if l.RegistryBytes < 1 || l.RegistryBytes > maxRegistryBytes {
		return fmt.Errorf("limits.registry_bytes must be between 1 and %d", maxRegistryBytes)
	}
	if l.HarnessOutput < 1 || l.HarnessOutput > maxHarnessOutput {
		return fmt.Errorf("limits.harness_output_bytes must be between 1 and %d", maxHarnessOutput)
	}
	return nil
}

func validateConfigJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := consumeConfigJSONValue(decoder, ""); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("expected one JSON value")
	}
	return nil
}

func consumeConfigJSONValue(decoder *json.Decoder, parent string) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return errors.New("null values are not allowed")
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("invalid JSON key")
			}
			if _, duplicate := seen[key]; duplicate {
				return errors.New("duplicate JSON key")
			}
			seen[key] = struct{}{}
			if !configFieldAllowed(parent, key) {
				return errors.New("unknown JSON field")
			}
			if err := consumeConfigJSONValue(decoder, configChild(key)); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := consumeConfigJSONValue(decoder, ""); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return errors.New("invalid JSON delimiter")
	}
}

func configFieldAllowed(parent, key string) bool {
	var fields map[string]struct{}
	switch parent {
	case "":
		fields = map[string]struct{}{"mode": {}, "listen_address": {}, "repositories": {}, "harness": {}, "verification_checks": {}, "api_key_file": {}, "persistence": {}, "provider": {}, "limits": {}}
	case "harness":
		fields = map[string]struct{}{"executable": {}, "args": {}}
	case "persistence":
		fields = map[string]struct{}{"backend": {}, "path": {}}
	case "provider":
		if key == "backend" || key == "token_file" || key == "base_branch" {
			return true
		}
		if key == "repositories" {
			return true
		}
	case "limits":
		fields = map[string]struct{}{"request_body_bytes": {}, "task_bytes": {}, "queue_capacity": {}, "workers": {}, "max_records": {}, "max_events_per_job": {}, "registry_bytes": {}, "job_timeout": {}, "harness_output_bytes": {}}
	case "repositories":
		return aliasPattern.MatchString(key)
	default:
		return true
	}
	_, ok := fields[key]
	return ok
}

func validateRequiredConfigFields(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return errors.New("invalid root object")
	}
	for _, field := range []string{"mode", "repositories", "harness", "api_key_file"} {
		if _, ok := fields[field]; !ok {
			return fmt.Errorf("required field %q is missing", field)
		}
	}
	var harness map[string]json.RawMessage
	if err := json.Unmarshal(fields["harness"], &harness); err != nil {
		return errors.New("harness must be an object")
	}
	for _, field := range []string{"executable", "args"} {
		if _, ok := harness[field]; !ok {
			return fmt.Errorf("required harness field %q is missing", field)
		}
	}
	return nil
}

func configChild(key string) string {
	switch key {
	case "harness", "limits", "persistence", "provider", "repositories", "verification_checks":
		return key
	default:
		return ""
	}
}
