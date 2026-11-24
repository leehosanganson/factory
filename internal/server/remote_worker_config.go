package restserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// RemoteWorkerConfig contains only local execution choices and Controller
// credentials. It deliberately has no persistence or human API fields.
type RemoteWorkerConfig struct {
	ControllerBaseURL  string                         `json:"controller_base_url"`
	AllowInsecureHTTP  bool                           `json:"allow_insecure_http,omitempty"`
	SharedSecretFile   string                         `json:"shared_secret_file"`
	WorkerID           string                         `json:"worker_id"`
	MaxConcurrentJobs  int                            `json:"max_concurrent_jobs,omitempty"`
	Repositories       map[string]string              `json:"repositories"`
	HarnessProfiles    map[string]RemoteHarnessConfig `json:"harness_profiles"`
	VerificationChecks [][]string                     `json:"verification_checks,omitempty"`
	Limits             RemoteWorkerLimits             `json:"limits"`
}

// RemoteHarnessConfig is a locally trusted harness and its repository scope.
type RemoteHarnessConfig struct {
	Adapter      string   `json:"adapter,omitempty"`
	Executable   string   `json:"executable"`
	Args         []string `json:"args"`
	Repositories []string `json:"repositories"`
}

// RemoteWorkerLimits bounds execution on this Worker.
type RemoteWorkerLimits struct {
	JobTimeout         string `json:"job_timeout"`
	HarnessOutputBytes int    `json:"harness_output_bytes"`
}

var remoteWorkerProfileNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

// LoadRemoteWorkerConfig reads a strict standalone configuration. It does not
// read server, API-caller, or database settings.
func LoadRemoteWorkerConfig(path string) (RemoteWorkerConfig, error) {
	if path == "" {
		return RemoteWorkerConfig{}, errors.New("remote Worker config path must not be empty")
	}
	file, err := os.Open(path)
	if err != nil {
		return RemoteWorkerConfig{}, fmt.Errorf("open remote Worker config: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, configFileLimit+1))
	if err != nil || len(data) > configFileLimit {
		return RemoteWorkerConfig{}, errors.New("read remote Worker config: invalid or oversized config")
	}
	if err := validateUniqueJSON(data); err != nil {
		return RemoteWorkerConfig{}, errors.New("parse remote Worker config: invalid JSON")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return RemoteWorkerConfig{}, errors.New("parse remote Worker config: invalid schema")
	}
	if raw, exists := fields["allow_insecure_http"]; exists && string(bytes.TrimSpace(raw)) != "true" && string(bytes.TrimSpace(raw)) != "false" {
		return RemoteWorkerConfig{}, errors.New("parse remote Worker config: allow_insecure_http must be a boolean")
	}
	config := RemoteWorkerConfig{MaxConcurrentJobs: 1}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil || decoder.Decode(new(any)) != io.EOF {
		return RemoteWorkerConfig{}, errors.New("parse remote Worker config: invalid schema")
	}
	if config.MaxConcurrentJobs == 0 {
		return RemoteWorkerConfig{}, errors.New("invalid remote Worker config: max_concurrent_jobs must be 1 or 2")
	}
	if err := config.Validate(); err != nil {
		return RemoteWorkerConfig{}, fmt.Errorf("invalid remote Worker config: %w", err)
	}
	return config, nil
}

func (c RemoteWorkerConfig) Validate() error {
	parsed, err := url.Parse(c.ControllerBaseURL)
	if err != nil || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" && parsed.Path != "/" || strings.ContainsRune(c.ControllerBaseURL, 0) {
		return errors.New("controller_base_url must be an HTTPS origin or an HTTP localhost origin")
	}
	switch parsed.Scheme {
	case "https":
	case "http":
		host := parsed.Hostname()
		ip := net.ParseIP(host)
		if !c.AllowInsecureHTTP && host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return errors.New("HTTP Controller URLs are allowed only on loopback unless allow_insecure_http is enabled")
		}
	default:
		return errors.New("controller_base_url must use HTTPS, except for loopback HTTP or explicit allow_insecure_http")
	}
	if !filepath.IsAbs(c.SharedSecretFile) || strings.TrimSpace(c.SharedSecretFile) == "" || strings.ContainsRune(c.SharedSecretFile, 0) {
		return errors.New("shared_secret_file must be an absolute path")
	}
	if !postgresWorkerIDPattern.MatchString(c.WorkerID) {
		return errors.New("worker_id is invalid")
	}
	if c.MaxConcurrentJobs < 0 || c.MaxConcurrentJobs > 2 {
		return errors.New("max_concurrent_jobs must be 1 or 2")
	}
	if len(c.Repositories) == 0 || len(c.HarnessProfiles) == 0 {
		return errors.New("repositories and harness_profiles must not be empty")
	}
	for alias, root := range c.Repositories {
		if !aliasPattern.MatchString(alias) || !filepath.IsAbs(root) || strings.TrimSpace(root) == "" || strings.ContainsRune(root, 0) {
			return errors.New("repositories must map valid aliases to absolute local paths")
		}
	}
	supportedRepositories := make(map[string]struct{})
	for name, profile := range c.HarnessProfiles {
		if !remoteWorkerProfileNamePattern.MatchString(name) || strings.TrimSpace(profile.Executable) == "" || strings.ContainsAny(profile.Executable, "{}") || strings.ContainsRune(profile.Executable, 0) {
			return errors.New("harness profile name or executable is invalid")
		}
		switch profile.Adapter {
		case "":
			if c.MaxConcurrentJobs > 1 {
				return errors.New("max_concurrent_jobs greater than 1 requires the pi adapter")
			}
			if err := validateHarnessArgs(profile.Args); err != nil {
				return fmt.Errorf("harness profile %q: %w", name, err)
			}
		case "pi":
			if !filepath.IsAbs(profile.Executable) || len(profile.Args) != 0 || len(c.HarnessProfiles) != 1 || len(c.VerificationChecks) != 0 {
				return errors.New("pi requires an absolute executable, no args or verification checks, and a single harness profile")
			}
		default:
			return fmt.Errorf("harness profile %q: unknown adapter", name)
		}
		if len(profile.Repositories) == 0 {
			return fmt.Errorf("harness profile %q must support at least one repository", name)
		}
		seen := make(map[string]struct{}, len(profile.Repositories))
		for _, alias := range profile.Repositories {
			if _, ok := c.Repositories[alias]; !ok {
				return fmt.Errorf("harness profile %q references an unknown repository", name)
			}
			if _, ok := seen[alias]; ok {
				return fmt.Errorf("harness profile %q contains a duplicate repository", name)
			}
			seen[alias] = struct{}{}
			supportedRepositories[alias] = struct{}{}
		}
	}
	if len(supportedRepositories) > 16 {
		return errors.New("harness_profiles support more than 16 repository aliases")
	}
	if len(c.VerificationChecks) > 0 {
		if err := validateVerificationChecks(c.VerificationChecks); err != nil {
			return err
		}
	}
	if _, err := time.ParseDuration(c.Limits.JobTimeout); err != nil || strings.TrimSpace(c.Limits.JobTimeout) == "" {
		return errors.New("limits.job_timeout must be a positive duration")
	}
	timeout, _ := time.ParseDuration(c.Limits.JobTimeout)
	if timeout <= 0 || timeout > maxJobTimeout || c.Limits.HarnessOutputBytes < 1 || c.Limits.HarnessOutputBytes > maxHarnessOutput {
		return errors.New("limits are outside supported bounds")
	}
	return nil
}

// validateUniqueJSON rejects duplicate object keys without imposing a schema;
// the typed decoder below applies strict field validation.
func validateUniqueJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := consumeUniqueJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("expected one JSON value")
	}
	return nil
}

func consumeUniqueJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil || token == nil {
		return errors.New("invalid JSON value")
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok {
				return errors.New("invalid object key")
			}
			if _, ok := seen[name]; ok {
				return errors.New("duplicate object key")
			}
			seen[name] = struct{}{}
			if err := consumeUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := consumeUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return errors.New("invalid JSON delimiter")
	}
}
