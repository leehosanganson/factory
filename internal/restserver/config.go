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
	"strconv"
	"strings"
)

const configLimit = 1 << 20

var aliasPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
var githubOwnerPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$`)
var githubRepoPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$`)

// Config contains server-only settings. Secret values are intentionally not
// representable here; only paths to protected secret files are accepted.
type Config struct {
	Version       int                   `json:"version"`
	ListenAddress string                `json:"listen_address"`
	PostgresDSN   string                `json:"postgres_dsn"`
	MigrationDir  string                `json:"migration_dir"`
	Repositories  map[string]Repository `json:"repositories"`
	APIKeyFile    string                `json:"api_key_file"`
	PATFile       string                `json:"pat_file"`
}

// Repository maps a caller-visible alias to one approved checkout and its
// GitHub identity.
type Repository struct {
	CheckoutPath string `json:"checkout_path"`
	GitHubOwner  string `json:"github_owner"`
	GitHubRepo   string `json:"github_repo"`
}

// LoadConfig reads one strict JSON config and validates all server settings.
func LoadConfig(path string) (Config, error) {
	if path == "" {
		return Config{}, fmt.Errorf("REST server config path must not be empty")
	}
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open REST server config: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, configLimit+1))
	if err != nil {
		return Config{}, fmt.Errorf("read REST server config: %w", err)
	}
	if len(data) > configLimit {
		return Config{}, fmt.Errorf("REST server config exceeds %d bytes", configLimit)
	}
	if err := rejectDuplicateConfigFields(data); err != nil {
		return Config{}, fmt.Errorf("parse REST server config: duplicate or malformed JSON fields")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var cfg Config
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse REST server config: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("parse REST server config: expected one JSON value")
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid REST server config: %w", err)
	}
	return cfg, nil
}

// Validate checks that server config is complete and that configured checkout
// and migration paths resolve to existing, absolute directories.
func (c Config) Validate() error {
	if c.Version != 1 {
		return fmt.Errorf("version must be 1")
	}
	if err := validateListenAddress(c.ListenAddress); err != nil {
		return err
	}
	if err := validatePostgresDSN(c.PostgresDSN); err != nil {
		return err
	}
	if err := validateDirectory("migration_dir", c.MigrationDir); err != nil {
		return err
	}
	if len(c.Repositories) == 0 {
		return fmt.Errorf("repositories must contain at least one alias")
	}
	for alias, repository := range c.Repositories {
		if !aliasPattern.MatchString(alias) {
			return fmt.Errorf("repository alias %q is invalid", alias)
		}
		if err := validateDirectory("repository checkout_path", repository.CheckoutPath); err != nil {
			return fmt.Errorf("repository alias %q: %w", alias, err)
		}
		if !githubOwnerPattern.MatchString(repository.GitHubOwner) || strings.Contains(repository.GitHubOwner, "--") {
			return fmt.Errorf("repository alias %q has invalid github_owner", alias)
		}
		if !githubRepoPattern.MatchString(repository.GitHubRepo) {
			return fmt.Errorf("repository alias %q has invalid github_repo", alias)
		}
	}
	if c.APIKeyFile == "" || !filepath.IsAbs(c.APIKeyFile) || strings.ContainsRune(c.APIKeyFile, 0) {
		return fmt.Errorf("api_key_file must be a non-empty absolute path")
	}
	if c.PATFile == "" || !filepath.IsAbs(c.PATFile) || strings.ContainsRune(c.PATFile, 0) {
		return fmt.Errorf("pat_file must be a non-empty absolute path")
	}
	if filepath.Clean(c.APIKeyFile) == filepath.Clean(c.PATFile) {
		return fmt.Errorf("api_key_file and pat_file must be different files")
	}
	return nil
}

func rejectDuplicateConfigFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := consumeConfigJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("expected one JSON value")
	}
	return nil
}

func consumeConfigJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("invalid object key")
			}
			if _, exists := keys[key]; exists {
				return fmt.Errorf("duplicate object key")
			}
			keys[key] = struct{}{}
			if err := consumeConfigJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := consumeConfigJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("invalid JSON delimiter")
	}
}

func validateListenAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("listen_address must be host:port: %w", err)
	}
	if strings.ContainsRune(host, 0) || strings.TrimSpace(port) == "" {
		return fmt.Errorf("listen_address must contain a valid host and port")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("listen_address port must be between 1 and 65535")
	}
	return nil
}

func validatePostgresDSN(dsn string) error {
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" || strings.Trim(parsed.Path, "/") == "" {
		return fmt.Errorf("postgres_dsn must be a PostgreSQL URL with host and database")
	}
	if strings.ContainsRune(dsn, 0) {
		return fmt.Errorf("postgres_dsn contains NUL")
	}
	return nil
}

func validateDirectory(name, path string) error {
	if path == "" || !filepath.IsAbs(path) || strings.ContainsRune(path, 0) {
		return fmt.Errorf("%s must be a non-empty absolute path", name)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("%s must exist and resolve without symlinks: %w", name, err)
	}
	if filepath.Clean(path) != filepath.Clean(resolved) {
		return fmt.Errorf("%s must not contain symlinks", name)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("stat %s: %w", name, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s must be a directory", name)
	}
	return nil
}
