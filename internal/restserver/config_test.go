package restserver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	root := t.TempDir()
	checkout := filepath.Join(root, "checkout")
	migrations := filepath.Join(root, "migrations")
	if err := os.Mkdir(checkout, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(migrations, 0o700); err != nil {
		t.Fatal(err)
	}
	return Config{
		Version:       1,
		ListenAddress: "127.0.0.1:8080",
		PostgresDSN:   "postgres://factory@localhost/factory?sslmode=disable",
		MigrationDir:  migrations,
		Repositories: map[string]Repository{
			"factory": {CheckoutPath: checkout, GitHubOwner: "example-owner", GitHubRepo: "factory-repo"},
		},
		APIKeyFile: filepath.Join(root, "api-key"),
		PATFile:    filepath.Join(root, "pat"),
	}
}

func TestLoadConfigRejectsMalformedAndInvalidConfig(t *testing.T) {
	valid := func(t *testing.T) []byte {
		t.Helper()
		cfg := testConfig(t)
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}

	for _, tc := range []struct {
		name    string
		content func(*testing.T) []byte
	}{
		{name: "malformed JSON", content: func(t *testing.T) []byte { return []byte(`{"version":`) }},
		{name: "trailing value", content: func(t *testing.T) []byte { return append(valid(t), []byte(` {}`)...) }},
		{name: "duplicate field", content: func(t *testing.T) []byte { return []byte(`{"version":1,"version":1}`) }},
		{name: "unknown field", content: func(t *testing.T) []byte { return append(valid(t)[:len(valid(t))-1], []byte(`,"pat":"secret"}`)...) }},
		{name: "wrong version", content: func(t *testing.T) []byte {
			return []byte(strings.Replace(string(valid(t)), `"version":1`, `"version":2`, 1))
		}},
		{name: "bad listen address", content: func(t *testing.T) []byte {
			return []byte(strings.Replace(string(valid(t)), `"127.0.0.1:8080"`, `"not-an-address"`, 1))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, tc.content(t), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadConfig(path); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}

func TestConfigValidatesAliasesAndCheckoutPaths(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *Config)
	}{
		{name: "invalid alias", mutate: func(_ *testing.T, cfg *Config) {
			cfg.Repositories["Bad Alias"] = cfg.Repositories["factory"]
			delete(cfg.Repositories, "factory")
		}},
		{name: "relative checkout", mutate: func(_ *testing.T, cfg *Config) {
			repo := cfg.Repositories["factory"]
			repo.CheckoutPath = "relative"
			cfg.Repositories["factory"] = repo
		}},
		{name: "missing checkout", mutate: func(_ *testing.T, cfg *Config) {
			repo := cfg.Repositories["factory"]
			repo.CheckoutPath += "/missing"
			cfg.Repositories["factory"] = repo
		}},
		{name: "symlink checkout", mutate: func(t *testing.T, cfg *Config) {
			link := filepath.Join(t.TempDir(), "checkout-link")
			if err := os.Symlink(cfg.Repositories["factory"].CheckoutPath, link); err != nil {
				t.Fatal(err)
			}
			repo := cfg.Repositories["factory"]
			repo.CheckoutPath = link
			cfg.Repositories["factory"] = repo
		}},
		{name: "invalid owner", mutate: func(_ *testing.T, cfg *Config) {
			repo := cfg.Repositories["factory"]
			repo.GitHubOwner = "owner/name"
			cfg.Repositories["factory"] = repo
		}},
		{name: "invalid repository", mutate: func(_ *testing.T, cfg *Config) {
			repo := cfg.Repositories["factory"]
			repo.GitHubRepo = "../repo"
			cfg.Repositories["factory"] = repo
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			tc.mutate(t, &cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("invalid repository mapping was accepted")
			}
		})
	}
}

func TestConfigValidatesListenDSNAndSecretPaths(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "zero port", mutate: func(cfg *Config) { cfg.ListenAddress = ":0" }},
		{name: "junk port", mutate: func(cfg *Config) { cfg.ListenAddress = ":80oops" }},
		{name: "non-PostgreSQL DSN", mutate: func(cfg *Config) { cfg.PostgresDSN = "mysql://user@localhost/db" }},
		{name: "missing database", mutate: func(cfg *Config) { cfg.PostgresDSN = "postgres://user@localhost" }},
		{name: "relative secret path", mutate: func(cfg *Config) { cfg.APIKeyFile = "secret" }},
		{name: "same secret paths", mutate: func(cfg *Config) { cfg.PATFile = cfg.APIKeyFile }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			tc.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("invalid server config was accepted")
			}
		})
	}
}
