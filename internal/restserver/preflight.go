package restserver

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// CheckLocalPrerequisites inspects configured local files and repository metadata
// without creating files, opening listeners/databases, or contacting providers.
// Returned messages contain config field names only.
func CheckLocalPrerequisites(config Config) []string {
	var issues []string
	if _, err := LoadAPIKey(config.APIKeyFile); err != nil {
		issues = append(issues, "api_key_file: unavailable or invalid")
	}
	for _, principal := range config.Principals {
		if principal.Enabled {
			if _, err := loadPrincipalTokenDigest(principal.TokenFile, principal.ID); err != nil {
				issues = append(issues, "principals: unavailable or invalid token verifier")
				break
			}
		}
	}
	if config.GitHubWebhook.Enabled() {
		if _, err := LoadWebhookSecret(config.GitHubWebhook.SecretFile); err != nil {
			issues = append(issues, "github_webhook.secret_file: unavailable or invalid")
		}
	}
	if config.Persistence.Backend == PersistenceBackendSQLite {
		if err := checkSQLitePathMetadata(config.Persistence.Path); err != nil {
			issues = append(issues, "persistence.path: unavailable or unsafe")
		}
	}
	if config.WorkersEnabledValue() {
		if config.Provider.Backend == "github" {
			if _, err := LoadProviderToken(config.Provider.TokenFile); err != nil {
				issues = append(issues, "provider.token_file: unavailable or invalid")
			}
		}
		if err := config.ValidateRepositoryRoots(); err != nil {
			issues = append(issues, "repositories: local checkout validation failed")
		}
		if _, err := exec.LookPath(config.Harness.Executable); err != nil {
			issues = append(issues, "harness.executable: not found")
		}
	}
	return issues
}

func checkSQLitePathMetadata(path string) error {
	if !filepath.IsAbs(path) || strings.TrimSpace(path) == "" {
		return errors.New("invalid database path")
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil || !parent.IsDir() || parent.Mode().Perm()&0o077 != 0 {
		return errors.New("database parent is unavailable or not private")
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return errors.New("database file is unavailable or unsafe")
	}
	return nil
}
