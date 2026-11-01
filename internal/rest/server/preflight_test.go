package restserver

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckLocalPrerequisitesSkipsWorkerRequirementsOnlyWhenWorkersDisabled(t *testing.T) {
	root := t.TempDir()
	missingRepository := filepath.Join(root, "private-repository")
	missingHarness := filepath.Join(root, "private-harness")
	missingProviderToken := filepath.Join(root, "private-provider-token")
	missingSQLitePath := filepath.Join(root, "private-sqlite-parent", "jobs.db")

	config := validConfig()
	config.APIKeyFile = filepath.Join(root, "private-api-key")
	config.Principals[0].TokenFile = filepath.Join(root, "private-principal-token")
	config.Repositories = map[string]string{"widget": missingRepository}
	config.Harness.Executable = missingHarness
	config.Provider = ProviderConfig{Backend: "github", TokenFile: missingProviderToken}
	config.Persistence = PersistenceConfig{Backend: PersistenceBackendSQLite, Path: missingSQLitePath}

	for _, test := range []struct {
		name           string
		workersEnabled bool
		want           []string
		omit           []string
	}{
		{
			name:           "control plane checks shared auth but skips worker-only requirements",
			workersEnabled: false,
			want: []string{
				"api_key_file: unavailable or invalid",
				"principals: unavailable or invalid token verifier",
				"persistence.path: unavailable or unsafe",
			},
			omit: []string{
				"provider.token_file:",
				"repositories:",
				"harness.executable:",
			},
		},
		{
			name:           "worker-enabled server retains all prerequisite checks",
			workersEnabled: true,
			want: []string{
				"api_key_file: unavailable or invalid",
				"principals: unavailable or invalid token verifier",
				"persistence.path: unavailable or unsafe",
				"provider.token_file: unavailable or invalid",
				"repositories: local checkout validation failed",
				"harness.executable: not found",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			config.WorkersEnabled = &test.workersEnabled
			issues := CheckLocalPrerequisites(config)
			joined := strings.Join(issues, "\n")
			for _, want := range test.want {
				if !strings.Contains(joined, want) {
					t.Errorf("preflight issues missing %q: %v", want, issues)
				}
			}
			for _, omitted := range test.omit {
				if strings.Contains(joined, omitted) {
					t.Errorf("preflight issues unexpectedly contain %q: %v", omitted, issues)
				}
			}
			for _, sensitive := range []string{root, missingRepository, missingHarness, missingProviderToken, missingSQLitePath, config.APIKeyFile, config.Principals[0].TokenFile} {
				if strings.Contains(joined, sensitive) {
					t.Errorf("preflight issues disclosed path %q: %v", sensitive, issues)
				}
			}
		})
	}
}

func TestCheckLocalPrerequisitesValidatesWebhookSecretWithoutLeakingPath(t *testing.T) {
	root := t.TempDir()
	secretPath := filepath.Join(root, "webhook-secret")
	config := validConfig()
	config.GitHubWebhook = GitHubWebhookConfig{SecretFile: secretPath, TriggerLabel: "ready", Sources: map[string]string{"acme/issues": "widget"}, Owners: map[string]string{}}
	issues := CheckLocalPrerequisites(config)
	joined := strings.Join(issues, "\\n")
	if !strings.Contains(joined, "github_webhook.secret_file: unavailable or invalid") || strings.Contains(joined, secretPath) {
		t.Fatalf("missing webhook secret preflight issues=%v", issues)
	}
	if err := os.WriteFile(secretPath, []byte(strings.Repeat("s", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
	issues = CheckLocalPrerequisites(config)
	if strings.Contains(strings.Join(issues, "\\n"), "github_webhook.secret_file:") {
		t.Fatalf("valid webhook secret preflight issues=%v", issues)
	}
}

func TestCheckLocalPrerequisitesValidatesEnabledPrincipalVerifierWithoutLeakingData(t *testing.T) {
	const token = "private-preflight-token"
	digest := sha256.Sum256([]byte(token))
	verifierPath := filepath.Join(t.TempDir(), "verifier.json")
	content := fmt.Sprintf(`{"version":1,"principals":[{"id":"test-operator","token_sha256":%q}]}`, hex.EncodeToString(digest[:]))
	if err := os.WriteFile(verifierPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	config := validConfig()
	config.Principals[0].TokenFile = verifierPath
	issues := CheckLocalPrerequisites(config)
	if strings.Contains(strings.Join(issues, "\n"), "principals:") {
		t.Fatalf("valid named operator verifier reported unavailable: %v", issues)
	}
	if err := os.Chmod(verifierPath, 0o640); err != nil {
		t.Fatal(err)
	}
	issues = CheckLocalPrerequisites(config)
	joined := strings.Join(issues, "\n")
	if !strings.Contains(joined, "principals: unavailable or invalid token verifier") {
		t.Fatalf("unsafe verifier preflight issues = %v", issues)
	}
	for _, secret := range []string{token, verifierPath, content} {
		if strings.Contains(joined, secret) {
			t.Fatalf("preflight issues disclosed secret/config path: %v", issues)
		}
	}
}
