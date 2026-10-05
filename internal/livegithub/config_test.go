package livegithub

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

const sandboxRepo = "sandbox-owner/test-factory-live-sandbox"
const approvedOrganization = "sandbox-owner"

func validEnv(t *testing.T) map[string]string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]string{
		EnvOptIn: "true", EnvAppID: "12345",
		EnvPrivateKey:     string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})),
		EnvInstallationID: "67890", EnvRepository: sandboxRepo, EnvRepositoryAllowlist: sandboxRepo,
		EnvExpectedOwner: approvedOrganization, EnvApprovedOrganization: approvedOrganization, EnvExpectedRepository: "test-factory-live-sandbox",
		EnvInstallationToken: "test-installation-token",
	}
}

func TestLoadTargetConfigRequiresExplicitOptInAndCompleteValidCredentials(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]string)
	}{
		{"opt-in absent", func(env map[string]string) { delete(env, EnvOptIn) }},
		{"opt-in false", func(env map[string]string) { env[EnvOptIn] = "false" }},
		{"App ID absent", func(env map[string]string) { delete(env, EnvAppID) }},
		{"App ID malformed", func(env map[string]string) { env[EnvAppID] = "12x" }},
		{"App ID zero", func(env map[string]string) { env[EnvAppID] = "0" }},
		{"private key absent", func(env map[string]string) { delete(env, EnvPrivateKey) }},
		{"private key malformed", func(env map[string]string) { env[EnvPrivateKey] = "not-a-key" }},
		{"private key weak", func(env map[string]string) {
			key, err := rsa.GenerateKey(rand.Reader, 1024)
			if err != nil {
				t.Fatal(err)
			}
			env[EnvPrivateKey] = string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
		}},
		{"installation ID absent", func(env map[string]string) { delete(env, EnvInstallationID) }},
		{"installation ID malformed", func(env map[string]string) { env[EnvInstallationID] = "0" }},
		{"installation ID negative", func(env map[string]string) { env[EnvInstallationID] = "-1" }},
		{"installation ID overflow", func(env map[string]string) { env[EnvInstallationID] = "999999999999999999999999" }},
		{"repository absent", func(env map[string]string) { delete(env, EnvRepository) }},
		{"allowlist absent", func(env map[string]string) { delete(env, EnvRepositoryAllowlist) }},
		{"allowlist mismatch", func(env map[string]string) { env[EnvRepositoryAllowlist] = "wrong/factory-live-sandbox" }},
		{"owner absent", func(env map[string]string) { delete(env, EnvExpectedOwner) }},
		{"approved organization absent", func(env map[string]string) { delete(env, EnvApprovedOrganization) }},
		{"approved organization differs", func(env map[string]string) { env[EnvApprovedOrganization] = "other-org" }},
		{"approved organization malformed", func(env map[string]string) { env[EnvApprovedOrganization] = "not/an/org" }},
		{"approved organization whitespace", func(env map[string]string) { env[EnvApprovedOrganization] = "sandbox-owner " }},
		{"approved organization case mismatch", func(env map[string]string) { env[EnvApprovedOrganization] = "Sandbox-owner" }},
		{"repository owner is not approved org", func(env map[string]string) {
			env[EnvRepository] = "other-org/test-factory-live-sandbox"
			env[EnvRepositoryAllowlist] = env[EnvRepository]
			env[EnvExpectedOwner] = "other-org"
		}},
		{"owner mismatch", func(env map[string]string) { env[EnvExpectedOwner] = "wrong-owner" }},
		{"repository name absent", func(env map[string]string) { delete(env, EnvExpectedRepository) }},
		{"repository name mismatch", func(env map[string]string) { env[EnvExpectedRepository] = "wrong-factory-live-sandbox" }},
		{"repository lacks suffix", func(env map[string]string) { env[EnvExpectedRepository] = "production" }},
		{"repository whitespace", func(env map[string]string) { env[EnvRepository] = sandboxRepo + " " }},
		{"repository malformed", func(env map[string]string) { env[EnvRepository] = "sandbox-owner/not-valid!/factory-live-sandbox" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := validEnv(t)
			test.mutate(env)
			if _, err := LoadTargetConfig(func(name string) string { return env[name] }); err == nil {
				t.Fatal("config loader accepted unsafe or incomplete environment")
			}
		})
	}
}

func TestLoadProviderConfigRequiresMaskedInstallationToken(t *testing.T) {
	for _, token := range []string{"", "bad token", " token", "token\n"} {
		t.Run(fmt.Sprintf("token %q", token), func(t *testing.T) {
			env := map[string]string{
				EnvOptIn: "true", EnvRepository: sandboxRepo, EnvRepositoryAllowlist: sandboxRepo,
				EnvExpectedOwner: approvedOrganization, EnvApprovedOrganization: approvedOrganization,
				EnvExpectedRepository: "test-factory-live-sandbox", EnvInstallationToken: token,
			}
			if _, err := LoadProviderConfig(func(name string) string { return env[name] }); err == nil {
				t.Fatal("provider config accepted a missing or malformed installation token")
			}
		})
	}
}

func TestLoadProviderConfigReadsOnlyMaskedInstallationTokenForCredentials(t *testing.T) {
	env := map[string]string{
		EnvOptIn: "true", EnvRepository: sandboxRepo, EnvRepositoryAllowlist: sandboxRepo,
		EnvExpectedOwner: approvedOrganization, EnvApprovedOrganization: approvedOrganization,
		EnvExpectedRepository: "test-factory-live-sandbox", EnvInstallationToken: "masked-token-value",
	}
	cfg, err := LoadProviderConfig(func(name string) string {
		switch name {
		case EnvAppID, EnvPrivateKey, EnvInstallationID:
			t.Fatalf("provider config unexpectedly read mint-only variable %s", name)
		case EnvOptIn, EnvRepository, EnvRepositoryAllowlist, EnvExpectedOwner, EnvApprovedOrganization, EnvExpectedRepository, EnvInstallationToken:
			return env[name]
		default:
			t.Fatalf("provider config read unexpected variable %s", name)
		}
		return ""
	})
	if err != nil || cfg.Token != env[EnvInstallationToken] || cfg.PrivateKey != nil || cfg.AppID != 0 || cfg.InstallationID != 0 {
		t.Fatalf("provider config=%+v err=%v; want token-only credentials", cfg, err)
	}
}

func TestLoadTargetConfigRequiresExactSandboxAllowlistAndSuffix(t *testing.T) {
	for _, repository := range []string{"sandbox-owner/production", "other-owner/test-factory-live-sandbox", "sandbox-owner/test-factory-live-sandbox-extra", "sandbox-owner/test-factory-live-sandbox/child", "sandbox-owner/test-factory-live-sandbox "} {
		t.Run(repository, func(t *testing.T) {
			env := validEnv(t)
			env[EnvRepository] = repository
			if _, err := LoadTargetConfig(func(name string) string { return env[name] }); err == nil {
				t.Fatalf("LoadTargetConfig accepted repository %q outside exact sandbox", repository)
			}
		})
	}
}

func TestMaskActionsSecretRejectsEmptyValues(t *testing.T) {
	if err := MaskActionsSecret(""); err == nil {
		t.Fatal("MaskActionsSecret accepted empty value")
	}
}

func TestMaskActionsSecretEscapesMultilinePEMForWorkflowCommand(t *testing.T) {
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	if err := MaskActionsSecret("line1%\r\nline2"); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	os.Stdout = old
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "::add-mask::line1%25%0D%0Aline2\n" {
		t.Fatalf("escaped command=%q", got)
	}
}

func TestMaskActionsSecretUsesWorkflowCommand(t *testing.T) {
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	if err := MaskActionsSecret("sensitive-value"); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	os.Stdout = old
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "::add-mask::sensitive-value\n" {
		t.Fatalf("mask command=%q", got)
	}
}

func TestLoadTargetConfigAcceptsPKCS8KeyAndDoesNotLeakKeyInErrors(t *testing.T) {
	env := validEnv(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	secret := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	env[EnvPrivateKey] = secret
	cfg, err := LoadTargetConfig(func(name string) string { return env[name] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AppID != 12345 || cfg.InstallationID != 67890 || cfg.Repository != sandboxRepo || cfg.ApprovedOrganization != approvedOrganization {
		t.Fatalf("unexpected validated config: %+v", cfg)
	}
	env[EnvPrivateKey] = fmt.Sprintf("%sinvalid%s", "-----BEGIN PRIVATE KEY-----\n", "\n-----END PRIVATE KEY-----")
	_, err = LoadTargetConfig(func(name string) string { return env[name] })
	if err == nil || strings.Contains(err.Error(), env[EnvPrivateKey]) {
		t.Fatalf("error missing or leaked key material: %v", err)
	}
}
