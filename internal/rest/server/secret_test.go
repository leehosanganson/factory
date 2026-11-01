package restserver

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestPrincipalResolverResolvesNamedRolesAndLegacyOperator(t *testing.T) {
	const operatorToken = "named-operator-token"
	const submitterToken = "named-submitter-token"
	keyPath := writeSecret(t, "legacy-key", 0o600)
	operatorPath := writeVerifier(t, "admin", operatorToken, 0o600)
	submitterPath := writeVerifier(t, "submitter", submitterToken, 0o400)
	config := validConfig()
	config.APIKeyFile = keyPath
	config.Principals = []PrincipalConfig{
		{ID: "admin", Role: "operator", TokenFile: operatorPath, Enabled: true},
		{ID: "submitter", Role: "submitter", TokenFile: submitterPath, Enabled: true},
		{ID: "disabled", Role: "operator", TokenFile: filepath.Join(t.TempDir(), "missing"), Enabled: false},
	}
	resolver, err := LoadPrincipalResolver(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		token string
		want  Principal
	}{
		{"legacy-key", Principal{ID: "legacy-shared", Role: "operator"}},
		{operatorToken, Principal{ID: "admin", Role: "operator"}},
		{submitterToken, Principal{ID: "submitter", Role: "submitter"}},
	} {
		got, ok := resolver.Resolve(tc.token)
		if !ok || got != tc.want || !resolver.APIKey().Authenticate(tc.token) {
			t.Errorf("Resolve(%q) = %+v, %v; want %+v", tc.token, got, ok, tc.want)
		}
		if got, ok := resolver.APIKey().Resolve(tc.token); !ok || got != tc.want {
			t.Errorf("APIKey.Resolve(%q) = %+v, %v; want %+v", tc.token, got, ok, tc.want)
		}
	}
	if _, ok := resolver.Resolve("disabled-token"); ok {
		t.Fatal("disabled principal authenticated")
	}
	if _, ok := resolver.Resolve(""); ok {
		t.Fatal("empty token authenticated")
	}
	for label, rendered := range map[string]string{
		"resolver format": fmt.Sprintf("%#v", resolver),
		"resolver JSON":   func() string { data, _ := json.Marshal(resolver); return string(data) }(),
	} {
		if strings.Contains(rendered, operatorToken) || strings.Contains(rendered, submitterToken) {
			t.Errorf("%s exposed raw bearer token: %s", label, rendered)
		}
	}
}

func TestLoadPrincipalResolverRejectsUnsafeOrMismatchedVerifiers(t *testing.T) {
	for _, tc := range []struct {
		name string
		path func(*testing.T) string
	}{
		{"missing", func(t *testing.T) string { return filepath.Join(t.TempDir(), "missing") }},
		{"symlink", func(t *testing.T) string {
			target := writeVerifier(t, "admin", "token", 0o600)
			link := filepath.Join(t.TempDir(), "link")
			if err := os.Symlink(target, link); err != nil {
				t.Fatal(err)
			}
			return link
		}},
		{"directory", func(t *testing.T) string { return t.TempDir() }},
		{"unsafe permissions", func(t *testing.T) string { return writeVerifier(t, "admin", "token", 0o640) }},
		{"wrong identity", func(t *testing.T) string { return writeVerifier(t, "other", "token", 0o600) }},
		{"duplicate verifier identity", func(t *testing.T) string {
			digest := principalDigest("token")
			return writeVerifierJSON(t, fmt.Sprintf(`{"version":1,"principals":[{"id":"admin","token_sha256":%q},{"id":"admin","token_sha256":%q}]}`, digest, principalDigest("other")), 0o600)
		}},
		{"uppercase digest", func(t *testing.T) string { return writeVerifierDigest(t, "admin", strings.Repeat("A", 64), 0o600) }},
		{"duplicate field", func(t *testing.T) string {
			return writeVerifierJSON(t, `{"version":1,"version":1,"principals":[{"id":"admin","token_sha256":"`+principalDigest("token")+`"}]}`, 0o600)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := validConfig()
			config.APIKeyFile = writeSecret(t, "legacy", 0o600)
			verifierPath := tc.path(t)
			config.Principals = []PrincipalConfig{{ID: "admin", Role: "operator", TokenFile: verifierPath, Enabled: true}}
			_, err := LoadPrincipalResolver(config)
			if err == nil {
				t.Fatal("unsafe or mismatched verifier was accepted")
			}
			if strings.Contains(err.Error(), verifierPath) || strings.Contains(err.Error(), "named-secret-token") {
				t.Fatalf("resolver error disclosed token material or file path: %v", err)
			}
		})
	}
}

func TestProviderTokenLoadsPrivatelyAndAuthenticates(t *testing.T) {
	path := writeSecret(t, "provider-token\n", 0o600)
	token, err := LoadProviderToken(path)
	if err != nil {
		t.Fatal(err)
	}
	if !token.Authenticate("provider-token") || token.Authenticate("wrong") {
		t.Fatal("provider token authentication mismatch")
	}
	if fmt.Sprint(token) != "[REDACTED]" || strings.Contains(fmt.Sprintf("%#v", token), "provider-token") {
		t.Fatal("provider token formatting leaked secret")
	}
}

func TestWebhookSecretRequiresPrivateBoundedFileAndIsRedacted(t *testing.T) {
	secret := strings.Repeat("s", 32)
	path := writeSecret(t, secret, 0o600)
	loaded, err := LoadWebhookSecret(path)
	if err != nil || string(loaded.Value()) != secret {
		t.Fatalf("LoadWebhookSecret()=(%q,%v)", loaded.Value(), err)
	}
	if fmt.Sprint(loaded) != "[REDACTED]" || strings.Contains(fmt.Sprintf("%#v", loaded), secret) {
		t.Fatal("webhook secret formatting disclosed value")
	}
	for name, data := range map[string]string{"short": "too-short", "newline": secret + "\n", "oversized": strings.Repeat("s", apiKeyFileLimit+1)} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadWebhookSecret(writeSecret(t, data, 0o600)); err == nil {
				t.Fatal("invalid webhook secret accepted")
			}
		})
	}
	if _, err := LoadWebhookSecret(writeSecret(t, secret, 0o640)); err == nil {
		t.Fatal("group-readable webhook secret accepted")
	}
}

func TestLoadProviderTokenRejectsOversizePrivateFile(t *testing.T) {
	path := writeSecret(t, strings.Repeat("a", apiKeyFileLimit+1), 0o600)
	if _, err := LoadProviderToken(path); err == nil {
		t.Fatal("oversized token accepted")
	}
}

func TestLoadAPIKeyAcceptsSingleLineEndingsAndAuthenticates(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
	}{
		{"no ending", "key-without-whitespace"},
		{"LF", "key-without-whitespace\n"},
		{"CRLF", "key-without-whitespace\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, err := LoadAPIKey(writeSecret(t, tc.data, 0o600))
			if err != nil {
				t.Fatal(err)
			}
			if !key.Authenticate("key-without-whitespace") || key.Authenticate("wrong") || key.Authenticate("") {
				t.Fatal("API key authentication did not distinguish matching and non-matching values")
			}
			if got, ok := key.Resolve("key-without-whitespace"); !ok || got != (Principal{ID: "legacy-shared", Role: "operator"}) {
				t.Fatalf("direct legacy key Resolve() = %+v, %v; want legacy operator", got, ok)
			}
			if _, ok := key.Resolve("wrong"); ok {
				t.Fatal("direct legacy key resolved an incorrect token")
			}
		})
	}
}

func TestAPIKeyRotationRequiresReload(t *testing.T) {
	path := writeSecret(t, "old-token", 0o600)
	old, err := LoadAPIKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("new-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !old.Authenticate("old-token") || old.Authenticate("new-token") {
		t.Fatal("loaded API key changed without reload")
	}
	reloaded, err := LoadAPIKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reloaded.Authenticate("new-token") || reloaded.Authenticate("old-token") {
		t.Fatal("reloaded API key did not observe rotation")
	}
}

func TestAPIKeyIsRedactedFromFormattingAndJSON(t *testing.T) {
	const secret = "very-sensitive-api-key"
	key, err := LoadAPIKey(writeSecret(t, secret, 0o400))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(key)
	if err != nil {
		t.Fatal(err)
	}
	for label, rendered := range map[string]string{
		"String":   fmt.Sprint(key),
		"GoString": fmt.Sprintf("%#v", key),
		"JSON":     string(encoded),
	} {
		if strings.Contains(rendered, secret) {
			t.Errorf("%s exposed secret: %s", label, rendered)
		}
	}
}

func TestLoadAPIKeyRejectsInvalidTokenBytesWithoutEchoing(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
	}{
		{"empty", ""},
		{"space", "secret token"},
		{"tab", "secret\ttoken"},
		{"embedded LF", "secret\ntoken"},
		{"multiple terminal LF", "secret\n\n"},
		{"embedded CR", "secret\rtoken"},
		{"nul", "secret\x00token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadAPIKey(writeSecret(t, tc.data, 0o600))
			if err == nil {
				t.Fatal("invalid API key was accepted")
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("error disclosed token contents: %v", err)
			}
		})
	}
}

func TestLoadAPIKeyRejectsUnsafeFiles(t *testing.T) {
	t.Run("empty path", func(t *testing.T) {
		if _, err := LoadAPIKey(""); err == nil {
			t.Fatal("empty path was accepted")
		}
	})
	t.Run("missing file", func(t *testing.T) {
		if _, err := LoadAPIKey(filepath.Join(t.TempDir(), "missing")); err == nil {
			t.Fatal("missing file was accepted")
		}
	})
	t.Run("symlink", func(t *testing.T) {
		target := writeSecret(t, "test-token", 0o600)
		link := filepath.Join(t.TempDir(), "key")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadAPIKey(link); err == nil {
			t.Fatal("symlink was accepted")
		}
	})
	t.Run("directory", func(t *testing.T) {
		if _, err := LoadAPIKey(t.TempDir()); err == nil {
			t.Fatal("directory was accepted")
		}
	})
	t.Run("FIFO", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "key.fifo")
		if err := syscall.Mkfifo(path, 0o600); err != nil {
			t.Skipf("cannot create FIFO: %v", err)
		}
		if _, err := LoadAPIKey(path); err == nil {
			t.Fatal("FIFO was accepted")
		}
	})
	t.Run("wrong owner", func(t *testing.T) {
		path := writeSecret(t, "test-token", 0o600)
		wrongUID := 1
		if os.Geteuid() == wrongUID {
			wrongUID = 2
		}
		if err := os.Chown(path, wrongUID, -1); err != nil {
			t.Skipf("cannot set file to another UID: %v", err)
		}
		if _, err := LoadAPIKey(path); err == nil {
			t.Fatal("file with wrong owner was accepted")
		}
	})
	for _, mode := range []os.FileMode{0o640, 0o604, 0o200, 0o700} {
		t.Run(fmt.Sprintf("mode_%o", mode), func(t *testing.T) {
			if _, err := LoadAPIKey(writeSecret(t, "test-token", mode)); err == nil {
				t.Fatalf("unsafe mode %o was accepted", mode)
			}
		})
	}
	t.Run("oversized", func(t *testing.T) {
		path := writeSecret(t, strings.Repeat("x", apiKeyFileLimit+1), 0o600)
		if _, err := LoadAPIKey(path); err == nil {
			t.Fatal("oversized key file was accepted")
		}
	})
}

func writeVerifier(t *testing.T, id, token string, mode os.FileMode) string {
	return writeVerifierDigest(t, id, principalDigest(token), mode)
}

func writeVerifierDigest(t *testing.T, id, digest string, mode os.FileMode) string {
	return writeVerifierJSON(t, fmt.Sprintf(`{"version":1,"principals":[{"id":%q,"token_sha256":%q}]}`, id, digest), mode)
}

func writeVerifierJSON(t *testing.T, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "verifier.json")
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func principalDigest(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

func writeSecret(t *testing.T, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "api-key")
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}
