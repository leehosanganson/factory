package factory

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

func TestPrincipalBearerVerifierMatchesOnlyConfiguredNonemptyTokens(t *testing.T) {
	const token = "high-entropy-test-token-do-not-store"
	const secondToken = "another-high-entropy-test-token"
	path := writePrincipalBearerConfig(t, fmt.Sprintf(`{"version":1,"principals":[{"id":"alice","token_sha256":%q},{"id":"bob","token_sha256":%q}]}`, principalTokenDigest(token), principalTokenDigest(secondToken)), 0o600)
	verifier, err := LoadPrincipalBearerVerifier(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		token     string
		wantID    string
		wantMatch bool
	}{
		{name: "matching token", token: token, wantID: "alice", wantMatch: true},
		{name: "second principal token", token: secondToken, wantID: "bob", wantMatch: true},
		{name: "wrong token", token: token + "-wrong"},
		{name: "empty token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, matched := verifier.PrincipalID(tc.token)
			if got != tc.wantID || matched != tc.wantMatch {
				t.Fatalf("PrincipalID() = %q, %v; want %q, %v", got, matched, tc.wantID, tc.wantMatch)
			}
		})
	}
}

func TestPrincipalBearerVerifierRotationRequiresReload(t *testing.T) {
	const oldToken = "old-high-entropy-token"
	const newToken = "new-high-entropy-token"
	path := writePrincipalBearerConfig(t, fmt.Sprintf(`{"version":1,"principals":[{"id":"alice","token_sha256":%q}]}`, principalTokenDigest(oldToken)), 0o600)
	loaded, err := LoadPrincipalBearerVerifier(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(fmt.Sprintf(`{"version":1,"principals":[{"id":"alice","token_sha256":%q}]}`, principalTokenDigest(newToken))), 0o600); err != nil {
		t.Fatal(err)
	}
	if id, ok := loaded.PrincipalID(oldToken); !ok || id != "alice" {
		t.Fatalf("loaded verifier changed after file rotation: %q, %v", id, ok)
	}
	if _, ok := loaded.PrincipalID(newToken); ok {
		t.Fatal("loaded verifier observed rotation without reload")
	}
	reloaded, err := LoadPrincipalBearerVerifier(path)
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := reloaded.PrincipalID(newToken); !ok || id != "alice" {
		t.Fatalf("reloaded verifier did not observe token rotation: %q, %v", id, ok)
	}
}

func TestPrincipalBearerVerifierDoesNotRetainOrSerializeRawToken(t *testing.T) {
	const token = "sensitive-bearer-material"
	verifier, err := LoadPrincipalBearerVerifier(writePrincipalBearerConfig(t,
		fmt.Sprintf(`{"version":1,"principals":[{"id":"alice","token_sha256":%q}]}`, principalTokenDigest(token)), 0o400))
	if err != nil {
		t.Fatal(err)
	}
	for label, encoded := range map[string]string{
		"formatted verifier": fmt.Sprintf("%#v", verifier),
		"JSON verifier":      func() string { value, _ := json.Marshal(verifier); return string(value) }(),
	} {
		if strings.Contains(encoded, token) {
			t.Errorf("%s exposed raw bearer token: %s", label, encoded)
		}
	}
}

func TestLoadPrincipalBearerVerifierRejectsMalformedConfiguration(t *testing.T) {
	digest := principalTokenDigest("token-a")
	cases := []struct {
		name string
		json string
	}{
		{name: "invalid JSON", json: `{"version":`},
		{name: "trailing value", json: `{"version":1,"principals":[{"id":"alice","token_sha256":"` + digest + `"}]} {}`},
		{name: "trailing garbage", json: `{"version":1,"principals":[{"id":"alice","token_sha256":"` + digest + `"}]} trailing`},
		{name: "unknown top-level field", json: `{"version":1,"principals":[],"unexpected":true}`},
		{name: "unknown principal field", json: `{"version":1,"principals":[{"id":"alice","token_sha256":"` + digest + `","token":"secret"}]}`},
		{name: "duplicate JSON field", json: `{"version":1,"version":1,"principals":[{"id":"alice","token_sha256":"` + digest + `"}]}`},
		{name: "case variant top-level field", json: `{"version":1,"Version":2,"principals":[{"id":"alice","token_sha256":"` + digest + `"}]}`},
		{name: "case variant principals field", json: `{"version":1,"Principals":[{"id":"alice","token_sha256":"` + digest + `"}]}`},
		{name: "case variant nested field", json: `{"version":1,"principals":[{"id":"alice","TokenSHA256":"` + digest + `"}]}`},
		{name: "case variant duplicate field", json: `{"version":1,"Version":1,"principals":[{"id":"alice","token_sha256":"` + digest + `"}]}`},
		{name: "wrong version", json: `{"version":2,"principals":[{"id":"alice","token_sha256":"` + digest + `"}]}`},
		{name: "missing version", json: `{"principals":[{"id":"alice","token_sha256":"` + digest + `"}]}`},
		{name: "missing principals", json: `{"version":1}`},
		{name: "empty principals", json: `{"version":1,"principals":[]}`},
		{name: "null principal", json: `{"version":1,"principals":[null]}`},
		{name: "missing ID", json: `{"version":1,"principals":[{"token_sha256":"` + digest + `"}]}`},
		{name: "missing digest", json: `{"version":1,"principals":[{"id":"alice"}]}`},
		{name: "duplicate principal ID", json: `{"version":1,"principals":[{"id":"alice","token_sha256":"` + digest + `"},{"id":"alice","token_sha256":"` + principalTokenDigest("token-b") + `"}]}`},
		{name: "duplicate verifier", json: `{"version":1,"principals":[{"id":"alice","token_sha256":"` + digest + `"},{"id":"bob","token_sha256":"` + digest + `"}]}`},
		{name: "invalid principal ID", json: `{"version":1,"principals":[{"id":"../alice","token_sha256":"` + digest + `"}]}`},
		{name: "uppercase digest", json: `{"version":1,"principals":[{"id":"alice","token_sha256":"` + strings.ToUpper(digest) + `"}]}`},
		{name: "short digest", json: `{"version":1,"principals":[{"id":"alice","token_sha256":"abcd"}]}`},
		{name: "wrongly typed digest", json: `{"version":1,"principals":[{"id":"alice","token_sha256":1}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writePrincipalBearerConfig(t, tc.json, 0o600)
			if verifier, err := LoadPrincipalBearerVerifier(path); err == nil || verifier != nil {
				t.Fatalf("LoadPrincipalBearerVerifier() = %v, %v; want failure", verifier, err)
			}
		})
	}
	t.Run("errors do not echo raw token values", func(t *testing.T) {
		const raw = "do-not-echo-this-token"
		content := fmt.Sprintf(`{"version":1,"principals":[{"id":"alice","token_sha256":%q,"token":%q}]}`, digest, raw)
		_, err := LoadPrincipalBearerVerifier(writePrincipalBearerConfig(t, content, 0o600))
		if err == nil || strings.Contains(err.Error(), raw) {
			t.Fatalf("error = %v; want failure without raw token", err)
		}
	})
}

func TestLoadPrincipalBearerVerifierRejectsUnsafeFiles(t *testing.T) {
	valid := fmt.Sprintf(`{"version":1,"principals":[{"id":"alice","token_sha256":%q}]}`, principalTokenDigest("token"))
	t.Run("missing file", func(t *testing.T) {
		if _, err := LoadPrincipalBearerVerifier(filepath.Join(t.TempDir(), "missing.json")); err == nil {
			t.Fatal("missing verifier file was accepted")
		}
	})
	t.Run("empty path", func(t *testing.T) {
		if _, err := LoadPrincipalBearerVerifier(""); err == nil {
			t.Fatal("empty verifier path was accepted")
		}
	})
	t.Run("symlink", func(t *testing.T) {
		base := t.TempDir()
		target := writePrincipalBearerConfig(t, valid, 0o600)
		link := filepath.Join(base, "link.json")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadPrincipalBearerVerifier(link); err == nil {
			t.Fatal("symlink verifier file was accepted")
		}
	})
	t.Run("wrong owner", func(t *testing.T) {
		path := writePrincipalBearerConfig(t, valid, 0o600)
		wrongUID := 1
		if os.Geteuid() == wrongUID {
			wrongUID = 2
		}
		if err := os.Chown(path, wrongUID, -1); err != nil {
			t.Skipf("cannot set verifier file to another owner: %v", err)
		}
		if _, err := LoadPrincipalBearerVerifier(path); err == nil {
			t.Fatal("verifier file owned by another UID was accepted")
		}
	})
	t.Run("non-regular file", func(t *testing.T) {
		if _, err := LoadPrincipalBearerVerifier(t.TempDir()); err == nil {
			t.Fatal("directory was accepted as a verifier file")
		}
	})
	t.Run("FIFO", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "verifier.fifo")
		if err := syscall.Mkfifo(path, 0o600); err != nil {
			t.Skipf("cannot create FIFO: %v", err)
		}
		if _, err := LoadPrincipalBearerVerifier(path); err == nil {
			t.Fatal("FIFO was accepted as a verifier file")
		}
	})
	t.Run("group-readable", func(t *testing.T) {
		assertPrincipalVerifierModeRejected(t, valid, 0o640)
	})
	t.Run("other-readable", func(t *testing.T) {
		assertPrincipalVerifierModeRejected(t, valid, 0o604)
	})
	t.Run("owner-unreadable", func(t *testing.T) {
		assertPrincipalVerifierModeRejected(t, valid, 0o200)
	})
	t.Run("owner-executable", func(t *testing.T) {
		assertPrincipalVerifierModeRejected(t, valid, 0o700)
	})
	t.Run("oversized", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "verifier.json")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(make([]byte, principalBearerConfigLimit+1)); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadPrincipalBearerVerifier(path); err == nil {
			t.Fatal("oversized verifier file was accepted")
		}
	})
}

func TestPrincipalBearerVerifierUsesConstantTimeDigestComparison(t *testing.T) {
	const token = "constant-time-test-token"
	verifier, err := LoadPrincipalBearerVerifier(writePrincipalBearerConfig(t,
		fmt.Sprintf(`{"version":1,"principals":[{"id":"alice","token_sha256":%q}]}`, principalTokenDigest(token)), 0o600))
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := verifier.PrincipalID(token); !ok || id != "alice" {
		t.Fatalf("matching comparison = %q, %v", id, ok)
	}
	if id, ok := verifier.PrincipalID("different-token"); ok || id != "" {
		t.Fatalf("non-matching comparison = %q, %v", id, ok)
	}
}

func writePrincipalBearerConfig(t *testing.T, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "principals.json")
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertPrincipalVerifierModeRejected(t *testing.T, content string, mode os.FileMode) {
	t.Helper()
	path := writePrincipalBearerConfig(t, content, mode)
	if _, err := LoadPrincipalBearerVerifier(path); err == nil {
		t.Fatalf("verifier mode %o was accepted", mode)
	}
}

func principalTokenDigest(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}
