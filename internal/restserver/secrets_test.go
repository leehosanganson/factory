package restserver

import (
	"encoding/json"
	"os"
	"strings"
	"syscall"
	"testing"
)

func TestLoadSecretsAcceptsPrivateEffectiveUserFiles(t *testing.T) {
	cfg := testConfig(t)
	if err := os.WriteFile(cfg.APIKeyFile, []byte("shared-api-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.PATFile, []byte("shared-github-pat"), 0o400); err != nil {
		t.Fatal(err)
	}
	secrets, err := LoadSecrets(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(secrets.APIKey)
	defer clear(secrets.PAT)
	if string(secrets.APIKey) != "shared-api-key" || string(secrets.PAT) != "shared-github-pat" {
		t.Fatal("loaded secret bytes do not match their files")
	}
	if strings.Contains(string(mustMarshal(t, cfg)), "shared-api-key") || strings.Contains(string(mustMarshal(t, cfg)), "shared-github-pat") {
		t.Fatal("secret bytes must never appear in ordinary config")
	}
	if got := string(mustMarshal(t, secrets)); strings.Contains(got, "shared-api-key") || strings.Contains(got, "shared-github-pat") {
		t.Fatal("secret bytes must never be JSON serializable")
	}
}

func TestLoadSecretsRejectsUnsafeFiles(t *testing.T) {
	for _, tc := range []struct {
		name       string
		prepare    func(*testing.T, string)
		wantErrSub string
	}{
		{name: "group-readable", prepare: func(t *testing.T, path string) { writeSecret(t, path, 0o640) }, wantErrSub: "inaccessible to group"},
		{name: "other-readable", prepare: func(t *testing.T, path string) { writeSecret(t, path, 0o604) }, wantErrSub: "inaccessible to group"},
		{name: "owner-unreadable", prepare: func(t *testing.T, path string) {
			if os.Geteuid() == 0 {
				writeSecret(t, path, 0o200)
				return
			}
			if err := os.WriteFile(path, []byte("secret"), 0o000); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o000); err != nil {
				t.Fatal(err)
			}
		}, wantErrSub: "secret file"},
		{name: "wrong owner", prepare: func(t *testing.T, path string) {
			if os.Geteuid() != 0 {
				t.Skip("changing file ownership requires root")
			}
			writeSecret(t, path, 0o600)
			if err := os.Chown(path, 65534, -1); err != nil {
				t.Skipf("cannot change test file owner: %v", err)
			}
		}, wantErrSub: "owned by the effective user"},
		{name: "directory", prepare: func(t *testing.T, path string) {
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}, wantErrSub: "regular file"},
		{name: "symlink", prepare: func(t *testing.T, path string) {
			target := path + ".target"
			writeSecret(t, target, 0o600)
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		}, wantErrSub: "secret file"},
		{name: "empty", prepare: func(t *testing.T, path string) {
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}, wantErrSub: "must not be empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			writeSecret(t, cfg.APIKeyFile, 0o600)
			tc.prepare(t, cfg.PATFile)
			_, err := LoadSecrets(cfg)
			if err == nil || (tc.wantErrSub != "" && !strings.Contains(err.Error(), tc.wantErrSub)) {
				t.Fatalf("LoadSecrets() error = %v, want substring %q", err, tc.wantErrSub)
			}
		})
	}
}

func TestLoadSecretsRejectsFIFOWithoutBlocking(t *testing.T) {
	cfg := testConfig(t)
	writeSecret(t, cfg.APIKeyFile, 0o600)
	if err := os.Mkdir(cfg.PATFile, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(cfg.PATFile); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(cfg.PATFile, 0o600); err != nil {
		t.Skipf("cannot create fifo: %v", err)
	}
	_, err := LoadSecrets(cfg)
	if err == nil {
		t.Fatal("FIFO secret path was accepted")
	}
}

func writeSecret(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte("secret"), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
