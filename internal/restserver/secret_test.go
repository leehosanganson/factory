package restserver

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

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
