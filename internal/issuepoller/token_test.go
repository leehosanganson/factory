package issuepoller

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadTokenRequiresPrivateRegularFileAndRedactsFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("scoped-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if token, err := loadToken(path); err != nil || token != "scoped-token" {
		t.Fatalf("token=%q err=%v", token, err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadToken(path); err == nil || strings.Contains(err.Error(), "scoped-token") {
		t.Fatalf("unsafe token accepted/leaked: %v", err)
	}
}
