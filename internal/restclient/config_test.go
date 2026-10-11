package restclient

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigRequiresPrivateSeparateBearerToken(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("secret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "client.json")
	config := `{"base_url":"http://127.0.0.1:8080","token_file":"` + tokenPath + `"}`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if loaded.BaseURL != "http://127.0.0.1:8080" || loaded.Token != "secret-token" {
		t.Fatalf("loaded config = %+v", loaded)
	}
	if err := os.Chmod(tokenPath, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(configPath); err == nil {
		t.Fatal("LoadConfig accepted a token file accessible to group/world")
	}
}
