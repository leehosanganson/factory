package restclient

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigRestrictsPlainHTTPToLoopback(t *testing.T) {
	cases := []struct {
		name    string
		baseURL string
		wantErr bool
	}{
		{name: "remote hostname", baseURL: "http://api.example.com:8080", wantErr: true},
		{name: "remote IPv4", baseURL: "http://192.0.2.1:8080", wantErr: true},
		{name: "HTTPS remote hostname", baseURL: "https://api.example.com:8080"},
		{name: "localhost", baseURL: "http://localhost:8080"},
		{name: "IPv4 loopback", baseURL: "http://127.0.0.2:8080"},
		{name: "IPv6 loopback", baseURL: "http://[::1]:8080"},
		{name: "localhost lookalike", baseURL: "http://localhost.example:8080", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "token"), []byte("secret-token\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(dir, "client.json")
			config := `{"base_url":"` + tc.baseURL + `","token_file":"token"}`
			if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadConfig(configPath)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfig error = %v, wantErr %t", err, tc.wantErr)
			}
		})
	}
}

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
