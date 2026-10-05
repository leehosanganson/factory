package livegithub

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMintInstallationTokenUsesExactInstallationRepositoryAndPermissions(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var auth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		if r.Method != http.MethodPost || r.URL.Path != "/app/installations/67890/access_tokens" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		var payload struct {
			Repositories []string          `json:"repositories"`
			Permissions  map[string]string `json:"permissions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if len(payload.Repositories) != 1 || payload.Repositories[0] != "test-factory-live-sandbox" || len(payload.Permissions) != 2 || payload.Permissions["contents"] != "write" || payload.Permissions["pull_requests"] != "write" {
			t.Errorf("installation token scope = %+v", payload)
		}
		_, _ = w.Write([]byte(`{"token":"short-lived-test-token"}`))
	}))
	defer server.Close()
	cfg := Config{AppID: 12345, PrivateKey: key, InstallationID: 67890, Repository: sandboxRepo}
	token, err := mintInstallationTokenAt(context.Background(), server.Client(), cfg, server.URL)
	if err != nil || token != "short-lived-test-token" {
		t.Fatalf("mint token=(%q,%v)", token, err)
	}
	parts := strings.Split(strings.TrimPrefix(auth, "Bearer "), ".")
	if len(parts) != 3 {
		t.Fatalf("App auth token is not JWT: %q", auth)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], signature); err != nil {
		t.Fatalf("App JWT signature did not verify: %v", err)
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims map[string]any
	if err := json.Unmarshal(data, &claims); err != nil {
		t.Fatal(err)
	}
	if claims["iss"] != float64(cfg.AppID) || claims["exp"].(float64)-claims["iat"].(float64) > float64((10*time.Minute).Seconds()) {
		t.Fatalf("JWT claims are not valid and bounded: %+v", claims)
	}
}

func TestMintInstallationTokenRejectsProviderErrorsWithoutLeakingResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "private-response-detail", http.StatusForbidden)
	}))
	defer server.Close()
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	_, err := mintInstallationTokenAt(context.Background(), server.Client(), Config{AppID: 12345, PrivateKey: key, InstallationID: 67890, Repository: sandboxRepo}, server.URL)
	if err == nil || strings.Contains(err.Error(), "private-response-detail") {
		t.Fatalf("token mint error absent or leaked response: %v", err)
	}
}
