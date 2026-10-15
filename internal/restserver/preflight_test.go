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
