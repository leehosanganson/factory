package livegithub

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/leehosanganson/factory/internal/restprovider"
)

func TestCleanupRefusesMatchingBranchWithDifferentPRIdentity(t *testing.T) {
	jobID, err := workflowJobID("43", "1")
	if err != nil {
		t.Fatal(err)
	}
	server := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/pulls") {
			_, _ = io.WriteString(w, `[{"number":2,"state":"open","body":"unrelated","html_url":"https://github.com/sandbox-owner/test-factory-live-sandbox/pull/2","head":{"ref":"`+restprovider.JobBranch(jobID)+`","sha":"different"},"base":{"ref":"main","repo":{"full_name":"sandbox-owner/test-factory-live-sandbox"}}}]`)
			return
		}
		t.Errorf("cleanup wrote after mismatched PR: %s %s", r.Method, r.URL.Path)
	})
	defer server.Close()
	if err := cleanupOwnedPullRequest(context.Background(), server.Client(), "token", "sandbox-owner/test-factory-live-sandbox", approvedOrganization, t.TempDir(), jobID, "expected", server.URL); err == nil {
		t.Fatal("cleanup accepted a PR with the run branch but an unrelated marker and commit")
	}
}
