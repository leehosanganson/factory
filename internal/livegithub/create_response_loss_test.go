package livegithub

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/leehosanganson/factory/internal/restprovider"
)

func TestCreateResponseLossTransportRecoversThroughProductionPublisherWithoutRetry(t *testing.T) {
	jobID, err := workflowJobID("987653", "1")
	if err != nil {
		t.Fatal(err)
	}
	request := restprovider.PublishRequest{
		JobID: jobID, Repository: sandboxRepo, Worktree: "/tmp/factory-live-test",
		Commit: strings.Repeat("a", 40), Title: "Live provider test",
		Summary: "safe test summary", BaseBranch: "main",
	}
	posts, gets := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gets++
			if posts == 0 {
				_, _ = io.WriteString(w, `[]`)
				return
			}
			_, _ = io.WriteString(w, `[{"number":72,"state":"open","body":"safe test summary\n\n<!-- factory-job:`+jobID+` -->","html_url":"https://github.com/`+sandboxRepo+`/pull/72","head":{"ref":"`+restprovider.JobBranch(jobID)+`","sha":"`+request.Commit+`"},"base":{"ref":"main","repo":{"full_name":"`+sandboxRepo+`"}}}]`)
			return
		}
		posts++
		if r.Method != http.MethodPost || r.URL.Path != "/repos/"+sandboxRepo+"/pulls" {
			t.Errorf("forwarded request target = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer sandbox-token" {
			t.Errorf("forwarded authorization = %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"number":72,"state":"open","body":"safe test summary\n\n<!-- factory-job:`+jobID+` -->","html_url":"https://github.com/`+sandboxRepo+`/pull/72","head":{"ref":"`+restprovider.JobBranch(jobID)+`","sha":"`+request.Commit+`"},"base":{"ref":"main","repo":{"full_name":"`+sandboxRepo+`"}}}`)
	}))
	defer server.Close()
	seam := newCreateResponseLossTransport(rewriteGitHubToTestServer(server.URL), "sandbox-token", request)
	client := &http.Client{Transport: seam}
	publisher := restprovider.NewGitHubPublisherWithTokenPush("sandbox-token", client, func(_ context.Context, token, repository, _ string, branch string) error {
		if token != "sandbox-token" || repository != sandboxRepo || branch != restprovider.JobBranch(jobID) {
			t.Errorf("push mapping token/repository/branch = %q/%q/%q", token, repository, branch)
		}
		return nil
	})
	outcome, err := publisher.Publish(context.Background(), request)
	if err != nil || outcome.Number != 72 || outcome.Repository != sandboxRepo || outcome.Branch != restprovider.JobBranch(jobID) || outcome.Commit != request.Commit {
		t.Fatalf("production publish outcome=(%+v,%v)", outcome, err)
	}
	confirmed, err := publisher.Reconcile(context.Background(), request)
	if err != nil || confirmed != outcome || posts != 1 || gets < 3 || seam.forwarded != 1 {
		t.Fatalf("production reconcile=(%+v,%v), POST=%d GET=%d forwarded=%d; want same outcome and exactly one create", confirmed, err, posts, gets, seam.forwarded)
	}
}

func TestCreateResponseLossTransportForwardsExactlyOnceThenDropsResponse(t *testing.T) {
	jobID, err := workflowJobID("987654", "2")
	if err != nil {
		t.Fatal(err)
	}
	request := restprovider.PublishRequest{
		JobID: jobID, Repository: sandboxRepo, Commit: strings.Repeat("a", 40),
		Title: "Live provider test", Summary: "safe test summary", BaseBranch: "main",
	}
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		if r.Method != http.MethodPost || r.URL.Path != "/repos/"+sandboxRepo+"/pulls" {
			t.Errorf("forwarded request target = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer sandbox-token" {
			t.Errorf("forwarded authorization = %q", r.Header.Get("Authorization"))
		}
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload["head"] != restprovider.JobBranch(jobID) || payload["base"] != request.BaseBranch || payload["title"] != request.Title || !containsJobMarker(payload["body"], jobID) {
			t.Errorf("forwarded job mapping = %+v", payload)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"number":73,"state":"open","body":"safe test summary\n\n<!-- factory-job:`+jobID+` -->","html_url":"https://github.com/`+sandboxRepo+`/pull/73","head":{"ref":"`+restprovider.JobBranch(jobID)+`","sha":"`+request.Commit+`"},"base":{"ref":"main","repo":{"full_name":"`+sandboxRepo+`"}}}`)
	}))
	defer server.Close()
	transport := newCreateResponseLossTransport(rewriteGitHubToTestServer(server.URL), "sandbox-token", request)
	url := "https://api.github.com/repos/" + sandboxRepo + "/pulls"
	httpRequest, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader(`{"title":"Live provider test","head":"`+restprovider.JobBranch(jobID)+`","base":"main","body":"safe test summary\n\n<!-- factory-job:`+jobID+` -->"}`))
	if err != nil {
		t.Fatal(err)
	}
	httpRequest.Header.Set("Authorization", "Bearer sandbox-token")
	response, err := transport.RoundTrip(httpRequest)
	if response != nil || err == nil || !strings.Contains(err.Error(), "deliberately dropped") {
		t.Fatalf("RoundTrip=(%v,%v), want dropped response after forwarded create", response, err)
	}
	if posts != 1 || transport.forwarded != 1 {
		t.Fatalf("remote POSTs=%d seam forwarded=%d, want one each", posts, transport.forwarded)
	}
	response, err = transport.RoundTrip(httpRequest)
	if response != nil || !errors.Is(err, errSecondCreateRefused) || posts != 1 {
		t.Fatalf("second POST=(%v,%v) remote POSTs=%d, want fail-closed without forwarding", response, err, posts)
	}
}

func TestCreateResponseLossTransportRejectsMismatchedMappingBeforeForward(t *testing.T) {
	jobID, err := workflowJobID("987655", "1")
	if err != nil {
		t.Fatal(err)
	}
	request := restprovider.PublishRequest{JobID: jobID, Repository: sandboxRepo, Commit: strings.Repeat("a", 40), Title: "expected", Summary: "safe", BaseBranch: "main"}
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { posts++; w.WriteHeader(http.StatusCreated) }))
	defer server.Close()
	transport := newCreateResponseLossTransport(rewriteGitHubToTestServer(server.URL), "sandbox-token", request)
	httpRequest, err := http.NewRequest(http.MethodPost, "https://api.github.com/repos/"+sandboxRepo+"/pulls", strings.NewReader(`{"title":"wrong","head":"factory/job/`+jobID+`","base":"main","body":"<!-- factory-job:`+jobID+` -->"}`))
	if err != nil {
		t.Fatal(err)
	}
	httpRequest.Header.Set("Authorization", "Bearer sandbox-token")
	if _, err := transport.RoundTrip(httpRequest); err == nil || posts != 0 || transport.forwarded != 0 {
		t.Fatalf("mapping mismatch err=%v remote POSTs=%d forwarded=%d", err, posts, transport.forwarded)
	}
}

type rewriteGitHubTransport struct {
	next   http.RoundTripper
	apiURL *url.URL
}

func rewriteGitHubToTestServer(rawURL string) http.RoundTripper {
	apiURL, err := url.Parse(rawURL)
	if err != nil {
		panic("invalid test server URL: " + err.Error())
	}
	return rewriteGitHubTransport{next: http.DefaultTransport, apiURL: apiURL}
}

func (transport rewriteGitHubTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	copy := request.Clone(request.Context())
	copy.URL.Scheme = transport.apiURL.Scheme
	copy.URL.Host = transport.apiURL.Host
	copy.Host = transport.apiURL.Host
	return transport.next.RoundTrip(copy)
}

func TestNewDirectGitHubTransportDisablesProxy(t *testing.T) {
	transport, ok := newDirectGitHubTransport().(*http.Transport)
	if !ok || transport.Proxy != nil {
		t.Fatalf("direct transport=%T hasProxy=%t, want HTTP transport with proxy disabled", transport, transport != nil && transport.Proxy != nil)
	}
}
