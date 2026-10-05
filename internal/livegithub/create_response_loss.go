package livegithub

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/leehosanganson/factory/internal/restprovider"
)

var errSecondCreateRefused = errors.New("live response-loss seam refused a second POST")

type createResponseLossTransport struct {
	next      http.RoundTripper
	token     string
	request   restprovider.PublishRequest
	forwarded int
}

func newCreateResponseLossTransport(next http.RoundTripper, token string, request restprovider.PublishRequest) *createResponseLossTransport {
	return &createResponseLossTransport{next: next, token: token, request: request}
}

func (p *createResponseLossTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method != http.MethodPost {
		return p.next.RoundTrip(request)
	}
	if p.forwarded != 0 {
		return nil, errSecondCreateRefused
	}
	if request.URL.Scheme != "https" || request.URL.Host != "api.github.com" || request.URL.User != nil || request.URL.RawQuery != "" || request.URL.Fragment != "" || request.URL.Path != "/repos/"+p.request.Repository+"/pulls" || request.Header.Get("Authorization") != "Bearer "+p.token {
		return nil, errors.New("live response-loss seam rejected an unexpected GitHub create target")
	}
	if request.Body == nil {
		return nil, errors.New("live response-loss seam received an empty create payload")
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, (64<<10)+1))
	_ = request.Body.Close()
	if err != nil || len(body) > 64<<10 {
		return nil, errors.New("live response-loss seam could not validate create payload")
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	var payload struct {
		Title string `json:"title"`
		Head  string `json:"head"`
		Base  string `json:"base"`
		Body  string `json:"body"`
	}
	expectedBody := p.request.Summary + "\n\n<!-- factory-job:" + p.request.JobID + " -->"
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&payload) != nil || decoder.Decode(new(any)) != io.EOF || payload.Title != p.request.Title || payload.Head != restprovider.JobBranch(p.request.JobID) || payload.Base != p.request.BaseBranch || payload.Body != expectedBody {
		return nil, errors.New("live response-loss seam rejected mismatched repository/job/branch/base mapping")
	}
	p.forwarded++
	response, err := p.next.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if response == nil || response.Body == nil {
		return nil, errors.New("live PR create returned no response to discard")
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	_ = response.Body.Close()
	if readErr != nil || len(responseBody) > 1<<20 || response.StatusCode != http.StatusCreated || !validCreatedPRResponse(responseBody, p.request) {
		return nil, errors.New("live PR create response did not match the requested repository/job/branch/commit")
	}
	return nil, errors.New("live PR create response deliberately dropped after one successful forward")
}

func containsJobMarker(body, jobID string) bool {
	return bytes.Contains([]byte(body), []byte("<!-- factory-job:"+jobID+" -->"))
}

func validCreatedPRResponse(body []byte, request restprovider.PublishRequest) bool {
	var pr restprovider.PullRequest
	if json.Unmarshal(body, &pr) != nil {
		return false
	}
	wantURL := fmt.Sprintf("https://github.com/%s/pull/%d", request.Repository, pr.Number)
	return pr.Number > 0 && pr.State == "open" && pr.URL == wantURL && pr.Head.Ref == restprovider.JobBranch(request.JobID) && pr.Head.SHA == request.Commit && pr.Base.Ref == request.BaseBranch && pr.Base.Repo.FullName == request.Repository && pr.Body == request.Summary+"\n\n<!-- factory-job:"+request.JobID+" -->"
}

func newDirectGitHubTransport() http.RoundTripper {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return transport
}
