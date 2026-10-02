package restprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type GitHubPublisher struct {
	token         string
	client        *http.Client
	apiURL        string
	push          func(context.Context, string, string, string) error
	pushWithToken func(context.Context, string, string, string, string) error
}

type PullRequest struct {
	Number int    `json:"number"`
	State  string `json:"state"`
	Body   string `json:"body"`
	URL    string `json:"html_url"`
	Head   struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"head"`
	Base struct {
		Ref  string `json:"ref"`
		Repo struct {
			FullName string `json:"full_name"`
		} `json:"repo"`
	} `json:"base"`
}

func newGitHubPublisher(token string, client *http.Client, apiURL string, push func(context.Context, string, string, string) error) *GitHubPublisher {
	if client == nil {
		client = http.DefaultClient
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &GitHubPublisher{token: token, client: &clientCopy, apiURL: strings.TrimRight(apiURL, "/"), push: push}
}

func NewGitHubPublisher(token string, client *http.Client, push func(context.Context, string, string, string) error) *GitHubPublisher {
	return newGitHubPublisher(token, client, "https://api.github.com", push)
}

func NewGitHubPublisherWithTokenPush(token string, client *http.Client, push func(context.Context, string, string, string, string) error) *GitHubPublisher {
	publisher := newGitHubPublisher(token, client, "https://api.github.com", nil)
	publisher.pushWithToken = push
	return publisher
}

func (g *GitHubPublisher) Publish(ctx context.Context, request PublishRequest) (Outcome, error) {
	if ctx == nil || ctx.Err() != nil {
		return Outcome{}, errors.New("provider request canceled")
	}
	if err := validateRequest(request); err != nil {
		return Outcome{}, err
	}
	if g == nil || g.client == nil || g.token == "" || (g.push == nil && g.pushWithToken == nil) || validateProviderURL(g.apiURL) != nil {
		return Outcome{}, errors.New("GitHub provider is not configured")
	}
	branch := JobBranch(request.JobID)
	var pushErr error
	if g.pushWithToken != nil {
		pushErr = g.pushWithToken(ctx, g.token, request.Repository, request.Worktree, branch)
	} else {
		pushErr = g.push(ctx, request.Repository, request.Worktree, branch)
	}
	if pushErr != nil {
		return Outcome{}, fmt.Errorf("%w: push job branch", ErrUncertain)
	}
	endpoint, err := url.JoinPath(g.apiURL, "repos", request.Repository, "pulls")
	if err != nil {
		return Outcome{}, errors.New("GitHub PR endpoint is invalid")
	}
	payload, err := json.Marshal(map[string]string{
		"title": request.Title,
		"head":  branch,
		"base":  request.BaseBranch,
		"body":  request.Summary + "\n\n<!-- factory-job:" + request.JobID + " -->",
	})
	if err != nil {
		return Outcome{}, errors.New("encode GitHub pull request")
	}
	if outcome, found, err := g.findJobPR(ctx, endpoint, request); err != nil {
		return Outcome{}, err
	} else if found {
		updatePayload, err := json.Marshal(map[string]string{"title": request.Title})
		if err != nil {
			return Outcome{}, errors.New("encode GitHub pull request update")
		}
		var updated PullRequest
		updateEndpoint, err := url.JoinPath(g.apiURL, "repos", request.Repository, "pulls", fmt.Sprint(outcome.Number))
		if err != nil {
			return Outcome{}, errors.New("GitHub PR endpoint is invalid")
		}
		for attempt := 0; attempt < 2; attempt++ {
			if err := g.doJSON(ctx, http.MethodPatch, updateEndpoint, updatePayload, &updated); err == nil {
				if !validPullRequest(updated, request, branch) {
					return Outcome{}, fmt.Errorf("%w: GitHub pull request update identity mismatch", ErrUncertain)
				}
				return outcomeFor(updated, request, branch), nil
			}
			if ctx.Err() != nil {
				return Outcome{}, fmt.Errorf("%w: GitHub pull request update was interrupted", ErrUncertain)
			}
			var reconciled []PullRequest
			if err := g.doJSON(ctx, http.MethodGet, endpoint+"?"+url.Values{"head": {strings.Split(request.Repository, "/")[0] + ":" + branch}, "state": {"all"}, "per_page": {"100"}}.Encode(), nil, &reconciled); err != nil {
				return Outcome{}, fmt.Errorf("%w: reconcile GitHub pull request update", ErrUncertain)
			}
			found := false
			for _, current := range reconciled {
				if current.Number == outcome.Number && current.Head.Ref == branch && current.Head.SHA == request.Commit && current.Base.Ref == request.BaseBranch && current.Base.Repo.FullName == request.Repository && current.State == "open" {
					found = true
					break
				}
			}
			if !found {
				return Outcome{}, fmt.Errorf("%w: matching pull request could not be confirmed after update", ErrUncertain)
			}
		}
		return Outcome{}, fmt.Errorf("%w: GitHub pull request update remains unresolved", ErrUncertain)
	}
	var created PullRequest
	if err := g.doJSON(ctx, http.MethodPost, endpoint, payload, &created); err != nil {
		if outcome, found, lookupErr := g.findJobPR(ctx, endpoint, request); lookupErr == nil && found {
			return outcome, nil
		}
		return Outcome{}, fmt.Errorf("%w: create or reconcile GitHub pull request", ErrUncertain)
	}
	if !validPullRequest(created, request, branch) {
		if outcome, found, lookupErr := g.findJobPR(ctx, endpoint, request); lookupErr == nil && found {
			return outcome, nil
		}
		return Outcome{}, fmt.Errorf("%w: GitHub pull request response identity mismatch", ErrUncertain)
	}
	return outcomeFor(created, request, branch), nil
}

func (g *GitHubPublisher) findJobPR(ctx context.Context, endpoint string, request PublishRequest) (Outcome, bool, error) {
	query := url.Values{"head": {strings.Split(request.Repository, "/")[0] + ":" + JobBranch(request.JobID)}, "state": {"all"}, "per_page": {"100"}}
	var existing []PullRequest
	if err := g.doJSON(ctx, http.MethodGet, endpoint+"?"+query.Encode(), nil, &existing); err != nil {
		return Outcome{}, false, fmt.Errorf("reconcile GitHub pull request: %w", err)
	}
	for _, pr := range existing {
		if pr.Head.Ref != JobBranch(request.JobID) {
			continue
		}
		if !validPullRequest(pr, request, JobBranch(request.JobID)) {
			return Outcome{}, false, errors.New("existing GitHub pull request does not match job identity")
		}
		return outcomeFor(pr, request, JobBranch(request.JobID)), true, nil
	}
	return Outcome{}, false, nil
}

func validPullRequest(pr PullRequest, request PublishRequest, branch string) bool {
	marker := "<!-- factory-job:" + request.JobID + " -->"
	return pr.Number > 0 && pr.URL == fmt.Sprintf("https://github.com/%s/pull/%d", request.Repository, pr.Number) && pr.Head.Ref == branch && pr.Head.SHA == request.Commit && pr.Base.Ref == request.BaseBranch && pr.Base.Repo.FullName == request.Repository && pr.State == "open" && strings.Contains(pr.Body, marker)
}

func outcomeFor(pr PullRequest, request PublishRequest, branch string) Outcome {
	return Outcome{Provider: "github", Repository: request.Repository, Number: pr.Number, URL: pr.URL, Branch: branch, Commit: request.Commit, State: pr.State}
}

func (g *GitHubPublisher) Ping(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil || g == nil || g.client == nil || g.token == "" || validateProviderURL(g.apiURL) != nil {
		return errors.New("GitHub provider is not configured")
	}
	var user struct {
		Login string `json:"login"`
	}
	endpoint, err := url.JoinPath(g.apiURL, "user")
	if err != nil {
		return errors.New("GitHub identity endpoint is invalid")
	}
	if err := g.doJSON(ctx, http.MethodGet, endpoint, nil, &user); err != nil {
		return err
	}
	if strings.TrimSpace(user.Login) == "" {
		return errors.New("GitHub provider identity response is incomplete")
	}
	return nil
}

func (g *GitHubPublisher) doJSON(ctx context.Context, method, endpoint string, body []byte, target any) error {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return errors.New("build GitHub request")
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+g.token)
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return errors.New("GitHub request transport failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GitHub API returned HTTP %d", resp.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 1<<20))
	if err := decoder.Decode(target); err != nil {
		return errors.New("GitHub API response was malformed")
	}
	return nil
}

var _ Publisher = (*GitHubPublisher)(nil)
