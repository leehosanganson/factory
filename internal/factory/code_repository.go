package factory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// CodeRepository reads pull request state from a repository provider. It is
// separate from IssueTracker because repository and issue-provider
// responsibilities are independent even when one provider implements both.
type CodeRepository interface {
	GetPullRequest(context.Context, string, int) (PullRequestSnapshot, error)
}

// PullRequestSnapshot is a provider-neutral view of pull request identity,
// content, branch tips, and state. Version fingerprints the exposed fields.
type PullRequestSnapshot struct {
	Repository string    `json:"repository"`
	Number     int       `json:"number"`
	Title      string    `json:"title"`
	Body       string    `json:"body"`
	State      string    `json:"state"`
	URL        string    `json:"url"`
	HeadBranch string    `json:"head_branch"`
	BaseBranch string    `json:"base_branch"`
	HeadCommit string    `json:"head_commit"`
	BaseCommit string    `json:"base_commit"`
	UpdatedAt  time.Time `json:"updated_at"`
	Version    string    `json:"version"`
}

type githubCodeRepositoryCommand func(context.Context, ...string) ([]byte, error)

// GitHubCodeRepository reads pull requests through the authenticated GitHub CLI.
// Authentication remains managed by gh; this type does not handle credentials.
type GitHubCodeRepository struct {
	run githubCodeRepositoryCommand
}

var _ CodeRepository = (*GitHubCodeRepository)(nil)

// NewGitHubCodeRepository creates a repository adapter that uses the installed
// gh executable and its existing authentication configuration.
func NewGitHubCodeRepository() *GitHubCodeRepository {
	return newGitHubCodeRepository(runGitHubCodeRepositoryCommand)
}

func newGitHubCodeRepository(run githubCodeRepositoryCommand) *GitHubCodeRepository {
	return &GitHubCodeRepository{run: run}
}

func (h *GitHubCodeRepository) GetPullRequest(ctx context.Context, repository string, number int) (PullRequestSnapshot, error) {
	if ctx == nil {
		return PullRequestSnapshot{}, errors.New("code repository context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return PullRequestSnapshot{}, err
	}
	if err := validateGitHubRepository(repository); err != nil {
		return PullRequestSnapshot{}, err
	}
	if number <= 0 {
		return PullRequestSnapshot{}, fmt.Errorf("invalid GitHub pull request number %d: expected a positive number", number)
	}
	if h == nil || h.run == nil {
		return PullRequestSnapshot{}, errors.New("GitHub code repository command is unavailable")
	}

	numberText := strconv.Itoa(number)
	endpoint := "repos/" + repository + "/pulls/" + numberText
	data, err := h.run(ctx, "api", "--method", "GET", endpoint)
	if err != nil {
		return PullRequestSnapshot{}, fmt.Errorf("read GitHub pull request %s#%d: %w", repository, number, err)
	}
	var response struct {
		Number    int       `json:"number"`
		Title     string    `json:"title"`
		Body      string    `json:"body"`
		State     string    `json:"state"`
		URL       string    `json:"html_url"`
		UpdatedAt time.Time `json:"updated_at"`
		Head      struct {
			Ref string `json:"ref"`
			SHA string `json:"sha"`
		} `json:"head"`
		Base struct {
			Ref  string `json:"ref"`
			SHA  string `json:"sha"`
			Repo struct {
				FullName string `json:"full_name"`
			} `json:"repo"`
		} `json:"base"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return PullRequestSnapshot{}, fmt.Errorf("invalid GitHub pull request JSON: %w", err)
	}
	state := strings.ToLower(response.State)
	if response.Number != number || response.Base.Repo.FullName != repository {
		return PullRequestSnapshot{}, errors.New("GitHub pull request response has an unexpected identity")
	}
	if response.Title == "" || response.URL == "" || response.UpdatedAt.IsZero() || (state != "open" && state != "closed") ||
		response.Head.Ref == "" || response.Head.SHA == "" || response.Base.Ref == "" || response.Base.SHA == "" {
		return PullRequestSnapshot{}, errors.New("GitHub pull request response is incomplete")
	}
	parsedURL, err := url.Parse(response.URL)
	wantPath := "/" + repository + "/pull/" + numberText
	if err != nil || parsedURL.Scheme != "https" || parsedURL.Host != "github.com" || parsedURL.User != nil || parsedURL.Path != wantPath || parsedURL.RawQuery != "" || parsedURL.Fragment != "" {
		return PullRequestSnapshot{}, errors.New("GitHub pull request response contains an unexpected URL")
	}

	snapshot := PullRequestSnapshot{
		Repository: repository,
		Number:     response.Number,
		Title:      response.Title,
		Body:       response.Body,
		State:      state,
		URL:        response.URL,
		HeadBranch: response.Head.Ref,
		BaseBranch: response.Base.Ref,
		HeadCommit: response.Head.SHA,
		BaseCommit: response.Base.SHA,
		UpdatedAt:  response.UpdatedAt.UTC(),
	}
	versionInput, err := json.Marshal(snapshot)
	if err != nil {
		return PullRequestSnapshot{}, fmt.Errorf("fingerprint GitHub pull request: %w", err)
	}
	fingerprint := sha256.Sum256(versionInput)
	snapshot.Version = hex.EncodeToString(fingerprint[:])
	return snapshot, nil
}

func runGitHubCodeRepositoryCommand(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "gh", args...)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("gh %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out.Bytes(), nil
}
