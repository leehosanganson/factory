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
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrNotIssue indicates that the requested GitHub number identifies a pull request.
	ErrNotIssue        = errors.New("reference is not an issue")
	githubOwnerPattern = regexp.MustCompile(`^[A-Za-z0-9-]+$`)
	githubRepoPattern  = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	githubIssuePattern = regexp.MustCompile(`^[0-9]+$`)
)

// IssueTracker reads the current state of an issue from a code-host provider.
// Repository is a provider-specific repository locator such as "owner/repo";
// issue is the provider's issue reference. Implementations must not modify issues.
type IssueTracker interface {
	GetIssue(context.Context, string, string) (IssueSnapshot, error)
}

// IssueSnapshot is a provider-neutral view of issue content and state. Version
// is a stable SHA-256 fingerprint of the normalized snapshot and changes when
// any exposed issue field changes. UpdatedAt is the provider's last-update time.
type IssueSnapshot struct {
	Repository string    `json:"repository"`
	Number     int       `json:"number"`
	Title      string    `json:"title"`
	Body       string    `json:"body"`
	State      string    `json:"state"`
	URL        string    `json:"url"`
	UpdatedAt  time.Time `json:"updated_at"`
	Version    string    `json:"version"`
}

type githubIssueCommand func(context.Context, ...string) ([]byte, error)

// GitHubIssueTracker reads issues through the authenticated GitHub CLI.
// Authentication remains managed by gh; this type does not handle credentials.
type GitHubIssueTracker struct {
	run githubIssueCommand
}

var _ IssueTracker = (*GitHubIssueTracker)(nil)

// NewGitHubIssueTracker creates a tracker that uses the installed gh executable
// and its existing authentication configuration.
func NewGitHubIssueTracker() *GitHubIssueTracker {
	return newGitHubIssueTracker(runGitHubIssueCommand)
}

func newGitHubIssueTracker(run githubIssueCommand) *GitHubIssueTracker {
	return &GitHubIssueTracker{run: run}
}

func (t *GitHubIssueTracker) GetIssue(ctx context.Context, repository, issue string) (IssueSnapshot, error) {
	if ctx == nil {
		return IssueSnapshot{}, errors.New("issue tracker context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return IssueSnapshot{}, err
	}
	if err := validateGitHubRepository(repository); err != nil {
		return IssueSnapshot{}, err
	}
	number, err := validateGitHubIssueNumber(issue)
	if err != nil {
		return IssueSnapshot{}, err
	}
	if t == nil || t.run == nil {
		return IssueSnapshot{}, errors.New("GitHub issue tracker command is unavailable")
	}

	endpoint := "repos/" + repository + "/issues/" + issue
	data, err := t.run(ctx, "api", "--method", "GET", endpoint)
	if err != nil {
		return IssueSnapshot{}, fmt.Errorf("read GitHub issue %s#%s: %w", repository, issue, err)
	}
	var response struct {
		ID          int64           `json:"id"`
		Number      int             `json:"number"`
		Title       string          `json:"title"`
		Body        string          `json:"body"`
		State       string          `json:"state"`
		URL         string          `json:"html_url"`
		UpdatedAt   time.Time       `json:"updated_at"`
		PullRequest json.RawMessage `json:"pull_request"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return IssueSnapshot{}, fmt.Errorf("invalid GitHub issue JSON: %w", err)
	}
	if len(response.PullRequest) != 0 && !bytes.Equal(bytes.TrimSpace(response.PullRequest), []byte("null")) {
		return IssueSnapshot{}, fmt.Errorf("GitHub reference %s#%s: %w", repository, issue, ErrNotIssue)
	}
	state := strings.ToLower(response.State)
	if response.ID <= 0 || response.Number != number || response.Title == "" || response.URL == "" || response.UpdatedAt.IsZero() || (state != "open" && state != "closed") {
		return IssueSnapshot{}, errors.New("GitHub issue response is incomplete or has an unexpected identity")
	}
	parsedURL, err := url.Parse(response.URL)
	wantPath := "/" + repository + "/issues/" + issue
	if err != nil || parsedURL.Scheme != "https" || parsedURL.Host == "" || parsedURL.User != nil || parsedURL.Path != wantPath || parsedURL.RawQuery != "" || parsedURL.Fragment != "" {
		return IssueSnapshot{}, errors.New("GitHub issue response contains an invalid URL")
	}
	snapshot := IssueSnapshot{
		Repository: repository,
		Number:     response.Number,
		Title:      response.Title,
		Body:       response.Body,
		State:      state,
		URL:        response.URL,
		UpdatedAt:  response.UpdatedAt.UTC(),
	}
	versionInput, err := json.Marshal(snapshot)
	if err != nil {
		return IssueSnapshot{}, fmt.Errorf("fingerprint GitHub issue: %w", err)
	}
	fingerprint := sha256.Sum256(versionInput)
	snapshot.Version = hex.EncodeToString(fingerprint[:])
	return snapshot, nil
}

func validateGitHubRepository(repository string) error {
	parts := strings.Split(repository, "/")
	if len(parts) != 2 || !githubOwnerPattern.MatchString(parts[0]) || !githubRepoPattern.MatchString(parts[1]) || parts[1] == "." || parts[1] == ".." {
		return fmt.Errorf("invalid GitHub repository %q: expected owner/repo", repository)
	}
	return nil
}

func validateGitHubIssueNumber(issue string) (int, error) {
	if !githubIssuePattern.MatchString(issue) {
		return 0, fmt.Errorf("invalid GitHub issue reference %q: expected a positive issue number", issue)
	}
	number, err := strconv.Atoi(issue)
	if err != nil || number <= 0 {
		return 0, fmt.Errorf("invalid GitHub issue reference %q: expected a positive issue number", issue)
	}
	return number, nil
}

func runGitHubIssueCommand(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "gh", args...)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("gh %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out.Bytes(), nil
}
