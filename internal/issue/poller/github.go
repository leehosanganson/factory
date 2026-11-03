package poller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	issuepkg "github.com/leehosanganson/factory/internal/issue"
)

type ghCommand func(context.Context, string, string, ...string) ([]byte, error)

type GitHub struct {
	token string
	run   ghCommand
}

func NewGitHub(token string) (*GitHub, error) {
	if strings.TrimSpace(token) == "" || strings.ContainsAny(token, "\r\n\x00") {
		return nil, errors.New("GitHub issue-poller token is invalid")
	}
	return &GitHub{token: token, run: runGH}, nil
}

func newGitHub(token string, run ghCommand) *GitHub { return &GitHub{token: token, run: run} }

func (g *GitHub) Candidates(ctx context.Context, repository, label string, limit, page int) (CandidatePage, error) {
	if !validSourceRepository(repository) || !labelPattern.MatchString(label) || limit < 1 || limit > maxCandidates || page < 1 {
		return CandidatePage{}, errors.New("invalid GitHub candidate query")
	}
	endpoint := "repos/" + repository + "/issues"
	data, err := g.run(ctx, g.token, endpoint, "-f", "state=open", "-f", "labels="+label, "-F", "per_page="+fmt.Sprint(limit), "-F", "page="+fmt.Sprint(page))
	if err != nil {
		return CandidatePage{}, errors.New("GitHub candidate query failed")
	}
	var entries []githubIssue
	if json.Unmarshal(data, &entries) != nil {
		return CandidatePage{}, errors.New("GitHub candidate response is invalid")
	}
	result := make([]issuepkg.IssueSnapshot, 0, len(entries))
	for _, entry := range entries {
		if hasPullRequest(entry.PullRequest) || !hasGitHubLabel(entry.Labels, label) {
			continue
		}
		snapshot, err := entry.snapshot(repository)
		if err != nil {
			return CandidatePage{}, errors.New("GitHub candidate identity is invalid")
		}
		result = append(result, snapshot)
	}
	return CandidatePage{Issues: result, HasNext: len(entries) == limit}, nil
}

func (g *GitHub) GetIssue(ctx context.Context, repository string, number int) (issuepkg.IssueSnapshot, []string, bool, error) {
	if !validSourceRepository(repository) || number < 1 {
		return issuepkg.IssueSnapshot{}, nil, false, errors.New("invalid GitHub issue identity")
	}
	endpoint := fmt.Sprintf("repos/%s/issues/%d", repository, number)
	data, err := g.run(ctx, g.token, endpoint)
	if err != nil {
		return issuepkg.IssueSnapshot{}, nil, false, errors.New("GitHub issue re-fetch failed")
	}
	var entry githubIssue
	if json.Unmarshal(data, &entry) != nil {
		return issuepkg.IssueSnapshot{}, nil, false, errors.New("GitHub issue response is invalid")
	}
	if hasPullRequest(entry.PullRequest) {
		return issuepkg.IssueSnapshot{}, nil, true, nil
	}
	snapshot, err := entry.snapshot(repository)
	if err != nil {
		return issuepkg.IssueSnapshot{}, nil, false, err
	}
	labels := make([]string, 0, len(entry.Labels))
	for _, item := range entry.Labels {
		labels = append(labels, item.Name)
	}
	return snapshot, labels, false, nil
}

func (g *GitHub) RemoveLabel(ctx context.Context, repository string, number int, label string) error {
	if !validSourceRepository(repository) || number < 1 || !labelPattern.MatchString(label) {
		return errors.New("invalid GitHub label removal request")
	}
	endpoint := fmt.Sprintf("repos/%s/issues/%d/labels/%s", repository, number, urlPathEscape(label))
	if _, err := g.run(ctx, g.token, endpoint, "--method", "DELETE"); err != nil {
		return errors.New("GitHub trigger-label removal failed")
	}
	return nil
}

func hasPullRequest(value json.RawMessage) bool {
	return len(value) != 0 && !bytes.Equal(bytes.TrimSpace(value), []byte("null"))
}

type githubIssue struct {
	ID          int64           `json:"id"`
	Number      int             `json:"number"`
	Title       string          `json:"title"`
	Body        string          `json:"body"`
	State       string          `json:"state"`
	URL         string          `json:"html_url"`
	UpdatedAt   time.Time       `json:"updated_at"`
	PullRequest json.RawMessage `json:"pull_request"`
	Labels      []struct {
		Name string `json:"name"`
	} `json:"labels"`
}

func (entry githubIssue) snapshot(repository string) (issuepkg.IssueSnapshot, error) {
	if entry.ID <= 0 || entry.Number <= 0 || entry.Title == "" || entry.UpdatedAt.IsZero() || (entry.State != "open" && entry.State != "closed") {
		return issuepkg.IssueSnapshot{}, errors.New("GitHub issue identity or state is invalid")
	}
	if !validIssueURL(entry.URL, repository, entry.Number) {
		return issuepkg.IssueSnapshot{}, errors.New("GitHub issue URL is invalid")
	}
	snapshot := issuepkg.IssueSnapshot{Repository: repository, Number: entry.Number, Title: entry.Title, Body: entry.Body, State: entry.State, URL: entry.URL, UpdatedAt: entry.UpdatedAt.UTC()}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return issuepkg.IssueSnapshot{}, errors.New("fingerprint GitHub issue")
	}
	hash := sha256.Sum256(encoded)
	snapshot.Version = hex.EncodeToString(hash[:])
	return snapshot, nil
}

func hasGitHubLabel(labels []struct {
	Name string `json:"name"`
}, expected string) bool {
	for _, label := range labels {
		if label.Name == expected {
			return true
		}
	}
	return false
}

func validSourceRepository(repository string) bool {
	parts := strings.Split(repository, "/")
	return len(parts) == 2 && repoPattern.MatchString(repository) && parts[1] != "." && parts[1] != ".."
}

func validIssueURL(value, repository string, number int) bool {
	return value == fmt.Sprintf("https://github.com/%s/issues/%d", repository, number)
}

func runGH(ctx context.Context, token, endpoint string, extra ...string) ([]byte, error) {
	directory, err := os.MkdirTemp("", "factory-issue-poller-gh-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(directory)
	if err := os.Chmod(directory, 0o700); err != nil {
		return nil, err
	}
	args := []string{"api", endpoint}
	args = append(args, extra...)
	command := exec.CommandContext(ctx, "gh", args...)
	command.Env = cleanEnv(os.Environ())
	command.Dir = directory
	command.Env = append(command.Env, "GH_TOKEN="+token, "GH_CONFIG_DIR="+filepath.Clean(directory), "GH_PROMPT_DISABLED=1")
	output := &boundedBuffer{limit: 2 << 20}
	command.Stdout = output
	command.Stderr = &boundedBuffer{limit: 8192}
	if err := command.Run(); err != nil {
		return nil, errors.New("GitHub CLI request failed")
	}
	if output.overflow {
		return nil, errors.New("GitHub response too large")
	}
	return output.data, nil
}

type boundedBuffer struct {
	limit    int
	data     []byte
	overflow bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	original := len(p)
	remaining := b.limit - len(b.data)
	if remaining > 0 {
		b.data = append(b.data, p[:min(remaining, len(p))]...)
	}
	if len(p) > remaining {
		b.overflow = true
	}
	return original, nil
}

func cleanEnv(input []string) []string {
	result := make([]string, 0, len(input))
	for _, entry := range input {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "GH_") || key == "GITHUB_TOKEN" {
			continue
		}
		result = append(result, entry)
	}
	return result
}

func urlPathEscape(value string) string {
	return strings.ReplaceAll(url.PathEscape(value), "+", "%20")
}
