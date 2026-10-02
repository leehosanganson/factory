package restprovider

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
)

var ErrUncertain = errors.New("provider write outcome is uncertain")

var jobIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
var repositoryPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
var branchPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$`)

type PublishRequest struct {
	JobID      string
	Repository string
	Worktree   string
	Branch     string
	Commit     string
	Title      string
	Summary    string
	BaseBranch string
}

type Outcome struct {
	Provider   string `json:"provider"`
	Repository string `json:"repository"`
	Number     int    `json:"number"`
	URL        string `json:"url"`
	Branch     string `json:"branch"`
	Commit     string `json:"commit"`
	State      string `json:"state"`
}

type Publisher interface {
	Publish(context.Context, PublishRequest) (Outcome, error)
	Ping(context.Context) error
}

func ValidRepository(value string) bool {
	return repositoryPattern.MatchString(value) && !strings.Contains(value, "..")
}

func JobBranch(jobID string) string { return "factory/job/" + jobID }
func JobIDValid(jobID string) bool  { return jobIDPattern.MatchString(jobID) }

func validateRequest(request PublishRequest) error {
	if !JobIDValid(request.JobID) || !ValidRepository(request.Repository) || !filepath.IsAbs(request.Worktree) || request.Commit == "" || len(request.Commit) > 64 || !validBranchName(request.BaseBranch) || strings.TrimSpace(request.Title) == "" || len(request.Title) > 256 || len(request.Summary) > 4096 {
		return errors.New("invalid provider publish request")
	}
	if request.Branch != "" && request.Branch != JobBranch(request.JobID) {
		return errors.New("provider branch does not match job identity")
	}
	return nil
}

func validBranchName(value string) bool {
	return branchPattern.MatchString(value) && !strings.Contains(value, "..") && !strings.Contains(value, "//") && !strings.HasSuffix(value, "/")
}

func validateProviderURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("GitHub API URL must be a clean origin")
	}
	if parsed.Scheme == "https" && parsed.Host != "api.github.com" {
		return errors.New("GitHub API URL host must be api.github.com")
	}
	if parsed.Scheme == "http" && parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost" {
		return errors.New("GitHub API URL must use HTTPS except for loopback tests")
	}
	return nil
}
