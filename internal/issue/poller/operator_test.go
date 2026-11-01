package poller

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	restjobs "github.com/leehosanganson/factory/internal/rest/jobs"
)

func TestOperatorCommandRejectsMalformedOrUnboundedOptionsBeforeStoreAccess(t *testing.T) {
	for _, args := range [][]string{
		{}, {"list"}, {"list", "--config"}, {"list", "--config", "relative.json", "--limit", "101"},
		{"resolve", "--config", "/etc/factory/poller.json", "--source", "other/repo", "--issue", "1", "--version", strings.Repeat("a", 64)},
	} {
		if err := OperatorCommand(context.Background(), args, io.Discard); err == nil {
			t.Errorf("OperatorCommand(%v) unexpectedly succeeded", args)
		}
	}
}

func TestOperatorListLimitHonorsRequestedLimit(t *testing.T) {
	got, err := parseOperatorListLimit("7")
	if err != nil {
		t.Fatal(err)
	}
	if got != 7 {
		t.Fatalf("list limit = %d, want 7", got)
	}
}

func TestOperatorListPassesRequestedLimitToStore(t *testing.T) {
	store := &operatorActionListStub{}
	if err := listOperatorActions(context.Background(), io.Discard, []string{"list", "--config", "/tmp/poller.json", "--limit", "7"}, store); err != nil {
		t.Fatal(err)
	}
	if store.limit != 7 {
		t.Fatalf("store list limit = %d, want 7", store.limit)
	}
}

type operatorActionListStub struct {
	limit int
}

func (s *operatorActionListStub) ListIssuePollerOperatorActions(_ context.Context, limit int) ([]restjobs.IssuePollerOperatorAction, error) {
	s.limit = limit
	return nil, nil
}

func TestOperatorActionJSONContainsOnlyBoundedSafeMetadata(t *testing.T) {
	encoded, err := json.Marshal([]restjobs.IssuePollerOperatorAction{{SourceRepository: "acme/repo", IssueNumber: 42, Version: strings.Repeat("a", 64), Outcome: "operator_action_required", ReasonClass: "label_removal_failed", Attempts: 5}})
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, field := range []string{"source_repository", "issue_number", "version", "outcome", "reason_class", "attempts"} {
		if !strings.Contains(text, field) {
			t.Errorf("safe operator output omitted %q: %s", field, text)
		}
	}
	for _, forbidden := range []string{"title", "body", "task", "secret", "password", "private issue"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("operator output contains forbidden content %q: %s", forbidden, text)
		}
	}
}
