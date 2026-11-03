package poller

import (
	"context"
	"testing"
	"time"

	issuepkg "github.com/leehosanganson/factory/internal/issue"
)

type blockingProvider struct{ started chan struct{} }

func (p blockingProvider) Candidates(ctx context.Context, _, _ string, _ int, _ int) (CandidatePage, error) {
	close(p.started)
	<-ctx.Done()
	return CandidatePage{}, ctx.Err()
}
func (blockingProvider) GetIssue(context.Context, string, int) (issuepkg.IssueSnapshot, []string, bool, error) {
	panic("unexpected")
}
func (blockingProvider) RemoveLabel(context.Context, string, int, string) error { panic("unexpected") }

func TestPollerRunStopsPromptlyOnShutdown(t *testing.T) {
	config, _, store := fixture()
	provider := blockingProvider{started: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- (Poller{Config: config, Provider: provider, Store: store}).Run(ctx) }()
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("poll did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run()=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("poller did not stop on shutdown")
	}
}
