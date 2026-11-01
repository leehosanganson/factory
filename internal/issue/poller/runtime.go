package poller

import (
	"context"
	"errors"
	"os"

	"github.com/leehosanganson/factory/internal/rest/jobs"
)

func Run(ctx context.Context, config Config) error {
	storeConfig := restjobs.Config{QueueCapacity: config.QueueCapacity, MaxConcurrentJobs: config.Workers, MaxRecords: config.MaxRecords, MaxEventsPerJob: config.MaxEvents, MaxTaskBytes: config.TaskBytes, RegistryBytes: config.RegistryBytes}
	store, err := restjobs.OpenPostgresServerStore(config.PostgresDSN, storeConfig)
	if err != nil {
		return errors.New("open canonical PostgreSQL job store")
	}
	defer store.CloseStore()
	guard, err := store.AcquireIssuePollerGuard(ctx)
	if err != nil {
		return err
	}
	defer guard.Close()
	token, err := loadToken(config.TokenFile)
	if err != nil {
		return err
	}
	provider, err := NewGitHub(token)
	if err != nil {
		return err
	}
	return (Poller{Config: config, Provider: provider, Store: store, Log: os.Stderr}).Run(ctx)
}
