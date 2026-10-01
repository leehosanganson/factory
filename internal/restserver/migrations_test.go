package restserver

import (
	"errors"
	"testing"

	"github.com/golang-migrate/migrate/v4"
)

type fakeMigration struct {
	upErr       error
	sourceErr   error
	databaseErr error
	upCalls     int
	closeCalls  int
}

func (m *fakeMigration) Up() error {
	m.upCalls++
	return m.upErr
}

func (m *fakeMigration) Close() (error, error) {
	m.closeCalls++
	return m.sourceErr, m.databaseErr
}

func TestRunMigrationsInjectsRunnerAndTreatsNoChangeAsSuccess(t *testing.T) {
	cfg := testConfig(t)
	instance := &fakeMigration{upErr: migrate.ErrNoChange}
	called := false
	err := RunMigrations(cfg, func(sourceURL, databaseURL string) (Migration, error) {
		called = true
		if sourceURL != "file://"+cfg.MigrationDir {
			t.Errorf("source URL = %q, want file://%s", sourceURL, cfg.MigrationDir)
		}
		if databaseURL != cfg.PostgresDSN {
			t.Errorf("database URL = %q, want configured DSN", databaseURL)
		}
		return instance, nil
	})
	if err != nil || !called || instance.upCalls != 1 || instance.closeCalls != 1 {
		t.Fatalf("RunMigrations() = %v; called=%v Up=%d Close=%d", err, called, instance.upCalls, instance.closeCalls)
	}
}

func TestRunMigrationsReturnsCreationAndUpErrors(t *testing.T) {
	cfg := testConfig(t)
	creationErr := errors.New("create runner")
	if err := RunMigrations(cfg, func(string, string) (Migration, error) { return nil, creationErr }); !errors.Is(err, creationErr) {
		t.Fatalf("creation error = %v, want %v", err, creationErr)
	}

	upErr := errors.New("database unavailable")
	instance := &fakeMigration{upErr: upErr}
	err := RunMigrations(cfg, func(string, string) (Migration, error) { return instance, nil })
	if !errors.Is(err, upErr) || instance.closeCalls != 1 {
		t.Fatalf("Up error = %v, Close calls = %d; want %v and one close", err, instance.closeCalls, upErr)
	}

	closeErr := errors.New("close failed")
	instance = &fakeMigration{sourceErr: closeErr}
	if err := RunMigrations(cfg, func(string, string) (Migration, error) { return instance, nil }); !errors.Is(err, closeErr) {
		t.Fatalf("Close error = %v, want %v", err, closeErr)
	}
}
