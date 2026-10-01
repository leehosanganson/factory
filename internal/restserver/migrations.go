package restserver

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// Migration defines the migration-framework operations used by startup. It
// keeps startup behavior testable without opening a PostgreSQL connection.
type Migration interface {
	Up() error
	Close() (sourceErr error, databaseErr error)
}

// MigrationFactory constructs a migration instance from its source and DB URL.
type MigrationFactory func(sourceURL, databaseURL string) (Migration, error)

// RunMigrations applies all project SQL migrations. Supplying newMigration
// injects a test or deployment-specific runner; nil uses golang-migrate.
func RunMigrations(cfg Config, newMigration MigrationFactory) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validate REST server config: %w", err)
	}
	if newMigration == nil {
		newMigration = func(sourceURL, databaseURL string) (Migration, error) {
			return migrate.New(sourceURL, databaseURL)
		}
	}
	sourceURL := (&url.URL{Scheme: "file", Path: filepath.ToSlash(cfg.MigrationDir)}).String()
	migration, err := newMigration(sourceURL, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("create database migration runner: %w", err)
	}
	upErr := migration.Up()
	sourceCloseErr, databaseCloseErr := migration.Close()
	if upErr != nil && !errors.Is(upErr, migrate.ErrNoChange) {
		return fmt.Errorf("run database migrations: %w", upErr)
	}
	if closeErr := errors.Join(sourceCloseErr, databaseCloseErr); closeErr != nil {
		return fmt.Errorf("close database migration runner: %w", closeErr)
	}
	return nil
}
