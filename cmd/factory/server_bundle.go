package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restserver"
)

func runServerBundle(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: factory server bundle <create|verify|restore|inspect> [options]")
	}
	switch args[0] {
	case "create":
		configPath, destination, err := parseBundleCreateOptions(args[1:])
		if err != nil {
			return err
		}
		config, err := restserver.LoadConfig(configPath)
		if err != nil {
			return err
		}
		if config.Persistence.Backend != restserver.PersistenceBackendSQLite {
			return errors.New("recovery bundles require configured SQLite persistence")
		}
		base, err := serverBundleResultsBase()
		if err != nil {
			return err
		}
		dirs := make(map[string]string, len(config.Repositories))
		for alias := range config.Repositories {
			digest := sha256.Sum256([]byte(alias))
			dirs[alias] = hex.EncodeToString(digest[:])
		}
		return restjobs.CreateRecoveryBundle(context.Background(), config.Persistence.Path, base, dirs, destination)
	case "verify", "inspect":
		source, err := parseBundleSourceOptions(args[1:])
		if err != nil {
			return err
		}
		summary, err := restjobs.InspectRecoveryBundle(source)
		if err != nil {
			return err
		}
		if args[0] == "verify" {
			if _, err := fmt.Fprintf(out, "recovery bundle valid (%d job records)\n", len(summary.Jobs)); err != nil {
				return errors.New("write bundle verification result")
			}
			return nil
		}
		for _, job := range summary.Jobs {
			if _, err := fmt.Fprintf(out, "%s %s %s workspace=%t\n", job.ID, job.Status, job.Repository, job.Workspace); err != nil {
				return errors.New("write bundle inspection result")
			}
		}
		return nil
	case "restore":
		source, destination, err := parseBundleRestoreOptions(args[1:])
		if err != nil {
			return err
		}
		return restjobs.RestoreRecoveryBundle(source, destination)
	default:
		return fmt.Errorf("usage: factory server bundle <create|verify|restore|inspect> [options]")
	}
}

func parseBundleCreateOptions(args []string) (string, string, error) {
	if len(args) != 4 || args[0] != "--config" || args[2] != "--destination" || !filepath.IsAbs(args[1]) || !filepath.IsAbs(args[3]) {
		return "", "", errors.New("usage: factory server bundle create --config <absolute-path> --destination <absolute-path>")
	}
	return args[1], args[3], nil
}
func parseBundleSourceOptions(args []string) (string, error) {
	if len(args) != 2 || args[0] != "--source" || !filepath.IsAbs(args[1]) {
		return "", errors.New("usage: factory server bundle verify|inspect --source <absolute-path>")
	}
	return args[1], nil
}
func parseBundleRestoreOptions(args []string) (string, string, error) {
	if len(args) != 4 || args[0] != "--source" || args[2] != "--destination" || !filepath.IsAbs(args[1]) || !filepath.IsAbs(args[3]) {
		return "", "", errors.New("usage: factory server bundle restore --source <absolute-path> --destination <absolute-path>")
	}
	return args[1], args[3], nil
}
func serverBundleResultsBase() (string, error) {
	stateHome := os.Getenv("XDG_STATE_HOME")
	if stateHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", errors.New("find state directory")
		}
		stateHome = filepath.Join(home, ".local", "state")
	}
	if !filepath.IsAbs(stateHome) || strings.TrimSpace(stateHome) == "" {
		return "", errors.New("XDG_STATE_HOME must be absolute")
	}
	return filepath.Join(stateHome, "factory", "rest-server"), nil
}
