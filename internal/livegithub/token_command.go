package livegithub

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
)

func MintAndExportInstallationToken(ctx context.Context) error {
	cfg, err := LoadTargetConfig(os.Getenv)
	if err != nil {
		return err
	}
	if err := MaskActionsSecret(os.Getenv(EnvPrivateKey)); err != nil {
		return err
	}
	token, err := mintInstallationToken(ctx, cfg)
	if err != nil {
		return errors.New("GitHub App installation token mint failed")
	}
	if err := MaskActionsSecret(token); err != nil {
		return err
	}
	path := os.Getenv("GITHUB_ENV")
	if path == "" || strings.ContainsAny(token, "\r\n") {
		return errors.New("workflow token export is unavailable")
	}
	envFile, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return errors.New("workflow environment file is unavailable")
	}
	_, writeErr := fmt.Fprintf(envFile, "%s=%s\n", EnvInstallationToken, token)
	closeErr := envFile.Close()
	if writeErr != nil || closeErr != nil {
		return errors.New("installation token could not be exported")
	}
	return nil
}
