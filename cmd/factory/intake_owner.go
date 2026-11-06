package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/leehosanganson/factory/internal/jobs"
)

type intakeOwnerConfig struct {
	PostgresDSN string `json:"postgres_dsn"`
}

func runIntakeOwnerCommand(args []string, out io.Writer) error {
	if len(args) < 1 {
		return errors.New("usage: factory intake-owner register|inspect|transfer --config <absolute-path> --source <owner/repo> --label <exact-label> [--kind webhook|poller] [--attest <statement>]")
	}
	action := args[0]
	values := make(map[string]string)
	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) || !strings.HasPrefix(args[i], "--") || args[i+1] == "" || values[args[i]] != "" {
			return errors.New("invalid intake-owner options")
		}
		values[args[i]] = args[i+1]
	}
	configPath, source, label := values["--config"], values["--source"], values["--label"]
	if !filepath.IsAbs(configPath) || source == "" || label == "" {
		return errors.New("usage: factory intake-owner register|inspect|transfer --config <absolute-path> --source <owner/repo> --label <exact-label> [--kind webhook|poller] [--attest <statement>]")
	}
	if action != "register" && action != "inspect" && action != "transfer" {
		return errors.New("intake-owner action must be register, inspect, or transfer")
	}
	for option := range values {
		if option != "--config" && option != "--source" && option != "--label" && option != "--kind" && option != "--attest" {
			return errors.New("invalid intake-owner option")
		}
	}
	if action == "register" {
		if _, err := intakeOwnerKind(values["--kind"]); err != nil || values["--attest"] != "" {
			return errors.New("register requires --kind webhook|poller and does not accept --attest")
		}
	}
	if action == "inspect" && (values["--kind"] != "" || values["--attest"] != "") {
		return errors.New("inspect does not accept --kind or --attest")
	}
	if action == "transfer" {
		if _, err := intakeOwnerKind(values["--kind"]); err != nil || values["--attest"] == "" {
			return errors.New("transfer requires --kind webhook|poller and --attest confirming the prior adapter is stopped and source-side effects are reconciled")
		}
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return errors.New("open intake-owner config")
	}
	var config intakeOwnerConfig
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil || strings.TrimSpace(config.PostgresDSN) == "" {
		return errors.New("intake-owner config must contain a valid postgres_dsn")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("intake-owner config must contain one JSON object")
	}
	store, err := restjobs.OpenPostgresServerStore(config.PostgresDSN, restjobs.Config{})
	if err != nil {
		return errors.New("open canonical PostgreSQL job store")
	}
	defer store.CloseStore()
	ctx := context.Background()
	var result any
	switch action {
	case "register":
		kind, _ := intakeOwnerKind(values["--kind"])
		result, err = store.RegisterIntakeOwner(ctx, source, label, kind)
		if err != nil {
			return err
		}
	case "inspect":
		owner, err := store.GetIntakeOwner(ctx, source, label)
		if err != nil {
			return err
		}
		audit, err := store.ListIntakeOwnerAudit(ctx, source, label)
		if err != nil {
			return err
		}
		result = struct {
			Owner restjobs.IntakeOwner        `json:"owner"`
			Audit []restjobs.IntakeOwnerAudit `json:"audit"`
		}{owner, audit}
	case "transfer":
		kind, _ := intakeOwnerKind(values["--kind"])
		result, err = store.TransferIntakeOwner(ctx, source, label, kind, values["--attest"])
		if err != nil {
			return err
		}
	}
	encoder := json.NewEncoder(out)
	encoder.SetEscapeHTML(true)
	return encoder.Encode(result)
}

func intakeOwnerKind(value string) (restjobs.IntakeOwnerKind, error) {
	kind := restjobs.IntakeOwnerKind(value)
	if kind != restjobs.IntakeOwnerWebhook && kind != restjobs.IntakeOwnerPoller {
		return "", fmt.Errorf("kind must be webhook or poller")
	}
	return kind, nil
}
