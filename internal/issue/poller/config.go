package poller

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const configLimit = 1 << 20
const maxCandidates = 100

var aliasPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
var repoPattern = regexp.MustCompile(`^[A-Za-z0-9-]+/[A-Za-z0-9._-]+$`)
var labelPattern = regexp.MustCompile(`^[^\x00-\x1f\x7f]{1,100}$`)

type Config struct {
	PostgresDSN   string            `json:"postgres_dsn"`
	TokenFile     string            `json:"github_token_file"`
	Sources       map[string]string `json:"sources"`
	Trigger       string            `json:"trigger_label"`
	PollEvery     string            `json:"poll_interval"`
	BatchSize     int               `json:"batch_size"`
	QueueCapacity int               `json:"queue_capacity"`
	Workers       int               `json:"workers"`
	MaxRecords    int               `json:"max_records"`
	MaxEvents     int               `json:"max_events_per_job"`
	TaskBytes     int               `json:"task_bytes"`
	RegistryBytes int64             `json:"registry_bytes"`
}

func LoadConfig(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, errors.New("open issue-poller config")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, configLimit+1))
	if err != nil || len(data) > configLimit {
		return Config{}, errors.New("read issue-poller config")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return Config{}, errors.New("parse issue-poller config: invalid schema")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return Config{}, errors.New("parse issue-poller config: expected one JSON value")
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.PostgresDSN) == "" || strings.ContainsRune(c.PostgresDSN, 0) {
		return errors.New("postgres_dsn is required")
	}
	parsed, err := pgx.ParseConfig(c.PostgresDSN)
	if err != nil || parsed.Host == "" || parsed.Database == "" {
		return errors.New("postgres_dsn must be a PostgreSQL connection string")
	}
	if strings.ContainsAny(c.PostgresDSN, "\r\n") {
		return errors.New("postgres_dsn must be a PostgreSQL connection string")
	}
	if !filepath.IsAbs(c.TokenFile) || strings.TrimSpace(c.TokenFile) == "" || strings.ContainsRune(c.TokenFile, 0) {
		return errors.New("github_token_file must be an absolute path")
	}
	if len(c.Sources) == 0 {
		return errors.New("sources must map at least one exact owner/repo to a destination alias")
	}
	for source, alias := range c.Sources {
		parts := strings.Split(source, "/")
		if !repoPattern.MatchString(source) || len(parts) != 2 || parts[1] == "." || parts[1] == ".." || !aliasPattern.MatchString(alias) || strings.ContainsAny(alias, ".:/\\") {
			return fmt.Errorf("sources must contain exact owner/repo keys and valid destination aliases")
		}
	}
	if !safeLabel(c.Trigger) {
		return errors.New("trigger_label must be a nonempty exact GitHub label name")
	}
	interval, err := time.ParseDuration(c.PollEvery)
	if err != nil || interval < time.Second || interval > 24*time.Hour {
		return errors.New("poll_interval must be between 1s and 24h")
	}
	if c.BatchSize < 1 || c.BatchSize > maxCandidates {
		return fmt.Errorf("batch_size must be between 1 and %d", maxCandidates)
	}
	if c.QueueCapacity < 1 || c.QueueCapacity > 1024 || c.Workers < 1 || c.Workers > 64 || c.MaxRecords < 1 || c.MaxRecords > 1000 || c.MaxEvents < 1 || c.MaxEvents > 200 || c.TaskBytes < 1 || c.TaskBytes > 256<<10 || c.RegistryBytes < 1 || c.RegistryBytes > 256<<20 {
		return errors.New("PostgreSQL Store limits must be configured within supported bounds and match the server/worker configs")
	}
	return nil
}

func (c Config) Interval() time.Duration {
	interval, _ := time.ParseDuration(c.PollEvery)
	return interval
}
