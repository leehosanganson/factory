package poller

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigRejectsUnknownFieldsWithoutLeakingDSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := `{"postgres_dsn":"postgres://user:secret@localhost/factory","github_token_file":"/run/issue-token","sources":{"acme/repo":"dest"},"trigger_label":"ready","poll_interval":"1s","batch_size":10,"queue_capacity":4,"workers":2,"max_records":10,"max_events_per_job":20,"unknown":"must fail"}`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(path)
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("strict/sanitized config error=%v", err)
	}
}

func TestLoadConfigRequiresAllMappingAndStorageSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	data := `{"postgres_dsn":"postgres://user:secret@localhost/factory","github_token_file":"/run/issue-token","sources":{"acme/repo":"dest"},"trigger_label":"ready","poll_interval":"1s","batch_size":10,"queue_capacity":4,"workers":2,"max_records":10,"max_events_per_job":20,"task_bytes":262144,"registry_bytes":268435456}`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(path)
	if err != nil || len(config.Sources) != 1 {
		t.Fatalf("LoadConfig=(%+v,%v)", config, err)
	}
	for _, bad := range []string{strings.Replace(data, `"poll_interval":"1s"`, `"poll_interval":"0s"`, 1), strings.Replace(data, `"sources":{"acme/repo":"dest"}`, `"sources":{"acme/repo/extra":"dest"}`, 1), strings.Replace(data, `"trigger_label":"ready"`, `"trigger_label":" ready"`, 1)} {
		if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path); err == nil {
			t.Fatalf("accepted invalid config: %s", bad)
		}
	}
}
