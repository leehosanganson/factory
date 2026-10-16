package restserver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigAppliesConservativeDefaults(t *testing.T) {
	path := writeConfig(t, `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["-p","{system_prompt}","{task}"]},"api_key_file":"/run/secrets/api-key"}`)
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.ListenAddress != "127.0.0.1:8080" {
		t.Fatalf("default listener = %q, want loopback-only default", config.ListenAddress)
	}
	if len(config.VerificationChecks) != 0 {
		t.Fatalf("default verification checks = %#v, want none", config.VerificationChecks)
	}
	if config.Limits.RequestBodyBytes != 512<<10 || config.Limits.TaskBytes != 256<<10 || config.Limits.QueueCapacity != 32 || config.Limits.Workers != 2 || config.Limits.MaxRecords != 1000 || config.Limits.MaxEventsPerJob != 200 || config.Limits.RegistryBytes != 256<<20 || config.Limits.HarnessOutput != 1<<20 {
		t.Fatalf("unexpected defaults: %+v", config.Limits)
	}
	timeout, err := time.ParseDuration(config.Limits.JobTimeout)
	if err != nil || timeout != 30*time.Minute {
		t.Fatalf("default timeout = %q, %v", config.Limits.JobTimeout, err)
	}
	if config.Persistence.Backend != PersistenceBackendMemory {
		t.Fatalf("default persistence backend = %q, want %q", config.Persistence.Backend, PersistenceBackendMemory)
	}
}

func TestLoadConfigAcceptsOptInParallelSubtasks(t *testing.T) {
	for _, tc := range []struct {
		name            string
		parallel        string
		wantConcurrency int
	}{
		{"default concurrency", `{"enabled":true}`, 4},
		{"minimum concurrency", `{"enabled":true,"max_concurrency":1}`, 1},
		{"bounded concurrency", `{"enabled":true,"max_concurrency":2}`, 2},
		{"maximum concurrency", `{"enabled":true,"max_concurrency":8}`, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","parallel_subtasks":`+tc.parallel+`}`)
			config, err := LoadConfig(path)
			if err != nil {
				t.Fatalf("LoadConfig() rejected parallel_subtasks: %v", err)
			}
			if config.ParallelSubtasks == nil || !config.ParallelSubtasks.Enabled {
				t.Fatalf("parallel subtasks = %+v, want enabled", config.ParallelSubtasks)
			}
			concurrency := 4
			if config.ParallelSubtasks.MaxConcurrency != nil {
				concurrency = *config.ParallelSubtasks.MaxConcurrency
			}
			if concurrency != tc.wantConcurrency {
				t.Fatalf("parallel concurrency = %d, want %d", concurrency, tc.wantConcurrency)
			}
		})
	}
}

func TestLoadConfigRejectsInvalidParallelSubtasksConfig(t *testing.T) {
	base := `"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key"`
	for _, parallel := range []string{
		`{"enabled":true,"max_concurrency":0}`,
		`{"enabled":true,"max_concurrency":9}`,
		`{"enabled":true,"max_concurrency":null}`,
		`{"enabled":true,"extra":true}`,
		`{"enabled":"yes"}`,
	} {
		if _, err := LoadConfig(writeConfig(t, `{`+base+`,"parallel_subtasks":`+parallel+`}`)); err == nil {
			t.Errorf("LoadConfig() accepted parallel_subtasks %s", parallel)
		}
	}
}

func TestLoadConfigMapsNamedPrincipalConfiguration(t *testing.T) {
	path := writeConfig(t, `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","principals":[{"id":"submit-bot","role":"submitter","token_file":"/run/submit-verifier.json","enabled":true},{"id":"disabled-operator","role":"operator","token_file":"/run/disabled-verifier.json","enabled":false},{"id":"admin","role":"operator","token_file":"/run/admin-verifier.json","enabled":true}]}`)
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Principals) != 3 || config.Principals[0] != (PrincipalConfig{ID: "submit-bot", Role: "submitter", TokenFile: "/run/submit-verifier.json", Enabled: true}) || config.Principals[1].Enabled || config.Principals[2].Role != "operator" {
		t.Fatalf("principals = %+v, want exact identity, role, path, enabled mapping", config.Principals)
	}
}

func TestLoadConfigRequiresNamedOperatorEvenWithLegacyKey(t *testing.T) {
	base := `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key"}`
	for _, document := range []string{
		strings.TrimSuffix(base, "}") + `,"principals":[]}`,
		strings.TrimSuffix(base, "}") + `,"principals":[{"id":"submitter","role":"submitter","token_file":"/run/token","enabled":true}]}`,
		strings.TrimSuffix(base, "}") + `,"principals":[{"id":"operator","role":"operator","token_file":"/run/token","enabled":false}]}`,
	} {
		path := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path); err == nil {
			t.Errorf("LoadConfig accepted configuration without an enabled named operator")
		}
	}
}

func TestLoadConfigRejectsInvalidNamedPrincipals(t *testing.T) {
	base := `"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key"`
	for _, value := range []string{
		`[{"id":"same","role":"operator","token_file":"/run/a","enabled":true},{"id":"same","role":"submitter","token_file":"/run/b","enabled":true}]`,
		`[{"id":"bad id","role":"operator","token_file":"/run/a","enabled":true}]`,
		`[{"id":"admin","role":"owner","token_file":"/run/a","enabled":true}]`,
		`[{"id":"legacy-shared","role":"operator","token_file":"/run/a","enabled":true}]`,
		`[{"id":"admin","role":"operator","token_file":"relative","enabled":true}]`,
		`[{"id":"admin","role":"operator","token_file":"/run/a","enabled":false}]`,
		`[{"id":"submit","role":"submitter","token_file":"/run/a","enabled":true}]`,
	} {
		if _, err := LoadConfig(writeConfig(t, `{`+base+`,"principals":`+value+`}`)); err == nil {
			t.Errorf("LoadConfig accepted principals %s", value)
		}
	}
}

func TestLoadControlPlaneConfigRequiresPostgresAndExplicitlyDisabledWorkers(t *testing.T) {
	base := `"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","workers_enabled":false`
	valid := writeConfig(t, `{`+base+`,"persistence":{"backend":"postgres","dsn":"postgres://worker:secret@localhost/factory"}}`)
	config, err := LoadControlPlaneConfig(valid)
	if err != nil || config.Persistence.Backend != PersistenceBackendPostgres || config.WorkersEnabledValue() {
		t.Fatalf("LoadControlPlaneConfig() = (%+v, %v), want PostgreSQL with workers disabled", config, err)
	}

	for name, document := range map[string]string{
		"workers omitted defaults enabled": `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","persistence":{"backend":"postgres","dsn":"postgres://worker:secret@localhost/factory"}}`,
		"workers enabled":                  strings.Replace(base, `"workers_enabled":false`, `"workers_enabled":true`, 1) + `,"persistence":{"backend":"postgres","dsn":"postgres://worker:secret@localhost/factory"}`,
		"memory persistence":               strings.Replace(base, `"workers_enabled":false`, `"workers_enabled":false`, 1) + `,"persistence":{"backend":"memory"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadControlPlaneConfig(writeConfig(t, document)); err == nil {
				t.Fatal("LoadControlPlaneConfig() accepted non-control-plane config")
			}
		})
	}
}

func TestLoadWorkerConfigDoesNotRequireCallerCredentialsButKeepsStrictConfig(t *testing.T) {
	config, err := LoadWorkerConfig(writeConfig(t, `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/secret/api-key","principals":[{"id":"unused","role":"operator","token_file":"/secret/verifier","enabled":true}],"persistence":{"backend":"postgres","dsn":"postgres://worker:secret@localhost/factory"}}`))
	if err != nil || config.APIKeyFile != "" || len(config.Principals) != 0 {
		t.Fatalf("worker-only config=(%+v,%v)", config, err)
	}
	if err := config.ValidateServerAuth(); err == nil {
		t.Fatal("worker-only config unexpectedly passed API auth validation")
	}
	if _, err := LoadConfig(writeConfig(t, `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"persistence":{"backend":"postgres","dsn":"postgres://worker:secret@localhost/factory"}}`)); err == nil {
		t.Fatal("server config accepted without caller authentication")
	}
}

func TestLoadConfigSelectsMemoryPersistenceExplicitly(t *testing.T) {
	path := writeConfig(t, `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","persistence":{"backend":"memory"}}`)
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.Persistence.Backend != PersistenceBackendMemory {
		t.Fatalf("persistence backend = %q, want %q", config.Persistence.Backend, PersistenceBackendMemory)
	}
}

func TestLoadConfigAcceptsSQLitePersistenceWithAbsoluteFilePath(t *testing.T) {
	path := writeConfig(t, `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","persistence":{"backend":"sqlite","path":"/var/lib/factory/jobs.db"}}`)
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.Persistence.Backend != PersistenceBackendSQLite || config.Persistence.Path != "/var/lib/factory/jobs.db" {
		t.Fatalf("persistence = %+v, want explicit SQLite path", config.Persistence)
	}
}

func TestLoadConfigRejectsSQLitePersistenceWithoutPath(t *testing.T) {
	path := writeConfig(t, `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","persistence":{"backend":"sqlite"}}`)
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "persistence.path") {
		t.Fatalf("LoadConfig() error = %v, want persistence path error", err)
	}
}

func TestLoadConfigRejectsSQLiteRelativePathAndMismatchedPersistenceFields(t *testing.T) {
	for _, persistence := range []string{`{"backend":"sqlite","path":"jobs.db"}`, `{"backend":"memory","path":"/tmp/jobs.db"}`, `{"backend":"memory","dsn":"postgres://user:secret@localhost/jobs"}`, `{"backend":"sqlite","path":"/tmp/jobs.db","dsn":"postgres://user:secret@localhost/jobs"}`, `{"backend":"postgres","path":"/tmp/jobs.db","dsn":"postgres://user:secret@localhost/jobs"}`} {
		path := writeConfig(t, `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","persistence":`+persistence+`}`)
		if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "persistence") {
			t.Errorf("LoadConfig(%s) error = %v, want persistence validation error", persistence, err)
		}
	}
}

func TestLoadConfigAcceptsPostgresPersistenceAndSanitizesDSNErrors(t *testing.T) {
	validDSN := "postgres://factory:password-secret@localhost/factory?sslmode=disable"
	config, err := LoadConfig(writeConfig(t, `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","persistence":{"backend":"postgres","dsn":"`+validDSN+`"}}`))
	if err != nil {
		t.Fatalf("LoadConfig() rejected PostgreSQL persistence: %v", err)
	}
	if config.Persistence.Backend != PersistenceBackendPostgres || config.Persistence.DSN != validDSN || config.Persistence.Path != "" {
		t.Fatalf("persistence = %+v, want exact PostgreSQL DSN and no SQLite path", config.Persistence)
	}

	for _, persistence := range []string{
		`{"backend":"postgres"}`,
		`{"backend":"postgres","dsn":"not a valid connection string"}`,
		`{"backend":"postgres","dsn":"postgres://user:password-secret@localhost/%zz"}`,
		`{"backend":"postgres","dsn":"postgres://"}`,
		`{"backend":"postgres","dsn":"postgres://localhost"}`,
		`{"backend":"postgres","dsn":"postgresql://user:password-secret@localhost/factory?sslmode=%zz"}`,
	} {
		_, err := LoadConfig(writeConfig(t, `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","persistence":`+persistence+`}`))
		if err == nil || !strings.Contains(err.Error(), "persistence.dsn") {
			t.Errorf("LoadConfig(%s) error=%v, want sanitized DSN validation error", persistence, err)
		}
		if err != nil && strings.Contains(err.Error(), "secret") {
			t.Errorf("LoadConfig(%s) leaked DSN value: %v", persistence, err)
		}
	}
}

func TestLoadConfigAcceptsGitHubProviderAndRejectsPartialProviderConfig(t *testing.T) {
	valid := `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","provider":{"backend":"github","token_file":"/run/secrets/provider","base_branch":"main","repositories":{"widget":"acme/widget"}}}`
	path := writeConfig(t, valid)
	config, err := LoadConfig(path)
	if err != nil || config.Provider.Backend != "github" || config.Provider.Repositories["widget"] != "acme/widget" {
		t.Fatalf("LoadConfig() = (%+v,%v)", config.Provider, err)
	}
	for _, provider := range []string{`{"backend":"github","token_file":"relative","base_branch":"main","repositories":{"widget":"acme/widget"}}`, `{"backend":"github","token_file":"/run/token","base_branch":"main","repositories":{"other":"acme/widget"}}`, `{"backend":"gitlab","token_file":"/run/token"}`} {
		bad := writeConfig(t, `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","provider":`+provider+`}`)
		if _, err := LoadConfig(bad); err == nil {
			t.Errorf("provider config %s accepted", provider)
		}
	}
}

func TestLoadConfigParsesOperatorVerificationChecksWithoutShellParsing(t *testing.T) {
	path := writeConfig(t, `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{task}","{system_prompt}"]},"verification_checks":[["go","test","./..."],["./scripts/check.sh","--strict mode"]],"api_key_file":"/run/secrets/key"}`)
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"go", "test", "./..."}, {"./scripts/check.sh", "--strict mode"}}
	if len(config.VerificationChecks) != len(want) {
		t.Fatalf("verification checks = %#v, want %#v", config.VerificationChecks, want)
	}
	for i := range want {
		if strings.Join(config.VerificationChecks[i], "\x00") != strings.Join(want[i], "\x00") {
			t.Fatalf("verification check %d = %#v, want exact argv %#v", i, config.VerificationChecks[i], want[i])
		}
	}
}

func TestLoadConfigAcceptsBoundedOverrides(t *testing.T) {
	path := writeConfig(t, `{"mode":"local_process","listen_address":"0.0.0.0:65535","repositories":{"repo_1":"/tmp/checkout"},"harness":{"executable":"/usr/bin/pi","args":["{task}","{system_prompt}"]},"api_key_file":"/tmp/key","limits":{"request_body_bytes":2097152,"task_bytes":262144,"queue_capacity":1024,"workers":64,"max_records":1000,"max_events_per_job":200,"registry_bytes":268435456,"job_timeout":"24h","harness_output_bytes":16777216}}`)
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.ListenAddress != "0.0.0.0:65535" {
		t.Fatalf("explicit listener override = %q", config.ListenAddress)
	}
	if config.Limits.RequestBodyBytes != maxRequestBodyBytes || config.Limits.Workers != maxWorkers || config.Limits.TaskBytes != maxTaskBytes || config.Limits.MaxRecords != maxRecords || config.Limits.MaxEventsPerJob != maxEventsPerJob || config.Limits.RegistryBytes != maxRegistryBytes {
		t.Fatalf("configured upper-bound settings were not retained: %+v", config.Limits)
	}
}

func TestLoadConfigReportsSanitizedFieldForWrongTypes(t *testing.T) {
	valid := `"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{task}","{system_prompt}"]},"api_key_file":"/run/key"`
	cases := []struct {
		name, json, field, secret string
	}{
		{"root credential field", `{` + strings.Replace(valid, `"api_key_file":"/run/key"`, `"api_key_file":["api-secret-value"]`, 1) + `}`, "api_key_file", "api-secret-value"},
		{"provider credential field", `{` + valid + `,"provider":{"backend":"github","token_file":["provider-secret-value"],"base_branch":"main","repositories":{"widget":"acme/widget"}}}`, "provider.token_file", "provider-secret-value"},
		{"named verifier path", `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{task}","{system_prompt}"]},"api_key_file":"/run/key","principals":[{"id":"operator","role":"operator","token_file":["verifier-secret-path"],"enabled":true}]}`, "principals.token_file", "verifier-secret-path"},
		{"SQLite field", `{` + valid + `,"persistence":{"backend":"sqlite","path":["database-path"]}}`, "persistence.path", "database-path"},
		{"PostgreSQL DSN field", `{` + valid + `,"persistence":{"backend":"postgres","dsn":["postgres://user:password-secret@host/db"]}}`, "persistence.dsn", "password-secret"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadConfig(writeConfig(t, tc.json))
			if err == nil || !strings.Contains(err.Error(), tc.field) || !strings.Contains(err.Error(), "invalid type") {
				t.Fatalf("LoadConfig() error = %v, want sanitized invalid-type diagnostic for %s", err, tc.field)
			}
			if strings.Contains(err.Error(), tc.secret) {
				t.Fatalf("LoadConfig() error exposed config value: %v", err)
			}
		})
	}
}

func TestLoadConfigRejectsStrictSchemaViolations(t *testing.T) {
	valid := `"mode":"local_process","listen_address":"127.0.0.1:8080","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{task}","{system_prompt}"]},"api_key_file":"/run/key"`
	cases := []struct {
		name string
		json string
	}{
		{"malformed", `{"mode":`},
		{"trailing value", `{` + valid + `} {}`},
		{"trailing garbage", `{` + valid + `} junk`},
		{"unknown root field", `{` + valid + `,"pat_file":"/secret"}`},
		{"unknown persistence field", `{` + valid + `,"persistence":{"backend":"postgres","dsn":"postgres://localhost/db","password":"secret"}}`},
		{"duplicate persistence field", `{` + valid + `,"persistence":{"backend":"postgres","dsn":"postgres://localhost/db","dsn":"postgres://localhost/db"}}`},
		{"unknown nested field", `{` + strings.Replace(valid, `"executable":"pi"`, `"executable":"pi","shell":true`, 1) + `}`},
		{"duplicate field", `{` + valid + `,"mode":"local_process"}`},
		{"case-variant field", `{` + valid + `,"Mode":"docker"}`},
		{"missing required field", `{"mode":"local_process"}`},
		{"null nested value", `{` + strings.Replace(valid, `"executable":"pi"`, `"executable":null`, 1) + `}`},
		{"invalid repository alias", `{` + strings.Replace(valid, `"widget":"/srv/widget"`, `"../widget":"/srv/widget"`, 1) + `}`},
		{"null optional limit", `{` + valid + `,"limits":{"workers":null}}`},
		{"unknown verification field", `{` + valid + `,"verification_checks":[["make","test"]],"verification_shell":"make test"}`},
		{"empty verification command", `{` + valid + `,"verification_checks":[[]]}`},
		{"non-string verification argument", `{` + valid + `,"verification_checks":[["make",1]]}`},
		{"null verification checks", `{` + valid + `,"verification_checks":null}`},
		{"duplicate verification checks", `{` + valid + `,"verification_checks":[],"verification_checks":[]}`},
		{"verification command count over limit", `{` + valid + `,"verification_checks":[["x"],["x"],["x"],["x"],["x"],["x"],["x"],["x"],["x"],["x"],["x"],["x"],["x"],["x"],["x"],["x"],["x"]]}`},
		{"verification argv count over limit", `{` + valid + `,"verification_checks":[["x","a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z","aa","ab","ac","ad","ae","af"]]}`},
		{"verification argument with nul", `{` + valid + `,"verification_checks":[["make","bad\u0000arg"]]}`},
		{"verification blank argument", `{` + valid + `,"verification_checks":[["make"," "]]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LoadConfig(writeConfig(t, tc.json)); err == nil {
				t.Fatal("invalid config was accepted")
			}
		})
	}
}

func TestConfigValidateRejectsUnsupportedAuthorityAndUnsafeValues(t *testing.T) {
	base := validConfig()
	cases := []struct {
		name   string
		change func(*Config)
	}{
		{"docker mode", func(c *Config) { c.Mode = "docker" }},
		{"postgres backend without DSN", func(c *Config) { c.Persistence = PersistenceConfig{Backend: PersistenceBackendPostgres} }},
		{"postgres backend with SQLite path", func(c *Config) {
			c.Persistence = PersistenceConfig{Backend: PersistenceBackendPostgres, DSN: "postgres://localhost/factory", Path: "/tmp/jobs.db"}
		}},
		{"memory backend with DSN", func(c *Config) {
			c.Persistence = PersistenceConfig{Backend: PersistenceBackendMemory, DSN: "postgres://localhost/factory"}
		}},
		{"SQLite backend with DSN", func(c *Config) {
			c.Persistence = PersistenceConfig{Backend: PersistenceBackendSQLite, Path: "/tmp/jobs.db", DSN: "postgres://localhost/factory"}
		}},
		{"postgres malformed DSN", func(c *Config) {
			c.Persistence = PersistenceConfig{Backend: PersistenceBackendPostgres, DSN: "postgres://user:secret@host/%zz"}
		}},

		{"missing listener", func(c *Config) { c.ListenAddress = "" }},
		{"invalid listener port", func(c *Config) { c.ListenAddress = "localhost:65536" }},
		{"missing repositories", func(c *Config) { c.Repositories = nil }},
		{"invalid alias", func(c *Config) { c.Repositories = map[string]string{"../repo": "/srv/repo"} }},
		{"relative checkout", func(c *Config) { c.Repositories["widget"] = "relative/repo" }},
		{"missing harness", func(c *Config) { c.Harness.Executable = " " }},
		{"placeholder in executable", func(c *Config) { c.Harness.Executable = "pi {task}" }},
		{"missing task placeholder", func(c *Config) { c.Harness.Args = []string{"{system_prompt}"} }},
		{"duplicate task placeholder", func(c *Config) { c.Harness.Args = []string{"{task}", "{task}", "{system_prompt}"} }},
		{"unknown placeholder", func(c *Config) { c.Harness.Args = []string{"{task}", "{system_prompt}", "{workspace}"} }},
		{"nul argument", func(c *Config) { c.Harness.Args = []string{"{task}\x00", "{system_prompt}"} }},
		{"relative key file", func(c *Config) { c.APIKeyFile = "key" }},
		{"body limit below range", func(c *Config) { c.Limits.RequestBodyBytes = 0 }},
		{"body limit over ceiling", func(c *Config) { c.Limits.RequestBodyBytes = maxRequestBodyBytes + 1 }},
		{"task limit below range", func(c *Config) { c.Limits.TaskBytes = 0 }},
		{"task larger than body", func(c *Config) { c.Limits.TaskBytes = c.Limits.RequestBodyBytes + 1 }},
		{"task limit over ceiling", func(c *Config) { c.Limits.TaskBytes = maxTaskBytes + 1 }},
		{"queue limit below range", func(c *Config) { c.Limits.QueueCapacity = 0 }},
		{"queue limit over ceiling", func(c *Config) { c.Limits.QueueCapacity = maxQueueCapacity + 1 }},
		{"zero worker count", func(c *Config) { c.Limits.Workers = 0 }},
		{"worker count over ceiling", func(c *Config) { c.Limits.Workers = maxWorkers + 1 }},
		{"invalid timeout", func(c *Config) { c.Limits.JobTimeout = "not-a-duration" }},
		{"zero timeout", func(c *Config) { c.Limits.JobTimeout = "0s" }},
		{"timeout over ceiling", func(c *Config) { c.Limits.JobTimeout = (maxJobTimeout + time.Second).String() }},
		{"record limit below range", func(c *Config) { c.Limits.MaxRecords = 0 }},
		{"record limit over ceiling", func(c *Config) { c.Limits.MaxRecords = maxRecords + 1 }},
		{"event limit below range", func(c *Config) { c.Limits.MaxEventsPerJob = 0 }},
		{"event limit over ceiling", func(c *Config) { c.Limits.MaxEventsPerJob = maxEventsPerJob + 1 }},
		{"registry budget below range", func(c *Config) { c.Limits.RegistryBytes = 0 }},
		{"registry budget over ceiling", func(c *Config) { c.Limits.RegistryBytes = maxRegistryBytes + 1 }},
		{"output limit below range", func(c *Config) { c.Limits.HarnessOutput = 0 }},
		{"output over ceiling", func(c *Config) { c.Limits.HarnessOutput = maxHarnessOutput + 1 }},
		{"nul checkout path", func(c *Config) { c.Repositories["widget"] = "/srv/widget\x00" }},
		{"nul key path", func(c *Config) { c.APIKeyFile = "/run/key\x00" }},
		{"host with whitespace", func(c *Config) { c.ListenAddress = "bad host:8080" }},
		{"empty harness arguments", func(c *Config) { c.Harness.Args = nil }},
		{"empty verification executable", func(c *Config) { c.VerificationChecks = [][]string{{" "}} }},
		{"empty verification arg", func(c *Config) { c.VerificationChecks = [][]string{{"go", ""}} }},
		{"nul verification arg", func(c *Config) { c.VerificationChecks = [][]string{{"go", "bad\x00arg"}} }},
		{"verification command count over limit", func(c *Config) { c.VerificationChecks = make([][]string, maxVerificationChecks+1) }},
		{"verification argv count over limit", func(c *Config) {
			c.VerificationChecks = [][]string{append([]string{"go"}, make([]string, maxVerificationArgs)...)}
		}},
		{"verification total bytes over limit", func(c *Config) { c.VerificationChecks = [][]string{{"go", strings.Repeat("x", maxVerificationBytes)}} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := base
			config.Repositories = map[string]string{"widget": "/srv/widget"}
			tc.change(&config)
			if err := config.Validate(); err == nil {
				t.Fatal("invalid config was accepted")
			}
		})
	}
}

func TestConfigJSONRoundTripDoesNotExposeSecretMaterial(t *testing.T) {
	config := validConfig()
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "api-key-value") {
		t.Fatalf("config JSON exposed secret value: %s", encoded)
	}
}

func validConfig() Config {
	config := DefaultConfig()
	config.ListenAddress = "127.0.0.1:8080"
	config.Repositories = map[string]string{"widget": "/srv/widget"}
	config.Harness = HarnessConfig{Executable: "pi", Args: []string{"-p", "{system_prompt}", "{task}"}}
	config.APIKeyFile = "/run/secrets/api-key"
	config.Principals = []PrincipalConfig{{ID: "test-operator", Role: "operator", TokenFile: "/run/test-token", Enabled: true}}
	return config
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	if !strings.Contains(content, `"principals"`) && !strings.Contains(content, `"api_key_file":""`) {
		content = strings.TrimSuffix(strings.TrimSpace(content), "}") + `,"principals":[{"id":"test-operator","role":"operator","token_file":"/run/test-token","enabled":true}]}`
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
