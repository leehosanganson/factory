package restserver

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRemoteWorkerConfigBoundsExecutableRepositoryCapabilitySet(t *testing.T) {
	repositories := make(map[string]string)
	profiles := make(map[string]RemoteHarnessConfig)
	for i := 0; i < 17; i++ {
		alias := fmt.Sprintf("repo-%02d", i)
		repositories[alias] = "/srv/" + alias
		profiles["profile-"+alias] = RemoteHarnessConfig{Executable: "pi", Args: []string{"{system_prompt}", "{task}"}, Repositories: []string{alias}}
	}
	config := RemoteWorkerConfig{ControllerBaseURL: "https://controller.example", SharedSecretFile: "/run/worker-secret", WorkerID: "worker-a", Repositories: repositories, HarnessProfiles: profiles, Limits: RemoteWorkerLimits{JobTimeout: "1m", HarnessOutputBytes: 1024}}
	if err := config.Validate(); err == nil {
		t.Fatal("worker config with 17 executable repository aliases validated; maximum is 16")
	}
}

func TestLoadRemoteWorkerConfigIsStandaloneAndStrict(t *testing.T) {
	valid := `{"controller_base_url":"https://controller.example","shared_secret_file":"/run/secrets/worker","worker_id":"worker-a","repositories":{"widget":"/srv/widget"},"harness_profiles":{"fast":{"executable":"pi","args":["{system_prompt}","{task}"],"repositories":["widget"]}},"limits":{"job_timeout":"2m","harness_output_bytes":4096}}`
	path := filepath.Join(t.TempDir(), "worker.json")
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadRemoteWorkerConfig(path)
	if err != nil {
		t.Fatalf("load standalone worker config: %v", err)
	}
	if config.WorkerID != "worker-a" || config.HarnessProfiles["fast"].Executable != "pi" || config.AllowInsecureHTTP {
		t.Fatalf("unexpected config or insecure HTTP was not disabled by default: %+v", config)
	}
	for name, document := range map[string]string{
		"unknown field":                  strings.Replace(valid, `"worker_id":"worker-a"`, `"worker_id":"worker-a","persistence":{"dsn":"postgres://secret"}`, 1),
		"duplicate key":                  strings.Replace(valid, `"worker_id":"worker-a"`, `"worker_id":"worker-a","worker_id":"worker-b"`, 1),
		"remote HTTP without opt-in":     strings.Replace(valid, "https://controller.example", "http://controller.example", 1),
		"non-boolean opt-in":             strings.Replace(valid, `"worker_id":"worker-a"`, `"worker_id":"worker-a","allow_insecure_http":"true"`, 1),
		"duplicate opt-in":               strings.Replace(valid, `"worker_id":"worker-a"`, `"worker_id":"worker-a","allow_insecure_http":true,"allow_insecure_http":false`, 1),
		"non-loopback HTTP opt-in false": strings.Replace(strings.Replace(valid, "https://controller.example", "http://controller.example", 1), `"worker_id":"worker-a"`, `"worker_id":"worker-a","allow_insecure_http":false`, 1),
		"unknown repository capability":  strings.Replace(valid, `"repositories":["widget"]`, `"repositories":["other"]`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadRemoteWorkerConfig(path); err == nil {
				t.Fatalf("accepted invalid remote Worker config %s", name)
			}
		})
	}
}

func TestRemoteWorkerConcurrencyConfig(t *testing.T) {
	const config = `{"controller_base_url":"https://controller.example","shared_secret_file":"/run/worker-secret","worker_id":"worker-a","repositories":{"widget":"/srv/widget"},"harness_profiles":{"fast":{%s,"repositories":["widget"]}},"limits":{"job_timeout":"1m","harness_output_bytes":4096}%s}`
	const pi = `"adapter":"pi","executable":"/usr/bin/pi"`
	const legacy = `"executable":"pi","args":["{system_prompt}","{task}"]`
	for _, test := range []struct {
		name, profile, concurrency string
		valid                      bool
	}{
		{"legacy default", legacy, "", true},
		{"legacy explicit one", legacy, `,"max_concurrent_jobs":1`, true},
		{"Pi default", pi, "", true},
		{"Pi explicit two", pi, `,"max_concurrent_jobs":2`, true},
		{"legacy cannot opt into two", legacy, `,"max_concurrent_jobs":2`, false},
		{"zero", pi, `,"max_concurrent_jobs":0`, false},
		{"negative", pi, `,"max_concurrent_jobs":-1`, false},
		{"over maximum", pi, `,"max_concurrent_jobs":3`, false},
		{"fraction", pi, `,"max_concurrent_jobs":1.5`, false},
		{"null concurrency", pi, `,"max_concurrent_jobs":null`, false},
		{"null adapter", `"adapter":null,` + legacy, "", false},
		{"null Pi args", pi + `,"args":null`, "", false},
		{"null verification checks", pi, `,"verification_checks":null`, false},
		{"null HTTP opt-in", pi, `,"allow_insecure_http":null`, false},
		{"duplicate", pi, `,"max_concurrent_jobs":1,"max_concurrent_jobs":2`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "worker.json")
			if err := os.WriteFile(path, fmt.Appendf(nil, config, test.profile, test.concurrency), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadRemoteWorkerConfig(path); (err == nil) != test.valid {
				t.Fatalf("LoadRemoteWorkerConfig() error=%v, want valid=%v", err, test.valid)
			}
		})
	}
}

func TestRemoteWorkerHTTPRequiresExplicitOptInOutsideLoopback(t *testing.T) {
	for _, test := range []struct {
		name          string
		url           string
		allowInsecure bool
		wantValid     bool
	}{
		{name: "localhost default", url: "http://localhost:8080", wantValid: true},
		{name: "IPv4 loopback default", url: "http://127.0.0.1:8080", wantValid: true},
		{name: "IPv6 loopback default", url: "http://[::1]:8080", wantValid: true},
		{name: "Compose hostname requires opt-in", url: "http://api:8080"},
		{name: "remote hostname requires opt-in", url: "http://controller.example"},
		{name: "Compose hostname explicit opt-in", url: "http://api:8080", allowInsecure: true, wantValid: true},
		{name: "remote hostname explicit opt-in", url: "http://controller.example", allowInsecure: true, wantValid: true},
		{name: "non-loopback IP explicit opt-in", url: "http://192.0.2.10:8080", allowInsecure: true, wantValid: true},
		{name: "HTTPS remains valid with opt-in", url: "https://controller.example", allowInsecure: true, wantValid: true},
		{name: "non-loopback IP explicit opt-in", url: "http://192.0.2.10:8080", allowInsecure: true, wantValid: true},
		{name: "HTTPS remains valid with opt-in", url: "https://controller.example", allowInsecure: true, wantValid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := RemoteWorkerConfig{ControllerBaseURL: test.url, AllowInsecureHTTP: test.allowInsecure, SharedSecretFile: "/run/worker-secret", WorkerID: "worker-a", Repositories: map[string]string{"widget": "/srv/widget"}, HarnessProfiles: map[string]RemoteHarnessConfig{"fast": {Executable: "pi", Args: []string{"{system_prompt}", "{task}"}, Repositories: []string{"widget"}}}, Limits: RemoteWorkerLimits{JobTimeout: "1m", HarnessOutputBytes: 1024}}
			err := config.Validate()
			if (err == nil) != test.wantValid {
				t.Fatalf("Validate(%q, allow_insecure_http=%t) error=%v, want valid=%t", test.url, test.allowInsecure, err, test.wantValid)
			}
		})
	}
}
