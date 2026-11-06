package restserver

import (
	"strings"
	"testing"
)

func TestWorkerListenerConfigRequiresSQLAndValidCertificateMappings(t *testing.T) {
	base := `"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","principals":[{"id":"admin","role":"operator","token_file":"/run/admin","enabled":true}],"persistence":{"backend":"postgres","dsn":"postgres://factory:secret@localhost/factory"}`
	valid := `"worker_listener":{"listen_address":"127.0.0.1:8081","server_cert_file":"/run/tls/server.crt","server_key_file":"/run/tls/server.key","client_ca_file":"/run/tls/ca.crt","max_client_cert_lifetime":"72h","certificates":[{"sha256":"` + strings.Repeat("a", 64) + `","principal_id":"worker-1","enabled":true}]}`
	overlap := strings.Replace(valid, `"principal_id":"worker-1","enabled":true`, `"principal_id":"worker-1","enabled":true},{"sha256":"`+strings.Repeat("b", 64)+`","principal_id":"worker-1","enabled":true`, 1)
	config, err := LoadConfig(writeConfig(t, `{`+base+`,`+overlap+`}`))
	if err != nil || config.WorkerListener == nil || config.WorkerListener.ListenAddress != "127.0.0.1:8081" || len(config.WorkerListener.Certificates) != 2 {
		t.Fatalf("LoadConfig() = %+v, %v; want overlapping certificate rotation for one principal", config.WorkerListener, err)
	}
	invalid := []string{
		strings.Replace(valid, `"max_client_cert_lifetime":"72h",`, ``, 1),
		strings.Replace(valid, `"principal_id":"worker-1"`, `"principal_id":"../worker"`, 1),
		strings.Replace(valid, strings.Repeat("a", 64), strings.Repeat("A", 64), 1),
		strings.Replace(valid, `"principal_id":"worker-1","enabled":true`, `"principal_id":"worker-1","enabled":false`, 1),
		strings.Replace(valid, `"enabled":true}]`, `"enabled":true},{"sha256":"`+strings.Repeat("a", 64)+`","principal_id":"worker-2","enabled":true}]`, 1),
	}
	for _, workerListener := range invalid {
		if _, err := LoadConfig(writeConfig(t, `{`+base+`,`+workerListener+`}`)); err == nil {
			t.Errorf("LoadConfig accepted invalid worker listener %s", workerListener)
		}
	}
	memoryConfig := strings.Replace(base, `"backend":"postgres","dsn":"postgres://factory:secret@localhost/factory"`, `"backend":"memory"`, 1)
	if _, err := LoadConfig(writeConfig(t, `{`+memoryConfig+`,`+valid+`}`)); err == nil {
		t.Error("LoadConfig accepted worker listener with memory persistence")
	}
	sqliteConfig := strings.Replace(base, `"backend":"postgres","dsn":"postgres://factory:secret@localhost/factory"`, `"backend":"sqlite","path":"/var/lib/factory/jobs.db"`, 1)
	if _, err := LoadConfig(writeConfig(t, `{`+sqliteConfig+`,`+valid+`}`)); err != nil {
		t.Errorf("LoadConfig rejected worker listener with SQLite persistence: %v", err)
	}
}

func TestWorkerListenerRequiresValidMaximumClientCertificateLifetime(t *testing.T) {
	base := `"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","principals":[{"id":"admin","role":"operator","token_file":"/run/admin","enabled":true}],"persistence":{"backend":"postgres","dsn":"postgres://factory:secret@localhost/factory"}`
	listener := `"worker_listener":{"listen_address":"127.0.0.1:8081","server_cert_file":"/run/tls/server.crt","server_key_file":"/run/tls/server.key","client_ca_file":"/run/tls/ca.crt","max_client_cert_lifetime":"72h","certificates":[{"sha256":"` + strings.Repeat("a", 64) + `","principal_id":"worker-1","enabled":true}]}`
	if _, err := LoadConfig(writeConfig(t, `{`+base+`,`+listener+`}`)); err != nil {
		t.Fatalf("LoadConfig rejected valid maximum lifetime: %v", err)
	}
	if _, err := LoadConfig(writeConfig(t, `{`+base+`,`+strings.Replace(listener, `"max_client_cert_lifetime":"72h",`, `"max_client_cert_lifetime":72,`, 1)+`}`)); err == nil {
		t.Fatal("LoadConfig accepted a non-string maximum lifetime")
	}
	for _, lifetime := range []string{"", "0s", "-1s", "not-a-duration", "999999999999999999999999999999999999h"} {
		candidate := strings.Replace(listener, `"max_client_cert_lifetime":"72h"`, `"max_client_cert_lifetime":"`+lifetime+`"`, 1)
		if _, err := LoadConfig(writeConfig(t, `{`+base+`,`+candidate+`}`)); err == nil {
			t.Errorf("LoadConfig accepted maximum lifetime %q", lifetime)
		}
	}
	if _, err := LoadConfig(writeConfig(t, `{`+base+`} `)); err != nil {
		t.Errorf("LoadConfig required worker listener lifetime when listener is absent: %v", err)
	}
}

func TestWorkerListenerMappingFileConfig(t *testing.T) {
	base := `"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","principals":[{"id":"admin","role":"operator","token_file":"/run/admin","enabled":true}],"persistence":{"backend":"postgres","dsn":"postgres://factory:secret@localhost/factory"}`
	listener := `"worker_listener":{"listen_address":"127.0.0.1:8081","server_cert_file":"/run/tls/server.crt","server_key_file":"/run/tls/server.key","client_ca_file":"/run/tls/ca.crt","max_client_cert_lifetime":"72h","mapping_file":"/run/worker-mappings.json"}`
	config, err := LoadConfig(writeConfig(t, `{`+base+`,`+listener+`}`))
	if err != nil || config.WorkerListener == nil || config.WorkerListener.MappingFile != "/run/worker-mappings.json" {
		t.Fatalf("LoadConfig() mapping_file=(%+v,%v)", config.WorkerListener, err)
	}
	if _, err := LoadConfig(writeConfig(t, `{`+base+`,"worker_listener":{"listen_address":"127.0.0.1:8081","server_cert_file":"/run/cert","server_key_file":"/run/key","client_ca_file":"/run/ca","max_client_cert_lifetime":"72h"}}`)); err == nil {
		t.Fatal("accepted worker listener with neither certificates nor mapping file")
	}
}

func TestWorkerListenerConfigRejectsIncompleteAndUnknownFields(t *testing.T) {
	base := `{"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","principals":[{"id":"admin","role":"operator","token_file":"/run/admin","enabled":true}],"persistence":{"backend":"postgres","dsn":"postgres://factory:secret@localhost/factory"}}`
	for _, workerListener := range []string{
		`{"listen_address":"127.0.0.1:8081"}`,
		`{"listen_address":"127.0.0.1:8081","server_cert_file":"/run/cert","server_key_file":"/run/key","client_ca_file":"/run/ca","certificates":[{"sha256":"` + strings.Repeat("a", 64) + `","principal_id":"worker","enabled":true,"unknown":true}]}`,
	} {
		if _, err := LoadConfig(writeConfig(t, strings.TrimSuffix(base, "}")+`,"worker_listener":`+workerListener+`}`)); err == nil {
			t.Errorf("LoadConfig accepted worker_listener %s", workerListener)
		}
	}
}
