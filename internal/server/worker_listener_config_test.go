package restserver

import (
	"strings"
	"testing"
)

func TestWorkerListenerConfigRequiresSQLAndValidCertificateMappings(t *testing.T) {
	base := `"mode":"local_process","repositories":{"widget":"/srv/widget"},"harness":{"executable":"pi","args":["{system_prompt}","{task}"]},"api_key_file":"/run/key","principals":[{"id":"admin","role":"operator","token_file":"/run/admin","enabled":true}],"persistence":{"backend":"postgres","dsn":"postgres://factory:secret@localhost/factory"}`
	valid := `"worker_listener":{"listen_address":"127.0.0.1:8081","server_cert_file":"/run/tls/server.crt","server_key_file":"/run/tls/server.key","client_ca_file":"/run/tls/ca.crt","certificates":[{"sha256":"` + strings.Repeat("a", 64) + `","principal_id":"worker-1","enabled":true}]}`
	overlap := strings.Replace(valid, `"principal_id":"worker-1","enabled":true`, `"principal_id":"worker-1","enabled":true},{"sha256":"`+strings.Repeat("b", 64)+`","principal_id":"worker-1","enabled":true`, 1)
	config, err := LoadConfig(writeConfig(t, `{`+base+`,`+overlap+`}`))
	if err != nil || config.WorkerListener == nil || config.WorkerListener.ListenAddress != "127.0.0.1:8081" || len(config.WorkerListener.Certificates) != 2 {
		t.Fatalf("LoadConfig() = %+v, %v; want overlapping certificate rotation for one principal", config.WorkerListener, err)
	}
	invalid := []string{
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
