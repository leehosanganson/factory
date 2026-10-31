package restserver

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/restjobs"
)

func TestWorkerListenerLogsBoundedAuthenticationOutcomesWithoutCredentials(t *testing.T) {
	store, err := restjobs.OpenSQLiteStore(filepath.Join(privateSQLiteTestDir(t), "worker-listener-auth-log.db"), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const credential = "private-client-certificate-and-key-material"
	clientCert := workerTestCertificate(t, credential, true)
	fingerprint := sha256.Sum256(clientCert.Certificate[0])
	var logs strings.Builder
	logger := log.New(&logs, "", 0)
	handler, err := NewWorkerListenerHandler(WorkerListenerConfig{Certificates: []WorkerCertificatePrincipal{{SHA256: hex.EncodeToString(fingerprint[:]), PrincipalID: "worker-log-test", Enabled: true}}}, store, func() bool { return true }, logger)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/worker/session", strings.NewReader(`{"boot_id":"boot-log-test"}`))
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{Raw: clientCert.Certificate[0]}}}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("known certificate status=%d, want 201", response.Code)
	}
	unknown := httptest.NewRequest(http.MethodPost, "/v1/worker/session", strings.NewReader(`{"boot_id":"boot-log-test"}`))
	unknown.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{Raw: []byte("unmapped certificate material")}}}
	unknownResponse := httptest.NewRecorder()
	handler.ServeHTTP(unknownResponse, unknown)
	if unknownResponse.Code != http.StatusUnauthorized {
		t.Fatalf("unknown certificate status=%d, want 401", unknownResponse.Code)
	}
	output := logs.String()
	for _, diagnostic := range []string{"worker identity authentication succeeded", "worker identity authentication failed"} {
		if !strings.Contains(output, diagnostic) {
			t.Errorf("authentication diagnostic %q missing from %q", diagnostic, output)
		}
	}
	for _, secret := range []string{credential, "unmapped certificate material", hex.EncodeToString(clientCert.Certificate[0])} {
		if strings.Contains(output, secret) {
			t.Errorf("authentication log exposed credential material %q", secret)
		}
	}
	if len(output) > 256 {
		t.Errorf("authentication diagnostics unbounded: %d bytes", len(output))
	}
}

func TestWorkerListenerMapsExactClientCertificateAndOnlyExposesSessionRoutes(t *testing.T) {
	store, err := restjobs.OpenSQLiteStore(filepath.Join(privateSQLiteTestDir(t), "worker-listener.db"), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	clientCert := workerTestCertificate(t, "client", true)
	fingerprint := sha256.Sum256(clientCert.Certificate[0])
	wrongCert := workerTestCertificate(t, "other-client", true)
	config := WorkerListenerConfig{Certificates: []WorkerCertificatePrincipal{{SHA256: hex.EncodeToString(fingerprint[:]), PrincipalID: "worker-one", Enabled: true}}}
	ready := true
	handler, err := NewWorkerListenerHandler(config, store, func() bool { return ready })
	if err != nil {
		t.Fatal(err)
	}
	serverCert := workerTestCertificate(t, "server", false)
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	defer server.Close()
	clientFor := func(cert tls.Certificate) *http.Client {
		return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{cert}}}} // test certificates are intentionally ephemeral
	}
	knownClient := clientFor(clientCert)
	unknownClient := clientFor(wrongCert)
	register := func(client *http.Client, body string) *http.Response {
		t.Helper()
		response, err := client.Post(server.URL+"/v1/worker/session", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	response := register(knownClient, `{"boot_id":"boot-1"}`)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("known certificate registration status=%d", response.StatusCode)
	}
	var session restjobs.WorkerSession
	if err := json.NewDecoder(response.Body).Decode(&session); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if session.PrincipalID != "worker-one" || session.BootID != "boot-1" || session.Status != "active" {
		t.Fatalf("session identity derived incorrectly: %+v", session)
	}
	response = register(knownClient, `{"boot_id":"boot-1"}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("same boot retry status=%d, want 200", response.StatusCode)
	}
	response.Body.Close()
	response = register(knownClient, `{"boot_id":"boot-2"}`)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("different boot status=%d, want conflict", response.StatusCode)
	}
	response.Body.Close()
	var race sync.WaitGroup
	statuses := make(chan int, 8)
	for range 8 {
		race.Add(1)
		go func() {
			defer race.Done()
			candidate := register(knownClient, `{"boot_id":"boot-1"}`)
			statuses <- candidate.StatusCode
			candidate.Body.Close()
		}()
	}
	race.Wait()
	close(statuses)
	created, replayed := 0, 0
	for status := range statuses {
		if status == http.StatusCreated {
			created++
		} else if status == http.StatusOK {
			replayed++
		} else {
			t.Errorf("same-boot racing registration status=%d", status)
		}
	}
	if created != 0 || replayed != 8 {
		t.Fatalf("racing registrations created=%d replayed=%d", created, replayed)
	}
	response = register(unknownClient, `{"boot_id":"boot-other"}`)
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown cert status=%d, want unauthorized", response.StatusCode)
	}
	response.Body.Close()
	response, err = knownClient.Get(server.URL + "/v1/jobs")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("unbounded job route status=%d, want not found", response.StatusCode)
	}
	response.Body.Close()
	for _, body := range []string{`{"boot_id":"boot-3","principal_id":"spoofed"}`, `{"boot_id":""}`, `{"boot_id":"boot-3"} {}`} {
		response = register(knownClient, body)
		if response.StatusCode != http.StatusBadRequest {
			t.Errorf("invalid registration %s status=%d", body, response.StatusCode)
		}
		response.Body.Close()
	}
	request, _ := http.NewRequest(http.MethodDelete, server.URL+"/v1/worker/session/"+session.ID, strings.NewReader(`{"boot_id":"boot-1"}`))
	response, err = knownClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("retirement status=%d", response.StatusCode)
	}
	response.Body.Close()
	request, _ = http.NewRequest(http.MethodDelete, server.URL+"/v1/worker/session/"+session.ID, strings.NewReader(`{"boot_id":"boot-1"}`))
	response, err = unknownClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown identity retirement status=%d", response.StatusCode)
	}
	response.Body.Close()
	ready = false
	response, err = knownClient.Get(server.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("draining readiness status=%d", response.StatusCode)
	}
	response.Body.Close()
}

func TestWorkerListenerRequiresVerifiedMTLSAndExactCertificateFingerprint(t *testing.T) {
	serverCert := workerTestCertificate(t, "server", false)
	clientCert := workerTestCertificate(t, "client", true)
	caPEM, _ := createWorkerTestCA(t)
	dir := t.TempDir()
	certFile, keyFile, caFile := filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key"), filepath.Join(dir, "ca.crt")
	if err := osWriteFile(certFile, encodeWorkerCert(serverCert)); err != nil {
		t.Fatal(err)
	}
	if err := osWriteFile(keyFile, encodeWorkerKey(serverCert)); err != nil {
		t.Fatal(err)
	}
	if err := osWriteFile(caFile, caPEM); err != nil {
		t.Fatal(err)
	}
	tlsConfig, err := NewWorkerListenerTLS(WorkerListenerConfig{ServerCertFile: certFile, ServerKeyFile: keyFile, ClientCAFile: caFile})
	if err != nil {
		t.Fatal(err)
	}
	if tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert || tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("TLS policy = %+v", tlsConfig)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("test CA not parseable")
	}
	fingerprint := sha256.Sum256(clientCert.Certificate[0])
	if got := hex.EncodeToString(fingerprint[:]); got == "" {
		t.Fatal("fingerprint unexpectedly empty")
	}
}

func workerTestCertificate(t *testing.T, commonName string, client bool) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: commonName}, DNSNames: []string{"example.test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign}
	if client {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	parsedKey, err := x509.ParsePKCS8PrivateKey(keyDER)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: parsedKey}
}

func createWorkerTestCA(t *testing.T) ([]byte, *x509.Certificate) {
	t.Helper()
	cert := workerTestCertificate(t, "test-ca", true)
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: parsed.Raw}), parsed
}

func encodeWorkerCert(cert tls.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
}
func encodeWorkerKey(cert tls.Certificate) []byte {
	key, _ := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
}

var osWriteFile = func(path string, data []byte) error { return os.WriteFile(path, data, 0o600) }

func privateSQLiteTestDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}
