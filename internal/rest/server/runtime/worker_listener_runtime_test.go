package runtime

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/rest/jobs"
	"github.com/leehosanganson/factory/internal/rest/server"
	"github.com/leehosanganson/factory/internal/rest/worker"
)

func TestHTTPServersShutdownConcurrentlyWithinSharedDeadline(t *testing.T) {
	entered := []chan struct{}{make(chan struct{}), make(chan struct{})}
	shutdownStarted := []chan struct{}{make(chan struct{}), make(chan struct{})}
	release := make(chan struct{})
	servers := make([]*http.Server, len(entered))
	for i := range servers {
		index := i
		servers[i] = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(entered[index])
			<-release
		})}
		servers[i].RegisterOnShutdown(func() { close(shutdownStarted[index]) })
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go func(server *http.Server, listener net.Listener) { _ = server.Serve(listener) }(servers[i], listener)
		go func(address string) {
			response, err := http.Get("http://" + address)
			if err == nil {
				response.Body.Close()
			}
		}(listener.Addr().String())
	}
	for i := range entered {
		select {
		case <-entered[i]:
		case <-time.After(time.Second):
			close(release)
			t.Fatalf("HTTP server %d did not enter active handler", i)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	shutdownDone := make(chan []error, 1)
	go func() { shutdownDone <- shutdownHTTPServers(ctx, servers...) }()
	for i := range shutdownStarted {
		select {
		case <-shutdownStarted[i]:
		case <-time.After(250 * time.Millisecond):
			close(release)
			t.Fatalf("HTTP server %d shutdown was not initiated alongside its peer", i)
		}
	}
	close(release)
	select {
	case errs := <-shutdownDone:
		for i, err := range errs {
			if err != nil {
				t.Errorf("HTTP server %d shutdown error=%v", i, err)
			}
		}
	case <-ctx.Done():
		t.Fatal("HTTP servers did not drain within the shared shutdown deadline")
	}
	for _, server := range servers {
		_ = server.Close()
	}
}

func TestRunStartsIndependentMutualTLSWorkerListenerAndDrainsBoth(t *testing.T) {
	config, _ := runtimeFixture(t)
	stateDir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(stateDir, "jobs.db")}
	serverCert, serverCA := runtimeTestCertificate(t, "server", false)
	clientCert, workerCA := runtimeTestCertificate(t, "worker", true)
	certPath, keyPath, caPath := filepath.Join(stateDir, "server.crt"), filepath.Join(stateDir, "server.key"), filepath.Join(stateDir, "ca.crt")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCert.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	keyBytes, _ := x509.MarshalPKCS8PrivateKey(serverCert.PrivateKey)
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, workerCA, 0o600); err != nil {
		t.Fatal(err)
	}
	clientFingerprint := sha256.Sum256(clientCert.Certificate[0])
	apiAddress, workerAddress := runtimeFreeAddress(t), runtimeFreeAddress(t)
	config.ListenAddress = apiAddress
	apiListenerReady, workerListenerReady := make(chan net.Listener, 1), make(chan net.Listener, 1)
	config.WorkerListener = &restserver.WorkerListenerConfig{ListenAddress: workerAddress, ServerCertFile: certPath, ServerKeyFile: keyPath, ClientCAFile: caPath, Certificates: []restserver.WorkerCertificatePrincipal{{SHA256: hex.EncodeToString(clientFingerprint[:]), PrincipalID: "worker-runtime", Enabled: true}}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config, runtimeOptions{
			Listen: func(network, _ string) (net.Listener, error) {
				listener, err := net.Listen(network, apiAddress)
				if err == nil {
					apiListenerReady <- listener
				}
				return listener, err
			},
			ListenWorker: func(network, _ string, tlsConfig *tls.Config) (net.Listener, error) {
				listener, err := net.Listen(network, workerAddress)
				if err == nil {
					secure := tls.NewListener(listener, tlsConfig)
					workerListenerReady <- secure
					return secure, nil
				}
				return nil, err
			},
		})
	}()
	var apiListener, workerListener net.Listener
	select {
	case apiListener = <-apiListenerReady:
	case err := <-done:
		t.Fatalf("runtime failed before API listener: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("API listener did not start")
	}
	select {
	case workerListener = <-workerListenerReady:
	case err := <-done:
		t.Fatalf("runtime failed before worker listener: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("worker listener did not start")
	}
	defer apiListener.Close()
	defer workerListener.Close()
	waitForStatus(t, "http://"+apiListener.Addr().String()+"/readyz", http.StatusOK)
	serverRoots := x509.NewCertPool()
	serverRoots.AppendCertsFromPEM(serverCA)
	workerClient := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{Certificates: []tls.Certificate{clientCert}, RootCAs: serverRoots}}}
	apiClient := &http.Client{Timeout: 2 * time.Second}
	for _, tc := range []struct {
		name, url string
		client    *http.Client
		want      int
	}{
		{"api listener doesn't expose worker route", "http://" + apiListener.Addr().String() + "/v1/worker/session", apiClient, http.StatusNotFound},
		{"worker listener doesn't expose job route", "https://" + workerListener.Addr().String() + "/v1/jobs", workerClient, http.StatusNotFound},
	} {
		response, err := tc.client.Get(tc.url)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != tc.want {
			t.Errorf("%s status=%d want=%d", tc.name, response.StatusCode, tc.want)
		}
	}
	request, _ := http.NewRequest(http.MethodPost, "https://"+workerListener.Addr().String()+"/v1/worker/session", strings.NewReader(`{"boot_id":"boot-runtime"}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := workerClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("register status=%d body=%s", response.StatusCode, data)
	}
	response.Body.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime failed to shut down both listeners")
	}
	for _, listener := range []net.Listener{apiListener, workerListener} {
		if _, err := net.DialTimeout("tcp", listener.Addr().String(), 100*time.Millisecond); err == nil {
			t.Errorf("listener %s still accepts TCP after shutdown", listener.Addr())
		}
	}
}

func TestRunWorkerListenerBindFailureClosesAPIAndCleansUpBeforeReadiness(t *testing.T) {
	config, _ := runtimeFixture(t)
	stateDir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(stateDir, "jobs.db")}
	serverCert, _ := runtimeTestCertificate(t, "server", false)
	_, clientCA := runtimeTestCertificate(t, "worker", true)
	certPath, keyPath, caPath := filepath.Join(stateDir, "server.crt"), filepath.Join(stateDir, "server.key"), filepath.Join(stateDir, "ca.crt")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCert.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(serverCert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, clientCA, 0o600); err != nil {
		t.Fatal(err)
	}
	config.WorkerListener = &restserver.WorkerListenerConfig{
		ListenAddress: runtimeFreeAddress(t), ServerCertFile: certPath, ServerKeyFile: keyPath, ClientCAFile: caPath,
		Certificates: []restserver.WorkerCertificatePrincipal{{SHA256: strings.Repeat("a", 64), PrincipalID: "worker-runtime", Enabled: true}},
	}

	underlying, err := restworker.NewLocalServerJobManager(config)
	if err != nil {
		t.Fatal(err)
	}
	workerStore, ok := underlying.(runtimeWorkerStore)
	if !ok {
		t.Fatal("SQLite manager does not expose worker session store and close seams")
	}
	store := &bindFailureTrackingStore{runtimeWorkerStore: workerStore}
	apiListener := &closeTrackingListener{}
	var output strings.Builder
	gotErr := run(context.Background(), config, runtimeOptions{
		OpenStore: func(restserver.Config) (restjobs.Store, error) { return store, nil },
		Listen: func(network, address string) (net.Listener, error) {
			listener, err := net.Listen(network, address)
			if err != nil {
				return nil, err
			}
			apiListener.Listener = listener
			return apiListener, nil
		},
		ListenWorker: func(string, string, *tls.Config) (net.Listener, error) {
			return nil, errors.New("private worker bind details")
		},
		Log: log.New(&output, "", 0),
	})
	if gotErr == nil || gotErr.Error() != "listen for worker identity API" {
		t.Fatalf("worker listener failure = %v, want generic worker-listener error", gotErr)
	}
	if strings.Contains(gotErr.Error(), "private") {
		t.Fatalf("worker listener error leaked details: %v", gotErr)
	}
	if !apiListener.closed.Load() {
		t.Fatal("API listener remained open after worker listener bind failure")
	}
	if !store.closed.Load() || !store.managerClosed.Load() {
		t.Fatalf("cleanup incomplete: store closed=%v manager closed=%v", store.closed.Load(), store.managerClosed.Load())
	}
	if strings.Contains(output.String(), "REST server ready") {
		t.Fatalf("runtime reported readiness after worker listener bind failure: %q", output.String())
	}
	if _, err := net.DialTimeout("tcp", apiListener.Addr().String(), 100*time.Millisecond); err == nil {
		t.Fatal("API listener still accepted connections after worker listener bind failure")
	}
}

type closeTrackingListener struct {
	net.Listener
	closed atomic.Bool
}

func (l *closeTrackingListener) Close() error {
	l.closed.Store(true)
	return l.Listener.Close()
}

type runtimeWorkerStore interface {
	restjobs.Manager
	restjobs.WorkerSessionStore
	CloseStore() error
}

type bindFailureTrackingStore struct {
	runtimeWorkerStore
	closed        atomic.Bool
	managerClosed atomic.Bool
}

func (s *bindFailureTrackingStore) CloseStore() error {
	s.closed.Store(true)
	return s.runtimeWorkerStore.CloseStore()
}

func (s *bindFailureTrackingStore) Close() {
	s.managerClosed.Store(true)
	s.runtimeWorkerStore.Close()
}

func TestRunRejectsWorkerListenerOnSameAddressBeforeOpeningResources(t *testing.T) {
	config, _ := runtimeFixture(t)
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(t.TempDir(), "jobs.db")}
	config.WorkerListener = &restserver.WorkerListenerConfig{ListenAddress: config.ListenAddress, ServerCertFile: "/missing/cert", ServerKeyFile: "/missing/key", ClientCAFile: "/missing/ca", Certificates: []restserver.WorkerCertificatePrincipal{{SHA256: strings.Repeat("a", 64), PrincipalID: "worker", Enabled: true}}}
	opened := false
	err := run(context.Background(), config, runtimeOptions{OpenStore: func(restserver.Config) (restjobs.Store, error) {
		opened = true
		return nil, errors.New("should not open")
	}})
	if err == nil || !strings.Contains(err.Error(), "distinct") || opened {
		t.Fatalf("run error=%v store opened=%v", err, opened)
	}
}

func runtimeFreeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func runtimeTestCertificate(t *testing.T, name string, client bool) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(101), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	usage := x509.ExtKeyUsageServerAuth
	if client {
		usage = x509.ExtKeyUsageClientAuth
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(102), Subject: pkix.Name{CommonName: name}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	parsedKey, err := x509.ParsePKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der, caDER}, PrivateKey: parsedKey}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
}
