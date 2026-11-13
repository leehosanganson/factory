package runtime

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
	restworker "github.com/leehosanganson/factory/internal/worker"
)

func TestWorkerSessionClientRegistersAndHeartbeatsWithMutualTLS(t *testing.T) {
	clientCert, clientCA := runtimeTestCertificate(t, "worker-client", true)
	serverCert, serverCA := runtimeTestCertificate(t, "controller-server", false)
	clientCAPool := x509.NewCertPool()
	if !clientCAPool.AppendCertsFromPEM(clientCA) {
		t.Fatal("load client CA")
	}
	var registered, heartbeated atomic.Bool
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			t.Error("request did not present client certificate")
		}
		if r.URL.Path == "/v1/worker/session" {
			var input struct {
				BootID string `json:"boot_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.BootID != "process-boot" {
				t.Errorf("registration payload=%+v err=%v", input, err)
			}
			registered.Store(true)
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"id":"session","principal_id":"worker","boot_id":"process-boot","status":"active","created_at":"2026-01-01T00:00:00Z","capability":"process-only-secret","capability_expires_at":"2099-01-01T00:00:00Z"}`)
			return
		}
		if r.Header.Get("Authorization") != "Bearer process-only-secret" {
			t.Errorf("heartbeat authorization=%q", r.Header.Get("Authorization"))
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{}` {
			t.Errorf("heartbeat body=%q, want {}", body)
		}
		heartbeated.Store(true)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"heartbeat_at":"2026-01-01T00:00:00Z"}`)
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientCAs: clientCAPool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()
	controller := workerSessionTestClient(t, server.URL, clientCert, serverCA)
	if err := controller.register(context.Background(), "process-boot"); err != nil {
		t.Fatal(err)
	}
	if err := controller.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !registered.Load() || !heartbeated.Load() || controller.token != "process-only-secret" {
		t.Fatal("registration/heartbeat did not retain the capability and complete both authenticated calls")
	}
}

func TestWorkerSessionClientRejectsRedirectAndStopsAfterHeartbeatError(t *testing.T) {
	clientCert, clientCA := runtimeTestCertificate(t, "worker-client", true)
	serverCert, serverCA := runtimeTestCertificate(t, "controller-server", false)
	clientCAPool := x509.NewCertPool()
	clientCAPool.AppendCertsFromPEM(clientCA)
	var calls atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/target", http.StatusTemporaryRedirect)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientCAs: clientCAPool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()
	controller := workerSessionTestClient(t, server.URL, clientCert, serverCA)
	controller.token = "capability"
	response, cancel, err := controller.request(context.Background(), http.MethodGet, "/redirect", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	response.Body.Close()
	if response.StatusCode != http.StatusTemporaryRedirect || calls.Load() != 1 {
		t.Fatalf("redirect status=%d calls=%d, want redirect returned without following", response.StatusCode, calls.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var failures atomic.Int32
	done := controller.runHeartbeats(ctx, time.Millisecond, func() { failures.Add(1) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat failure did not stop the heartbeat loop")
	}
	if failures.Load() != 1 {
		t.Fatalf("failure callbacks=%d, want one", failures.Load())
	}
}

func TestHeartbeatFailureStopsClaimsWithoutCancelingRunningExecution(t *testing.T) {
	manager, err := restjobs.NewManager(restjobs.Config{QueueCapacity: 2, MaxConcurrentJobs: 1, MaxRecords: 2})
	if err != nil {
		t.Fatal(err)
	}
	active, _, err := manager.Admit("heartbeat-active", restjobs.Request{Repository: "trusted", Task: "active"})
	if err != nil {
		t.Fatal(err)
	}
	queued, _, err := manager.Admit("heartbeat-queued", restjobs.Request{Repository: "trusted", Task: "queued"})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	executionCanceled := make(chan struct{})
	coordinator, err := restworker.New(manager, runtimeExecutor(func(ctx context.Context, job restjobs.Snapshot) error {
		if job.ID != active.ID {
			t.Errorf("executor claimed job %s after heartbeat failure", job.ID)
			return nil
		}
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			close(executionCanceled)
			return ctx.Err()
		}
	}), restworker.CoordinatorConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := coordinator.Shutdown(ctx); err != nil {
			t.Errorf("coordinator shutdown: %v", err)
		}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("active job did not start")
	}

	var requests, retirements atomic.Int32
	client := &workerSessionClient{
		baseURL: "https://controller.invalid",
		token:   "test-capability",
		client: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests.Add(1)
			if request.URL.Path == "/v1/worker/session" && request.Method == http.MethodDelete {
				retirements.Add(1)
			}
			if request.URL.Path != "/v1/worker/session/heartbeat" || request.Method != http.MethodPost {
				t.Errorf("unexpected controller request: %s %s", request.Method, request.URL.Path)
			}
			return nil, errors.New("controller unavailable")
		})},
	}
	heartbeatCtx, stopHeartbeats := context.WithCancel(context.Background())
	heartbeatFailed := make(chan struct{})
	heartbeatDone := client.runHeartbeats(heartbeatCtx, time.Millisecond, func() {
		coordinator.StopClaims()
		close(heartbeatFailed)
	})
	select {
	case <-heartbeatFailed:
	case <-time.After(time.Second):
		t.Fatal("heartbeat failure did not stop coordinator claims")
	}
	stopHeartbeats()
	select {
	case <-heartbeatDone:
	case <-time.After(time.Second):
		t.Fatal("heartbeat loop did not stop after failure")
	}

	if current, err := manager.Get(queued.ID); err != nil || current.Status != restjobs.StatusQueued {
		t.Fatalf("queued job after heartbeat failure=(%+v, %v), want queued", current, err)
	}
	shutdownCtx, cancelShutdown := newWorkerShutdownContext(true, 20*time.Millisecond)
	defer cancelShutdown()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- coordinator.Shutdown(shutdownCtx) }()
	select {
	case <-executionCanceled:
		t.Fatal("heartbeat failure canceled the active execution")
	case err := <-shutdownDone:
		t.Fatalf("shutdown ended before active execution was released: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("heartbeat-failure shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after active execution was released")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		current, err := manager.Get(active.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status == restjobs.StatusSucceeded {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if current, err := manager.Get(active.ID); err != nil || current.Status != restjobs.StatusSucceeded {
		t.Fatalf("active job after release=(%+v, %v), want succeeded", current, err)
	}
	if requests.Load() != 1 || retirements.Load() != 0 {
		t.Fatalf("worker-session controller requests=%d retirements=%d, want one heartbeat and no retirement", requests.Load(), retirements.Load())
	}

	ordinaryShutdownCtx, cancelOrdinaryShutdown := newWorkerShutdownContext(false, 20*time.Millisecond)
	defer cancelOrdinaryShutdown()
	select {
	case <-ordinaryShutdownCtx.Done():
		if !errors.Is(ordinaryShutdownCtx.Err(), context.DeadlineExceeded) {
			t.Fatalf("ordinary shutdown error=%v, want deadline exceeded", ordinaryShutdownCtx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("ordinary shutdown did not retain its bounded timeout")
	}
}

func TestWorkerSessionClientKeepsRequestContextUntilResponseIsConsumed(t *testing.T) {
	client := &workerSessionClient{baseURL: "https://controller.invalid", client: &http.Client{}}
	client.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       &requestContextBody{ctx: request.Context()},
			Header:     make(http.Header),
			Request:    request,
		}, nil
	})
	client.token = "capability"

	if err := client.heartbeat(context.Background()); err != nil {
		t.Fatalf("heartbeat read body after request returned: %v", err)
	}
}

type requestContextBody struct {
	ctx  context.Context
	read bool
}

func (b *requestContextBody) Read(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	if b.read {
		return 0, io.EOF
	}
	b.read = true
	return copy(p, `{"heartbeat_at":"2026-01-01T00:00:00Z"}`), nil
}

func (*requestContextBody) Close() error { return nil }

func TestWorkerSessionClientRefusesUntrustedServerCertificate(t *testing.T) {
	clientCert, _ := runtimeTestCertificate(t, "worker-client", true)
	_, otherCA := runtimeTestCertificate(t, "untrusted-controller", false)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusCreated) }))
	defer server.Close()
	controller := workerSessionTestClient(t, server.URL, clientCert, otherCA)
	if err := controller.register(context.Background(), "process-boot"); err == nil {
		t.Fatal("worker accepted an untrusted controller certificate")
	}
}

func TestWorkerSessionClientUsesBoundedTimeoutAndSanitizedErrors(t *testing.T) {
	client := &workerSessionClient{baseURL: "https://controller.invalid", client: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})}, token: "private-capability"}
	if err := client.heartbeat(context.Background()); err == nil || err.Error() != "worker controller heartbeat failed" {
		t.Fatalf("heartbeat error=%v, want fixed sanitized failure", err)
	}
	if workerSessionRequestTimeout <= 0 || workerSessionRequestTimeout > 30*time.Second {
		t.Fatalf("request timeout=%s is not bounded", workerSessionRequestTimeout)
	}
}

func workerSessionTestClient(t *testing.T, baseURL string, clientCert tls.Certificate, serverCA []byte) *workerSessionClient {
	t.Helper()
	state := t.TempDir()
	certPath, keyPath, caPath := filepath.Join(state, "client.crt"), filepath.Join(state, "client.key"), filepath.Join(state, "server-ca.crt")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCert.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(clientCert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, serverCA, 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := newWorkerSessionClient(restserver.WorkerControllerConfig{BaseURL: baseURL, ClientCertFile: certPath, ClientKeyFile: keyPath, ServerCAFile: caPath})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }
