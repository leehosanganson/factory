package restserver

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
	"encoding/json"
	"encoding/pem"
	"errors"
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

	"github.com/leehosanganson/factory/internal/jobs"
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
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{workerTestParsedCertificate(t, clientCert.Certificate[0])}}
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

func TestWorkerListenerRejectsClientCertificatesOutsideValidityWindowBeforeAuthentication(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name      string
		notBefore time.Time
		notAfter  time.Time
	}{
		{name: "expired", notBefore: now.Add(-2 * time.Hour), notAfter: now.Add(-time.Hour)},
		{name: "not yet valid", notBefore: now.Add(time.Hour), notAfter: now.Add(2 * time.Hour)},
		{name: "valid", notBefore: now.Add(-time.Hour), notAfter: now.Add(time.Hour)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clientCert := workerTestCertificateWithValidity(t, "validity-"+test.name, true, test.notBefore, test.notAfter)
			lookupCalls := 0
			store := &workerSessionStoreSpy{}
			handler, err := NewWorkerListenerHandler(WorkerListenerConfig{MappingFile: "configured"}, store, func() bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			handler.SetMappingLookup(workerMappingLookupFunc(func(context.Context, string) (string, bool, error) {
				lookupCalls++
				return "worker-validity", true, nil
			}))
			leaf, err := x509.ParseCertificate(clientCert.Certificate[0])
			if err != nil {
				t.Fatal(err)
			}
			peer := &tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}
			request := httptest.NewRequest(http.MethodPost, "/v1/worker/session", strings.NewReader(`{"boot_id":"boot-validity"}`))
			request.TLS = peer
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if test.name == "valid" {
				if response.Code != http.StatusCreated || store.registrationCalls != 1 || lookupCalls != 1 {
					t.Fatalf("valid peer response=%d, mapping lookups=%d, registrations=%d; want 201 and one call each", response.Code, lookupCalls, store.registrationCalls)
				}
				return
			}
			if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), `"code":"unauthenticated"`) {
				t.Fatalf("invalid peer response=%d %s, want generic unauthenticated", response.Code, response.Body.String())
			}
			request = httptest.NewRequest(http.MethodGet, "/v1/worker/session", nil)
			request.TLS = peer
			request.Header.Set("Authorization", "Bearer capability")
			response = httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), `"code":"unauthenticated"`) {
				t.Fatalf("invalid peer capability response=%d %s, want generic unauthenticated", response.Code, response.Body.String())
			}
			if lookupCalls != 0 || store.registrationCalls != 0 || store.capabilityCalls != 0 {
				t.Fatalf("invalid peer reached downstream authentication: mapping lookups=%d registrations=%d capability checks=%d", lookupCalls, store.registrationCalls, store.capabilityCalls)
			}
		})
	}
}

type workerSessionStoreSpy struct {
	restjobs.WorkerSessionStore
	registrationCalls int
	capabilityCalls   int
}

func (s *workerSessionStoreSpy) RegisterWorkerSession(_ context.Context, principal, bootID string) (restjobs.WorkerSession, bool, error) {
	s.registrationCalls++
	return restjobs.WorkerSession{ID: "session", PrincipalID: principal, BootID: bootID, Status: "active"}, false, nil
}

func (s *workerSessionStoreSpy) AuthenticateWorkerCapability(context.Context, string, string) (restjobs.WorkerSession, error) {
	s.capabilityCalls++
	return restjobs.WorkerSession{}, restjobs.ErrNotFound
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
	if session.PrincipalID != "worker-one" || session.BootID != "boot-1" || session.Status != "active" || session.Capability == "" || session.CapabilityExpiresAt == nil {
		t.Fatalf("session identity/capability derived incorrectly: %+v", session)
	}
	firstCapability := session.Capability
	response = register(knownClient, `{"boot_id":"boot-1"}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("same boot retry status=%d, want 200", response.StatusCode)
	}
	var retried restjobs.WorkerSession
	if err := json.NewDecoder(response.Body).Decode(&retried); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if retried.ID != session.ID || retried.Capability == "" || retried.Capability == firstCapability {
		t.Fatalf("same-boot retry did not rotate capability: first=%+v retry=%+v", session, retried)
	}
	if _, err := store.AuthenticateWorkerCapability(context.Background(), "worker-one", firstCapability); !errors.Is(err, restjobs.ErrNotFound) {
		t.Fatalf("prior registration capability error=%v, want invalid", err)
	}
	if _, err := store.AuthenticateWorkerCapability(context.Background(), "worker-one", retried.Capability); err != nil {
		t.Fatalf("replacement registration capability invalid: %v", err)
	}
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
	if _, err := store.AuthenticateWorkerCapability(context.Background(), "worker-one", retried.Capability); !errors.Is(err, restjobs.ErrNotFound) {
		t.Fatalf("retired session capability error=%v, want invalid", err)
	}
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

func TestWorkerCapabilitySessionEndpointsUseOnlyAuthenticatedOwnSession(t *testing.T) {
	store, err := restjobs.OpenSQLiteStore(filepath.Join(privateSQLiteTestDir(t), "worker-session-capability-http.db"), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ownCert := workerTestCertificate(t, "own-session", true)
	otherCert := workerTestCertificate(t, "other-session", true)
	ownFingerprint := sha256.Sum256(ownCert.Certificate[0])
	otherFingerprint := sha256.Sum256(otherCert.Certificate[0])
	handler, err := NewWorkerListenerHandler(WorkerListenerConfig{Certificates: []WorkerCertificatePrincipal{
		{SHA256: hex.EncodeToString(ownFingerprint[:]), PrincipalID: "worker-own", Enabled: true},
		{SHA256: hex.EncodeToString(otherFingerprint[:]), PrincipalID: "worker-other", Enabled: true},
	}}, store, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	serverCert := workerTestCertificate(t, "session-server", false)
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAnyClientCert}
	server.StartTLS()
	defer server.Close()
	clientFor := func(cert tls.Certificate) *http.Client {
		return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{cert}}}} // test certificates are intentionally ephemeral
	}
	ownClient, otherClient := clientFor(ownCert), clientFor(otherCert)
	own, _, err := store.RegisterWorkerSession(context.Background(), "worker-own", "boot-own")
	if err != nil {
		t.Fatal(err)
	}
	other, _, err := store.RegisterWorkerSession(context.Background(), "worker-other", "boot-other")
	if err != nil {
		t.Fatal(err)
	}
	do := func(client *http.Client, method, path, capability string) *http.Response {
		t.Helper()
		request, err := http.NewRequest(method, server.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if capability != "" {
			request.Header.Set("Authorization", "Bearer "+capability)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	response := do(ownClient, http.MethodGet, "/v1/worker/session", own.Capability)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("own-session inspection status=%d, want 200", response.StatusCode)
	}
	var inspected restjobs.WorkerSession
	if err := json.NewDecoder(response.Body).Decode(&inspected); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if inspected.ID != own.ID || inspected.PrincipalID != "worker-own" || inspected.BootID != "boot-own" || inspected.Status != "active" {
		t.Fatalf("inspection returned wrong session: %+v", inspected)
	}
	if inspected.Capability != "" {
		t.Fatal("inspection response leaked the bearer capability")
	}
	response = do(ownClient, http.MethodPut, "/v1/worker/session", own.Capability)
	if response.StatusCode != http.StatusNotFound {
		t.Errorf("unsupported session method status=%d, want 404", response.StatusCode)
	}
	response.Body.Close()
	response = do(ownClient, http.MethodGet, "/v1/worker/session?session_id="+other.ID, own.Capability)
	if response.StatusCode != http.StatusBadRequest {
		t.Errorf("caller-selected session query status=%d, want 400", response.StatusCode)
	}
	response.Body.Close()
	response = do(ownClient, http.MethodDelete, "/v1/worker/session?session_id="+other.ID, own.Capability)
	if response.StatusCode != http.StatusBadRequest {
		t.Errorf("DELETE with caller-selected session query status=%d, want 400", response.StatusCode)
	}
	response.Body.Close()
	if _, err := store.AuthenticateWorkerCapability(context.Background(), "worker-own", own.Capability); err != nil {
		t.Fatalf("rejected caller-selected DELETE changed own session: %v", err)
	}
	response = do(ownClient, http.MethodGet, "/v1/worker/session/"+other.ID, own.Capability)
	if response.StatusCode != http.StatusNotFound {
		t.Errorf("caller-selected session path status=%d, want 404", response.StatusCode)
	}
	response.Body.Close()
	for _, test := range []struct {
		name       string
		client     *http.Client
		capability string
	}{
		{name: "invalid bearer", client: ownClient, capability: "invalid"},
		{name: "wrong mTLS principal", client: otherClient, capability: own.Capability},
	} {
		for _, method := range []string{http.MethodGet, http.MethodDelete} {
			response = do(test.client, method, "/v1/worker/session", test.capability)
			if response.StatusCode != http.StatusUnauthorized {
				t.Errorf("%s %s status=%d, want 401", test.name, method, response.StatusCode)
			}
			response.Body.Close()
		}
	}
	response = do(ownClient, http.MethodDelete, "/v1/worker/session", own.Capability)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("own-session retirement status=%d, want 200", response.StatusCode)
	}
	var retired restjobs.WorkerSession
	if err := json.NewDecoder(response.Body).Decode(&retired); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if retired.ID != own.ID || retired.Status != "retired" || retired.Capability != "" {
		t.Fatalf("retirement returned wrong or sensitive session: %+v", retired)
	}
	if _, err := store.AuthenticateWorkerCapability(context.Background(), "worker-own", own.Capability); !errors.Is(err, restjobs.ErrNotFound) {
		t.Fatalf("retired capability authentication error=%v, want invalid", err)
	}
	if _, err := store.AuthenticateWorkerCapability(context.Background(), "worker-other", other.Capability); err != nil {
		t.Fatalf("retiring own session affected another principal: %v", err)
	}
}

type workerMappingLookupFunc func(context.Context, string) (string, bool, error)

func (f workerMappingLookupFunc) LookupWorkerMapping(ctx context.Context, fingerprint string) (string, bool, error) {
	return f(ctx, fingerprint)
}

func TestWorkerCapabilityUsesLiveMappingBeforeBearerAuthentication(t *testing.T) {
	store, err := restjobs.OpenSQLiteStore(filepath.Join(privateSQLiteTestDir(t), "worker-live-mapping.db"), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	clientCert := workerTestCertificate(t, "live-mapping", true)
	fingerprint := sha256.Sum256(clientCert.Certificate[0])
	key := hex.EncodeToString(fingerprint[:])
	if _, err := store.ReconcileWorkerMappings(context.Background(), map[string]string{key: "worker-before"}); err != nil {
		t.Fatal(err)
	}
	session, _, err := store.RegisterWorkerSession(context.Background(), "worker-before", "boot-live")
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewWorkerListenerHandler(WorkerListenerConfig{MappingFile: "configured"}, store, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	handler.SetMappingLookup(store)
	request := httptest.NewRequest(http.MethodGet, "/v1/worker/session", nil)
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{workerTestParsedCertificate(t, clientCert.Certificate[0])}}
	request.Header.Set("Authorization", "Bearer "+session.Capability)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("current mapping status=%d, want 200", response.Code)
	}
	if _, err := store.ReconcileWorkerMappings(context.Background(), map[string]string{key: "worker-after"}); err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), `"code":"unauthenticated"`) {
		t.Fatalf("remapped certificate response=%d %s, want generic unauthenticated", response.Code, response.Body.String())
	}
	if _, err := store.ReconcileWorkerMappings(context.Background(), map[string]string{}); err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), `"code":"unauthenticated"`) {
		t.Fatalf("removed certificate response=%d %s, want generic unauthenticated", response.Code, response.Body.String())
	}
}

func TestWorkerMappingLookupFailureFailsClosedWithSanitized503(t *testing.T) {
	store, err := restjobs.OpenSQLiteStore(filepath.Join(privateSQLiteTestDir(t), "worker-mapping-outage.db"), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	clientCert := workerTestCertificate(t, "mapping-outage", true)
	fingerprint := sha256.Sum256(clientCert.Certificate[0])
	handler, err := NewWorkerListenerHandler(WorkerListenerConfig{MappingFile: "configured"}, store, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	handler.SetMappingLookup(workerMappingLookupFunc(func(context.Context, string) (string, bool, error) {
		return "", false, errors.New("private database credential and host")
	}))
	request := httptest.NewRequest(http.MethodPost, "/v1/worker/session", strings.NewReader(`{"boot_id":"boot-outage"}`))
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{workerTestParsedCertificate(t, clientCert.Certificate[0])}}
	request.Header.Set("Authorization", "Bearer unused")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"service_unavailable"`) {
		t.Fatalf("lookup outage response=%d %s, want sanitized 503", response.Code, response.Body.String())
	}
	for _, private := range []string{"private database", hex.EncodeToString(fingerprint[:])} {
		if strings.Contains(response.Body.String(), private) {
			t.Errorf("lookup failure exposed %q in %s", private, response.Body.String())
		}
	}
}

func TestWorkerListenerInlineCertificateMappingRemainsProcessConfigured(t *testing.T) {
	store, err := restjobs.OpenSQLiteStore(filepath.Join(privateSQLiteTestDir(t), "worker-inline-mapping.db"), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	clientCert := workerTestCertificate(t, "inline-mapping", true)
	digest := sha256.Sum256(clientCert.Certificate[0])
	handler, err := NewWorkerListenerHandler(WorkerListenerConfig{Certificates: []WorkerCertificatePrincipal{{SHA256: hex.EncodeToString(digest[:]), PrincipalID: "worker-inline", Enabled: true}}}, store, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/worker/session", strings.NewReader(`{"boot_id":"boot-inline"}`))
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{workerTestParsedCertificate(t, clientCert.Certificate[0])}}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated || !strings.Contains(response.Body.String(), `"principal_id":"worker-inline"`) {
		t.Fatalf("inline certificate mapping registration=%d %s, want configured principal", response.Code, response.Body.String())
	}
}

func TestWorkerCapabilityRequiresBearerAndMatchingMTLSPrincipal(t *testing.T) {
	store, err := restjobs.OpenSQLiteStore(filepath.Join(privateSQLiteTestDir(t), "worker-capability-auth.db"), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	clientCert := workerTestCertificate(t, "capability-client", true)
	fingerprint := sha256.Sum256(clientCert.Certificate[0])
	handler, err := NewWorkerListenerHandler(WorkerListenerConfig{Certificates: []WorkerCertificatePrincipal{{SHA256: hex.EncodeToString(fingerprint[:]), PrincipalID: "worker-capability", Enabled: true}}}, store, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	session, _, err := store.RegisterWorkerSession(context.Background(), "worker-capability", "boot-capability")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/worker/session", nil)
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{workerTestParsedCertificate(t, clientCert.Certificate[0])}}
	request.Header.Set("Authorization", "Bearer "+session.Capability)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("valid matching credentials status=%d, want 200", response.Code)
	}
	request.Header.Set("Authorization", "Bearer invalid")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("invalid bearer status=%d, want 401", response.Code)
	}
	request.Header.Set("Authorization", "Bearer  "+session.Capability)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("malformed bearer status=%d, want 401", response.Code)
	}
	request.Header.Del("Authorization")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("mTLS-only request status=%d, want 401", response.Code)
	}
	otherCert := workerTestCertificate(t, "other", true)
	otherFingerprint := sha256.Sum256(otherCert.Certificate[0])
	otherHandler, err := NewWorkerListenerHandler(WorkerListenerConfig{Certificates: []WorkerCertificatePrincipal{{SHA256: hex.EncodeToString(otherFingerprint[:]), PrincipalID: "worker-other", Enabled: true}}}, store, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{workerTestParsedCertificate(t, otherCert.Certificate[0])}}
	request.Header.Set("Authorization", "Bearer "+session.Capability)
	response = httptest.NewRecorder()
	otherHandler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("wrong mTLS principal status=%d, want 401", response.Code)
	}
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

func workerTestParsedCertificate(t *testing.T, der []byte) *x509.Certificate {
	t.Helper()
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

func workerTestCertificate(t *testing.T, commonName string, client bool) tls.Certificate {
	return workerTestCertificateWithValidity(t, commonName, client, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
}

func workerTestCertificateWithValidity(t *testing.T, commonName string, client bool, notBefore, notAfter time.Time) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: commonName}, DNSNames: []string{"example.test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: notBefore, NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign}
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
