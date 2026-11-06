package runtime

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
)

func TestPostgresWorkerMappingAuthenticationIsLiveAcrossHandlers(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL worker mapping integration tests")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		t.Fatal("worker mapping integration requires a PostgreSQL URL DSN")
	}
	adminConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid test PostgreSQL URL")
	}
	admin := stdlib.OpenDB(*adminConfig)
	t.Cleanup(func() { _ = admin.Close() })
	var version int
	if err := admin.QueryRow(`SHOW server_version_num`).Scan(&version); err != nil {
		t.Fatal("PostgreSQL test service is unavailable")
	}
	if version < 180000 || version >= 190000 {
		t.Fatalf("PostgreSQL version=%d, integration coverage requires PostgreSQL 18", version)
	}
	schema := fmt.Sprintf("factory_worker_mapping_%d", time.Now().UnixNano())
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal("create PostgreSQL worker mapping test schema")
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop PostgreSQL worker mapping test schema: %v", err)
		}
	})
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	scopedDSN := parsed.String()
	newStore := func() *restjobs.PostgresStore {
		t.Helper()
		config, err := pgx.ParseConfig(scopedDSN)
		if err != nil {
			t.Fatal("invalid scoped PostgreSQL URL")
		}
		store, err := restjobs.NewPostgresStore(stdlib.OpenDB(*config), restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.CloseStore() })
		return store
	}
	firstStore, secondStore := newStore(), newStore()
	clientDER := []byte("shared-worker-certificate-der")
	certificateTime := time.Now()
	clientCertificate := &x509.Certificate{
		Raw:       clientDER,
		NotBefore: certificateTime.Add(-time.Hour),
		NotAfter:  certificateTime.Add(time.Hour),
	}
	digest := sha256.Sum256(clientDER)
	fingerprint := hex.EncodeToString(digest[:])
	if _, err := firstStore.ReconcileWorkerMappings(context.Background(), map[string]string{fingerprint: "worker-before"}); err != nil {
		t.Fatal(err)
	}
	newHandler := func(store *restjobs.PostgresStore) *restserver.WorkerListenerHandler {
		t.Helper()
		handler, err := restserver.NewWorkerListenerHandler(restserver.WorkerListenerConfig{MaxClientCertLifetime: "72h", MappingFile: "mapping-file-mode"}, store, func() bool { return true })
		if err != nil {
			t.Fatal(err)
		}
		handler.SetMappingLookup(store)
		return handler
	}
	firstHandler, secondHandler := newHandler(firstStore), newHandler(secondStore)
	request := func(handler http.Handler, capability string) int {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, "/v1/worker/session", nil)
		r.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{clientCertificate}}
		r.Header.Set("Authorization", "Bearer "+capability)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, r)
		return response.Code
	}
	registered, _, err := firstStore.RegisterWorkerSession(context.Background(), "worker-before", "boot-live-pg")
	if err != nil {
		t.Fatal(err)
	}
	if status := request(secondHandler, registered.Capability); status != http.StatusOK {
		t.Fatalf("second independent handler status=%d before change, want 200", status)
	}
	if _, err := firstStore.ReconcileWorkerMappings(context.Background(), map[string]string{fingerprint: "worker-after"}); err != nil {
		t.Fatal(err)
	}
	if status := request(secondHandler, registered.Capability); status != http.StatusUnauthorized {
		t.Fatalf("second handler accepted capability after principal remap: status=%d, want 401", status)
	}
	remapped, _, err := secondStore.RegisterWorkerSession(context.Background(), "worker-after", "boot-live-pg")
	if err != nil {
		t.Fatal(err)
	}
	if status := request(firstHandler, remapped.Capability); status != http.StatusOK {
		t.Fatalf("first independent handler missed committed principal remap: status=%d, want 200", status)
	}
	if _, err := firstStore.ReconcileWorkerMappings(context.Background(), map[string]string{}); err != nil {
		t.Fatal(err)
	}
	if status := request(secondHandler, remapped.Capability); status != http.StatusUnauthorized {
		t.Fatalf("second handler accepted capability after committed removal: status=%d, want 401", status)
	}
	if err := secondStore.CloseStore(); err != nil {
		t.Fatal(err)
	}
	if status := request(secondHandler, remapped.Capability); status != http.StatusServiceUnavailable {
		t.Fatalf("mapping database outage status=%d, want sanitized 503", status)
	}
}
