package restserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
)

type memoryMappingStore struct {
	generation      uint64
	active, revoked map[string]string
	fail            bool
}

func (m *memoryMappingStore) ReconcileWorkerMappings(_ context.Context, candidate map[string]string) (restjobs.WorkerMappingState, error) {
	if m.fail {
		return restjobs.WorkerMappingState{}, errors.New("audit unavailable")
	}
	for fingerprint := range candidate {
		if _, revoked := m.revoked[fingerprint]; revoked {
			return restjobs.WorkerMappingState{}, errors.New("revoked")
		}
	}
	changed := false
	for fingerprint, principal := range candidate {
		if m.active[fingerprint] != principal {
			changed = true
		}
	}
	for fingerprint := range m.active {
		if _, ok := candidate[fingerprint]; !ok {
			delete(m.active, fingerprint)
			m.revoked[fingerprint] = ""
			changed = true
		}
	}
	for fingerprint, principal := range candidate {
		m.active[fingerprint] = principal
	}
	if changed {
		m.generation++
	}
	copy := make(map[string]string, len(m.active))
	for key, value := range m.active {
		copy[key] = value
	}
	return restjobs.WorkerMappingState{Generation: m.generation, Mappings: copy}, nil
}

func TestWorkerMappingReloadKeepsLastKnownGoodAndReportsCuratedStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mappings.json")
	fingerprint := strings.Repeat("b", 64)
	if err := os.WriteFile(path, []byte(`{"version":1,"mappings":[{"sha256":"`+fingerprint+`","principal_id":"worker-private"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store := &memoryMappingStore{active: map[string]string{}, revoked: map[string]string{}}
	registry, err := NewWorkerMappingRegistry(path, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if principal, ok := registry.Principal(fingerprint); !ok || principal != "worker-private" {
		t.Fatalf("active mapping=(%q,%v)", principal, ok)
	}
	if err := os.WriteFile(path, []byte(`{"version":2,"mappings":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := registry.Reload(context.Background()); err == nil {
		t.Fatal("invalid mapping candidate was accepted")
	}
	if principal, ok := registry.Principal(fingerprint); !ok || principal != "worker-private" {
		t.Fatalf("invalid reload changed last known good mapping=(%q,%v)", principal, ok)
	}
	status := registry.Status()
	if status.Generation != 1 || status.Count != 1 || status.Result != "rejected" || status.At == nil {
		t.Fatalf("status=%+v", status)
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"worker-private", fingerprint, path, "invalid mapping"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Errorf("status exposed %q: %s", forbidden, encoded)
		}
	}
}

func TestWorkerMappingAuditFailureRetainsLastKnownGoodPublication(t *testing.T) {
	dbDir := t.TempDir()
	if err := os.Chmod(dbDir, 0o700); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(dbDir, "worker-mapping-audit.db")
	store, err := restjobs.OpenSQLiteStore(dbPath, restjobs.Config{QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 10, MaxEventsPerJob: 10, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	path := filepath.Join(t.TempDir(), "mappings.json")
	fingerprint := strings.Repeat("d", 64)
	write := func(principal string) {
		t.Helper()
		contents := `{"version":1,"mappings":[{"sha256":"` + fingerprint + `","principal_id":"` + principal + `"}]}`
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("worker-before")
	registry, err := NewWorkerMappingRegistry(path, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER reject_worker_mapping_audit BEFORE INSERT ON factory_worker_mapping_audit BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	write("worker-after")
	if err := registry.Reload(context.Background()); err == nil {
		t.Fatal("mapping reload succeeded when its audit write failed")
	}
	if principal, ok := registry.Principal(fingerprint); !ok || principal != "worker-before" {
		t.Fatalf("published mapping after failed audit=(%q,%v); want last known good worker-before", principal, ok)
	}
	status := registry.Status()
	if status.Generation != 1 || status.Count != 1 || status.Result != "rejected" {
		t.Fatalf("status after failed audit=%+v; want rejected reload retaining generation 1", status)
	}
	var generation, auditRows int
	var principal string
	if err := db.QueryRow(`SELECT generation FROM factory_worker_mapping_state WHERE singleton=1`).Scan(&generation); err != nil || generation != 1 {
		t.Fatalf("database generation after failed audit=%d, err=%v; want 1", generation, err)
	}
	if err := db.QueryRow(`SELECT principal_id FROM factory_worker_mappings WHERE fingerprint=?`, fingerprint).Scan(&principal); err != nil || principal != "worker-before" {
		t.Fatalf("database mapping after failed audit=%q, err=%v; want worker-before", principal, err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM factory_worker_mapping_audit WHERE fingerprint=?`, fingerprint).Scan(&auditRows); err != nil || auditRows != 1 {
		t.Fatalf("audit rows after failed change=%d, err=%v; want only initial accepted event", auditRows, err)
	}
}

type blockingMappingStore struct {
	inner   *memoryMappingStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingMappingStore) ReconcileWorkerMappings(ctx context.Context, candidate map[string]string) (restjobs.WorkerMappingState, error) {
	s.once.Do(func() {
		close(s.entered)
		<-s.release
	})
	return s.inner.ReconcileWorkerMappings(ctx, candidate)
}

func TestWorkerMappingConcurrentReloadsPublishInSerializedOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mappings.json")
	fingerprint := strings.Repeat("e", 64)
	write := func(principal string) {
		t.Helper()
		contents := `{"version":1,"mappings":[{"sha256":"` + fingerprint + `","principal_id":"` + principal + `"}]}`
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("worker-first")
	store := &blockingMappingStore{inner: &memoryMappingStore{active: map[string]string{}, revoked: map[string]string{}}, entered: make(chan struct{}), release: make(chan struct{})}
	registry, err := NewWorkerMappingRegistry(path, store)
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- registry.Reload(context.Background()) }()
	<-store.entered
	write("worker-second")
	secondDone := make(chan error, 1)
	go func() { secondDone <- registry.Reload(context.Background()) }()
	close(store.release)
	for name, done := range map[string]<-chan error{"first": firstDone, "second": secondDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s reload failed: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s reload did not complete", name)
		}
	}
	if principal, ok := registry.Principal(fingerprint); !ok || principal != "worker-second" {
		t.Fatalf("final published mapping=(%q,%v), want latest serialized mapping", principal, ok)
	}
	if status := registry.Status(); status.Result != "accepted" {
		t.Fatalf("final reload status=%+v, want successful final reload", status)
	}
}

func TestWorkerMappingRemovalCreatesPermanentTombstone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mappings.json")
	fingerprint := strings.Repeat("c", 64)
	write := func(contents string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store := &memoryMappingStore{active: map[string]string{}, revoked: map[string]string{}}
	registry, err := NewWorkerMappingRegistry(path, store)
	if err != nil {
		t.Fatal(err)
	}
	write(`{"version":1,"mappings":[{"sha256":"` + fingerprint + `","principal_id":"worker"}]}`)
	if err := registry.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	write(`{"version":1,"mappings":[]}`)
	if err := registry.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	write(`{"version":1,"mappings":[{"sha256":"` + fingerprint + `","principal_id":"worker"}]}`)
	if err := registry.Reload(context.Background()); err == nil {
		t.Fatal("removed fingerprint was re-added")
	}
	if _, ok := registry.Principal(fingerprint); ok {
		t.Fatal("revoked fingerprint remains active")
	}
}
