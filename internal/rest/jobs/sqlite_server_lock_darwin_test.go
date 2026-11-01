//go:build darwin

package restjobs

import (
	"path/filepath"
	"testing"
)

func TestSQLiteServerStoreCanUseAndInspectDatabaseWhileOwned(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "jobs.db")
	owner, err := OpenSQLiteServerStore(path, testConfig())
	if err != nil {
		t.Fatalf("open owner store: %v", err)
	}
	defer owner.CloseStore()

	job, _, err := owner.Admit("darwin-owner", Request{Repository: "widget", Task: "use SQLite while holding server ownership"})
	if err != nil {
		t.Fatalf("owner admit job while holding ownership: %v", err)
	}
	if claimed, err := owner.ClaimNext(); err != nil || claimed.ID != job.ID {
		t.Fatalf("owner claim while holding ownership = (%+v, %v), want %q", claimed, err, job.ID)
	}

	observer, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatalf("open administrative observer while server owns database: %v", err)
	}
	defer observer.CloseStore()
	inspected, err := observer.Get(job.ID)
	if err != nil || inspected.Status != StatusRunning {
		t.Fatalf("inspect owned database = (%+v, %v), want running job", inspected, err)
	}
}
