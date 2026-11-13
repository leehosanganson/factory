package restjobs

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSQLiteWorkerSessionHeartbeatRequiresActiveCapabilityAndAdvancesMonotonically(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "session-heartbeat.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	session, _, err := store.RegisterWorkerSession(ctx, "worker-heartbeat", "boot-heartbeat")
	if err != nil {
		t.Fatal(err)
	}
	nearExpiry := time.Now().UTC().Add(5 * time.Minute)
	if _, err := store.db.ExecContext(ctx, `UPDATE factory_worker_sessions SET capability_expires_at=? WHERE id=?`, nearExpiry.Format(time.RFC3339Nano), session.ID); err != nil {
		t.Fatal(err)
	}
	session.CapabilityExpiresAt = &nearExpiry
	first, err := store.HeartbeatWorkerSession(ctx, "worker-heartbeat", session.Capability)
	if err != nil || first.IsZero() {
		t.Fatalf("first heartbeat=(%v,%v), want persisted timestamp", first, err)
	}
	var renewedExpiry string
	if err := store.db.QueryRowContext(ctx, `SELECT capability_expires_at FROM factory_worker_sessions WHERE id=?`, session.ID).Scan(&renewedExpiry); err != nil {
		t.Fatal(err)
	}
	parsedRenewedExpiry, err := time.Parse(time.RFC3339Nano, renewedExpiry)
	if err != nil || session.CapabilityExpiresAt == nil || !parsedRenewedExpiry.After(*session.CapabilityExpiresAt) {
		t.Fatalf("heartbeat expiry=%q (%v), want extension beyond initial expiry %v", renewedExpiry, err, session.CapabilityExpiresAt)
	}
	var renewalAudits int
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM factory_worker_session_audit WHERE session_id=? AND event='capability_renewed' AND capability_hash IS NOT NULL AND capability_expires_at IS NOT NULL`, session.ID).Scan(&renewalAudits); err != nil || renewalAudits != 1 {
		t.Fatalf("heartbeat renewal audit count=%d err=%v, want one digest/expiry audit", renewalAudits, err)
	}
	second, err := store.HeartbeatWorkerSession(ctx, "worker-heartbeat", session.Capability)
	if err != nil || second.Before(first) {
		t.Fatalf("second heartbeat=(%v,%v), must not move backwards from %v", second, err, first)
	}
	if _, err := store.HeartbeatWorkerSession(ctx, "other-worker", session.Capability); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-principal heartbeat error=%v, want not found", err)
	}
	if _, err := store.RetireWorkerCapability(ctx, "worker-heartbeat", session.Capability); err != nil {
		t.Fatal(err)
	}
	if _, err := store.HeartbeatWorkerSession(ctx, "worker-heartbeat", session.Capability); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retired-session heartbeat error=%v, want not found", err)
	}
	expired, _, err := store.RegisterWorkerSession(ctx, "worker-heartbeat-expired", "boot-heartbeat-expired")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE factory_worker_sessions SET capability_expires_at=? WHERE id=?`, time.Now().UTC().Add(-time.Second).Format("2006-01-02T15:04:05.000000000Z07:00"), expired.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.HeartbeatWorkerSession(ctx, expired.PrincipalID, expired.Capability); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired-session heartbeat error=%v, want not found", err)
	}
	var stored string
	if err := store.db.QueryRowContext(ctx, `SELECT heartbeat_at FROM factory_worker_sessions WHERE id=?`, session.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	persisted, err := time.Parse(time.RFC3339Nano, stored)
	if err != nil || persisted.Before(second) {
		t.Fatalf("persisted heartbeat=%q (%v), want at least %v", stored, err, second)
	}
}

func TestSQLiteWorkerSessionHeartbeatExpiryExtensionIsRetrySafe(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "session-heartbeat-expiry.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	session, _, err := store.RegisterWorkerSession(ctx, "worker-heartbeat-expiry", "boot-heartbeat-expiry")
	if err != nil {
		t.Fatal(err)
	}
	initialExpiry := *session.CapabilityExpiresAt
	now := initialExpiry.Add(-time.Minute)
	first, err := heartbeatWorkerSessionAt(ctx, store.db, session.PrincipalID, session.Capability, "?", now)
	if err != nil {
		t.Fatal(err)
	}
	var expiryAfterFirst string
	if err := store.db.QueryRowContext(ctx, `SELECT capability_expires_at FROM factory_worker_sessions WHERE id=?`, session.ID).Scan(&expiryAfterFirst); err != nil {
		t.Fatal(err)
	}
	renewedExpiry, err := time.Parse(time.RFC3339Nano, expiryAfterFirst)
	if err != nil || !renewedExpiry.After(initialExpiry) {
		t.Fatalf("expiry after renewal=%q (%v), want after initial %v", expiryAfterFirst, err, initialExpiry)
	}
	auditCount := func() int {
		t.Helper()
		var count int
		if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM factory_worker_session_audit WHERE session_id=? AND event='capability_renewed'`, session.ID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	if got := auditCount(); got != 1 {
		t.Fatalf("renewal audit count=%d, want one", got)
	}
	// The same request time models an exact retry after a lost response. It
	// must neither shorten expiry nor add duplicate audit records.
	retry, err := heartbeatWorkerSessionAt(ctx, store.db, session.PrincipalID, session.Capability, "?", now)
	if err != nil || retry.Before(first) {
		t.Fatalf("retry heartbeat=(%v,%v), want at least prior timestamp %v", retry, err, first)
	}
	var expiryAfterRetry string
	if err := store.db.QueryRowContext(ctx, `SELECT capability_expires_at FROM factory_worker_sessions WHERE id=?`, session.ID).Scan(&expiryAfterRetry); err != nil {
		t.Fatal(err)
	}
	if expiryAfterRetry != expiryAfterFirst || auditCount() != 1 {
		t.Fatalf("idempotent retry changed expiry/audit: expiry %q -> %q, audit count %d", expiryAfterFirst, expiryAfterRetry, auditCount())
	}
}

func TestSQLiteWorkerSessionHeartbeatTimestampComparesDifferentFractionalPrecisionByTime(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "session-heartbeat-precision.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	session, _, err := store.RegisterWorkerSession(ctx, "worker-heartbeat-precision", "boot-heartbeat-precision")
	if err != nil {
		t.Fatal(err)
	}

	previous := time.Date(2025, time.January, 2, 3, 4, 5, 200_000_000, time.UTC)
	next := previous.Add(time.Nanosecond)
	if _, err := store.db.ExecContext(ctx, `UPDATE factory_worker_sessions SET heartbeat_at=? WHERE id=?`, previous.Format("2006-01-02T15:04:05.0Z"), session.ID); err != nil {
		t.Fatal(err)
	}

	got, err := heartbeatWorkerSessionAt(ctx, store.db, session.PrincipalID, session.Capability, "?", next)
	if err != nil || got.Before(next) {
		t.Fatalf("heartbeat=(%v,%v), want at least %v despite different fractional precision", got, err, next)
	}
	var stored string
	if err := store.db.QueryRowContext(ctx, `SELECT heartbeat_at FROM factory_worker_sessions WHERE id=?`, session.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	persisted, err := time.Parse(time.RFC3339Nano, stored)
	if err != nil || persisted.Before(next) {
		t.Fatalf("persisted heartbeat=%q (%v), want at least %v", stored, err, next)
	}
}

func TestSQLiteWorkerCapabilityExpiryComparesDifferentFractionalPrecisionByTime(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "session-expiry-precision.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	comparisonTime := time.Date(2025, time.January, 2, 3, 4, 5, 110_000_000, time.UTC)
	expiredAt := comparisonTime.Add(-10 * time.Millisecond)
	validAt := comparisonTime.Add(10 * time.Millisecond)
	setExpiry := func(session WorkerSession, expiry time.Time) {
		t.Helper()
		if _, err := store.db.ExecContext(ctx, `UPDATE factory_worker_sessions SET capability_expires_at=? WHERE id=?`, expiry.Format(time.RFC3339Nano), session.ID); err != nil {
			t.Fatal(err)
		}
	}
	register := func(principal string) WorkerSession {
		t.Helper()
		session, _, err := store.RegisterWorkerSession(ctx, principal, "boot-expiry-precision")
		if err != nil {
			t.Fatal(err)
		}
		return session
	}

	expiredHeartbeat := register("worker-expired-heartbeat-precision")
	setExpiry(expiredHeartbeat, expiredAt) // Stored as ...05.1Z; comparison is ...05.11Z.
	if _, err := heartbeatWorkerSessionAt(ctx, store.db, expiredHeartbeat.PrincipalID, expiredHeartbeat.Capability, "?", comparisonTime); !errors.Is(err, ErrNotFound) {
		t.Errorf("expired capability heartbeat error=%v, want not found", err)
	}

	expiredAuth := register("worker-expired-auth-precision")
	setExpiry(expiredAuth, expiredAt)
	if _, err := authenticateWorkerCapabilityAt(ctx, store.db, expiredAuth.PrincipalID, expiredAuth.Capability, "?", comparisonTime); !errors.Is(err, ErrNotFound) {
		t.Errorf("expired capability authentication error=%v, want not found", err)
	}

	expiredRetirement := register("worker-expired-retirement-precision")
	setExpiry(expiredRetirement, expiredAt)
	if _, err := retireSQLiteWorkerCapabilityAt(ctx, store.db, expiredRetirement.PrincipalID, expiredRetirement.Capability, comparisonTime); !errors.Is(err, ErrNotFound) {
		t.Errorf("expired capability retirement error=%v, want not found", err)
	}
	var status string
	if err := store.db.QueryRowContext(ctx, `SELECT status FROM factory_worker_sessions WHERE id=?`, expiredRetirement.ID).Scan(&status); err != nil || status != "active" {
		t.Errorf("expired capability retirement status=%q err=%v, want active", status, err)
	}

	validHeartbeat := register("worker-valid-heartbeat-precision")
	setExpiry(validHeartbeat, validAt)
	if _, err := heartbeatWorkerSessionAt(ctx, store.db, validHeartbeat.PrincipalID, validHeartbeat.Capability, "?", comparisonTime); err != nil {
		t.Errorf("valid capability heartbeat error=%v, want success", err)
	}
	validAuth := register("worker-valid-auth-precision")
	setExpiry(validAuth, validAt)
	if _, err := authenticateWorkerCapabilityAt(ctx, store.db, validAuth.PrincipalID, validAuth.Capability, "?", comparisonTime); err != nil {
		t.Errorf("valid capability authentication error=%v, want success", err)
	}
	validRetirement := register("worker-valid-retirement-precision")
	setExpiry(validRetirement, validAt)
	if _, err := retireSQLiteWorkerCapabilityAt(ctx, store.db, validRetirement.PrincipalID, validRetirement.Capability, comparisonTime); err != nil {
		t.Errorf("valid capability retirement error=%v, want success", err)
	}
}

func TestSQLiteWorkerCapabilityExpiryIsCheckedAfterWaitingForWriteLock(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "session-expiry-lock.db")
	firstStore, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer firstStore.Close()
	secondStore, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer secondStore.Close()
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		call func(WorkerSession) error
	}{
		{
			name: "heartbeat",
			call: func(session WorkerSession) error {
				_, err := secondStore.HeartbeatWorkerSession(ctx, session.PrincipalID, session.Capability)
				return err
			},
		},
		{
			name: "retirement",
			call: func(session WorkerSession) error {
				_, err := secondStore.RetireWorkerCapability(ctx, session.PrincipalID, session.Capability)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session, _, err := firstStore.RegisterWorkerSession(ctx, "worker-expiry-lock-"+tc.name, "boot-expiry-lock")
			if err != nil {
				t.Fatal(err)
			}
			expiresAt := time.Now().UTC().Add(100 * time.Millisecond)
			if _, err := firstStore.db.ExecContext(ctx, `UPDATE factory_worker_sessions SET capability_expires_at=? WHERE id=?`, expiresAt.Format(time.RFC3339Nano), session.ID); err != nil {
				t.Fatal(err)
			}
			lock, err := firstStore.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := lock.ExecContext(ctx, `UPDATE factory_worker_sessions SET heartbeat_at=heartbeat_at WHERE id=?`, session.ID); err != nil {
				_ = lock.Rollback()
				t.Fatal(err)
			}

			result := make(chan error, 1)
			started := make(chan struct{})
			go func() {
				close(started)
				result <- tc.call(session)
			}()
			<-started
			deadline := time.Now().Add(time.Until(expiresAt) / 2)
			for secondStore.db.Stats().InUse == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if secondStore.db.Stats().InUse == 0 {
				t.Fatal("capability operation did not acquire a database connection before expiry")
			}
			select {
			case err := <-result:
				t.Fatalf("capability operation completed before lock release: %v", err)
			case <-time.After(time.Until(expiresAt) + 50*time.Millisecond):
			}
			if err := lock.Commit(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-result:
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("expired capability operation error=%v, want not found", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("capability operation did not complete after lock release")
			}
		})
	}
}

func TestSQLiteWorkerSessionConcurrentHeartbeatsNeverMoveBackwards(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "session-heartbeat-race.db")
	firstStore, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer firstStore.Close()
	secondStore, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer secondStore.Close()
	session, _, err := firstStore.RegisterWorkerSession(context.Background(), "worker-heartbeat-race", "boot-heartbeat-race")
	if err != nil {
		t.Fatal(err)
	}
	stores := []*SQLiteStore{firstStore, secondStore}
	const contenders = 12
	var wg sync.WaitGroup
	errCh := make(chan error, contenders)
	times := make(chan time.Time, contenders)
	for i := range contenders {
		wg.Add(1)
		go func(store *SQLiteStore) {
			defer wg.Done()
			heartbeatAt, err := store.HeartbeatWorkerSession(context.Background(), session.PrincipalID, session.Capability)
			if err != nil {
				errCh <- err
				return
			}
			times <- heartbeatAt
		}(stores[i%len(stores)])
	}
	wg.Wait()
	close(errCh)
	close(times)
	for err := range errCh {
		t.Errorf("concurrent heartbeat: %v", err)
	}
	var latestReturned time.Time
	for heartbeatAt := range times {
		if heartbeatAt.After(latestReturned) {
			latestReturned = heartbeatAt
		}
	}
	var stored sql.NullString
	if err := firstStore.db.QueryRow(`SELECT heartbeat_at FROM factory_worker_sessions WHERE id=?`, session.ID).Scan(&stored); err != nil || !stored.Valid {
		t.Fatalf("concurrent heartbeat timestamp=%v err=%v, want persisted timestamp", stored, err)
	}
	persisted, err := time.Parse(time.RFC3339Nano, stored.String)
	if err != nil {
		t.Fatalf("parse concurrent heartbeat timestamp %q: %v", stored.String, err)
	}
	if persisted.Before(latestReturned) {
		t.Fatalf("persisted heartbeat %v is earlier than a concurrent response %v", persisted, latestReturned)
	}
	for i := range contenders {
		got, err := stores[i%len(stores)].HeartbeatWorkerSession(context.Background(), session.PrincipalID, session.Capability)
		if err != nil || got.Before(persisted) {
			t.Fatalf("post-concurrency heartbeat %d=(%v,%v), before persisted %v", i, got, err, persisted)
		}
		persisted = got
	}
}

func TestSQLiteWorkerSessionRegistrationRotatesShortLivedCapability(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "session-capability.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	first, replay, err := store.RegisterWorkerSession(ctx, "worker-capability", "boot-capability")
	if err != nil || replay || first.Capability == "" || first.CapabilityExpiresAt == nil {
		t.Fatalf("first registration=(%+v,%v,%v)", first, replay, err)
	}
	if got := first.CapabilityExpiresAt.Sub(first.CreatedAt); got < WorkerSessionCapabilityTTL-time.Second || got > WorkerSessionCapabilityTTL+time.Second {
		t.Fatalf("capability lifetime=%s, want %s", got, WorkerSessionCapabilityTTL)
	}
	if got, err := store.AuthenticateWorkerCapability(ctx, "worker-capability", first.Capability); err != nil || got.ID != first.ID {
		t.Fatalf("authenticate first capability=(%+v,%v)", got, err)
	}
	second, replay, err := store.RegisterWorkerSession(ctx, "worker-capability", "boot-capability")
	if err != nil || !replay || second.ID != first.ID || second.Capability == first.Capability {
		t.Fatalf("same-boot rotation=(%+v,%v,%v)", second, replay, err)
	}
	if _, err := store.AuthenticateWorkerCapability(ctx, "worker-capability", first.Capability); !errors.Is(err, ErrNotFound) {
		t.Fatalf("superseded capability error=%v, want invalid", err)
	}
	if got, err := store.AuthenticateWorkerCapability(ctx, "worker-capability", second.Capability); err != nil || got.ID != second.ID {
		t.Fatalf("authenticate replacement capability=(%+v,%v)", got, err)
	}
	if _, err := store.db.Exec(`UPDATE factory_worker_sessions SET capability_expires_at=? WHERE id=?`, time.Now().UTC().Add(-time.Second).Format("2006-01-02T15:04:05.000000000Z07:00"), first.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateWorkerCapability(ctx, "worker-capability", second.Capability); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired capability error=%v, want invalid", err)
	}
	var stored string
	if err := store.db.QueryRow(`SELECT capability_hash FROM factory_worker_sessions WHERE id=?`, first.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stored, first.Capability) || strings.Contains(stored, second.Capability) {
		t.Fatal("session store contains a raw capability")
	}
}

func TestSQLiteWorkerSessionCapabilityAuditFailuresRollBackIssuanceAndRotation(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "session-capability-audit-failure.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	if _, err := store.db.Exec(`CREATE TRIGGER reject_capability_issuance BEFORE INSERT ON factory_worker_session_audit WHEN NEW.event='capability_issued' BEGIN SELECT RAISE(ABORT,'injected audit failure'); END`); err != nil {
		t.Fatal(err)
	}
	const issuePrincipal = "worker-failed-issuance"
	if _, _, err := store.RegisterWorkerSession(ctx, issuePrincipal, "boot-issuance"); err == nil {
		t.Fatal("capability issuance succeeded despite injected audit-write failure")
	}
	var sessions, auditRows int
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_sessions WHERE principal_id=?`, issuePrincipal).Scan(&sessions); err != nil || sessions != 0 {
		t.Fatalf("failed issuance left %d session rows (query error %v), want none", sessions, err)
	}
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit WHERE principal_id=?`, issuePrincipal).Scan(&auditRows); err != nil || auditRows != 0 {
		t.Fatalf("failed issuance left %d audit rows (query error %v), want none", auditRows, err)
	}
	if _, err := store.db.Exec(`DROP TRIGGER reject_capability_issuance`); err != nil {
		t.Fatal(err)
	}

	const rotationPrincipal = "worker-failed-rotation"
	prior, replay, err := store.RegisterWorkerSession(ctx, rotationPrincipal, "boot-rotation")
	if err != nil || replay {
		t.Fatalf("initial registration=(%+v,%v,%v)", prior, replay, err)
	}
	if _, err := store.db.Exec(`CREATE TRIGGER reject_capability_rotation BEFORE INSERT ON factory_worker_session_audit WHEN NEW.event='capability_rotated' BEGIN SELECT RAISE(ABORT,'injected audit failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, rotationPrincipal, "boot-rotation"); err == nil {
		t.Fatal("capability rotation succeeded despite injected audit-write failure")
	}
	if got, err := store.AuthenticateWorkerCapability(ctx, rotationPrincipal, prior.Capability); err != nil || got.ID != prior.ID {
		t.Fatalf("failed rotation did not preserve the prior valid capability: session=%+v err=%v", got, err)
	}
	var storedHash string
	if err := store.db.QueryRow(`SELECT capability_hash FROM factory_worker_sessions WHERE id=?`, prior.ID).Scan(&storedHash); err != nil {
		t.Fatal(err)
	}
	priorHash := sha256.Sum256([]byte(prior.Capability))
	if storedHash != hex.EncodeToString(priorHash[:]) {
		t.Fatal("failed rotation changed the stored capability")
	}
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit WHERE session_id=? AND event='capability_rotated'`, prior.ID).Scan(&auditRows); err != nil || auditRows != 0 {
		t.Fatalf("failed rotation left %d rotation audit rows (query error %v), want none", auditRows, err)
	}
}

func TestSQLiteWorkerSessionsAreAtomicIdempotentRetirableAndAudited(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "sessions.db")
	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	const contenders = 16
	var wg sync.WaitGroup
	results := make(chan WorkerSession, contenders)
	errs := make(chan error, contenders)
	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			session, replay, err := store.RegisterWorkerSession(ctx, "worker-1", "boot-1")
			if err == nil && replay && session.ID == "" {
				err = errors.New("idempotent registration returned no identity")
			}
			if err != nil {
				errs <- err
				return
			}
			results <- session
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent registration: %v", err)
	}
	var session WorkerSession
	for candidate := range results {
		if session.ID == "" {
			session = candidate
		} else if candidate.ID != session.ID {
			t.Fatalf("concurrent registrations created %q and %q", session.ID, candidate.ID)
		}
	}
	if session.ID == "" {
		t.Fatal("no session registered")
	}
	replay, isReplay, err := store.RegisterWorkerSession(ctx, "worker-1", "boot-1")
	if err != nil || !isReplay || replay.ID != session.ID {
		t.Fatalf("same-boot retry=(%+v,%v,%v), want same session replay", replay, isReplay, err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, "worker-1", "boot-2"); !errors.Is(err, ErrWorkerSessionConflict) {
		t.Fatalf("different boot registration error=%v, want conflict", err)
	}
	retired, err := store.RetireWorkerSession(ctx, "worker-1", "boot-1", session.ID)
	if err != nil || retired.Status != "retired" || retired.RetiredAt == nil {
		t.Fatalf("retirement=(%+v,%v)", retired, err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, "worker-1", "boot-1"); !errors.Is(err, ErrWorkerSessionConflict) {
		t.Fatalf("re-register retired principal/boot pair error=%v, want conflict", err)
	}
	next, nextReplay, err := store.RegisterWorkerSession(ctx, "worker-1", "boot-2")
	if err != nil || nextReplay || next.ID == session.ID {
		t.Fatalf("post-retirement registration=(%+v,%v,%v)", next, replay, err)
	}
	var auditCount int
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit WHERE event IN ('registered', 'retired')`).Scan(&auditCount); err != nil || auditCount != 3 {
		t.Fatalf("registration/retirement audit events=%d err=%v; want registered, retired, registered", auditCount, err)
	}
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit WHERE event IN ('capability_issued', 'capability_rotated')`).Scan(&auditCount); err != nil || auditCount < 3 {
		t.Fatalf("capability audit events=%d err=%v; want issuance and retries recorded", auditCount, err)
	}
	var digest, expiresAt string
	if err := store.db.QueryRow(`SELECT capability_hash,capability_expires_at FROM factory_worker_session_audit WHERE session_id=? AND event='capability_issued'`, session.ID).Scan(&digest, &expiresAt); err != nil {
		t.Fatal(err)
	}
	firstDigest := sha256.Sum256([]byte(session.Capability))
	if digest != hex.EncodeToString(firstDigest[:]) || expiresAt == "" || strings.Contains(digest, session.Capability) {
		t.Fatal("issuance audit does not contain the matching digest and expiry")
	}
	rotatedDigest := sha256.Sum256([]byte(replay.Capability))
	var rotationCount int
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit WHERE session_id=? AND event='capability_rotated' AND capability_hash=? AND capability_expires_at IS NOT NULL`, session.ID, hex.EncodeToString(rotatedDigest[:])).Scan(&rotationCount); err != nil || rotationCount != 1 {
		t.Fatalf("rotation audit records for replacement digest=%d err=%v, want 1", rotationCount, err)
	}
	if _, err := store.db.Exec(`UPDATE factory_worker_session_audit SET event='tampered'`); err == nil {
		t.Fatal("audit update succeeded despite append-only trigger")
	}
	if _, err := store.db.Exec(`DELETE FROM factory_worker_session_audit`); err == nil {
		t.Fatal("audit delete succeeded despite append-only trigger")
	}
}

func TestSQLiteWorkerSessionSurvivesStoreReopen(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "sessions-reopen.db")
	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	created, replay, err := store.RegisterWorkerSession(context.Background(), "worker-reopen", "boot-reopen")
	if err != nil || replay {
		t.Fatalf("register=(%+v,%v,%v)", created, replay, err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, replay, err := reopened.RegisterWorkerSession(context.Background(), "worker-reopen", "boot-reopen")
	if err != nil || !replay || got.ID != created.ID {
		t.Fatalf("reopen retry=(%+v,%v,%v), want persisted session", got, replay, err)
	}
}

func TestSQLiteWorkerSessionConcurrentSameBootRegistrationIsIdempotent(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "sessions-same-boot-race.db")
	first, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	stores := []*SQLiteStore{first, second}
	var wg sync.WaitGroup
	results := make(chan struct {
		session WorkerSession
		replay  bool
		err     error
	}, len(stores))
	for _, candidate := range stores {
		wg.Add(1)
		go func(candidate *SQLiteStore) {
			defer wg.Done()
			session, replay, err := candidate.RegisterWorkerSession(context.Background(), "worker-same", "boot-same")
			results <- struct {
				session WorkerSession
				replay  bool
				err     error
			}{session, replay, err}
		}(candidate)
	}
	wg.Wait()
	close(results)
	var id string
	for result := range results {
		if result.err != nil {
			t.Fatalf("same-boot concurrent registration: %v", result.err)
		}
		if id == "" {
			id = result.session.ID
		} else if id != result.session.ID {
			t.Fatalf("same-boot registrations returned %q and %q", id, result.session.ID)
		}
	}
	if id == "" {
		t.Fatal("concurrent registration created no session")
	}
}

func TestSQLiteWorkerSessionConcurrentDifferentBootHasOneWinner(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "sessions-race.db")
	first, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	stores := []*SQLiteStore{first, second}
	var wg sync.WaitGroup
	results := make(chan error, len(stores))
	for i, candidate := range stores {
		wg.Add(1)
		go func(i int, candidate *SQLiteStore) {
			defer wg.Done()
			_, _, err := candidate.RegisterWorkerSession(context.Background(), "worker-race", fmt.Sprintf("boot-%d", i))
			results <- err
		}(i, candidate)
	}
	wg.Wait()
	close(results)
	successes, conflicts := 0, 0
	for err := range results {
		if err == nil {
			successes++
		} else if errors.Is(err, ErrWorkerSessionConflict) {
			conflicts++
		} else {
			t.Errorf("registration error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d; want one each", successes, conflicts)
	}
}

func TestWorkerSessionStoreRejectsInvalidIdentityAndUnknownRetirement(t *testing.T) {
	store, err := OpenSQLiteStore(filepath.Join(privateSQLiteDir(t), "sessions-invalid.db"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, pair := range [][2]string{{"bad/id", "boot"}, {"worker", "bad/boot"}, {"", "boot"}} {
		if _, _, err := store.RegisterWorkerSession(context.Background(), pair[0], pair[1]); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("RegisterWorkerSession(%q,%q) error=%v", pair[0], pair[1], err)
		}
	}
	if _, err := store.RetireWorkerSession(context.Background(), "worker", "boot", "unknown"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown retirement error=%v", err)
	}
}

func TestSQLiteScheduleDispatchMigrationBackfillsOldScheduleRows(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "schedule-upgrade.db")
	dsn, err := sqliteDSN(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensurePrivateSQLiteFile(path); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	files, err := MigrationNames()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE factory_schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for index, name := range files[:20] {
		payload, readErr := migrationFiles.ReadFile(name)
		if readErr != nil {
			t.Fatal(readErr)
		}
		tx, beginErr := db.Begin()
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		if _, err := tx.Exec(string(payload)); err != nil {
			t.Fatalf("apply legacy migration %s: %v", name, err)
		}
		if _, err := tx.Exec(`INSERT INTO factory_schema_migrations(version,name,applied_at) VALUES(?,?,?)`, index+1, filepath.Base(name), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	defer db.Close()
	created := time.Now().UTC().Truncate(time.Minute).Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO factory_schedules(id,owner,cron,repository,task,status,revision,created_at,updated_at) VALUES('old-schedule','alice','* * * * *','widget','task','active',3,?,?)`, created, created); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence,submitter) VALUES('legacy-schedule-job','{"repository":"widget","task":"old"}','queued',?,?,1,'alice')`, created, created); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO factory_job_idempotency(principal_id,key,job_id,request_json) VALUES('alice',?,'legacy-schedule-job','{"repository":"widget","task":"old"}')`, "schedule:old-schedule:3:"+created); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenSQLiteStore(path, Config{QueueCapacity: 32, MaxConcurrentJobs: 2, MaxRecords: 1000, MaxEventsPerJob: 200, MaxTaskBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	schedule, err := store.GetSchedule("old-schedule")
	if err != nil || !schedule.NextOccurrence().After(schedule.UpdatedAt) {
		t.Fatalf("upgraded schedule=(%+v,%v)", schedule, err)
	}
	var auditRows int
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_schedule_audit WHERE schedule_id='old-schedule'`).Scan(&auditRows); err != nil || auditRows != 0 {
		t.Fatalf("pre-upgrade schedule audit rows=%d err=%v; want empty history", auditRows, err)
	}
	var runRows int
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_schedule_runs WHERE schedule_id='old-schedule'`).Scan(&runRows); err != nil || runRows != 0 {
		t.Fatalf("pre-upgrade schedule run provenance rows=%d err=%v; want no backfill", runRows, err)
	}
	legacyRuns, err := store.ListScheduleRuns("old-schedule", "alice", false, 0, 10)
	if err != nil || len(legacyRuns.Runs) != 0 {
		t.Fatalf("historical matching idempotency key produced schedule runs=%+v err=%v; want no backfill", legacyRuns, err)
	}
	history, err := store.ListScheduleHistory("old-schedule", "admin", true, 0, MaxScheduleHistoryLimit)
	if err != nil || len(history.Events) != 0 {
		t.Fatalf("pre-upgrade schedule history=%+v err=%v; history must begin after upgrade", history, err)
	}
	if _, err := store.MutateScheduleAs("old-schedule", "admin", true, 3, SchedulePause, nil); err != nil {
		t.Fatalf("mutate pre-upgrade schedule after upgrade: %v", err)
	}
	history, err = store.ListScheduleHistory("old-schedule", "admin", true, 0, MaxScheduleHistoryLimit)
	if err != nil || len(history.Events) != 1 || history.Events[0].Revision != 4 || history.Events[0].Actor != "admin" || history.Events[0].Action != SchedulePauseAction {
		t.Fatalf("post-upgrade schedule history=%+v err=%v; expected only the committed post-upgrade action", history, err)
	}
}

func TestSQLiteWorkerSessionAndWebhookMigrationsUpgradeFromV14(t *testing.T) {
	path := filepath.Join(privateSQLiteDir(t), "pre-v15.db")
	dsn, err := sqliteDSN(path)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE factory_schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	files, err := MigrationNames()
	if err != nil {
		t.Fatal(err)
	}
	for index, name := range files[:14] {
		payload, err := migrationFiles.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(payload)); err != nil {
			t.Fatalf("apply SQLite migration %s: %v", name, err)
		}
		if _, err := db.Exec(`INSERT INTO factory_schema_migrations(version,name,applied_at) VALUES(?,?,?)`, index+1, filepath.Base(name), "2026-01-01T00:00:00Z"); err != nil {
			t.Fatal(err)
		}
	}
	const legacyCreatedAt = "2025-05-06T07:08:09Z"
	const legacyRequest = `{"repository":"legacy-widget","task":"preserve before migration"}`
	const memberRequest = `{"repository":"legacy-widget","task":"preserve thread member"}`
	for _, job := range []struct {
		id, request string
		sequence    int
	}{
		{"legacy-job-1", legacyRequest, 1},
		{"legacy-job-2", memberRequest, 2},
	} {
		if _, err := db.Exec(`INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence) VALUES(?,?,'queued',?,?,?)`, job.id, job.request, legacyCreatedAt, legacyCreatedAt, job.sequence); err != nil {
			t.Fatalf("seed pre-upgrade SQLite job %s: %v", job.id, err)
		}
	}
	if _, err := db.Exec(`INSERT INTO factory_threads(id,submitter,created_at,initial_job_id) VALUES(?,?,?,?)`, "legacy-thread", "legacy-shared", legacyCreatedAt, "legacy-job-1"); err != nil {
		t.Fatalf("seed pre-upgrade SQLite Thread: %v", err)
	}
	for position, jobID := range []string{"legacy-job-1", "legacy-job-2"} {
		if _, err := db.Exec(`INSERT INTO factory_thread_jobs(thread_id,job_id,position,created_at) VALUES(?,?,?,?)`, "legacy-thread", jobID, position+1, legacyCreatedAt); err != nil {
			t.Fatalf("seed pre-upgrade SQLite Thread membership for %s: %v", jobID, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := OpenSQLiteStore(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	var version int
	if err := store.db.QueryRow(`SELECT MAX(version) FROM factory_schema_migrations`).Scan(&version); err != nil || version != 25 {
		t.Fatalf("upgraded SQLite schema version=%d err=%v; want 25", version, err)
	}
	var retainedAudit int
	if err := store.db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit WHERE event IN ('registered', 'retired')`).Scan(&retainedAudit); err != nil || retainedAudit != 0 {
		t.Fatalf("unexpected legacy session audit rows after migration: count=%d err=%v", retainedAudit, err)
	}
	if _, replay, err := store.RegisterWorkerSession(context.Background(), "legacy-worker", "post-upgrade-boot"); err != nil || replay {
		t.Fatalf("register against migrated SQLite schema: replay=%v err=%v", replay, err)
	}
	assertUpgradedLegacyRecords(t, store)
	body := sha256.Sum256([]byte("pre-v15 SQLite webhook"))
	if _, replay, err := store.AdmitGitHubIssueWebhook(context.Background(), "post-upgrade-delivery", fmt.Sprintf("%x", body), "issue-owner", Request{Repository: "widget", Task: "migrated webhook"}, IntakeOwner{}); !errors.Is(err, ErrGitHubWebhookUnavailable) || replay {
		t.Fatalf("webhook admission against migrated SQLite schema: replay=%v err=%v, want unavailable", replay, err)
	}
}

func TestPostgresScheduleAuditMigrationLeavesLegacyHistoryEmpty(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL schedule audit migration integration")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	db := scopedPostgresDB(t, admin, dsn)
	files, err := postgresMigrationNames()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE factory_rest_schema_migrations (version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		t.Fatal(err)
	}
	for index, name := range files[:len(files)-1] {
		payload, err := postgresMigrationFiles.ReadFile("postgres_migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(payload)); err != nil {
			t.Fatalf("apply PostgreSQL migration %s: %v", name, err)
		}
		if _, err := db.Exec(`INSERT INTO factory_rest_schema_migrations(version,name) VALUES($1,$2)`, index+1, name); err != nil {
			t.Fatal(err)
		}
	}
	const createdAt = "2025-05-06T07:08:09Z"
	if _, err := db.Exec(`INSERT INTO factory_schedules(id,owner,cron,repository,task,status,revision,created_at,updated_at) VALUES('legacy-schedule','alice','* * * * *','widget','schedule task','active',3,$1,$1)`, createdAt); err != nil {
		t.Fatalf("seed pre-upgrade PostgreSQL schedule: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence,submitter) VALUES('legacy-schedule-job','{"repository":"widget","task":"old"}','queued',$1,$1,1,'alice')`, createdAt); err != nil {
		t.Fatalf("seed historical PostgreSQL schedule Job: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO factory_job_idempotency(principal_id,key,job_id,request_json) VALUES('alice',$1,'legacy-schedule-job','{"repository":"widget","task":"old"}')`, "schedule:legacy-schedule:3:2025-05-06T07:08:09Z"); err != nil {
		t.Fatalf("seed historical PostgreSQL schedule key: %v", err)
	}
	store, err := NewPostgresStore(db, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.CloseStore() })
	var auditRows int
	if err := db.QueryRow(`SELECT count(*) FROM factory_schedule_audit WHERE schedule_id='legacy-schedule'`).Scan(&auditRows); err != nil || auditRows != 0 {
		t.Fatalf("pre-upgrade PostgreSQL schedule audit rows=%d err=%v; want empty history", auditRows, err)
	}
	var runRows int
	if err := db.QueryRow(`SELECT count(*) FROM factory_schedule_runs WHERE schedule_id='legacy-schedule'`).Scan(&runRows); err != nil || runRows != 0 {
		t.Fatalf("pre-upgrade PostgreSQL schedule run provenance rows=%d err=%v; want no backfill", runRows, err)
	}
	legacyRuns, err := store.ListScheduleRuns("legacy-schedule", "alice", false, 0, 10)
	if err != nil || len(legacyRuns.Runs) != 0 {
		t.Fatalf("historical matching idempotency key produced PostgreSQL schedule runs=%+v err=%v; want no backfill", legacyRuns, err)
	}
	if _, err := store.MutateScheduleAs("legacy-schedule", "admin", true, 3, SchedulePause, nil); err != nil {
		t.Fatalf("mutate pre-upgrade PostgreSQL schedule after upgrade: %v", err)
	}
	history, err := store.ListScheduleHistory("legacy-schedule", "admin", true, 0, MaxScheduleHistoryLimit)
	if err != nil || len(history.Events) != 1 || history.Events[0].Revision != 4 || history.Events[0].Actor != "admin" || history.Events[0].Action != SchedulePauseAction {
		t.Fatalf("post-upgrade PostgreSQL schedule history=%+v err=%v; expected only the committed post-upgrade action", history, err)
	}
}

func TestPostgresWorkerSessionAndWebhookMigrationsUpgradeFromV10(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL migration upgrade integration")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	db := scopedPostgresDB(t, admin, dsn)
	files, err := postgresMigrationNames()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE factory_rest_schema_migrations (version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		t.Fatal(err)
	}
	for index, name := range files[:10] {
		payload, err := postgresMigrationFiles.ReadFile("postgres_migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(payload)); err != nil {
			t.Fatalf("apply PostgreSQL migration %s: %v", name, err)
		}
		if _, err := db.Exec(`INSERT INTO factory_rest_schema_migrations(version,name) VALUES($1,$2)`, index+1, name); err != nil {
			t.Fatal(err)
		}
	}
	var startingVersion int
	if err := db.QueryRow(`SELECT MAX(version) FROM factory_rest_schema_migrations`).Scan(&startingVersion); err != nil || startingVersion != 10 {
		t.Fatalf("starting PostgreSQL schema version=%d err=%v; want 10", startingVersion, err)
	}
	const legacyCreatedAt = "2025-05-06T07:08:09Z"
	const legacyRequest = `{"repository":"legacy-widget","task":"preserve before migration"}`
	const memberRequest = `{"repository":"legacy-widget","task":"preserve thread member"}`
	for _, job := range []struct {
		id, request string
		sequence    int64
	}{
		{"legacy-job-1", legacyRequest, 1},
		{"legacy-job-2", memberRequest, 2},
	} {
		if _, err := db.Exec(`INSERT INTO factory_jobs(id,request_json,status,created_at,updated_at,queue_sequence) VALUES($1,$2,'queued',$3,$3,$4)`, job.id, job.request, legacyCreatedAt, job.sequence); err != nil {
			t.Fatalf("seed pre-upgrade PostgreSQL job %s: %v", job.id, err)
		}
	}
	if _, err := db.Exec(`INSERT INTO factory_threads(id,submitter,created_at,initial_job_id) VALUES($1,$2,$3,$4)`, "legacy-thread", "legacy-shared", legacyCreatedAt, "legacy-job-1"); err != nil {
		t.Fatalf("seed pre-upgrade PostgreSQL Thread: %v", err)
	}
	for position, jobID := range []string{"legacy-job-1", "legacy-job-2"} {
		if _, err := db.Exec(`INSERT INTO factory_thread_jobs(thread_id,job_id,position,created_at) VALUES($1,$2,$3,$4)`, "legacy-thread", jobID, position+1, legacyCreatedAt); err != nil {
			t.Fatalf("seed pre-upgrade PostgreSQL Thread membership for %s: %v", jobID, err)
		}
	}
	if _, err := db.Exec(`SELECT setval('factory_rest_job_sequence', 100)`); err != nil {
		t.Fatalf("advance PostgreSQL queue sequence past seeded legacy Jobs: %v", err)
	}
	store, err := NewPostgresStore(db, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.CloseStore() })
	var version int
	if err := db.QueryRow(`SELECT MAX(version) FROM factory_rest_schema_migrations`).Scan(&version); err != nil || version != 22 {
		t.Fatalf("upgraded PostgreSQL schema version=%d err=%v; want 22", version, err)
	}
	var retainedAudit int
	if err := db.QueryRow(`SELECT count(*) FROM factory_worker_session_audit WHERE event IN ('registered', 'retired')`).Scan(&retainedAudit); err != nil || retainedAudit != 0 {
		t.Fatalf("unexpected legacy session audit rows after migration: count=%d err=%v", retainedAudit, err)
	}
	if _, replay, err := store.RegisterWorkerSession(context.Background(), "legacy-worker", "post-upgrade-boot"); err != nil || replay {
		t.Fatalf("register against migrated PostgreSQL schema: replay=%v err=%v", replay, err)
	}
	assertUpgradedLegacyRecords(t, store)
	body := sha256.Sum256([]byte("pre-v11 PostgreSQL webhook"))
	owner, err := store.RegisterIntakeOwner(context.Background(), "acme/issues", "Ready", IntakeOwnerWebhook)
	if err != nil {
		t.Fatal(err)
	}
	if _, replay, err := store.AdmitGitHubIssueWebhook(context.Background(), "post-upgrade-delivery", fmt.Sprintf("%x", body), "issue-owner", Request{Repository: "widget", Task: "migrated webhook"}, owner); err != nil || replay {
		t.Fatalf("webhook admission against migrated PostgreSQL schema: replay=%v err=%v", replay, err)
	}
}

func assertUpgradedLegacyRecords(t *testing.T, store interface {
	Get(string) (Snapshot, error)
	GetThread(string) (ThreadSnapshot, error)
}) {
	t.Helper()
	for _, want := range []struct {
		id, task string
	}{
		{"legacy-job-1", "preserve before migration"},
		{"legacy-job-2", "preserve thread member"},
	} {
		got, err := store.Get(want.id)
		if err != nil {
			t.Fatalf("read pre-upgrade Job %s after migration: %v", want.id, err)
		}
		if got.ID != want.id || got.Request.Repository != "legacy-widget" || got.Request.Task != want.task || got.Status != StatusQueued || got.Submitter != legacySharedPrincipalID {
			t.Errorf("pre-upgrade Job %s after migration = %+v; content, status, and legacy attribution must survive", want.id, got)
		}
	}
	thread, err := store.GetThread("legacy-thread")
	if err != nil {
		t.Fatalf("read pre-upgrade Thread after migration: %v", err)
	}
	if thread.Thread.ID != "legacy-thread" || thread.Thread.Submitter != legacySharedPrincipalID || len(thread.Thread.JobIDs) != 2 || thread.Thread.JobIDs[0] != "legacy-job-1" || thread.Thread.JobIDs[1] != "legacy-job-2" || len(thread.Jobs) != 2 {
		t.Fatalf("pre-upgrade Thread after migration = %+v; thread and ordered membership must survive", thread)
	}
	if thread.Jobs[0].Request.Task != "preserve before migration" || thread.Jobs[1].Request.Task != "preserve thread member" {
		t.Fatalf("pre-upgrade Thread member content after migration = %+v; member Jobs must remain unchanged", thread.Jobs)
	}
}

func TestPostgresWorkerSessionHeartbeatIsDurableAndFenced(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL worker heartbeat integration")
	}
	admin, err := sqlOpenPGX(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	db := scopedPostgresDB(t, admin, dsn)
	store, err := NewPostgresStore(db, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id, err := newJobID()
	if err != nil {
		t.Fatal(err)
	}
	principal := "heartbeat-worker-" + id
	session, _, err := store.RegisterWorkerSession(ctx, principal, "boot-heartbeat")
	if err != nil {
		t.Fatal(err)
	}
	nearExpiry := time.Now().UTC().Add(2 * time.Minute).Truncate(time.Microsecond)
	if _, err := db.ExecContext(ctx, `UPDATE factory_worker_sessions SET capability_expires_at=$1 WHERE id=$2`, nearExpiry, session.ID); err != nil {
		t.Fatal(err)
	}
	session.CapabilityExpiresAt = &nearExpiry
	first, err := store.HeartbeatWorkerSession(ctx, principal, session.Capability)
	if err != nil || first.IsZero() {
		t.Fatalf("first PostgreSQL heartbeat=(%v,%v)", first, err)
	}
	var renewedExpiry time.Time
	if err := db.QueryRowContext(ctx, `SELECT capability_expires_at FROM factory_worker_sessions WHERE id=$1`, session.ID).Scan(&renewedExpiry); err != nil || session.CapabilityExpiresAt == nil || !renewedExpiry.After(*session.CapabilityExpiresAt) {
		t.Fatalf("PostgreSQL heartbeat expiry=%v err=%v, want extension beyond %v", renewedExpiry, err, session.CapabilityExpiresAt)
	}
	var renewalAudits int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM factory_worker_session_audit WHERE session_id=$1 AND event='capability_renewed' AND capability_hash IS NOT NULL AND capability_expires_at IS NOT NULL`, session.ID).Scan(&renewalAudits); err != nil || renewalAudits != 1 {
		t.Fatalf("PostgreSQL renewal audit count=%d err=%v, want one", renewalAudits, err)
	}
	if _, err := store.HeartbeatWorkerSession(ctx, principal, session.Capability); err != nil {
		t.Fatal(err)
	}
	var renewalAuditsAfterRetry int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM factory_worker_session_audit WHERE session_id=$1 AND event='capability_renewed'`, session.ID).Scan(&renewalAuditsAfterRetry); err != nil || renewalAuditsAfterRetry != renewalAudits {
		t.Fatalf("same-capability retry added renewal audit: before=%d after=%d err=%v", renewalAudits, renewalAuditsAfterRetry, err)
	}
	second, err := store.HeartbeatWorkerSession(ctx, principal, session.Capability)
	if err != nil || second.Before(first) {
		t.Fatalf("second PostgreSQL heartbeat=(%v,%v), after %v", second, err, first)
	}
	const contenders = 12
	var wg sync.WaitGroup
	errCh := make(chan error, contenders)
	times := make(chan time.Time, contenders)
	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			heartbeatAt, err := store.HeartbeatWorkerSession(ctx, principal, session.Capability)
			if err != nil {
				errCh <- err
				return
			}
			times <- heartbeatAt
		}()
	}
	wg.Wait()
	close(errCh)
	close(times)
	for err := range errCh {
		t.Errorf("concurrent PostgreSQL heartbeat: %v", err)
	}
	latestReturned := second
	for heartbeatAt := range times {
		if heartbeatAt.After(latestReturned) {
			latestReturned = heartbeatAt
		}
	}
	if _, err := store.HeartbeatWorkerSession(ctx, principal+"-wrong", session.Capability); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-principal PostgreSQL heartbeat error=%v, want not found", err)
	}
	var persisted time.Time
	if err := db.QueryRowContext(ctx, `SELECT heartbeat_at FROM factory_worker_sessions WHERE id=$1`, session.ID).Scan(&persisted); err != nil || persisted.Before(latestReturned.Truncate(time.Microsecond)) {
		t.Fatalf("durable PostgreSQL heartbeat=%v err=%v, want at least %v", persisted, err, latestReturned)
	}
	if _, err := store.RetireWorkerCapability(ctx, principal, session.Capability); err != nil {
		t.Fatal(err)
	}
	if _, err := store.HeartbeatWorkerSession(ctx, principal, session.Capability); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retired PostgreSQL heartbeat error=%v, want not found", err)
	}
	if err := store.CloseStore(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresWorkerSessionIntegration(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL worker session integration")
	}
	ctx := context.Background()
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	db := scopedPostgresDB(t, admin, dsn)
	store, err := NewPostgresStore(db, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	id, err := newJobID()
	if err != nil {
		t.Fatal(err)
	}
	principal := "test-worker-" + id
	created, replay, err := store.RegisterWorkerSession(ctx, principal, "boot-one")
	if err != nil || replay {
		t.Fatalf("register=(%+v,%v,%v)", created, replay, err)
	}
	if _, err := db.ExecContext(ctx, `CREATE FUNCTION reject_worker_capability_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected worker capability audit failure'; END; $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER reject_worker_capability_rotation BEFORE INSERT ON factory_worker_session_audit FOR EACH ROW WHEN (NEW.event = 'capability_rotated') EXECUTE FUNCTION reject_worker_capability_audit()`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, principal, "boot-one"); err == nil {
		t.Fatal("PostgreSQL capability rotation succeeded despite audit-write failure")
	}
	if _, err := store.AuthenticateWorkerCapability(ctx, principal, created.Capability); err != nil {
		t.Fatalf("failed PostgreSQL rotation invalidated previous capability: %v", err)
	}
	var failedRotationAudits int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM factory_worker_session_audit WHERE session_id=$1 AND event='capability_rotated'`, created.ID).Scan(&failedRotationAudits); err != nil || failedRotationAudits != 0 {
		t.Fatalf("failed PostgreSQL rotation audit rows=%d err=%v, want 0", failedRotationAudits, err)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER reject_worker_capability_rotation ON factory_worker_session_audit`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER reject_worker_capability_issuance BEFORE INSERT ON factory_worker_session_audit FOR EACH ROW WHEN (NEW.event = 'capability_issued') EXECUTE FUNCTION reject_worker_capability_audit()`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, principal+"-failed-issue", "boot-one"); err == nil {
		t.Fatal("PostgreSQL capability issuance succeeded despite audit-write failure")
	}
	var failedSessionRows int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM factory_worker_sessions WHERE principal_id=$1`, principal+"-failed-issue").Scan(&failedSessionRows); err != nil || failedSessionRows != 0 {
		t.Fatalf("failed PostgreSQL issuance session rows=%d err=%v, want 0 after rollback", failedSessionRows, err)
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER reject_worker_capability_issuance ON factory_worker_session_audit`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DROP FUNCTION reject_worker_capability_audit()`); err != nil {
		t.Fatal(err)
	}
	again, replay, err := store.RegisterWorkerSession(ctx, principal, "boot-one")
	if err != nil || !replay || again.ID != created.ID || again.Capability == created.Capability {
		t.Fatalf("retry=(%+v,%v,%v)", again, replay, err)
	}
	if _, err := store.AuthenticateWorkerCapability(ctx, principal, created.Capability); !errors.Is(err, ErrNotFound) {
		t.Fatalf("prior PostgreSQL capability error=%v, want invalid", err)
	}
	if got, err := store.AuthenticateWorkerCapability(ctx, principal, again.Capability); err != nil || got.ID != again.ID {
		t.Fatalf("replacement PostgreSQL capability=(%+v,%v)", got, err)
	}
	var issuedDigest string
	var issuedExpiry time.Time
	issuedHash := sha256.Sum256([]byte(created.Capability))
	if err := db.QueryRowContext(ctx, `SELECT capability_hash, capability_expires_at FROM factory_worker_session_audit WHERE session_id=$1 AND event='capability_issued'`, created.ID).Scan(&issuedDigest, &issuedExpiry); err != nil {
		t.Fatal(err)
	}
	if issuedDigest != hex.EncodeToString(issuedHash[:]) || created.CapabilityExpiresAt == nil || !issuedExpiry.Equal(created.CapabilityExpiresAt.Truncate(time.Microsecond)) {
		t.Fatalf("PostgreSQL issuance audit digest/expiry=(%q,%s), want matching digest and returned expiry %v", issuedDigest, issuedExpiry, created.CapabilityExpiresAt)
	}
	rotatedHash := sha256.Sum256([]byte(again.Capability))
	var rotatedExpiry time.Time
	if err := db.QueryRowContext(ctx, `SELECT capability_expires_at FROM factory_worker_session_audit WHERE session_id=$1 AND event='capability_rotated' AND capability_hash=$2`, created.ID, hex.EncodeToString(rotatedHash[:])).Scan(&rotatedExpiry); err != nil {
		t.Fatal(err)
	}
	if again.CapabilityExpiresAt == nil || !rotatedExpiry.Equal(again.CapabilityExpiresAt.Truncate(time.Microsecond)) {
		t.Fatalf("PostgreSQL rotation audit expiry=%s, want returned expiry %v", rotatedExpiry, again.CapabilityExpiresAt)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, principal+"-wrong", "boot-one"); err != nil {
		t.Fatalf("register separate PostgreSQL principal: %v", err)
	}
	if _, err := store.AuthenticateWorkerCapability(ctx, principal+"-wrong", again.Capability); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-principal PostgreSQL capability error=%v, want invalid", err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, principal, "boot-two"); !errors.Is(err, ErrWorkerSessionConflict) {
		t.Fatalf("competing boot error=%v", err)
	}
	if _, err := store.RetireWorkerSession(ctx, principal, "boot-one", created.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, principal, "boot-one"); !errors.Is(err, ErrWorkerSessionConflict) {
		t.Fatalf("re-register retired principal/boot pair error=%v, want conflict", err)
	}
	if _, _, err := store.RegisterWorkerSession(ctx, principal, "boot-two"); err != nil {
		t.Fatalf("register different boot after explicit retirement: %v", err)
	}
}
