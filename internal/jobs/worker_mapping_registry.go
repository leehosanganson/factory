package restjobs

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"
)

type WorkerMappingState struct {
	Generation uint64
	Mappings   map[string]string
}

// lookupWorkerMapping returns the current active principal for a certificate fingerprint.
func lookupWorkerMapping(ctx context.Context, db *sql.DB, fingerprint, placeholder string) (string, bool, error) {
	if ctx == nil || len(fingerprint) != 64 || !validWorkerFingerprint(fingerprint) {
		return "", false, ErrInvalidInput
	}
	query := `SELECT principal_id FROM factory_worker_mappings WHERE fingerprint=? AND revoked=?`
	if placeholder != "?" {
		query = `SELECT principal_id FROM factory_worker_mappings WHERE fingerprint=$1 AND revoked=$2`
	}
	var principal sql.NullString
	if err := db.QueryRowContext(ctx, query, fingerprint, false).Scan(&principal); errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	} else if err != nil {
		return "", false, errors.New("read worker mapping")
	}
	if !principal.Valid || !validPostgresOwnerID(principal.String) || principal.String == "legacy-shared" {
		return "", false, errors.New("decode worker mapping")
	}
	return principal.String, true, nil
}

func validateWorkerMappingInput(mappings map[string]string) error {
	for fingerprint, principal := range mappings {
		if len(fingerprint) != 64 || !validWorkerFingerprint(fingerprint) || !validPostgresOwnerID(principal) || principal == "legacy-shared" {
			return ErrInvalidInput
		}
	}
	return nil
}

func validWorkerFingerprint(value string) bool {
	for _, char := range value {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func reconcileSQLiteWorkerMappings(ctx context.Context, db *sql.DB, candidate map[string]string) (WorkerMappingState, error) {
	if ctx == nil || validateWorkerMappingInput(candidate) != nil {
		return WorkerMappingState{}, ErrInvalidInput
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return WorkerMappingState{}, errors.New("begin SQLite worker mapping reconciliation")
	}
	defer tx.Rollback()
	state, err := reconcileWorkerMappingsTx(ctx, tx, "?", candidate)
	if err != nil {
		return WorkerMappingState{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkerMappingState{}, errors.New("commit SQLite worker mapping reconciliation")
	}
	return state, nil
}

func reconcilePostgresWorkerMappings(ctx context.Context, db *sql.DB, candidate map[string]string) (WorkerMappingState, error) {
	if ctx == nil || validateWorkerMappingInput(candidate) != nil {
		return WorkerMappingState{}, ErrInvalidInput
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return WorkerMappingState{}, errors.New("begin PostgreSQL worker mapping reconciliation")
	}
	defer tx.Rollback()
	var lockedGeneration int64
	if err := tx.QueryRowContext(ctx, `SELECT generation FROM factory_worker_mapping_state WHERE singleton=1 FOR UPDATE`).Scan(&lockedGeneration); err != nil {
		return WorkerMappingState{}, errors.New("lock PostgreSQL worker mapping registry")
	}
	state, err := reconcileWorkerMappingsTx(ctx, tx, "$", candidate)
	if err != nil {
		return WorkerMappingState{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkerMappingState{}, errors.New("commit PostgreSQL worker mapping reconciliation")
	}
	return state, nil
}

type workerMappingTx interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func reconcileWorkerMappingsTx(ctx context.Context, tx workerMappingTx, placeholder string, candidate map[string]string) (WorkerMappingState, error) {
	selectState := `SELECT generation FROM factory_worker_mapping_state WHERE singleton=1`
	selectRows := `SELECT fingerprint,principal_id,revoked FROM factory_worker_mappings ORDER BY fingerprint`
	if placeholder != "?" {
		selectState += ` FOR UPDATE`
		selectRows += ` FOR UPDATE`
	}
	var generation int64
	if err := tx.QueryRowContext(ctx, selectState).Scan(&generation); err != nil || generation < 0 {
		return WorkerMappingState{}, errors.New("read worker mapping registry state")
	}
	rows, err := tx.QueryContext(ctx, selectRows)
	if err != nil {
		return WorkerMappingState{}, errors.New("read worker mappings")
	}
	stored := make(map[string]storedWorkerMapping)
	for rows.Next() {
		var item storedWorkerMapping
		var principal sql.NullString
		if err := rows.Scan(&item.fingerprint, &principal, &item.revoked); err != nil {
			rows.Close()
			return WorkerMappingState{}, errors.New("decode worker mappings")
		}
		if principal.Valid {
			item.principal = principal.String
		}
		stored[item.fingerprint] = item
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return WorkerMappingState{}, errors.New("read worker mappings")
	}
	rows.Close()
	for fingerprint := range candidate {
		if old, exists := stored[fingerprint]; exists && old.revoked {
			return WorkerMappingState{}, errors.New("worker mapping fingerprint is permanently revoked")
		}
	}
	changed := false
	fingerprints := make([]string, 0, len(candidate)+len(stored))
	seen := make(map[string]struct{}, len(candidate)+len(stored))
	for fingerprint := range candidate {
		fingerprints = append(fingerprints, fingerprint)
		seen[fingerprint] = struct{}{}
	}
	for fingerprint, old := range stored {
		if !old.revoked {
			if _, exists := seen[fingerprint]; !exists {
				fingerprints = append(fingerprints, fingerprint)
			}
		}
	}
	sort.Strings(fingerprints)
	if generation == int64(^uint64(0)>>1) {
		return WorkerMappingState{}, errors.New("worker mapping generation exhausted")
	}
	nextGeneration := generation + 1
	for _, fingerprint := range fingerprints {
		principal, present := candidate[fingerprint]
		old, exists := stored[fingerprint]
		event := ""
		switch {
		case !exists && present:
			event = "accepted"
		case exists && !old.revoked && !present:
			event = "revoked"
		case exists && !old.revoked && present && old.principal != principal:
			event = "changed"
		default:
			continue
		}
		changed = true
		var storedPrincipal any
		if present {
			storedPrincipal = principal
		}
		revoked := false
		if event == "revoked" {
			storedPrincipal, revoked = nil, true
		}
		if event == "accepted" {
			query := `INSERT INTO factory_worker_mappings(fingerprint,principal_id,revoked) VALUES(?,?,?)`
			if placeholder != "?" {
				query = `INSERT INTO factory_worker_mappings(fingerprint,principal_id,revoked) VALUES($1,$2,$3)`
			}
			if _, err := tx.ExecContext(ctx, query, fingerprint, storedPrincipal, revoked); err != nil {
				return WorkerMappingState{}, errors.New("persist worker mapping")
			}
		} else {
			query := `UPDATE factory_worker_mappings SET principal_id=?,revoked=? WHERE fingerprint=?`
			if placeholder != "?" {
				query = `UPDATE factory_worker_mappings SET principal_id=$1,revoked=$2 WHERE fingerprint=$3`
			}
			if _, err := tx.ExecContext(ctx, query, storedPrincipal, revoked, fingerprint); err != nil {
				return WorkerMappingState{}, errors.New("update worker mapping")
			}
		}
		query := `INSERT INTO factory_worker_mapping_audit(fingerprint,principal_id,event,generation,created_at) VALUES(?,?,?,?,?)`
		args := []any{fingerprint, storedPrincipal, event, nextGeneration, time.Now().UTC().Format(time.RFC3339Nano)}
		if placeholder != "?" {
			query = `INSERT INTO factory_worker_mapping_audit(fingerprint,principal_id,event,generation) VALUES($1,$2,$3,$4)`
			args = []any{fingerprint, storedPrincipal, event, nextGeneration}
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return WorkerMappingState{}, errors.New("audit worker mapping change")
		}
	}
	if changed {
		query := `UPDATE factory_worker_mapping_state SET generation=? WHERE singleton=1`
		if placeholder != "?" {
			query = `UPDATE factory_worker_mapping_state SET generation=$1 WHERE singleton=1`
		}
		if _, err := tx.ExecContext(ctx, query, nextGeneration); err != nil {
			return WorkerMappingState{}, errors.New("advance worker mapping generation")
		}
		generation = nextGeneration
	}
	accepted := make(map[string]string, len(candidate))
	for fingerprint, principal := range candidate {
		accepted[fingerprint] = principal
	}
	return WorkerMappingState{Generation: uint64(generation), Mappings: accepted}, nil
}

type storedWorkerMapping struct {
	fingerprint, principal string
	revoked                bool
}

func (s *SQLiteStore) ReconcileWorkerMappings(ctx context.Context, mappings map[string]string) (WorkerMappingState, error) {
	return reconcileSQLiteWorkerMappings(ctx, s.db, mappings)
}

// LookupWorkerMapping returns the current active principal for a certificate fingerprint.
func (s *SQLiteStore) LookupWorkerMapping(ctx context.Context, fingerprint string) (string, bool, error) {
	return lookupWorkerMapping(ctx, s.db, fingerprint, "?")
}
func (s *PostgresStore) ReconcileWorkerMappings(ctx context.Context, mappings map[string]string) (WorkerMappingState, error) {
	return reconcilePostgresWorkerMappings(ctx, s.db, mappings)
}

// LookupWorkerMapping returns the current active principal for a certificate fingerprint.
func (s *PostgresStore) LookupWorkerMapping(ctx context.Context, fingerprint string) (string, bool, error) {
	return lookupWorkerMapping(ctx, s.db, fingerprint, "$")
}
