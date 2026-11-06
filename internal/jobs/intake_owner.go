package restjobs

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// IntakeOwnerKind identifies the one system-controlled adapter for a source/label pair.
type IntakeOwnerKind string

const (
	IntakeOwnerWebhook IntakeOwnerKind = "webhook"
	IntakeOwnerPoller  IntakeOwnerKind = "poller"
)

// IntakeOwner is the current, sticky owner of one exact GitHub source/label pair.
type IntakeOwner struct {
	SourceRepository string          `json:"source_repository"`
	TriggerLabel     string          `json:"trigger_label"`
	Kind             IntakeOwnerKind `json:"kind"`
	Generation       int64           `json:"generation"`
}

// IntakeOwnerAudit records registration or transfer without mutating past events.
type IntakeOwnerAudit struct {
	IntakeOwner
	PreviousKind IntakeOwnerKind `json:"previous_kind,omitempty"`
	Operation    string          `json:"operation"`
	Attestation  string          `json:"attestation,omitempty"`
}

func validateIntakeOwner(source, label string, kind IntakeOwnerKind, generation int64) error {
	parts := strings.Split(source, "/")
	if len(parts) != 2 || !githubIssueOwnerPattern.MatchString(parts[0]) || !githubIssueRepoPattern.MatchString(parts[1]) || parts[1] == "." || parts[1] == ".." || label == "" || len(label) > 100 || strings.TrimSpace(label) != label || strings.IndexFunc(label, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 || generation < 1 || (kind != IntakeOwnerWebhook && kind != IntakeOwnerPoller) {
		return ErrInvalidInput
	}
	return nil
}

// RegisterIntakeOwner creates an owner only when the pair has never been registered.
// Repeating registration by the same owner is an idempotent read, not a takeover.
func (s *PostgresStore) RegisterIntakeOwner(ctx context.Context, source, label string, kind IntakeOwnerKind) (IntakeOwner, error) {
	if ctx == nil || s == nil || s.db == nil || validateIntakeOwner(source, label, kind, 1) != nil {
		return IntakeOwner{}, ErrInvalidInput
	}
	return s.changeIntakeOwner(ctx, source, label, kind, false, "")
}

// TransferIntakeOwner increments the generation after an operator attests that
// the prior adapter is stopped and its source-side effects are reconciled.
func (s *PostgresStore) TransferIntakeOwner(ctx context.Context, source, label string, kind IntakeOwnerKind, attestation string) (IntakeOwner, error) {
	if ctx == nil || s == nil || s.db == nil || validateIntakeOwner(source, label, kind, 1) != nil || strings.TrimSpace(attestation) == "" || len(attestation) > 1024 {
		return IntakeOwner{}, ErrInvalidInput
	}
	return s.changeIntakeOwner(ctx, source, label, kind, true, attestation)
}

func (s *PostgresStore) changeIntakeOwner(ctx context.Context, source, label string, kind IntakeOwnerKind, transfer bool, attestation string) (IntakeOwner, error) {
	if s == nil || s.db == nil {
		return IntakeOwner{}, errors.New("PostgreSQL intake-owner registry is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return IntakeOwner{}, errors.New("PostgreSQL intake-owner registry is unavailable")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1 || chr(31) || $2, 0))`, source, label); err != nil {
		return IntakeOwner{}, errors.New("lock PostgreSQL intake-owner pair")
	}
	var current IntakeOwner
	err = tx.QueryRowContext(ctx, `SELECT source_repository,trigger_label,owner_kind,generation FROM factory_intake_owners WHERE source_repository=$1 AND trigger_label=$2 FOR UPDATE`, source, label).Scan(&current.SourceRepository, &current.TriggerLabel, &current.Kind, &current.Generation)
	if errors.Is(err, sql.ErrNoRows) {
		if transfer {
			return IntakeOwner{}, errors.New("intake-owner pair is not registered; register it before transfer")
		}
		current = IntakeOwner{SourceRepository: source, TriggerLabel: label, Kind: kind, Generation: 1}
		if _, err := tx.ExecContext(ctx, `INSERT INTO factory_intake_owners(source_repository,trigger_label,owner_kind,generation) VALUES($1,$2,$3,1)`, source, label, kind); err != nil {
			return IntakeOwner{}, errors.New("register PostgreSQL intake owner")
		}
		if err := insertIntakeOwnerAudit(ctx, tx, IntakeOwnerAudit{IntakeOwner: current, Operation: "registered"}); err != nil {
			return IntakeOwner{}, err
		}
	} else if err != nil {
		return IntakeOwner{}, errors.New("read PostgreSQL intake owner")
	} else if !transfer {
		if current.Kind != kind {
			return IntakeOwner{}, errors.New("intake-owner pair is already assigned to another adapter")
		}
	} else {
		if current.Kind == kind {
			return IntakeOwner{}, errors.New("intake-owner transfer must change adapter kind")
		}
		previous := current.Kind
		if current.Generation == int64(^uint64(0)>>1) {
			return IntakeOwner{}, errors.New("intake-owner generation is exhausted")
		}
		current.Kind = kind
		current.Generation++
		if _, err := tx.ExecContext(ctx, `UPDATE factory_intake_owners SET owner_kind=$3,generation=$4,updated_at=clock_timestamp() WHERE source_repository=$1 AND trigger_label=$2`, source, label, kind, current.Generation); err != nil {
			return IntakeOwner{}, errors.New("transfer PostgreSQL intake owner")
		}
		if err := insertIntakeOwnerAudit(ctx, tx, IntakeOwnerAudit{IntakeOwner: current, PreviousKind: previous, Operation: "transferred", Attestation: attestation}); err != nil {
			return IntakeOwner{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return IntakeOwner{}, errors.New("commit PostgreSQL intake-owner change")
	}
	return current, nil
}

func insertIntakeOwnerAudit(ctx context.Context, tx *sql.Tx, audit IntakeOwnerAudit) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO factory_intake_owner_audit(source_repository,trigger_label,owner_kind,generation,previous_kind,operation,attestation) VALUES($1,$2,$3,$4,$5,$6,$7)`, audit.SourceRepository, audit.TriggerLabel, audit.Kind, audit.Generation, audit.PreviousKind, audit.Operation, audit.Attestation); err != nil {
		return errors.New("audit PostgreSQL intake-owner change")
	}
	return nil
}

// ListIntakeOwnerAudit reads the append-only change history for an exact pair.
func (s *PostgresStore) ListIntakeOwnerAudit(ctx context.Context, source, label string) ([]IntakeOwnerAudit, error) {
	if ctx == nil || s == nil || s.db == nil || validateIntakeOwner(source, label, IntakeOwnerWebhook, 1) != nil {
		return nil, ErrInvalidInput
	}
	rows, err := s.db.QueryContext(ctx, `SELECT source_repository,trigger_label,owner_kind,generation,previous_kind,operation,attestation FROM factory_intake_owner_audit WHERE source_repository=$1 AND trigger_label=$2 ORDER BY generation`, source, label)
	if err != nil {
		return nil, errors.New("read PostgreSQL intake-owner audit")
	}
	defer rows.Close()
	var audit []IntakeOwnerAudit
	for rows.Next() {
		var item IntakeOwnerAudit
		if err := rows.Scan(&item.SourceRepository, &item.TriggerLabel, &item.Kind, &item.Generation, &item.PreviousKind, &item.Operation, &item.Attestation); err != nil {
			return nil, errors.New("read PostgreSQL intake-owner audit")
		}
		audit = append(audit, item)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("read PostgreSQL intake-owner audit")
	}
	return audit, nil
}

// GetIntakeOwner reads the current owner of an exact source/label pair.
func (s *PostgresStore) GetIntakeOwner(ctx context.Context, source, label string) (IntakeOwner, error) {
	if ctx == nil || s == nil || s.db == nil || validateIntakeOwner(source, label, IntakeOwnerWebhook, 1) != nil {
		return IntakeOwner{}, ErrInvalidInput
	}
	var owner IntakeOwner
	err := s.db.QueryRowContext(ctx, `SELECT source_repository,trigger_label,owner_kind,generation FROM factory_intake_owners WHERE source_repository=$1 AND trigger_label=$2`, source, label).Scan(&owner.SourceRepository, &owner.TriggerLabel, &owner.Kind, &owner.Generation)
	if errors.Is(err, sql.ErrNoRows) {
		return IntakeOwner{}, errors.New("intake-owner pair is not registered")
	}
	if err != nil {
		return IntakeOwner{}, errors.New("read PostgreSQL intake owner")
	}
	return owner, nil
}

func (s *PostgresStore) checkIntakeOwnerTx(ctx context.Context, tx *sql.Tx, source, label string, kind IntakeOwnerKind, generation int64) error {
	if tx == nil {
		return ErrInvalidInput
	}
	if validateIntakeOwner(source, label, kind, generation) != nil {
		return ErrInvalidInput
	}
	var currentKind IntakeOwnerKind
	var currentGeneration int64
	err := tx.QueryRowContext(ctx, `SELECT owner_kind,generation FROM factory_intake_owners WHERE source_repository=$1 AND trigger_label=$2 FOR UPDATE`, source, label).Scan(&currentKind, &currentGeneration)
	if err != nil || currentKind != kind || currentGeneration != generation {
		return errors.New("intake-owner registration is missing, mismatched, or stale")
	}
	return nil
}
