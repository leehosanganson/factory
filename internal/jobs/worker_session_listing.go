package restjobs

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"
)

const (
	DefaultWorkerSessionListLimit = 50
	MaxWorkerSessionListLimit     = 100
)

// WorkerSessionSummary is the bounded operator projection. It intentionally
// omits worker-supplied boot IDs and session capability data.
type WorkerSessionSummary struct {
	ID          string     `json:"id"`
	PrincipalID string     `json:"principal_id"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	RetiredAt   *time.Time `json:"retired_at"`
	HeartbeatAt *time.Time `json:"heartbeat_at"`
}

type WorkerSessionPage struct {
	Sessions      []WorkerSessionSummary `json:"sessions"`
	HasMore       bool                   `json:"has_more"`
	NextCreatedAt *time.Time             `json:"next_created_at,omitempty"`
	NextID        string                 `json:"next_id,omitempty"`
}

// WorkerSessionListingStore provides a bounded, read-only control-plane view.
type WorkerSessionListingStore interface {
	ListWorkerSessions(context.Context, *time.Time, string, int) (WorkerSessionPage, error)
}

func validateWorkerSessionPage(ctx context.Context, before *time.Time, beforeID string, limit int) error {
	maxCursorTimestamp := time.Unix(0, int64(^uint64(0)>>1))
	if ctx == nil || limit < 1 || limit > MaxWorkerSessionListLimit || (before == nil) != (beforeID == "") || before != nil && (!validPostgresOwnerID(beforeID) || before.IsZero() || before.After(maxCursorTimestamp)) {
		return ErrInvalidInput
	}
	return ctx.Err()
}

func (s *SQLiteStore) ListWorkerSessions(ctx context.Context, before *time.Time, beforeID string, limit int) (WorkerSessionPage, error) {
	if err := validateWorkerSessionPage(ctx, before, beforeID, limit); err != nil {
		return WorkerSessionPage{}, err
	}
	if err := s.checkOpen(); err != nil {
		return WorkerSessionPage{}, err
	}
	const normalizedCreatedAt = `substr(created_at,1,19)||'.'||substr(rtrim((CASE WHEN substr(created_at,20,1)='.' THEN substr(created_at,21,9) ELSE '' END),'Z')||'000000000',1,9)||'Z'`
	query := `SELECT id,principal_id,status,created_at,retired_at,heartbeat_at FROM factory_worker_sessions`
	args := []any{}
	if before != nil {
		query += ` WHERE (` + normalizedCreatedAt + ` < ? OR (` + normalizedCreatedAt + ` = ? AND id < ?))`
		stamp := before.UTC().Format("2006-01-02T15:04:05.000000000Z")
		args = append(args, stamp, stamp, beforeID)
	}
	query += ` ORDER BY ` + normalizedCreatedAt + ` DESC,id DESC LIMIT ?`
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return WorkerSessionPage{}, errors.New("read SQLite worker session listing")
	}
	defer rows.Close()
	page := WorkerSessionPage{Sessions: make([]WorkerSessionSummary, 0, limit)}
	for rows.Next() {
		var item WorkerSessionSummary
		var created string
		var retired, heartbeat sql.NullString
		if err := rows.Scan(&item.ID, &item.PrincipalID, &item.Status, &created, &retired, &heartbeat); err != nil {
			return WorkerSessionPage{}, errors.New("decode SQLite worker session listing")
		}
		item.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return WorkerSessionPage{}, errors.New("decode SQLite worker session created timestamp")
		}
		if retired.Valid {
			value, parseErr := time.Parse(time.RFC3339Nano, retired.String)
			if parseErr != nil {
				return WorkerSessionPage{}, errors.New("decode SQLite worker session retired timestamp")
			}
			item.RetiredAt = &value
		}
		if heartbeat.Valid {
			value, parseErr := time.Parse(time.RFC3339Nano, heartbeat.String)
			if parseErr != nil {
				return WorkerSessionPage{}, errors.New("decode SQLite worker session heartbeat timestamp")
			}
			item.HeartbeatAt = &value
		}
		if len(page.Sessions) == limit {
			page.HasMore = true
			break
		}
		page.Sessions = append(page.Sessions, item)
	}
	if err := rows.Err(); err != nil {
		return WorkerSessionPage{}, errors.New("read SQLite worker session listing")
	}
	if page.HasMore {
		last := page.Sessions[len(page.Sessions)-1]
		created := last.CreatedAt
		page.NextCreatedAt, page.NextID = &created, last.ID
	}
	return page, nil
}

func (s *PostgresStore) ListWorkerSessions(ctx context.Context, before *time.Time, beforeID string, limit int) (WorkerSessionPage, error) {
	if err := validateWorkerSessionPage(ctx, before, beforeID, limit); err != nil {
		return WorkerSessionPage{}, err
	}
	if err := s.checkOpen(); err != nil {
		return WorkerSessionPage{}, err
	}
	query := `SELECT id,principal_id,status,created_at,retired_at,heartbeat_at FROM factory_worker_sessions`
	args := []any{}
	if before != nil {
		query += ` WHERE (created_at < $1 OR (created_at = $1 AND id < $2))`
		args = append(args, before.UTC(), beforeID)
	}
	query += ` ORDER BY created_at DESC,id DESC LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return WorkerSessionPage{}, errors.New("read PostgreSQL worker session listing")
	}
	defer rows.Close()
	page := WorkerSessionPage{Sessions: make([]WorkerSessionSummary, 0, limit)}
	for rows.Next() {
		var item WorkerSessionSummary
		var retired, heartbeat sql.NullTime
		if err := rows.Scan(&item.ID, &item.PrincipalID, &item.Status, &item.CreatedAt, &retired, &heartbeat); err != nil {
			return WorkerSessionPage{}, errors.New("decode PostgreSQL worker session listing")
		}
		if retired.Valid {
			value := retired.Time
			item.RetiredAt = &value
		}
		if heartbeat.Valid {
			value := heartbeat.Time
			item.HeartbeatAt = &value
		}
		if len(page.Sessions) == limit {
			page.HasMore = true
			break
		}
		page.Sessions = append(page.Sessions, item)
	}
	if err := rows.Err(); err != nil {
		return WorkerSessionPage{}, errors.New("read PostgreSQL worker session listing")
	}
	if page.HasMore {
		last := page.Sessions[len(page.Sessions)-1]
		created := last.CreatedAt
		page.NextCreatedAt, page.NextID = &created, last.ID
	}
	return page, nil
}
