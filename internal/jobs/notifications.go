package restjobs

import (
	"context"
	"sort"
	"time"
)

const (
	DefaultNotificationListLimit = 50
	MaxNotificationListLimit     = 100
)

// ThreadNotification is a safe inbox entry created when a Thread first becomes
// Done in one completion cycle. It contains no job request or task payload.
type ThreadNotification struct {
	ID        int64        `json:"id"`
	ThreadID  string       `json:"thread_id"`
	Status    ThreadStatus `json:"status"`
	CreatedAt time.Time    `json:"created_at"`
}

// NotificationPage is ordered newest-first. NextCursor is supplied as the
// before query parameter for the next page when HasMore is true.
type NotificationPage struct {
	Notifications []ThreadNotification `json:"notifications"`
	HasMore       bool                 `json:"has_more"`
	NextCursor    int64                `json:"next_cursor,omitempty"`
}

// NotificationStore exposes only the authenticated principal's bounded inbox.
type NotificationStore interface {
	ListNotificationsForPrincipal(ctx context.Context, principalID string, before int64, limit int) (NotificationPage, error)
}

type memoryNotification struct {
	owner string
	ThreadNotification
}

func validateNotificationPage(ctx context.Context, before int64, limit int) error {
	if ctx == nil || before < 0 || limit < 1 || limit > MaxNotificationListLimit {
		return ErrInvalidInput
	}
	return ctx.Err()
}

func (m *LocalManager) ListNotificationsForPrincipal(ctx context.Context, principalID string, before int64, limit int) (NotificationPage, error) {
	if ctx == nil {
		return NotificationPage{}, ErrInvalidInput
	}
	principalID, err := normalizePrincipalID(principalID)
	if err != nil {
		return NotificationPage{}, ErrInvalidInput
	}
	if err := validateNotificationPage(ctx, before, limit); err != nil {
		return NotificationPage{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return NotificationPage{}, err
	}
	items := make([]memoryNotification, 0, limit+1)
	for _, item := range m.notifications {
		if item.owner != principalID || (before != 0 && item.ID >= before) {
			continue
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID > items[j].ID })
	page := NotificationPage{Notifications: make([]ThreadNotification, 0, limit), HasMore: len(items) > limit}
	if page.HasMore {
		items = items[:limit]
	}
	for _, item := range items {
		page.Notifications = append(page.Notifications, item.ThreadNotification)
	}
	if page.HasMore && len(items) > 0 {
		page.NextCursor = items[len(items)-1].ID
	}
	return page, nil
}

func (m *LocalManager) addThreadNotificationLocked(thread *threadRecord, now time.Time) {
	if thread == nil || thread.completedAt != nil {
		return
	}
	thread.doneCycle++
	m.nextNotificationID++
	m.notifications = append(m.notifications, memoryNotification{
		owner: thread.submitter,
		ThreadNotification: ThreadNotification{
			ID: m.nextNotificationID, ThreadID: thread.id, Status: ThreadStatusDone, CreatedAt: now,
		},
	})
	if len(m.notifications) > m.config.MaxRecords {
		copy(m.notifications, m.notifications[len(m.notifications)-m.config.MaxRecords:])
		m.notifications = m.notifications[:m.config.MaxRecords]
	}
}

func (m *LocalManager) finishThreadTransitionLocked(entry *job, status Status, now time.Time) {
	thread := m.threads[entry.threadID]
	var prior ThreadStatus
	if thread != nil {
		prior = m.threadSnapshotLocked(thread).Thread.Status
	}
	entry.snapshot.Status = status
	entry.snapshot.CancellationRequested = false
	entry.snapshot.UpdatedAt = now
	if thread != nil && thread.completedAt == nil && prior != ThreadStatusDone && m.threadSnapshotLocked(thread).Thread.Status == ThreadStatusDone {
		m.addThreadNotificationLocked(thread, now)
	}
}
