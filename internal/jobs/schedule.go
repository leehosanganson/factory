package restjobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrScheduleConflict    = errors.New("schedule revision conflict")
	ErrScheduleIDConflict  = errors.New("schedule idempotency conflict")
	ErrScheduleTransition  = errors.New("invalid schedule transition")
	ErrInvalidScheduleCron = errors.New("invalid five-field UTC cron expression")
)

type ScheduleStatus string

const (
	ScheduleActive  ScheduleStatus = "active"
	SchedulePaused  ScheduleStatus = "paused"
	ScheduleRemoved ScheduleStatus = "removed"
)

type Schedule struct {
	ID                  string                      `json:"id"`
	Owner               string                      `json:"owner"`
	Cron                string                      `json:"cron"`
	Repository          string                      `json:"repository"`
	Task                string                      `json:"task"`
	Status              ScheduleStatus              `json:"status"`
	Revision            int64                       `json:"revision"`
	CreatedAt           time.Time                   `json:"created_at"`
	UpdatedAt           time.Time                   `json:"updated_at"`
	Skipped             *SkippedScheduleOccurrences `json:"skipped_occurrences,omitempty"`
	nextOccurrenceAt    time.Time
	pendingOccurrenceAt *time.Time
	skippedRevision     int64
	skippedFirstAt      *time.Time
	skippedLastAt       *time.Time
	skippedCount        int64
}

const (
	DefaultScheduleHistoryLimit = 50
	MaxScheduleHistoryLimit     = 100
)

type ScheduleStore interface {
	CreateSchedule(owner, key string, input ScheduleInput) (Schedule, bool, error)
	GetSchedule(id string) (Schedule, error)
	ListSchedules(owner string) ([]Schedule, error)
	ListAllSchedules() ([]Schedule, error)
	ListScheduleHistory(id, actor string, operator bool, afterRevision int64, limit int) (ScheduleAuditPage, error)
	MutateSchedule(id, owner string, revision int64, mutation ScheduleMutation, input *ScheduleInput) (Schedule, error)
	MutateScheduleAs(id, actor string, operator bool, revision int64, mutation ScheduleMutation, input *ScheduleInput) (Schedule, error)
}

type ScheduleAuditEvent struct {
	ScheduleID string         `json:"schedule_id"`
	Creator    string         `json:"creator"`
	Actor      string         `json:"actor"`
	Action     ScheduleAction `json:"action"`
	Revision   int64          `json:"revision"`
	At         time.Time      `json:"at"`
}

type ScheduleAuditPage struct {
	Events       []ScheduleAuditEvent
	HasMore      bool
	NextRevision int64
}

type ScheduleAction string

const (
	ScheduleCreated       ScheduleAction = "create"
	ScheduleUpdated       ScheduleAction = "update"
	SchedulePauseAction   ScheduleAction = "pause"
	ScheduleResumeAction  ScheduleAction = "resume"
	ScheduleRemovedAction ScheduleAction = "remove"
)

type ScheduleDispatcher interface {
	DispatchScheduleOccurrences(context.Context, time.Time, int) error
}

type ScheduleBatchDispatcher interface {
	DispatchDueScheduleOccurrences(context.Context, time.Time, int) error
}

type SkippedScheduleOccurrences struct {
	Revision int64     `json:"revision"`
	FirstAt  time.Time `json:"first_at"`
	LastAt   time.Time `json:"last_at"`
	Count    int64     `json:"count"`
}

func (s Schedule) NextOccurrence() time.Time { return s.nextOccurrenceAt }
func (s Schedule) PendingOccurrence() *time.Time {
	if s.pendingOccurrenceAt == nil {
		return nil
	}
	value := *s.pendingOccurrenceAt
	return &value
}
func (s Schedule) SkippedOccurrences() *SkippedScheduleOccurrences {
	if s.Skipped != nil {
		value := *s.Skipped
		return &value
	}
	if s.skippedCount == 0 || s.skippedFirstAt == nil || s.skippedLastAt == nil {
		return nil
	}
	return &SkippedScheduleOccurrences{Revision: s.skippedRevision, FirstAt: *s.skippedFirstAt, LastAt: *s.skippedLastAt, Count: s.skippedCount}
}

const maxScheduleCronLength = 128

type ScheduleInput struct{ Cron, Repository, Task string }
type ScheduleMutation string

const (
	SchedulePause  ScheduleMutation = "pause"
	ScheduleResume ScheduleMutation = "resume"
	ScheduleEdit   ScheduleMutation = "edit"
	ScheduleRemove ScheduleMutation = "remove"
)

func scheduleAuditAction(mutation ScheduleMutation) ScheduleAction {
	switch mutation {
	case SchedulePause:
		return SchedulePauseAction
	case ScheduleResume:
		return ScheduleResumeAction
	case ScheduleEdit:
		return ScheduleUpdated
	case ScheduleRemove:
		return ScheduleRemovedAction
	default:
		return ""
	}
}

func ValidateScheduleCron(expression string) error {
	if _, err := parseScheduleCron(expression); err != nil {
		return ErrInvalidScheduleCron
	}
	_, err := NextScheduleOccurrence(expression, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		return ErrInvalidScheduleCron
	}
	return nil
}

type cronField struct {
	values   map[int]bool
	wildcard bool
}
type scheduleCron struct{ minute, hour, dom, month, dow cronField }

func parseScheduleCron(expression string) (scheduleCron, error) {
	if utf8.RuneCountInString(expression) > maxScheduleCronLength {
		return scheduleCron{}, ErrInvalidInput
	}
	parts := strings.Fields(expression)
	if len(parts) != 5 {
		return scheduleCron{}, ErrInvalidInput
	}
	bounds := [][2]int{{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 7}}
	fields := make([]cronField, 5)
	for i, part := range parts {
		field, err := parseCronField(part, bounds[i][0], bounds[i][1])
		if err != nil {
			return scheduleCron{}, ErrInvalidInput
		}
		if i == 4 && field.values[7] {
			field.values[0] = true
			delete(field.values, 7)
		}
		fields[i] = field
	}
	return scheduleCron{fields[0], fields[1], fields[2], fields[3], fields[4]}, nil
}
func parseCronField(text string, min, max int) (cronField, error) {
	field := cronField{values: make(map[int]bool)}
	for _, item := range strings.Split(text, ",") {
		if item == "" {
			return cronField{}, ErrInvalidInput
		}
		base, stepText, hasStep := strings.Cut(item, "/")
		if base == "*" {
			field.wildcard = true
		}
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepText)
			if err != nil || n < 1 || n > max-min+1 {
				return cronField{}, ErrInvalidInput
			}
			step = n
		}
		lo, hi := min, max
		switch {
		case base == "*":
		case strings.Contains(base, "-"):
			a, b, ok := strings.Cut(base, "-")
			if !ok {
				return cronField{}, ErrInvalidInput
			}
			var err error
			lo, err = strconv.Atoi(a)
			if err != nil {
				return cronField{}, ErrInvalidInput
			}
			hi, err = strconv.Atoi(b)
			if err != nil || lo < min || hi > max || lo > hi {
				return cronField{}, ErrInvalidInput
			}
		default:
			value, err := strconv.Atoi(base)
			if err != nil || value < min || value > max {
				return cronField{}, ErrInvalidInput
			}
			lo, hi = value, value
			if hasStep {
				return cronField{}, ErrInvalidInput
			}
		}
		for value := lo; value <= hi; value += step {
			field.values[value] = true
		}
	}
	if len(field.values) == 0 {
		return cronField{}, ErrInvalidInput
	}
	return field, nil
}

// NextScheduleOccurrence returns the next matching minute strictly after after.
// Schedule time is always UTC; DOM and DOW use cron's OR rule when both are
// restricted. Five fields guarantee no occurrence can be closer than a minute.
func NextScheduleOccurrence(expression string, after time.Time) (time.Time, error) {
	cron, err := parseScheduleCron(expression)
	if err != nil {
		return time.Time{}, err
	}
	start := after.UTC().Truncate(time.Minute).Add(time.Minute)
	startDay := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC)
	for day := startDay; day.Before(startDay.AddDate(8, 0, 0)); day = day.AddDate(0, 0, 1) {
		if !cron.month.values[int(day.Month())] {
			continue
		}
		dom, dow := cron.dom.values[day.Day()], cron.dow.values[int(day.Weekday())]
		dayMatches := dom && dow
		if !cron.dom.wildcard && !cron.dow.wildcard {
			dayMatches = dom || dow
		} else if cron.dom.wildcard {
			dayMatches = dow
		} else {
			dayMatches = dom
		}
		if !dayMatches {
			continue
		}
		for hour := 0; hour < 24; hour++ {
			if !cron.hour.values[hour] {
				continue
			}
			for minute := 0; minute < 60; minute++ {
				if !cron.minute.values[minute] {
					continue
				}
				candidate := day.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute)
				if candidate.After(after.UTC()) {
					return candidate, nil
				}
			}
		}
	}
	return time.Time{}, fmt.Errorf("cron expression has no occurrence in eight years: %w", ErrInvalidInput)
}

func newScheduleID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	encoded := hex.EncodeToString(value[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", encoded[:8], encoded[8:12], encoded[12:16], encoded[16:20], encoded[20:]), nil
}
