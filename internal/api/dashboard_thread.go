package restapi

import (
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
)

type dashboardThreadResponse struct {
	ID        string                `json:"id"`
	Status    restjobs.ThreadStatus `json:"status"`
	CreatedAt time.Time             `json:"created_at"`
	UpdatedAt time.Time             `json:"updated_at"`
	Completed *time.Time            `json:"completed_at,omitempty"`
}

type dashboardThreadPage struct {
	Threads          []dashboardThreadResponse `json:"threads"`
	SnapshotSequence uint64                    `json:"snapshot_sequence"`
	NextSequence     uint64                    `json:"next_sequence,omitempty"`
	HasMore          bool                      `json:"has_more"`
}

type submitterThreadPage struct {
	Threads    []restjobs.ThreadSummary `json:"threads"`
	HasMore    bool                     `json:"has_more"`
	NextCursor string                   `json:"next_cursor,omitempty"`
}

type dashboardThreadJobResponse struct {
	ID        string          `json:"id"`
	Status    restjobs.Status `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type dashboardThreadCreateResponse struct {
	Thread   dashboardThreadResponse    `json:"thread"`
	Job      dashboardThreadJobResponse `json:"job"`
	Replayed bool                       `json:"replayed"`
}

type dashboardThreadDetailResponse struct {
	Thread dashboardThreadResponse      `json:"thread"`
	Jobs   []dashboardThreadJobResponse `json:"jobs"`
}

type dashboardThreadAppendResponse struct {
	Thread   dashboardThreadResponse    `json:"thread"`
	Job      dashboardThreadJobResponse `json:"job"`
	Replayed bool                       `json:"replayed"`
}

func projectDashboardThreadSummaries(threads []restjobs.ThreadSummary) []dashboardThreadResponse {
	result := make([]dashboardThreadResponse, 0, len(threads))
	for _, thread := range threads {
		result = append(result, dashboardThreadResponse{ID: thread.ID, Status: thread.Status, CreatedAt: thread.CreatedAt, UpdatedAt: thread.UpdatedAt, Completed: thread.CompletedAt})
	}
	return result
}

func projectDashboardThread(thread restjobs.Thread) dashboardThreadResponse {
	return dashboardThreadResponse{
		ID: thread.ID, Status: thread.Status, CreatedAt: thread.CreatedAt, UpdatedAt: thread.UpdatedAt,
		Completed: thread.CompletedAt,
	}
}

func projectDashboardThreadJob(job restjobs.Snapshot) dashboardThreadJobResponse {
	return dashboardThreadJobResponse{ID: job.ID, Status: job.Status, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt}
}

func projectDashboardThreadJobs(jobs []restjobs.Snapshot) []dashboardThreadJobResponse {
	result := make([]dashboardThreadJobResponse, 0, len(jobs))
	for _, job := range jobs {
		result = append(result, projectDashboardThreadJob(job))
	}
	return result
}
