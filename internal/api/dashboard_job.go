package restapi

import (
	"context"
	"net/http"
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
)

type dashboardJobDetail struct {
	ID                    string                         `json:"id"`
	Submitter             string                         `json:"submitted_by"`
	Status                restjobs.Status                `json:"status"`
	CancellationRequested bool                           `json:"cancellation_requested,omitempty"`
	CreatedAt             time.Time                      `json:"created_at"`
	UpdatedAt             time.Time                      `json:"updated_at"`
	Provider              *restjobs.ProviderOutcome      `json:"provider,omitempty"`
	Verification          *restjobs.VerificationEvidence `json:"verification,omitempty"`
	Worker                *restjobs.WorkerSummary        `json:"worker,omitempty"`
	Progress              *restjobs.ProgressSummary      `json:"progress,omitempty"`
	RecoveryState         string                         `json:"recovery_state"`
}

func (h *Handler) writeDashboardJob(w http.ResponseWriter, ctx context.Context, job restjobs.Snapshot) {
	state := "unavailable"
	if reader, ok := h.manager.(restjobs.RecoveryReader); ok {
		if status, needed, err := reader.ReadRecoveryNeeded(ctx, job.ID); err == nil && status == job.Status {
			state = "not_needed"
			if needed {
				state = "needs_operator"
			}
		}
	}
	if job.Verification != nil {
		evidence := *job.Verification
		evidence.Reply, evidence.ReplyTruncated = "", false
		job.Verification = &evidence
	}
	writeJSON(w, http.StatusOK, dashboardJobDetail{
		ID: job.ID, Submitter: job.Submitter, Status: job.Status, CancellationRequested: job.CancellationRequested,
		CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt, Provider: job.Provider,
		Verification: job.Verification, Worker: job.Worker, Progress: job.Progress, RecoveryState: state,
	})
}
