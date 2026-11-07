package factory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

const jobLogPollInterval = 200 * time.Millisecond

func followLog(ctx context.Context, path string, terminal func() (bool, error), out io.Writer) error {
	var offset int64
	ticker := time.NewTicker(jobLogPollInterval)
	defer ticker.Stop()
	for {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return err
		}
		if info.Size() < offset {
			offset = 0
		}
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			_ = file.Close()
			return err
		}
		written, copyErr := io.Copy(out, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		offset += written
		finished, err := terminal()
		if err != nil {
			return err
		}
		if finished {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			info, err := file.Stat()
			if err != nil {
				_ = file.Close()
				return err
			}
			if info.Size() < offset {
				offset = 0
			}
			if _, err := file.Seek(offset, io.SeekStart); err != nil {
				_ = file.Close()
				return err
			}
			written, copyErr := io.Copy(out, file)
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			offset += written
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func monitorRecoverableFailureStopped(store *JobStore, id string) (bool, error) {
	worker, err := store.ReadWorker(id)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return !processAlive(worker.PID), nil
}

func monitorStoppedError(id string) error {
	return fmt.Errorf("job %s is resumable but its worker exited with status recoverable_failure; use factory monitor reset %s to resume monitoring", id, id)
}

// AttachJob follows a worker transcript until it reaches a terminal lifecycle state.
// Canceling ctx detaches this observer without requesting worker cancellation.
func AttachJob(ctx context.Context, store *JobStore, id string, out io.Writer) error {
	job, err := store.GetJob(id)
	if err != nil {
		return err
	}
	path, err := store.JobLogPath(id)
	if job.Type == monitorJobType {
		path, err = store.SessionLogPath(id, monitorSessionID)
	}
	if err != nil {
		return err
	}
	var status string
	err = followLog(ctx, path, func() (bool, error) {
		job, err := store.reconcileJob(id)
		if err != nil {
			return false, err
		}
		status = job.Status
		if status == "recoverable_failure" {
			return monitorRecoverableFailureStopped(store, id)
		}
		return isTerminalStatus(status), nil
	}, out)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	switch status {
	case "complete", "closed":
		return nil
	case "failed":
		return fmt.Errorf("job %s failed; inspect with factory job logs %s", id, id)
	case "recoverable_failure":
		return monitorStoppedError(id)
	case "interrupted", "stopped", "cancelled":
		return fmt.Errorf("job %s ended with status %s", id, status)
	default:
		return fmt.Errorf("job %s ended with unexpected status %q", id, status)
	}
}

// AttachImplementationJob follows an implementation job and prints its persisted
// terminal outcome after the transcript. Canceling the observer still only
// detaches it, and terminal job errors retain AttachJob's existing behavior.
func AttachImplementationJob(ctx context.Context, store *JobStore, id string, out io.Writer) error {
	attachErr := AttachJob(ctx, store, id, out)
	if ctx.Err() != nil {
		return attachErr
	}
	job, err := store.reconcileJob(id)
	if err != nil {
		return errors.Join(attachErr, err)
	}
	if job.Type != implementationJobType || !isTerminalStatus(job.Status) {
		return attachErr
	}
	if err := writeImplementationJobSummary(out, job); err != nil {
		return errors.Join(attachErr, err)
	}
	return attachErr
}

func writeImplementationJobSummary(out io.Writer, job JobRecord) error {
	if _, err := fmt.Fprintf(out, "\nImplementation job summary\nWorkflow status: %s\n", job.Status); err != nil {
		return err
	}
	publication := job.PublicationStatus
	if !containsJobListValue("published, unpublished, no-op", publication) {
		publication = "not recorded"
	}
	if _, err := fmt.Fprintf(out, "Publication status: %s\n", publication); err != nil {
		return err
	}

	var next string
	switch {
	case job.Status == "interrupted":
		next = fmt.Sprintf("Factory will not replay this job. Inspect `factory job get %s --details` and `factory job logs %s --session workflow`; review any retained worktree and branch shown in the details.", job.ID, job.ID)
	case job.Status == "failed" || job.Status == "stopped" || job.Status == "cancelled":
		next = fmt.Sprintf("Inspect `factory job get %s --details` and `factory job logs %s --session workflow`.", job.ID, job.ID)
	case job.PublicationStatus == "published":
		next = fmt.Sprintf("Inspect the recorded publication with `factory job get %s --details`.", job.ID)
	case job.PublicationStatus == "unpublished":
		next = fmt.Sprintf("Publication is incomplete. Inspect `factory job get %s --details` and `factory job logs %s --session workflow`; review the retained worktree and branch shown in the details before recovery.", job.ID, job.ID)
	case job.PublicationStatus == "no-op":
		next = fmt.Sprintf("No PR was created because there were no changes. Inspect `factory job get %s --details` and the retained worktree if needed.", job.ID)
	case job.Status == "complete":
		next = fmt.Sprintf("Inspect the completed result with `factory job get %s --details`.", job.ID)
	default:
		return nil
	}
	_, err := fmt.Fprintf(out, "Next: %s\n", next)
	return err
}
