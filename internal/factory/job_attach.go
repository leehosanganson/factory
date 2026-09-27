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
