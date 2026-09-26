package factory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

func monitorLegacyLogPath(store *JobStore, id, fallback string) (string, error) {
	job, err := store.GetJob(id)
	if err != nil {
		return "", err
	}
	if job.Type != monitorJobType {
		return "", fmt.Errorf("job %s is not a monitor job", id)
	}
	legacyRoot := filepath.Dir(store.Root())
	legacyDir, err := babysitJobDir(legacyRoot, id)
	if err != nil {
		return "", err
	}
	legacyPath := filepath.Join(legacyDir, "actions.log")
	if _, err := os.Lstat(legacyDir); errors.Is(err, os.ErrNotExist) {
		return fallback, nil
	} else if err != nil {
		return "", err
	}
	if err := ensureRealDirectory(legacyRoot, legacyDir); err != nil {
		return "", err
	}
	if err := ensureRegularIfExists(legacyPath); err != nil {
		return "", err
	}
	if _, err := os.Lstat(legacyPath); errors.Is(err, os.ErrNotExist) {
		return fallback, nil
	} else if err != nil {
		return "", err
	}
	if err := ensureRegularIfExists(filepath.Join(legacyDir, "job.json")); err != nil {
		return "", err
	}
	legacy, err := readBabysitJob(legacyDir)
	if err != nil {
		return "", fmt.Errorf("read legacy monitor job: %w", err)
	}
	if !filepath.IsAbs(job.TargetPath) || !filepath.IsAbs(legacy.RepoRoot) {
		return "", fmt.Errorf("legacy monitor job has an invalid target path")
	}
	v2Target, err := canonicalPath(job.TargetPath)
	if err != nil {
		return "", err
	}
	legacyTarget, err := canonicalPath(legacy.RepoRoot)
	if err != nil {
		return "", err
	}
	if legacy.ID != id || v2Target != legacyTarget {
		return "", fmt.Errorf("legacy monitor job does not match v2 job %s", id)
	}
	return legacyPath, nil
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
	return fmt.Errorf("job %s is resumable but its worker exited with status recoverable_failure; use factory babysit reset %s to resume monitoring", id, id)
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
		if err == nil {
			path, err = monitorLegacyLogPath(store, id, path)
		}
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
	case "complete", "completed", "merged", "closed":
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
