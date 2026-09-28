package factory

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func monitorJobStore(dir string) (*JobStore, bool, error) {
	id := filepath.Base(dir)
	store, err := NewJobStore(filepath.Dir(dir))
	if err != nil {
		return nil, false, err
	}
	if _, err := store.GetJob(id); errors.Is(err, os.ErrNotExist) {
		return store, false, nil
	} else if err != nil {
		return nil, false, err
	}
	return store, true, nil
}

func appendMonitorAgentLog(dir, id string, data []byte) error {
	store, exists, err := monitorJobStore(dir)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("detached monitor job %s does not exist", id)
	}
	return monitorSessionLogAppend(store, id, data)
}

func createMonitorJobRecord(cfg Config, monitor *monitorJob) error {
	root, err := JobStateRoot(cfg.StateDir)
	if err != nil {
		return err
	}
	store, err := NewJobStore(root)
	if err != nil {
		return err
	}
	copy := *monitor
	job := JobRecord{ID: monitor.ID, Type: monitorJobType, TaskDescription: monitor.Description, TargetPath: monitor.RepoRoot, Status: "queued", Monitor: &copy}
	if err := store.CreateJob(job); err != nil {
		return err
	}
	if _, err := store.CreateSession(monitor.ID, monitorSessionID, "queued"); err != nil {
		_ = os.RemoveAll(filepath.Join(store.Root(), monitor.ID))
		return err
	}
	return nil
}

func syncMonitorJobStatus(dir string, monitor *monitorJob) error {
	return syncMonitorJob(dir, monitor, false)
}

func syncMonitorJobEvent(dir string, monitor *monitorJob) error {
	return syncMonitorJob(dir, monitor, true)
}

func syncMonitorJob(dir string, monitor *monitorJob, recordEvent bool) error {
	store, exists, err := monitorJobStore(dir)
	if err != nil || !exists {
		return err
	}
	job, err := store.GetJob(monitor.ID)
	if err != nil {
		return err
	}
	status := monitor.Status
	session, err := store.GetSession(monitor.ID, monitorSessionID)
	if err != nil {
		return err
	}
	statusChanged := job.Status != status || session.Status != status
	if !recordEvent && !statusChanged {
		return nil
	}
	message := monitor.LastEvent
	if message == "" {
		message = "Monitor status changed to " + status + "."
	}
	if recordEvent {
		if err := store.AppendSessionEvent(monitor.ID, monitorSessionID, "monitor.status", message); err != nil {
			return err
		}
	}
	if job.Status != status {
		if _, err := store.UpdateJob(monitor.ID, func(record *JobRecord) error {
			record.Status = status
			return nil
		}); err != nil {
			return err
		}
	}
	if session.Status != status {
		_, err = store.UpdateSession(monitor.ID, monitorSessionID, func(session *SessionRecord) error {
			session.Status = status
			return nil
		})
	}
	return err
}

func storeMonitorSessionRunning(id, dir string) error {
	store, exists, err := monitorJobStore(dir)
	if err != nil || !exists {
		return err
	}
	if _, err := store.UpdateJob(id, func(record *JobRecord) error {
		record.Status = "running"
		return nil
	}); err != nil {
		return err
	}
	_, err = store.UpdateSession(id, monitorSessionID, func(session *SessionRecord) error {
		session.Status = "running"
		return nil
	})
	if err != nil {
		return err
	}
	return store.AppendSessionEvent(id, monitorSessionID, "monitor.started", "Detached PR monitor worker registered.")
}

func registerMonitorWorkerIfMissing(id, dir string, pid int) error {
	store, exists, err := monitorJobStore(dir)
	if err != nil || !exists {
		return err
	}
	if _, err := store.ReadWorker(id); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return store.WriteWorker(id, WorkerRecord{ID: id, PID: pid})
}

func registerMonitorWorker(id, dir string, pid int) error {
	store, exists, err := monitorJobStore(dir)
	if err != nil || !exists {
		return err
	}
	return store.WriteWorker(id, WorkerRecord{ID: id, PID: pid})
}

func heartbeatMonitorWorker(id, dir string) error {
	store, exists, err := monitorJobStore(dir)
	if err != nil || !exists {
		return err
	}
	_, err = store.HeartbeatWorker(id)
	return err
}

func clearMonitorWorker(id, dir string) error {
	store, exists, err := monitorJobStore(dir)
	if err != nil || !exists {
		return err
	}
	return store.ClearWorker(id)
}

func finishMonitorWorkerLockTimeout(root, id string) (bool, error) {
	dir, err := monitorJobDir(root, id)
	if err != nil {
		return false, err
	}
	unlockWorker, err := monitorAcquireWorkerLock(filepath.Join(dir, "worker.lock"))
	if err != nil {
		return false, nil
	}
	defer unlockWorker()

	store, err := NewJobStore(root)
	if err != nil {
		return false, err
	}
	unlockJob, err := store.LockJob(id)
	if err != nil {
		return false, err
	}
	defer unlockJob()

	job, err := store.GetJob(id)
	if err != nil {
		return false, err
	}
	if job.Type != monitorJobType || job.Monitor == nil {
		return false, fmt.Errorf("invalid canonical monitor job record")
	}
	monitor := job.Monitor
	if monitor.StopRequested || (monitor.Status != "queued" && monitor.Status != "recoverable_failure") {
		return false, nil
	}
	if monitor.DeadlineAt.IsZero() || time.Now().Before(monitor.DeadlineAt) {
		return false, nil
	}
	// This worker owns worker.lock, so another worker can no longer be in
	// startup or active work. A live PID may refer to this timed-out worker.
	message := "Monitor lifetime timeout reached; monitoring stopped."
	now := time.Now().UTC()
	setLifecycleTimes(job.Status, "stopped", &job.StartedAt, &job.EndedAt)
	job.Status, job.UpdatedAt = "stopped", now
	monitor.Status, monitor.Phase, monitor.UpdatedAt = "stopped", "stopped", now
	recordMonitorRecentEvent(monitor, monitor.Phase, message, now)
	session, err := store.GetSession(id, monitorSessionID)
	if err != nil {
		return false, err
	}
	setLifecycleTimes(session.Status, "stopped", &session.StartedAt, &session.EndedAt)
	session.Status, session.UpdatedAt = "stopped", now
	for i := range job.Sessions {
		if job.Sessions[i].ID == monitorSessionID {
			job.Sessions[i] = sessionMetadata(session)
			break
		}
	}

	sessionDir, err := store.sessionDir(id, monitorSessionID, false)
	if err != nil {
		return false, err
	}
	if err := appendLockedMonitorEvent(sessionDir, message, now); err != nil {
		return false, err
	}
	if err := appendLockedMonitorLog(sessionDir, message, now); err != nil {
		return false, err
	}
	if err := writeJSONAtomic(sessionDir, "session.json", session); err != nil {
		return false, err
	}
	jobDir, err := store.jobDir(id, false)
	if err != nil {
		return false, err
	}
	if err := writeJSONAtomic(jobDir, "job.json", job); err != nil {
		return false, err
	}
	return true, nil
}

func appendLockedMonitorEvent(sessionDir, message string, at time.Time) error {
	path := filepath.Join(sessionDir, "events.jsonl")
	if err := ensureRegularIfExists(path); err != nil {
		return err
	}
	line, err := json.Marshal(SessionEvent{At: at, Type: "monitor.status", Message: message})
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(line, '\n')); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func appendLockedMonitorLog(sessionDir, message string, at time.Time) error {
	path := filepath.Join(sessionDir, "session.log")
	if err := ensureRegularIfExists(path); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(file, "[%s] %s\n", at.Format(time.RFC3339), message); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func persistMonitorDeadline(root, id string, timeout time.Duration, deadline time.Time) (*monitorJob, error) {
	store, err := NewJobStore(root)
	if err != nil {
		return nil, err
	}
	unlock, err := store.LockJob(id)
	if err != nil {
		return nil, err
	}
	defer unlock()
	job, err := store.GetJob(id)
	if err != nil {
		return nil, err
	}
	if job.Type != monitorJobType || job.Monitor == nil {
		return nil, fmt.Errorf("invalid canonical monitor job record")
	}
	if job.Monitor.DeadlineAt.IsZero() {
		startedAt := job.StartedAt
		if !startedAt.IsZero() {
			// Older records lack an absolute deadline. Anchor migration to
			// their recorded lifecycle timestamp, not to this restart.
			job.Monitor.DeadlineAt = startedAt.Add(timeout)
		} else {
			job.Monitor.DeadlineAt = deadline
		}
		job.UpdatedAt = time.Now().UTC()
		job.Monitor.UpdatedAt = job.UpdatedAt
		dir, err := store.jobDir(id, false)
		if err != nil {
			return nil, err
		}
		if err := writeJSONAtomic(dir, "job.json", job); err != nil {
			return nil, err
		}
	}
	copy := *job.Monitor
	return &copy, nil
}

func resetMonitorJobRecord(id, root string) error {
	store, err := NewJobStore(root)
	if err != nil {
		return err
	}
	job, err := store.GetJob(id)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if job.Status != "recoverable_failure" {
		return nil
	}
	if err := store.ClearStopRequest(id); err != nil {
		return err
	}
	if _, err := store.UpdateJob(id, func(record *JobRecord) error {
		record.Status = "queued"
		record.EndedAt = time.Time{}
		return nil
	}); err != nil {
		return err
	}
	_, err = store.UpdateSession(id, monitorSessionID, func(session *SessionRecord) error {
		session.Status = "queued"
		session.EndedAt = time.Time{}
		return nil
	})
	return err
}
