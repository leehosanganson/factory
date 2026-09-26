package factory

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func appendMonitorAgentLog(dir, id string, data []byte) error {
	store, exists, err := existingMonitorJobStore(dir, id)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("v2 monitor job %s does not exist", id)
	}
	return store.AppendSessionLog(id, monitorSessionID, data)
}

func existingMonitorJobStore(dir, id string) (*JobStore, bool, error) {
	root := filepath.Join(filepath.Dir(dir), "v2")
	path := filepath.Join(root, id, "job.json")
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	store, err := NewJobStore(root)
	return store, err == nil, err
}

func createMonitorJobRecord(cfg Config, legacy *babysitJob) error {
	root, err := JobStateRoot(cfg.StateDir)
	if err != nil {
		return err
	}
	store, err := NewJobStore(root)
	if err != nil {
		return err
	}
	job := JobRecord{
		ID: legacy.ID, Type: monitorJobType, TaskDescription: legacy.Description,
		TargetPath: legacy.RepoRoot, Status: "queued",
	}
	if err := store.CreateJob(job); err != nil {
		return err
	}
	if _, err := store.CreateSession(legacy.ID, monitorSessionID, "queued"); err != nil {
		_ = os.RemoveAll(filepath.Join(store.Root(), legacy.ID))
		return err
	}
	return nil
}

func syncMonitorJobStatus(dir string, legacy *babysitJob) error {
	return syncMonitorJob(dir, legacy, false)
}

func syncMonitorJobEvent(dir string, legacy *babysitJob) error {
	return syncMonitorJob(dir, legacy, true)
}

func syncMonitorJob(dir string, legacy *babysitJob, recordEvent bool) error {
	store, exists, err := existingMonitorJobStore(dir, legacy.ID)
	if err != nil || !exists {
		return err
	}
	job, err := store.GetJob(legacy.ID)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	status := monitorRecordStatus(legacy.Status)
	if status == "" {
		return nil
	}
	session, err := store.GetSession(legacy.ID, monitorSessionID)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	statusChanged := job.Status != status || session.Status != status
	if !recordEvent && !statusChanged {
		return nil
	}
	message := legacy.LastEvent
	if message == "" {
		message = "Monitor status changed to " + status + "."
	}
	if err := (JobSessionObserver{Store: store, JobID: legacy.ID, SessionID: monitorSessionID}).ObserveWorkflowEvent(WorkflowEvent{Type: "monitor.status", Message: message}); err != nil {
		return err
	}
	if job.Status != status {
		if _, err := store.UpdateJob(legacy.ID, func(record *JobRecord) error {
			record.Status = status
			return nil
		}); err != nil {
			return err
		}
	}
	if session.Status != status {
		_, err = store.UpdateSession(legacy.ID, monitorSessionID, func(session *SessionRecord) error {
			session.Status = status
			return nil
		})
	}
	return err
}

func monitorRecordStatus(status string) string {
	switch status {
	case "starting":
		return "queued"
	case "running", "awaiting_approval":
		return "running"
	case "recoverable_failure":
		return status
	case "completed", "complete", "merged":
		return "merged"
	case "closed":
		return "closed"
	case "stopped", "failed", "interrupted", "cancelled":
		return status
	default:
		return ""
	}
}

func storeMonitorSessionRunning(id, dir string) error {
	store, exists, err := existingMonitorJobStore(dir, id)
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
	event := WorkflowEvent{Type: "monitor.started", Message: "Detached PR monitor worker registered."}
	return (JobSessionObserver{Store: store, JobID: id, SessionID: monitorSessionID}).ObserveWorkflowEvent(event)
}

func registerMonitorWorkerIfMissing(id, dir string, pid int) error {
	store, exists, err := existingMonitorJobStore(dir, id)
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
	store, exists, err := existingMonitorJobStore(dir, id)
	if err != nil || !exists {
		return err
	}
	return store.WriteWorker(id, WorkerRecord{ID: id, PID: pid})
}

func heartbeatMonitorWorker(id, dir string) error {
	store, exists, err := existingMonitorJobStore(dir, id)
	if err != nil || !exists {
		return err
	}
	_, err = store.HeartbeatWorker(id)
	return err
}

func clearMonitorWorker(id, dir string) error {
	store, exists, err := existingMonitorJobStore(dir, id)
	if err != nil || !exists {
		return err
	}
	return store.ClearWorker(id)
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
