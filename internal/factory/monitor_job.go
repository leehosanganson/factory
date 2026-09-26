package factory

import (
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
