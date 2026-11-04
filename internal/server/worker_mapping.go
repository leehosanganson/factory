package restserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
)

const workerMappingFileLimit = 1 << 20

var workerMappingFingerprintPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type workerMappingFile struct {
	Version  int                  `json:"version"`
	Mappings []workerMappingEntry `json:"mappings"`
}

type workerMappingEntry struct {
	SHA256      string `json:"sha256"`
	PrincipalID string `json:"principal_id"`
}

type WorkerMappingRegistryStore interface {
	ReconcileWorkerMappings(context.Context, map[string]string) (restjobs.WorkerMappingState, error)
}

type acceptedWorkerMappingState struct {
	Generation uint64
	Mappings   map[string]string
}

type WorkerMappingStatus struct {
	Generation uint64
	Count      int
	Result     string
	At         *time.Time
}

// WorkerMappingRegistry publishes immutable accepted mappings and records the
// result of each reload without exposing candidate data or raw errors.
type WorkerMappingRegistry struct {
	path  string
	store WorkerMappingRegistryStore
	mu    sync.Mutex
	state atomic.Pointer[acceptedWorkerMappingState]
	last  atomic.Pointer[WorkerMappingStatus]
}

func NewWorkerMappingRegistry(path string, store WorkerMappingRegistryStore) (*WorkerMappingRegistry, error) {
	if path == "" || store == nil {
		return nil, errors.New("worker mapping registry dependencies are unavailable")
	}
	return &WorkerMappingRegistry{path: path, store: store}, nil
}

func decodeWorkerMappingFile(data []byte) (map[string]string, error) {
	if len(data) == 0 || len(data) > workerMappingFileLimit || rejectDuplicateJSONFields(data) != nil {
		return nil, errors.New("invalid mapping file")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var file workerMappingFile
	if err := decoder.Decode(&file); err != nil || file.Version != 1 || file.Mappings == nil || decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid mapping file")
	}
	mappings := make(map[string]string, len(file.Mappings))
	for _, entry := range file.Mappings {
		if !workerMappingFingerprintPattern.MatchString(entry.SHA256) || !principalIDPattern.MatchString(entry.PrincipalID) || entry.PrincipalID == "legacy-shared" {
			return nil, errors.New("invalid mapping file")
		}
		if _, exists := mappings[entry.SHA256]; exists {
			return nil, errors.New("invalid mapping file")
		}
		mappings[entry.SHA256] = entry.PrincipalID
	}
	return mappings, nil
}

func rejectDuplicateJSONFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var consume func() error
	consume = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok {
					return errors.New("invalid key")
				}
				if _, exists := seen[name]; exists {
					return errors.New("duplicate key")
				}
				seen[name] = struct{}{}
				if err := consume(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := consume(); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		default:
			return errors.New("invalid JSON delimiter")
		}
	}
	if err := consume(); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func (r *WorkerMappingRegistry) Reload(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	file, err := os.Open(r.path)
	var data []byte
	if err == nil {
		data, err = io.ReadAll(io.LimitReader(file, workerMappingFileLimit+1))
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
	}
	var mappings map[string]string
	if err == nil {
		mappings, err = decodeWorkerMappingFile(data)
	}
	var state restjobs.WorkerMappingState
	if err == nil {
		state, err = r.store.ReconcileWorkerMappings(ctx, mappings)
	}
	result := "accepted"
	if err != nil {
		result = "rejected"
	} else {
		snapshot := acceptedWorkerMappingState{Generation: state.Generation, Mappings: copyMapping(state.Mappings)}
		r.state.Store(&snapshot)
	}
	previous := r.state.Load()
	at := now
	status := WorkerMappingStatus{Result: result, At: &at}
	if previous != nil {
		status.Generation, status.Count = previous.Generation, len(previous.Mappings)
	}
	r.last.Store(&status)
	return err
}

func copyMapping(values map[string]string) map[string]string {
	copied := make(map[string]string, len(values))
	for key, value := range values {
		copied[key] = value
	}
	return copied
}

func (r *WorkerMappingRegistry) Principal(fingerprint string) (string, bool) {
	state := r.state.Load()
	if state == nil {
		return "", false
	}
	principal, ok := state.Mappings[fingerprint]
	return principal, ok
}

func (r *WorkerMappingRegistry) Status() WorkerMappingStatus {
	state := r.state.Load()
	status := WorkerMappingStatus{}
	if state != nil {
		status.Generation, status.Count = state.Generation, len(state.Mappings)
	}
	if last := r.last.Load(); last != nil {
		status.Result = last.Result
		if last.At != nil {
			at := *last.At
			status.At = &at
		}
	}
	return status
}
