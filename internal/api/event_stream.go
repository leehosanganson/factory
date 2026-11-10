package restapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
)

const (
	eventStreamConcurrency  = 16
	eventStreamPollInterval = 250 * time.Millisecond
	eventStreamHeartbeat    = 15 * time.Second
	eventStreamMaxLifetime  = 25 * time.Second
	eventStreamWriteTimeout = 2 * time.Second
)

func (h *Handler) streamEventsForPrincipal(w http.ResponseWriter, r *http.Request, id, principalID string) {
	if _, err := h.manager.GetForPrincipal(id, principalID); err != nil {
		h.writeManagerError(w, err)
		return
	}
	h.streamEvents(w, r, id)
}

func (h *Handler) streamEvents(w http.ResponseWriter, r *http.Request, id string) {
	if r.URL.RawQuery != "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	values := r.Header.Values("Last-Event-ID")
	if len(values) > 1 {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	var cursor uint64
	hasCursor := len(values) == 1
	if hasCursor {
		parsed, err := strconv.ParseUint(values[0], 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
			return
		}
		cursor = parsed
	}
	select {
	case h.eventStreams <- struct{}{}:
		defer func() { <-h.eventStreams }()
	default:
		writeError(w, http.StatusServiceUnavailable, "stream_capacity", "Event stream capacity is full.")
		return
	}
	readCtx, cancel := context.WithTimeout(r.Context(), eventStreamWriteTimeout)
	initial, err := h.readStreamState(readCtx, id)
	cancel()
	if err != nil {
		h.writeManagerError(w, err)
		return
	}
	if (hasCursor && staleEventCursor(initial.History, cursor) != "") || (!hasCursor && initial.History.Truncated && !initial.Snapshot.Status.Terminal()) {
		writeError(w, http.StatusConflict, "cursor_expired", "Event history is incomplete; fetch job status and history before reconnecting.")
		return
	}
	if hasCursor && cursor > initial.History.LatestSequence {
		writeError(w, http.StatusBadRequest, "invalid_request", "Request is invalid.")
		return
	}
	controller := http.NewResponseController(w)
	streamDeadline := time.Now().Add(eventStreamMaxLifetime)
	deadline := time.NewTimer(time.Until(streamDeadline))
	defer deadline.Stop()
	if err := setStreamWriteDeadline(controller, streamDeadline); err != nil {
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if err := controller.Flush(); err != nil {
		return
	}
	if err := clearStreamWriteDeadline(controller); err != nil {
		return
	}
	ticker := time.NewTicker(eventStreamPollInterval)
	defer ticker.Stop()
	heartbeat := time.NewTicker(eventStreamHeartbeat)
	defer heartbeat.Stop()
	sentTerminalEvent := false
	var lastProgress *restjobs.ProgressSummary
	for {
		state := initial
		if state.Snapshot.Progress != nil && !sameProgress(lastProgress, state.Snapshot.Progress) {
			if writeStreamProgress(w, controller, streamDeadline, *state.Snapshot.Progress) != nil {
				return
			}
			lastProgress = state.Snapshot.Progress
		}
		initial = streamState{}
		for _, event := range state.History.Events {
			if event.Sequence <= cursor {
				continue
			}
			if safeEventType(event.Type) {
				if err := writeStreamEvent(w, controller, streamDeadline, event); err != nil {
					return
				}
				if event.Type == string(state.Snapshot.Status) {
					sentTerminalEvent = true
				}
			}
			cursor = event.Sequence
		}
		if state.Snapshot.Status.Terminal() {
			if !sentTerminalEvent {
				data := fmt.Sprintf(`{"status":%q}`, state.Snapshot.Status)
				if err := writeStreamControl(w, controller, streamDeadline, "terminal_snapshot", data); err != nil {
					return
				}
			}
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-deadline.C:
			return
		case <-heartbeat.C:
			if err := writeStreamHeartbeat(w, controller, streamDeadline); err != nil {
				return
			}
		case <-ticker.C:
			pollCtx, cancel := context.WithTimeout(r.Context(), eventStreamWriteTimeout)
			state, err = h.readStreamState(pollCtx, id)
			cancel()
			if err != nil {
				return
			}
			if (cursor != 0 && staleEventCursor(state.History, cursor) != "") || (cursor == 0 && state.History.Truncated && !state.Snapshot.Status.Terminal()) {
				if writeStreamControl(w, controller, streamDeadline, "reset", `{"reason":"cursor_expired","fallback":"GET status and history"}`) != nil {
					return
				}
				return
			}
			initial = state
			if state.Snapshot.Progress != nil && !sameProgress(lastProgress, state.Snapshot.Progress) {
				if writeStreamProgress(w, controller, streamDeadline, *state.Snapshot.Progress) != nil {
					return
				}
				lastProgress = state.Snapshot.Progress
			}
			if state.Snapshot.Status.Terminal() {
				for _, event := range state.History.Events {
					if event.Sequence <= cursor {
						continue
					}
					if safeEventType(event.Type) {
						if err := writeStreamEvent(w, controller, streamDeadline, event); err != nil {
							return
						}
						if event.Type == string(state.Snapshot.Status) {
							sentTerminalEvent = true
						}
					}
					cursor = event.Sequence
				}
				if !sentTerminalEvent {
					data := fmt.Sprintf(`{"status":%q}`, state.Snapshot.Status)
					if err := writeStreamControl(w, controller, streamDeadline, "terminal_snapshot", data); err != nil {
						return
					}
				}
				return
			}
		}
	}
}

type streamState struct {
	Snapshot restjobs.Snapshot
	History  restjobs.History
}

func (h *Handler) readStreamState(ctx context.Context, id string) (streamState, error) {
	readCtx, cancel := context.WithTimeout(ctx, eventStreamWriteTimeout)
	defer cancel()
	snapshot, history, err := h.manager.EventStreamState(readCtx, id)
	if err != nil {
		return streamState{}, err
	}
	return streamState{Snapshot: snapshot, History: history}, nil
}

func staleEventCursor(history restjobs.History, cursor uint64) string {
	if len(history.Events) > 0 && cursor < history.Events[0].Sequence-1 {
		return "expired"
	}
	if history.Truncated && len(history.Events) == 0 && cursor < history.LatestSequence {
		return "expired"
	}
	return ""
}

func writeStreamEvent(w http.ResponseWriter, controller *http.ResponseController, streamDeadline time.Time, event restjobs.Event) error {
	data := fmt.Sprintf("{\"at\":%q,\"type\":%q}", event.At.UTC().Format(time.RFC3339Nano), event.Type)
	if event.Type == "stage_progress" {
		progress, ok := restjobs.ParseProgressEvent(event.Message)
		if !ok {
			return nil
		}
		data = fmt.Sprintf("{\"at\":%q,\"type\":%q,\"stage\":%q,\"state\":%q}", event.At.UTC().Format(time.RFC3339Nano), event.Type, progress.Stage, progress.State)
	}
	return writeStreamFrame(w, controller, streamDeadline, fmt.Sprintf("id: %d\nevent: %s\ndata: %s\n\n", event.Sequence, event.Type, data))
}

func sameProgress(left, right *restjobs.ProgressSummary) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func writeStreamProgress(w http.ResponseWriter, controller *http.ResponseController, streamDeadline time.Time, progress restjobs.ProgressSummary) error {
	data := fmt.Sprintf(`{"stage":%q,"state":%q}`, progress.Stage, progress.State)
	return writeStreamControl(w, controller, streamDeadline, "workflow_progress", data)
}

func writeStreamHeartbeat(w http.ResponseWriter, controller *http.ResponseController, streamDeadline time.Time) error {
	return writeStreamFrame(w, controller, streamDeadline, ": heartbeat\n\n")
}

func writeStreamControl(w http.ResponseWriter, controller *http.ResponseController, streamDeadline time.Time, event, data string) error {
	return writeStreamFrame(w, controller, streamDeadline, fmt.Sprintf("event: %s\ndata: %s\n\n", event, data))
}

func setStreamWriteDeadline(controller *http.ResponseController, streamDeadline time.Time) error {
	writeDeadline := time.Now().Add(eventStreamWriteTimeout)
	if streamDeadline.Before(writeDeadline) {
		writeDeadline = streamDeadline
	}
	if !time.Now().Before(writeDeadline) {
		return context.DeadlineExceeded
	}
	if err := controller.SetWriteDeadline(writeDeadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

func writeStreamFrame(w http.ResponseWriter, controller *http.ResponseController, streamDeadline time.Time, frame string) error {
	if err := setStreamWriteDeadline(controller, streamDeadline); err != nil {
		return err
	}
	_, writeErr := io.WriteString(w, frame)
	if writeErr == nil {
		writeErr = controller.Flush()
	}
	return errors.Join(writeErr, clearStreamWriteDeadline(controller))
}

func clearStreamWriteDeadline(controller *http.ResponseController) error {
	if err := controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}
