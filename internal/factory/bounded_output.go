package factory

import (
	"context"
	"io"
	"strings"
	"sync"
)

const outputTruncatedMarker = "\n[output truncated]"

// BoundedOutputWriter captures at most limit bytes. Writes are always reported
// as fully consumed so child processes continue draining output after the
// capture limit is reached.
type BoundedOutputWriter struct {
	mu        sync.Mutex
	ctx       context.Context
	dst       io.Writer
	limit     int
	capture   []byte
	truncated bool
	flushed   bool
}

// NewBoundedOutputWriter creates a context-aware bounded capture. A nil context
// behaves like context.Background; a nil destination retains output in memory.
func NewBoundedOutputWriter(ctx context.Context, dst io.Writer, limit int) *BoundedOutputWriter {
	if ctx == nil {
		ctx = context.Background()
	}
	if limit < 0 {
		limit = 0
	}
	return &BoundedOutputWriter{ctx: ctx, dst: dst, limit: limit}
}

func (w *BoundedOutputWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(p) == 0 || w.limit == 0 || w.ctx.Err() != nil || w.truncated || w.flushed {
		return len(p), nil
	}
	if len(w.capture)+len(p) <= w.limit {
		w.capture = append(w.capture, p...)
		return len(p), nil
	}
	marker := outputTruncatedMarker
	if len(marker) > w.limit {
		marker = marker[:w.limit]
	}
	payloadLimit := w.limit - len(marker)
	if len(w.capture) > payloadLimit {
		w.capture = w.capture[:payloadLimit]
	}
	remaining := payloadLimit - len(w.capture)
	if remaining > 0 {
		payload := p[:min(len(p), remaining)]
		w.capture = append(w.capture, payload...)
	}
	w.capture = append(w.capture, marker...)
	w.truncated = true
	return len(p), nil
}

// Flush writes the bounded capture to its destination once. Write deliberately
// never returns destination errors, so callers can keep draining child output.
func (w *BoundedOutputWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.flushed {
		return nil
	}
	w.flushed = true
	if w.dst == nil || w.ctx.Err() != nil || len(w.capture) == 0 {
		return nil
	}
	n, err := w.dst.Write(w.capture)
	if err != nil {
		return err
	}
	if n != len(w.capture) {
		return io.ErrShortWrite
	}
	return nil
}

// Bytes returns a copy of captured output.
func (w *BoundedOutputWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.capture...)
}

// String returns captured output as text.
func (w *BoundedOutputWriter) String() string { return string(w.Bytes()) }

// Truncated reports whether output exceeded the capture limit.
func (w *BoundedOutputWriter) Truncated() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.truncated
}

// TruncationMarker returns the marker text used in the capture.
func (w *BoundedOutputWriter) TruncationMarker() string {
	return strings.TrimSpace(outputTruncatedMarker)
}
