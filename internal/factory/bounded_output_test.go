package factory

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestBoundedOutputWriterCapsCaptureButDrainsAllWrites(t *testing.T) {
	var privateLog strings.Builder
	writer := NewBoundedOutputWriter(context.Background(), &privateLog, 24)
	chunks := []string{"first output; ", "second output; ", "final bytes beyond cap"}
	consumed := 0
	for _, chunk := range chunks {
		n, err := writer.Write([]byte(chunk))
		if err != nil || n != len(chunk) {
			t.Fatalf("Write() = %d, %v; want all %d bytes consumed", n, err, len(chunk))
		}
		consumed += n
	}
	if consumed != len(strings.Join(chunks, "")) {
		t.Fatalf("consumed = %d, want full child output", consumed)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(writer.Bytes()) > 24 || !writer.Truncated() || !strings.Contains(writer.String(), strings.TrimSpace(outputTruncatedMarker)) {
		t.Fatalf("capture = %q (len %d), truncated=%v; want bounded marked output", writer.String(), len(writer.Bytes()), writer.Truncated())
	}
}

func TestBoundedOutputWriterLeavesExactLimitUnmarkedAndStopsAfterCancellation(t *testing.T) {
	var privateLog strings.Builder
	writer := NewBoundedOutputWriter(context.Background(), &privateLog, 8)
	if n, err := writer.Write([]byte("12345678")); err != nil || n != 8 {
		t.Fatalf("exact-limit Write() = %d, %v", n, err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if writer.Truncated() || writer.String() != "12345678" {
		t.Fatalf("exact-limit capture = %q truncated=%v", writer.String(), writer.Truncated())
	}

	ctx, cancel := context.WithCancel(context.Background())
	var canceledLog strings.Builder
	canceledWriter := NewBoundedOutputWriter(ctx, &canceledLog, 8)
	cancel()
	if n, err := canceledWriter.Write([]byte("discard this output")); err != nil || n != len("discard this output") {
		t.Fatalf("canceled Write() = %d, %v; child output must drain", n, err)
	}
	if err := canceledWriter.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(canceledWriter.Bytes()) != 0 || canceledWriter.Truncated() {
		t.Fatalf("canceled output captured = %q truncated=%v", canceledWriter.String(), canceledWriter.Truncated())
	}
}

func TestBoundedOutputWriterContinuesDrainingWhenDestinationFails(t *testing.T) {
	writer := NewBoundedOutputWriter(context.Background(), failingWriter{}, 4)
	if n, err := writer.Write([]byte("larger than the capture")); err != nil || n != len("larger than the capture") {
		t.Fatalf("Write() = %d, %v; output must continue draining", n, err)
	}
	if err := writer.Flush(); err == nil {
		t.Fatal("Flush() discarded destination failure")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("destination unavailable") }
