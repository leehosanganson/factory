package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestContextStdinReadLineReturnsNormalLine(t *testing.T) {
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readEnd.Close()
	defer writeEnd.Close()

	want := "a normal line\n"
	if _, err := writeEnd.Write([]byte(want)); err != nil {
		t.Fatal(err)
	}
	reader := &contextStdin{file: readEnd, ctx: context.Background()}
	got, err := reader.ReadLineContext(context.Background())
	if err != nil {
		t.Fatalf("ReadLineContext() error = %v", err)
	}
	if got != want {
		t.Fatalf("ReadLineContext() = %q, want %q", got, want)
	}
}

func TestContextStdinReadLineRejectsOverlongInput(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "overlong-line")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write(make([]byte, maxContextStdinLineLength+1)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		t.Fatal(err)
	}

	reader := &contextStdin{file: file, ctx: context.Background()}
	got, err := reader.ReadLineContext(context.Background())
	if err == nil || !strings.Contains(err.Error(), "stdin line exceeds maximum length") {
		t.Fatalf("ReadLineContext() error = %v, want maximum-length error", err)
	}
	if len(got) > maxContextStdinLineLength {
		t.Fatalf("ReadLineContext() accumulated %d bytes, maximum is %d", len(got), maxContextStdinLineLength)
	}
}

func TestContextStdinReadHonorsReaderContext(t *testing.T) {
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readEnd.Close()
	defer writeEnd.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &contextStdin{file: readEnd, ctx: ctx}
	result := make(chan error, 1)
	go func() {
		var b [1]byte
		_, err := reader.Read(b[:])
		result <- err
	}()
	time.AfterFunc(50*time.Millisecond, cancel)
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Read error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read remained blocked after reader context cancellation")
	}
}

func TestContextStdinReadLineHonorsSuppliedContext(t *testing.T) {
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readEnd.Close()
	defer writeEnd.Close()

	reader := &contextStdin{file: readEnd, ctx: context.Background()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := reader.ReadLineContext(ctx)
		result <- err
	}()
	time.AfterFunc(50*time.Millisecond, cancel)
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ReadLineContext error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadLineContext remained blocked after supplied context cancellation")
	}
}
