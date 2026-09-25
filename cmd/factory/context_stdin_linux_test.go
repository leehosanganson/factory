//go:build linux

package main

import (
	"os"
	"testing"
	"time"
)

func TestWaitReadableHandlesDescriptorBeyond32(t *testing.T) {
	var reserved []*os.File
	defer func() {
		for _, file := range reserved {
			_ = file.Close()
		}
	}()

	for len(reserved) < 40 {
		file, err := os.Open("/dev/null")
		if err != nil {
			t.Fatal(err)
		}
		reserved = append(reserved, file)
	}

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	if reader.Fd() < 32 {
		t.Fatalf("pipe descriptor = %d, want >= 32", reader.Fd())
	}
	if _, err := writer.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}

	readable, err := waitReadable(reader.Fd(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !readable {
		t.Fatal("waitReadable reported the readable pipe as not readable")
	}
}
