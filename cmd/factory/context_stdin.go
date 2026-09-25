package main

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"time"
)

// maxContextStdinLineLength is the maximum number of bytes accepted in one stdin line.
const maxContextStdinLineLength = 64 * 1024

type contextStdin struct {
	file *os.File
	ctx  context.Context
}

func (r *contextStdin) Read(p []byte) (int, error) {
	return r.read(p, r.ctx)
}

func (r *contextStdin) read(p []byte, ctx context.Context) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		readable, err := waitReadable(r.file.Fd(), 100*time.Millisecond)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return 0, err
		}
		if !readable {
			continue
		}
		n, err := syscall.Read(int(r.file.Fd()), p)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if n == 0 && err == nil {
			return 0, io.EOF
		}
		return n, err
	}
}

func (r *contextStdin) ReadLineContext(ctx context.Context) (string, error) {
	var line strings.Builder
	for {
		if err := ctx.Err(); err != nil {
			return line.String(), err
		}
		var b [1]byte
		n, err := r.read(b[:], ctx)
		if err != nil {
			return line.String(), err
		}
		if n == 0 {
			return line.String(), io.EOF
		}
		if line.Len() >= maxContextStdinLineLength {
			return line.String(), errors.New("stdin line exceeds maximum length of 65536 bytes")
		}
		line.WriteByte(b[0])
		if b[0] == '\n' {
			return line.String(), nil
		}
	}
}
