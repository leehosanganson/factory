package pi

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/harness"
	"golang.org/x/sys/unix"
)

func TestRunReturnsBoundedReplyWithoutStderr(t *testing.T) {
	root := t.TempDir()
	exe := filepath.Join(root, "pi")
	if err := os.WriteFile(exe, []byte(`#!/bin/sh
 printf 'private diagnostic' >&2
 IFS= read -r prompt
 printf '%s' "$prompt"
 printf '%080d' 0
 `), 0700); err != nil {
		t.Fatal(err)
	}
	reply, err := (Runner{Executable: exe}).Run(context.Background(), harness.Task{WorkDir: root, StateDir: root, Prompt: "--not-a-flag\n", ReplyLimit: 64})
	if err != nil || !reply.Truncated || len(reply.Text) > 64 || !strings.HasPrefix(reply.Text, "--not-a-flag") {
		t.Fatalf("reply=%+v err=%v", reply, err)
	}
}

func TestRunCancellationCleansUpToolProcesses(t *testing.T) {
	for _, mode := range []string{"separate-group", "stubborn-group-member"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			self, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			exe := filepath.Join(root, "pi")
			script := fmt.Sprintf("#!/bin/sh\nexec '%s' -test.run=^TestPiProcessFixture$ -- %s\n", strings.ReplaceAll(self, "'", "'\\''"), mode)
			if err := os.WriteFile(exe, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := (Runner{Executable: exe}).Run(ctx, harness.Task{WorkDir: root, StateDir: root})
				done <- err
			}()
			waitForFixture(t, "tool startup", func() bool {
				_, err := os.Stat(filepath.Join(root, "tool-ready"))
				return err == nil
			})
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Run returned %v, want context.Canceled", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return after cancellation")
			}
			if _, err := os.Stat(filepath.Join(root, "term-handled")); err != nil {
				t.Errorf("Pi fixture did not complete its SIGTERM handler: %v", err)
			}
			lock, err := os.Open(filepath.Join(root, "tool.lock"))
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Close()
			// The lock is released on exit even if the tool has not been reaped yet.
			waitForFixture(t, "tool exit", func() bool {
				err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
				if err != nil && !errors.Is(err, unix.EWOULDBLOCK) {
					t.Fatal(err)
				}
				return err == nil
			})
		})
	}
}

func waitForFixture(t *testing.T, what string, ready func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if ready() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestPiProcessFixture(t *testing.T) {
	if len(os.Args) != 4 || os.Args[2] != "--" {
		return
	}
	// Keep a fixture from leaking indefinitely if the regression test fails.
	time.AfterFunc(15*time.Second, func() { os.Exit(2) })
	mode := os.Args[3]
	if mode == "tool" {
		signal.Ignore(syscall.SIGTERM)
		lock, err := os.Create("tool.lock")
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile("tool-ready", nil, 0600); err != nil {
			t.Fatal(err)
		}
		select {}
	}
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM)
	defer signal.Stop(term)
	tool := exec.Command(os.Args[0], "-test.run=^TestPiProcessFixture$", "--", "tool")
	tool.Stdout = os.Stdout
	if mode == "separate-group" {
		tool.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	if err := tool.Start(); err != nil {
		t.Fatal(err)
	}
	<-term
	if mode == "separate-group" {
		// Model Pi's print-mode cleanup of a tracked tool group.
		if err := syscall.Kill(-tool.Process.Pid, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		_ = tool.Wait()
	}
	if err := os.WriteFile("term-handled", nil, 0600); err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}
