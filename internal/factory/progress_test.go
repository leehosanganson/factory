package factory

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type progressTestWriter struct {
	mu     sync.Mutex
	output strings.Builder
	pulse  chan struct{}
}

func newProgressTestWriter() *progressTestWriter {
	return &progressTestWriter{pulse: make(chan struct{}, 16)}
}

func (w *progressTestWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.output.Write(p)
	if strings.Contains(string(p), "Progress update") {
		select {
		case w.pulse <- struct{}{}:
		default:
		}
	}
	return n, err
}

func (w *progressTestWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.output.String()
}

func (w *progressTestWriter) waitPulse(t *testing.T) {
	t.Helper()
	select {
	case <-w.pulse:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for progress update")
	}
}

func TestProgressNonTTYProvidesPlainProgressAndSafeBoundedPaths(t *testing.T) {
	logPath := "\x1b]0;hostile title\a" + strings.Repeat("/long/path", 40) + "\nlatest.log"
	output := newProgressTestWriter()
	progress := startProgressWithIntervals(output, false, "implement", 2, logPath, 10*time.Millisecond, time.Hour)
	output.waitPulse(t)
	progress.finish(nil)
	got := output.String()
	for _, want := range []string{"Stage: implement", "Attempt: 2/4", "Elapsed:", "Latest: waiting for log output", "completed in"} {
		if !strings.Contains(got, want) {
			t.Errorf("plain progress missing %q: %q", want, got)
		}
	}
	if strings.ContainsAny(got, "\x1b\a\r") || strings.Contains(got, "hostile title") || strings.Contains(got, "\nlatest.log") {
		t.Fatalf("plain progress path was not terminal-safe: %q", got)
	}
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "Log: ") || strings.Contains(line, "; log: ") {
			if len([]rune(line)) > progressLogLineLimit+8 {
				t.Errorf("plain path line was not bounded: width=%d line=%q", len([]rune(line)), line)
			}
		}
	}
}

func TestTTYProgressUsesCursorRowsWithoutClearingEachFrame(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	t.Setenv("COLUMNS", "72")
	t.Setenv("LINES", "16")
	t.Setenv("NO_COLOR", "")
	logPath := filepath.Join(t.TempDir(), "stage.log")
	if err := os.WriteFile(logPath, []byte("preparing\nwriting focused tests\nverifying behavior\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	progress := startProgressWithSizeQuery(&output, true, "implement", 3, logPath, time.Hour, time.Hour, func(uintptr) (int, int, error) {
		return 72, 16, nil
	})
	progress.finish(nil)
	got := output.String()
	for _, want := range []string{"\033[?1049h", "\033[?25l", "FACTORY", "CURRENT OPERATION", "IMPLEMENT", "ATTEMPT", "RECENT ACTIVITY", "verifying behavior", "LOG", "\033[?25h\033[?1049l", "implement attempt 3/4 completed in"} {
		if !strings.Contains(got, want) {
			t.Errorf("full-screen progress missing %q: %q", want, got)
		}
	}
	if strings.Count(got, "\033[2J") != 1 {
		t.Errorf("expected only entry to clear screen, got %d clears", strings.Count(got, "\033[2J"))
	}
	if strings.Count(got, "\033[1;1H") != 2 {
		t.Errorf("expected cursor-positioned initial and completion draws, got output %q", got)
	}
	if strings.Index(got, "\033[?25h\033[?1049l") > strings.Index(got, "implement attempt 3/4 completed in") {
		t.Fatal("completion summary was not written after returning to the original screen")
	}
}

func TestTTYAnimationUpdatesRowsWithoutClearingViewport(t *testing.T) {
	t.Setenv("TERM", "xterm")
	t.Setenv("NO_COLOR", "1")
	file, err := os.CreateTemp(t.TempDir(), "progress-animation")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	progress := startProgressWithSizeQuery(file, true, "animation", 1, "/tmp/animation.log", time.Hour, time.Millisecond, func(uintptr) (int, int, error) {
		return 80, 24, nil
	})
	time.Sleep(15 * time.Millisecond)
	progress.finish(nil)
	data, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	if clears := strings.Count(got, "\033[2J"); clears != 1 {
		t.Fatalf("animation cleared viewport %d times, want only once on entry", clears)
	}
	if redraws := strings.Count(got, "\033[5;1H"); redraws < 3 {
		t.Fatalf("animation did not redraw the changed operation row: %d redraws", redraws)
	}
}

func TestProgressResizeBetweenRendersRedrawsCurrentViewport(t *testing.T) {
	t.Setenv("TERM", "xterm")
	t.Setenv("NO_COLOR", "1")
	file, err := os.CreateTemp(t.TempDir(), "progress-screen")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	queries := 0
	progress := startProgressWithSizeQuery(file, true, "resize test", 1, "/tmp/a-very-long-stage-log-name.log", time.Hour, time.Hour, func(uintptr) (int, int, error) {
		queries++
		if queries == 1 {
			return 72, 16, nil
		}
		return 48, 8, nil
	})
	progress.finish(nil)
	if queries != 2 || progress.width != 48 || progress.rows != 8 {
		t.Fatalf("resize query/dimensions = %d, %dx%d; want 2 queries and 48x8", queries, progress.width, progress.rows)
	}
	if _, err := file.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	if strings.Count(got, "\033[2J") != 2 {
		t.Errorf("expected entry and resize redraw clears, got %d", strings.Count(got, "\033[2J"))
	}
	clear := strings.LastIndex(got, "\033[2J")
	if clear < 0 {
		t.Fatal("resize did not redraw the viewport")
	}
	resizedDraw := strings.SplitN(got[clear+len("\033[2J"):], "\033[?25h", 2)[0]
	if !strings.Contains(resizedDraw, "\033[1;1H\r\033[2K") {
		t.Errorf("resized draw omitted first row: %q", resizedDraw)
	}
	for row := 2; row <= 8; row++ {
		if !strings.Contains(resizedDraw, "\033["+itoa(row)+";1H\r\033[2K") {
			t.Errorf("resized draw omitted row %d: %q", row, resizedDraw)
		}
	}
	if strings.Contains(resizedDraw, "\033[9;1H") {
		t.Errorf("resized draw wrote beyond the 8-row viewport: %q", resizedDraw)
	}
	for _, row := range strings.Split(resizedDraw, "\033[") {
		if end := strings.Index(row, "H"); end >= 0 {
			text := row[end+1:]
			if width := progressTextWidth(stripProgressANSI(text)); width > 48 {
				t.Errorf("resized row width %d exceeds 48 columns: %q", width, text)
			}
		}
	}
}

func TestProgressResizeToFiveRowsKeepsStatusAttemptAndSanitizedLogPath(t *testing.T) {
	t.Setenv("TERM", "xterm")
	t.Setenv("NO_COLOR", "1")
	logPath := filepath.Join(t.TempDir(), "run\x1b]0;hostile title\a", "02-implement.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("latest activity\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "progress-resize-small")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	queries := 0
	progress := startProgressWithSizeQuery(file, true, "implement", 3, logPath, time.Hour, time.Hour, func(uintptr) (int, int, error) {
		queries++
		if queries == 1 {
			return 72, 16, nil
		}
		return 72, 5, nil
	})
	progress.finish(nil)
	if queries != 2 || progress.rows != 5 {
		t.Fatalf("resize query/dimensions = %d, %d rows; want 2 queries and 5 rows", queries, progress.rows)
	}
	data, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	if !strings.Contains(got, "\033[?25h\033[?1049l") {
		t.Fatal("resizing active TUI did not restore cursor and alternate screen on finish")
	}
	redraw := strings.SplitN(got[strings.LastIndex(got, "\033[2J")+len("\033[2J"):], "\033[?25h", 2)[0]
	rows := 0
	for _, sequence := range strings.Split(redraw, "\033[") {
		if strings.Contains(sequence, ";1H") {
			rows++
			if strings.HasPrefix(sequence, "6;") {
				t.Fatalf("5-row viewport wrote row 6: %q", redraw)
			}
		}
	}
	if rows != 5 {
		t.Fatalf("resized render wrote %d rows, want 5: %q", rows, redraw)
	}
	for _, want := range []string{"IMPLEMENT · COMPLETED", "Attempt: 3/4", "02-implement.log"} {
		if !strings.Contains(redraw, want) {
			t.Errorf("five-row render omitted %q: %q", want, redraw)
		}
	}
	if strings.Contains(redraw, "latest activity") || strings.Contains(redraw, "\033]") || strings.Contains(redraw, "hostile title") {
		t.Errorf("compact render should prioritize status/attempt/path without unsafe controls: %q", redraw)
	}
}

func TestCompactProgressScreenPreservesEssentialFieldsInTinyViewports(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	for rows := 1; rows <= 5; rows++ {
		t.Run(itoa(rows), func(t *testing.T) {
			lines := renderProgressScreen(80, rows, "build", 2, "/state/run/01-build.log", time.Second, "ACTIVE", false)
			if len(lines) > rows {
				t.Fatalf("rendered %d lines in %d-row viewport", len(lines), rows)
			}
			for _, line := range lines {
				if progressTextWidth(line) > 80 {
					t.Errorf("line exceeds viewport width: %q", line)
				}
			}
			joined := strings.Join(lines, "\n")
			for _, want := range []string{"BUILD", "2/4", "01-build.log"} {
				if !strings.Contains(joined, want) {
					t.Errorf("%d-row render omitted %q: %q", rows, want, joined)
				}
			}
		})
	}
}

func TestProgressWidthFallbackClippingAndWideUnicode(t *testing.T) {
	t.Setenv("COLUMNS", "invalid")
	t.Setenv("LINES", "0")
	width, rows := resolveProgressSize(&strings.Builder{}, nil, "invalid", "0")
	if width != progressDefaultWidth || rows != progressDefaultRows {
		t.Fatalf("invalid dimensions fallback = %dx%d, want %dx%d", width, rows, progressDefaultWidth, progressDefaultRows)
	}
	t.Setenv("COLUMNS", "56")
	t.Setenv("LINES", "14")
	file, err := os.CreateTemp(t.TempDir(), "progress-width")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	width, rows = resolveProgressSize(file, func(fd uintptr) (int, int, error) {
		if fd != file.Fd() {
			t.Errorf("terminal query fd=%d, want %d", fd, file.Fd())
		}
		return 42, 9, nil
	}, "56", "14")
	if width != 42 || rows != 9 {
		t.Fatalf("terminal dimensions = %dx%d, want queried 42x9", width, rows)
	}
	for _, text := range []string{"long 世界 activity", "界界界 wide", "⌚x"} {
		clipped := truncateProgressText(text, 2)
		if progressTextWidth(clipped) > 1 || !strings.HasSuffix(clipped, "…") {
			t.Errorf("clipped %q = %q with width %d; expected <= 1 and ellipsis", text, clipped, progressTextWidth(clipped))
		}
	}
	if got := truncateProgressText("unchanged", 12); got != "unchanged" {
		t.Errorf("fitting text changed: %q", got)
	}
	wrapped := wrapProgressText("wide 世界 activity", 7)
	if len(wrapped) < 2 {
		t.Fatalf("wide activity was not wrapped: %#v", wrapped)
	}
	for _, line := range wrapped {
		if progressTextWidth(line) > 7 {
			t.Errorf("wrapped line exceeds width: %q", line)
		}
	}
}

func TestProgressTerminalFallbackAndNoColor(t *testing.T) {
	for _, test := range []struct {
		name    string
		term    string
		columns string
		rows    string
	}{
		{name: "non tty", term: "xterm", columns: "80", rows: "24"},
		{name: "dumb terminal", term: "dumb", columns: "80", rows: "24"},
		{name: "narrow terminal", term: "xterm", columns: "39", rows: "24"},
		{name: "short terminal", term: "xterm", columns: "80", rows: "5"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("TERM", test.term)
			t.Setenv("COLUMNS", test.columns)
			t.Setenv("LINES", test.rows)
			var output strings.Builder
			progress := startProgressWithIntervals(&output, test.name != "non tty", "build", 1, "/tmp/build.log", time.Hour, time.Hour)
			progress.finish(nil)
			if strings.Contains(output.String(), "\033") {
				t.Fatalf("plain fallback emitted ANSI: %q", output.String())
			}
		})
	}

	t.Setenv("TERM", "xterm")
	t.Setenv("COLUMNS", "80")
	t.Setenv("LINES", "24")
	t.Setenv("NO_COLOR", "1")
	var output strings.Builder
	progress := startProgressWithSizeQuery(&output, true, "build", 1, "/tmp/build.log", time.Hour, time.Hour, func(uintptr) (int, int, error) { return 80, 24, nil })
	progress.finish(nil)
	got := output.String()
	if strings.Contains(got, "\033[1;36m") || strings.Contains(got, "\033[1;33m") || !strings.Contains(got, "\033[?1049h") {
		t.Fatalf("NO_COLOR should remove styling but retain screen controls: %q", got)
	}
}

func TestProgressCancellationUsesContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	t.Setenv("TERM", "xterm")
	t.Setenv("COLUMNS", "80")
	t.Setenv("LINES", "24")
	t.Setenv("NO_COLOR", "1")
	var output strings.Builder
	progress := startProgressWithSizeQuery(&output, true, "evaluate review", 2, "/tmp/evaluator.log", time.Hour, time.Hour, func(uintptr) (int, int, error) { return 80, 24, nil })
	progress.finish(ctx.Err())
	got := output.String()
	if !strings.Contains(got, "\033[?25h\033[?1049l") || !strings.Contains(got, "evaluate review attempt 2/4 failed in") {
		t.Fatalf("cancellation finish did not restore terminal and report outcome: %q", got)
	}
}

func TestFullScreenPathIsSanitizedAndWidthBounded(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	path := "/tmp/\x1b]0;hostile title\a" + strings.Repeat("very-long-directory/", 8) + "stage.log\ncontinued"
	lines := renderProgressScreen(40, 12, "review", 1, path, time.Second, "ACTIVE", true)
	if len(lines) != 12 {
		t.Fatalf("rendered %d rows, want 12", len(lines))
	}
	for i, line := range lines {
		if strings.ContainsAny(line, "\x1b\a\r\n") || strings.Contains(line, "hostile title") {
			t.Errorf("row %d contains unsanitized path data: %q", i, line)
		}
		if width := progressTextWidth(line); width > 40 {
			t.Errorf("row %d width %d exceeds 40: %q", i, width, line)
		}
	}
	summary := safeProgressPath(path, 38)
	if strings.ContainsAny(summary, "\x1b\a\r\n") || progressTextWidth(summary) > 38 {
		t.Errorf("summary path is unsafe or too wide: %q (width %d)", summary, progressTextWidth(summary))
	}
}

func TestReadProgressLogBoundsTailAndDisplayedLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.log")
	content := strings.Repeat("discarded output\n", progressTailBytes) + "latest one\nlatest two\nlatest three\nlatest four\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	lines := readProgressLog(path)
	if len(lines) != progressLogLines || strings.Join(lines, "|") != "latest two|latest three|latest four" {
		t.Fatalf("readProgressLog tail = %#v, want latest three lines", lines)
	}
	if err := os.WriteFile(path, []byte("\x1b]0;private title\a\x1b[31mvisible\x1b[0m\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	lines = readProgressLog(path)
	if strings.Join(lines, "\n") != "visible" {
		t.Fatalf("readProgressLog should sanitize terminal controls, got %#v", lines)
	}
}

func TestSanitizeProgressLineRemovesTerminalControls(t *testing.T) {
	got := sanitizeProgressLine("hello\x1b[31mred\x1b[0m\x1b]title\x07 world\twith controls\nnext\x1bPprivate payload\x1b\\ done")
	if got != "hellored world with controlsnext done" {
		t.Fatalf("sanitizeProgressLine = %q, want sanitized text", got)
	}
}

func stripProgressANSI(text string) string {
	var out strings.Builder
	for i := 0; i < len(text); {
		if text[i] == '\033' {
			i = skipProgressEscape(text, i)
			continue
		}
		out.WriteByte(text[i])
		i++
	}
	return out.String()
}

func itoa(n int) string { return string(rune('0' + n)) }
