package factory

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type heartbeatTestWriter struct {
	mu        sync.Mutex
	output    strings.Builder
	heartbeat chan struct{}
}

func newHeartbeatTestWriter() *heartbeatTestWriter {
	return &heartbeatTestWriter{heartbeat: make(chan struct{}, 8)}
}

func (w *heartbeatTestWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.output.Write(p)
	if strings.Contains(string(p), "Progress ") {
		select {
		case w.heartbeat <- struct{}{}:
		default:
		}
	}
	return n, err
}

func (w *heartbeatTestWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.output.String()
}

func (w *heartbeatTestWriter) waitForHeartbeats(t *testing.T, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-w.heartbeat:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for heartbeat %d of %d", i+1, count)
		}
	}
}

func TestProgressIsStaticAndIncludesAttemptElapsedAndLogPathOutsideTTY(t *testing.T) {
	var output strings.Builder
	progress := startProgress(&output, false, "requirements", 2, "/state/run/02-requirements.log")
	progress.finish(nil)
	got := output.String()
	for _, want := range []string{"Running requirements attempt 2/4", "completed in", "log: /state/run/02-requirements.log"} {
		if !strings.Contains(got, want) {
			t.Errorf("static progress missing %q: %q", want, got)
		}
	}
	if strings.Contains(got, "\033") || strings.Contains(got, "\r") {
		t.Errorf("non-TTY progress contains terminal control characters: %q", got)
	}
}

func TestProgressNonTTYHeartbeatIncludesLatestSanitizedLogActivity(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "stage.log")
	if err := os.WriteFile(logPath, []byte("starting\n\033[31mwriting tests\033[0m\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	output := newHeartbeatTestWriter()
	progress := startProgressWithIntervals(output, false, "implement", 1, logPath, 10*time.Millisecond, time.Hour)
	output.waitForHeartbeats(t, 2)
	progress.finish(nil)
	got := output.String()
	if !strings.Contains(got, "Progress implement attempt 1/4 after") || !strings.Contains(got, "latest: writing tests") {
		t.Fatalf("heartbeat did not report stage activity: %q", got)
	}
	if strings.Contains(got, "\033") || strings.Contains(got, "\r") {
		t.Errorf("non-TTY heartbeat contains terminal control characters: %q", got)
	}
	if strings.Count(got, "Progress implement attempt") < 2 {
		t.Errorf("expected periodic heartbeat output, got: %q", got)
	}
}

func TestTTYProgressDrawsFiveLineCardAndLeavesCompletedSummary(t *testing.T) {
	t.Setenv("TERM", "xterm")
	t.Setenv("COLUMNS", "100")
	t.Setenv("NO_COLOR", "")
	logPath := filepath.Join(t.TempDir(), "long", "path", "03-stage.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("preparing\n\033[32mwriting focused tests\033[0m\nverifying behavior\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	progress := startProgressWithIntervals(&output, true, "evaluate requirements", 3, logPath, time.Hour, time.Hour)
	progress.finish(nil)
	got := output.String()
	for _, want := range []string{"FACTORY / RUNNING", "evaluate requirements", "[■■■□]", "ACTIVE", "writing focused tests", "verifying behavior", "03-stage.log", "FACTORY / COMPLETED", "evaluate requirements attempt 3/4 completed in", "\033[5A", "\r\033[2K"} {
		if !strings.Contains(got, want) {
			t.Errorf("TTY dashboard missing %q: %q", want, got)
		}
	}
	initialRows := strings.SplitN(got, "\n", progressCardRows+1)
	if len(initialRows) < progressCardRows+1 {
		t.Fatalf("initial card has fewer than %d rows: %q", progressCardRows, got)
	}
	if !strings.HasPrefix(initialRows[0], "┌") || !strings.HasPrefix(initialRows[1], "│") || !strings.HasPrefix(initialRows[2], "│") || !strings.HasPrefix(initialRows[3], "│") || !strings.HasPrefix(initialRows[4], "└") {
		t.Fatalf("initial card does not have header, three content rows, and footer: %#v", initialRows[:progressCardRows])
	}
	if strings.Contains(got, "\033[31mwriting focused tests") || strings.Contains(got, "\033[0m\nverifying behavior") {
		t.Errorf("log-provided ANSI styling leaked into dashboard: %q", got)
	}
	if strings.Count(got, "\033[2K") < progressCardRows {
		t.Errorf("dashboard should clear every card row on redraw: %q", got)
	}
}

func TestTTYProgressHonorsNO_COLOR(t *testing.T) {
	t.Setenv("TERM", "xterm")
	t.Setenv("NO_COLOR", "1")
	var output strings.Builder
	progress := startProgressWithIntervals(&output, true, "build", 1, "/tmp/build.log", time.Hour, time.Hour)
	progress.finish(errors.New("build failed"))
	got := output.String()
	if strings.Contains(got, "\033[3") || strings.Contains(got, "\033[9") {
		t.Errorf("NO_COLOR output includes ANSI styling: %q", got)
	}
	if !strings.Contains(got, "\033[5A") {
		t.Errorf("NO_COLOR should retain card redraw controls: %q", got)
	}
	if !strings.Contains(got, "build attempt 1/4 failed in") {
		t.Errorf("failure summary missing: %q", got)
	}
}

func TestSanitizeProgressLineRemovesTerminalControls(t *testing.T) {
	got := sanitizeProgressLine("hello\x1b[31mred\x1b[0m\x1b]title\x07 world\twith controls\nnext\x1bPprivate payload\x1b\\ done")
	if got != "hellored world with controlsnext done" {
		t.Fatalf("sanitizeProgressLine = %q, want sanitized text", got)
	}
}

func TestProgressFallsBackFromDumbTerminalAndTruncatesToWidth(t *testing.T) {
	t.Setenv("TERM", "dumb")
	t.Setenv("COLUMNS", "24")
	logPath := filepath.Join(t.TempDir(), "stage.log")
	if err := os.WriteFile(logPath, []byte(strings.Repeat("activity ", 20)), 0o600); err != nil {
		t.Fatal(err)
	}
	output := newHeartbeatTestWriter()
	progress := startProgressWithIntervals(output, true, "implement", 2, logPath, 10*time.Millisecond, time.Hour)
	output.waitForHeartbeats(t, 1)
	progress.finish(nil)
	got := output.String()
	if strings.Contains(got, "\033[") || strings.Contains(got, "\r") {
		t.Fatalf("TERM=dumb should not receive cursor controls: %q", got)
	}
	if !strings.Contains(got, "Running implement attempt 2/4") || !strings.Contains(got, "completed in") || !strings.Contains(got, "Progress implement attempt 2/4 after") {
		t.Fatalf("TERM=dumb should use readable start and heartbeat progress: %q", got)
	}

	if truncated := truncateProgressText("wide 世界 activity", 10); progressTextWidth(truncated) > 9 || !strings.HasSuffix(truncated, "…") {
		t.Errorf("truncateProgressText = %q, want UTF-8-safe output under width", truncated)
	}
	if got := truncateProgressText("unchanged", 10); got != "unchanged" {
		t.Errorf("truncateProgressText changed fitting text: %q", got)
	}
}

func TestProgressCardTruncatesRowsAtConfiguredWidth(t *testing.T) {
	t.Setenv("TERM", "xterm")
	t.Setenv("NO_COLOR", "1")
	for _, columns := range []string{"18", "120"} {
		t.Run(columns, func(t *testing.T) {
			t.Setenv("COLUMNS", columns)
			logPath := filepath.Join(t.TempDir(), "a-very-long-log-directory", "long-stage-log.log")
			if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(logPath, []byte(strings.Repeat("long activity 世界 ", 10)), 0o600); err != nil {
				t.Fatal(err)
			}
			query := func(uintptr) (int, error) { return 0, errors.New("not attached to a terminal") }
			maxWidth, _ := strconv.Atoi(columns)
			if maxWidth < 40 {
				output := newHeartbeatTestWriter()
				progress := startProgressWithWidthQuery(output, true, "a very long stage name", 4, logPath, 10*time.Millisecond, time.Hour, query)
				output.waitForHeartbeats(t, 1)
				progress.finish(nil)
				got := output.String()
				if progress.terminal || strings.Contains(got, "\033") || strings.Contains(got, "\r") {
					t.Fatalf("narrow terminal should use plain heartbeat output: %q", got)
				}
				if !strings.Contains(got, "Running a very long stage name attempt 4/4") || !strings.Contains(got, "Progress a very long stage name attempt 4/4 after") {
					t.Fatalf("narrow fallback missing start or heartbeat: %q", got)
				}
				return
			}
			var output strings.Builder
			progress := startProgressWithWidthQuery(&output, true, "a very long stage name", 4, logPath, time.Hour, time.Hour, query)
			progress.finish(nil)
			got := output.String()
			rows := strings.SplitN(got, "\n", progressCardRows+1)
			if len(rows) < progressCardRows+1 {
				t.Fatalf("missing card rows: %q", got)
			}
			for i, row := range rows[:progressCardRows] {
				if width := progressTextWidth(row); width > maxWidth-1 {
					t.Errorf("card row %d width = %d, terminal allows %d: %q", i, width, maxWidth-1, row)
				}
			}
			if !strings.Contains(rows[progressCardRows-1], "log:") {
				t.Errorf("bottom row should identify the shortened log path: %q", rows[progressCardRows-1])
			}
		})
	}
}

func TestResolveProgressWidthPrefersTerminalQueryAndFallsBackSafely(t *testing.T) {
	query := func(uintptr) (int, error) { return 63, nil }
	widthFile, err := os.CreateTemp(t.TempDir(), "progress-width")
	if err != nil {
		t.Fatal(err)
	}
	defer widthFile.Close()
	if got := resolveProgressWidth(widthFile, query, "120"); got != 63 {
		t.Fatalf("terminal query width = %d, want 63", got)
	}

	t.Setenv("TERM", "xterm")
	t.Setenv("COLUMNS", "120")
	t.Setenv("NO_COLOR", "1")
	queries := 0
	outputFile, err := os.CreateTemp(t.TempDir(), "progress-output")
	if err != nil {
		t.Fatal(err)
	}
	defer outputFile.Close()
	wantFD := outputFile.Fd()
	var queriedFD uintptr
	progress := startProgressWithWidthQuery(outputFile, true, "build", 1, "", time.Hour, time.Hour, func(fd uintptr) (int, error) {
		queries++
		queriedFD = fd
		return 42, nil
	})
	progress.finish(nil)
	if _, err := outputFile.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	output, err := os.ReadFile(outputFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	rows := strings.SplitN(string(output), "\n", progressCardRows+1)
	if queries != 1 || queriedFD != wantFD || progress.width != 42 || len(rows) < progressCardRows {
		t.Fatalf("terminal query did not use the output descriptor once before rendering: queries=%d fd=%d want=%d width=%d output=%q", queries, queriedFD, wantFD, progress.width, output)
	}
	for i, row := range rows[:progressCardRows] {
		if width := progressTextWidth(row); width > 41 {
			t.Errorf("card row %d width = %d, exceeds queried terminal width minus reserved column: %q", i, width, row)
		}
	}
	failedQuery := func(uintptr) (int, error) { return 0, errors.New("ioctl failed") }
	for _, test := range []struct {
		columns string
		want    int
	}{{"48", 48}, {"invalid", progressDefaultWidth}, {"0", progressDefaultWidth}} {
		if got := resolveProgressWidth(widthFile, failedQuery, test.columns); got != test.want {
			t.Errorf("resolveProgressWidth(%q) = %d, want %d", test.columns, got, test.want)
		}
	}
}

func TestProgressWidthQueryRequiresTerminalAndFileOutput(t *testing.T) {
	t.Setenv("TERM", "xterm")
	t.Setenv("COLUMNS", "72")
	queryCalls := 0
	query := func(uintptr) (int, error) {
		queryCalls++
		return 120, nil
	}

	var mockOutput strings.Builder
	progress := startProgressWithWidthQuery(&mockOutput, true, "build", 1, "", time.Hour, time.Hour, query)
	progress.finish(nil)
	if queryCalls != 0 || progress.width != 72 || !progress.terminal {
		t.Fatalf("non-file writer should use COLUMNS without querying: calls=%d width=%d terminal=%v", queryCalls, progress.width, progress.terminal)
	}

	file, err := os.CreateTemp(t.TempDir(), "progress-not-terminal")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	progress = startProgressWithWidthQuery(file, false, "build", 1, "", time.Hour, time.Hour, query)
	progress.finish(nil)
	if queryCalls != 0 || progress.terminal {
		t.Fatalf("non-terminal output should never query terminal width: calls=%d terminal=%v", queryCalls, progress.terminal)
	}

	t.Setenv("TERM", "dumb")
	progress = startProgressWithWidthQuery(file, true, "build", 1, "", time.Hour, time.Hour, query)
	progress.finish(nil)
	if queryCalls != 0 || progress.terminal {
		t.Fatalf("TERM=dumb should never query terminal width: calls=%d terminal=%v", queryCalls, progress.terminal)
	}
}

func TestProgressCardAllocatesTwoColumnsForWatchEmoji(t *testing.T) {
	got := progressCardRow("⌚x", 6)
	if want := "│⌚x   │"; got != want {
		t.Fatalf("progressCardRow with watch emoji = %q, want %q", got, want)
	}
}

func TestReadProgressLogBoundsAndLimitsDisplayedLines(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "large.log")
	content := strings.Repeat("discarded output\n", progressTailBytes) + "latest one\nlatest two\nlatest three\nlatest four\n"
	if err := os.WriteFile(logPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	lines := readProgressLog(logPath)
	if len(lines) != progressLogLines {
		t.Fatalf("readProgressLog returned %d lines, want %d: %#v", len(lines), progressLogLines, lines)
	}
	if strings.Join(lines, "|") != "latest two|latest three|latest four" {
		t.Fatalf("readProgressLog returned wrong tail: %#v", lines)
	}
}
