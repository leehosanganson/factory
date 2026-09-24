package factory

import (
	"errors"
	"strings"
	"testing"
)

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

func TestTTYProgressCleansSpinnerAndReportsFailure(t *testing.T) {
	var output strings.Builder
	progress := startProgress(&output, true, "evaluate requirements", 3, "/state/run/03-evaluate-requirements.log")
	progress.finish(errors.New("agent failed"))
	got := output.String()
	for _, want := range []string{"\033[2K", "evaluate requirements attempt 3/4 failed in", "log: /state/run/03-evaluate-requirements.log"} {
		if !strings.Contains(got, want) {
			t.Errorf("TTY progress missing %q: %q", want, got)
		}
	}
}
