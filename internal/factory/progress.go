package factory

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	progressTailBytes       = 16 * 1024
	progressLogLineLimit    = 180
	progressLogLines        = 3
	progressCardRows        = 5
	progressDefaultWidth    = 80
	progressHeartbeatPeriod = 15 * time.Second
	progressAnimationPeriod = 500 * time.Millisecond
)

type stageProgress struct {
	out         io.Writer
	stage       string
	attempt     int
	logPath     string
	started     time.Time
	terminal    bool
	width       int
	hasRendered bool
	stop        chan struct{}
	done        chan struct{}
}

func startProgress(out io.Writer, terminal bool, stage string, attempt int, logPath string) *stageProgress {
	return startProgressWithIntervals(out, terminal, stage, attempt, logPath, progressHeartbeatPeriod, progressAnimationPeriod)
}

func startProgressWithIntervals(out io.Writer, terminal bool, stage string, attempt int, logPath string, heartbeat, animation time.Duration) *stageProgress {
	return startProgressWithWidthQuery(out, terminal, stage, attempt, logPath, heartbeat, animation, queryTerminalWidth)
}

func startProgressWithWidthQuery(out io.Writer, terminal bool, stage string, attempt int, logPath string, heartbeat, animation time.Duration, query func(uintptr) (int, error)) *stageProgress {
	terminal = terminal && strings.TrimSpace(os.Getenv("TERM")) != "dumb"
	width := 0
	if terminal {
		width = resolveProgressWidth(out, query, os.Getenv("COLUMNS"))
		terminal = width >= 40
	}
	p := &stageProgress{out: out, stage: stage, attempt: attempt, logPath: logPath, started: time.Now(), terminal: terminal, width: width}
	if terminal {
		p.stop = make(chan struct{})
		p.done = make(chan struct{})
		p.render(true, "ACTIVE")
		go p.run(animation)
		return p
	}

	fmt.Fprintf(out, "Running %s attempt %d/4; log: %s\n", stage, attempt, logPath)
	p.stop = make(chan struct{})
	p.done = make(chan struct{})
	go p.run(heartbeat)
	return p
}

func (p *stageProgress) run(interval time.Duration) {
	defer close(p.done)
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	frame := 0
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			if p.terminal {
				frame++
				p.render(true, "ACTIVE", frame)
			} else {
				p.renderHeartbeat()
			}
		}
	}
}

func (p *stageProgress) finish(err error) {
	close(p.stop)
	<-p.done

	elapsed := time.Since(p.started).Round(time.Second)
	result := "completed"
	status := "COMPLETED"
	if err != nil {
		result = "failed"
		status = "FAILED"
	}
	if p.terminal {
		p.render(false, status)
		fmt.Fprintln(p.out)
	}
	fmt.Fprintf(p.out, "%s attempt %d/4 %s in %s; log: %s\n", p.stage, p.attempt, result, elapsed, p.logPath)
}

func (p *stageProgress) renderHeartbeat() {
	lines := readProgressLog(p.logPath)
	activity := "waiting for log output"
	if len(lines) > 0 {
		activity = lines[len(lines)-1]
	}
	fmt.Fprintf(p.out, "Progress %s attempt %d/4 after %s; latest: %s\n", p.stage, p.attempt, time.Since(p.started).Round(time.Second), activity)
}

func (p *stageProgress) render(animated bool, status string, frame ...int) {
	frames := []rune{'⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'}
	spinner := "✓"
	if status == "FAILED" {
		spinner = "!"
	} else if animated {
		index := 0
		if len(frame) > 0 {
			index = frame[0]
		}
		spinner = string(frames[index%len(frames)])
	}

	lines := readProgressLog(p.logPath)
	activity := []string{"waiting for agent output", ""}
	if len(lines) > 0 {
		activity = []string{"", lines[len(lines)-1]}
		if len(lines) > 1 {
			activity = lines[len(lines)-2:]
		}
	}

	phase := "ACTIVE"
	cardStatus := "RUNNING"
	if status != "ACTIVE" {
		phase = status
		cardStatus = status
	}
	stageRow := fmt.Sprintf("%s %s %d/4 · %s · %s · %s", spinner, p.stage, p.attempt, progressAttemptMeter(p.attempt), phase, time.Since(p.started).Round(time.Second))

	innerWidth := max(0, p.width-3)
	title := "FACTORY / " + cardStatus
	stageColor := ""
	if os.Getenv("NO_COLOR") == "" {
		stageColor = "\033[36m"
		if status == "COMPLETED" {
			stageColor = "\033[32m"
		} else if status == "FAILED" {
			stageColor = "\033[31m"
		}
	}
	stageCardRow := progressCardRowColor(stageRow, innerWidth, spinner, stageColor)
	card := []string{
		progressBorderRow(title, innerWidth, true),
		stageCardRow,
		progressCardRow(activity[0], innerWidth),
		progressCardRow(activity[1], innerWidth),
		progressBorderRow("log: "+shortProgressPath(p.logPath, innerWidth-5), innerWidth, false),
	}
	if !p.hasRendered {
		fmt.Fprintln(p.out, strings.Join(card, "\n"))
	} else {
		fmt.Fprintf(p.out, "\033[%dA", progressCardRows)
		for _, row := range card {
			fmt.Fprintf(p.out, "\r\033[2K%s\n", row)
		}
	}
	p.hasRendered = true
}

func resolveProgressWidth(out io.Writer, query func(uintptr) (int, error), columns string) int {
	if file, ok := out.(*os.File); ok && query != nil {
		if width, err := query(file.Fd()); err == nil && width > 0 {
			return width
		}
	}
	if width, err := strconv.Atoi(columns); err == nil && width > 0 {
		return width
	}
	return progressDefaultWidth
}

func progressAttemptMeter(attempt int) string {
	attempt = min(max(attempt, 0), 4)
	return "[" + strings.Repeat("■", attempt) + strings.Repeat("□", 4-attempt) + "]"
}

func progressCardRow(text string, innerWidth int) string {
	return progressCardRowColor(text, innerWidth, "", "")
}

func progressCardRowColor(text string, innerWidth int, prefix, color string) string {
	content := truncateProgressText(text, innerWidth)
	if color != "" && prefix != "" && strings.HasPrefix(content, prefix) {
		content = color + prefix + "\033[0m" + content[len(prefix):]
	}
	return "│" + content + strings.Repeat(" ", max(0, innerWidth-progressTextWidth(truncateProgressText(text, innerWidth)))) + "│"
}

func progressBorderRow(text string, innerWidth int, top bool) string {
	border := "─"
	left, right := "└", "┘"
	if top {
		left, right = "┌", "┐"
	}
	content := truncateProgressText(text, innerWidth)
	leftPadding := max(0, (innerWidth-progressTextWidth(content))/2)
	rightPadding := max(0, innerWidth-progressTextWidth(content)-leftPadding)
	return left + strings.Repeat(border, leftPadding) + content + strings.Repeat(border, rightPadding) + right
}

func shortProgressPath(path string, width int) string {
	if width <= 0 {
		return ""
	}
	if progressTextWidth(path) <= width {
		return path
	}
	if width == 1 {
		return "…"
	}
	var suffix strings.Builder
	used := progressTextWidth("…")
	runes := []rune(path)
	for i := len(runes) - 1; i >= 0; i-- {
		runeWidth := progressRuneWidth(runes[i])
		if used+runeWidth > width {
			break
		}
		suffix.WriteRune(runes[i])
		used += runeWidth
	}
	reversed := []rune(suffix.String())
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	return "…" + string(reversed)
}

func truncateProgressText(text string, width int) string {
	limit := width - 1
	if progressTextWidth(text) <= limit {
		return text
	}
	if limit <= 1 {
		return "…"
	}
	limit--
	var out strings.Builder
	used := 0
	for _, r := range text {
		runeWidth := progressRuneWidth(r)
		if used+runeWidth > limit {
			break
		}
		out.WriteRune(r)
		used += runeWidth
	}
	out.WriteString("…")
	return out.String()
}

func progressTextWidth(text string) int {
	width := 0
	for _, r := range text {
		width += progressRuneWidth(r)
	}
	return width
}

func progressRuneWidth(r rune) int {
	if unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Me, r) || r == 0x200d {
		return 0
	}
	if r >= 0x1100 && (r <= 0x115f || r == 0x231a || r == 0x2329 || r == 0x232a ||
		(r >= 0x2e80 && r <= 0xa4cf && r != 0x303f) ||
		(r >= 0xac00 && r <= 0xd7a3) ||
		(r >= 0xf900 && r <= 0xfaff) ||
		(r >= 0xfe10 && r <= 0xfe19) ||
		(r >= 0xfe30 && r <= 0xfe6f) ||
		(r >= 0xff00 && r <= 0xff60) ||
		(r >= 0xffe0 && r <= 0xffe6) ||
		(r >= 0x1f300 && r <= 0x1faff) ||
		(r >= 0x20000 && r <= 0x3fffd)) {
		return 2
	}
	return 1
}

func readProgressLog(path string) []string {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() == 0 {
		return nil
	}
	offset := info.Size() - progressTailBytes
	if offset < 0 {
		offset = 0
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil
	}
	data := make([]byte, info.Size()-offset)
	n, _ := io.ReadFull(file, data)
	data = data[:n]
	if offset > 0 {
		if newline := bytes.IndexByte(data, '\n'); newline >= 0 {
			data = data[newline+1:]
		}
	}
	parts := strings.Split(string(data), "\n")
	lines := make([]string, 0, progressLogLines)
	for _, part := range parts {
		clean := sanitizeProgressLine(strings.TrimSuffix(part, "\r"))
		if clean == "" {
			continue
		}
		lines = append(lines, clean)
	}
	if len(lines) > progressLogLines {
		lines = lines[len(lines)-progressLogLines:]
	}
	return lines
}

func sanitizeProgressLine(line string) string {
	var clean strings.Builder
	clean.Grow(min(len(line), progressLogLineLimit))
	for i := 0; i < len(line); {
		if line[i] == '\x1b' {
			i = skipProgressEscape(line, i)
			continue
		}
		r, size := utf8.DecodeRuneInString(line[i:])
		if r == 0x9b || r == 0x9d || r == 0x9c || r == 0x90 || r == 0x98 || r == 0x9e || r == 0x9f {
			i = skipProgressC1(line, i, r, size)
			continue
		}
		i += size
		if r == '\t' {
			r = ' '
		} else if unicode.IsControl(r) || r == utf8.RuneError && size == 1 {
			continue
		}
		if clean.Len()+utf8.RuneLen(r) > progressLogLineLimit {
			clean.WriteString("…")
			break
		}
		clean.WriteRune(r)
	}
	return strings.TrimSpace(clean.String())
}

func skipProgressEscape(line string, start int) int {
	if start+1 >= len(line) {
		return len(line)
	}
	kind := line[start+1]
	if kind == ']' || kind == 'P' || kind == 'X' || kind == '^' || kind == '_' {
		return skipProgressStringEscape(line, start+2, kind == ']')
	}
	if kind == '[' {
		for i := start + 2; i < len(line); i++ {
			if line[i] >= 0x40 && line[i] <= 0x7e {
				return i + 1
			}
		}
		return len(line)
	}
	return start + 2
}

func skipProgressStringEscape(line string, start int, bellTerminates bool) int {
	for i := start; i < len(line); i++ {
		if bellTerminates && line[i] == '\a' {
			return i + 1
		}
		if line[i] == '\x1b' && i+1 < len(line) && line[i+1] == '\\' {
			return i + 2
		}
	}
	return len(line)
}

func skipProgressC1(line string, start int, kind rune, size int) int {
	if kind == 0x9d || kind == 0x90 || kind == 0x98 || kind == 0x9e || kind == 0x9f {
		return skipProgressStringEscape(line, start+size, kind == 0x9d)
	}
	if kind == 0x9b {
		for i := start + size; i < len(line); i++ {
			if line[i] >= 0x40 && line[i] <= 0x7e {
				return i + 1
			}
		}
		return len(line)
	}
	return start + 1
}
