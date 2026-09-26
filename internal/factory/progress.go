package factory

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	progressTailBytes       = 16 * 1024
	progressLogLineLimit    = 180
	progressLogLines        = 20
	progressDefaultWidth    = 80
	progressDefaultRows     = 24
	progressMaxWidth        = 500
	progressMaxRows         = 300
	progressHeartbeatPeriod = 15 * time.Second
	progressAnimationPeriod = 500 * time.Millisecond
)

type stageProgress struct {
	out         io.Writer
	stage       string
	logPath     string
	started     time.Time
	terminal    bool
	width       int
	rows        int
	lastLines   []string
	hasRendered bool
	stop        chan struct{}
	done        chan struct{}
	sizeQuery   func(uintptr) (int, int, error)
}

func startProgress(out io.Writer, terminal bool, stage, logPath string) *stageProgress {
	return startProgressWithIntervals(out, terminal, stage, logPath, progressHeartbeatPeriod, progressAnimationPeriod)
}

func startProgressWithIntervals(out io.Writer, terminal bool, stage, logPath string, heartbeat, animation time.Duration) *stageProgress {
	return startProgressWithSizeQuery(out, terminal, stage, logPath, heartbeat, animation, queryTerminalSize)
}

// startProgressWithWidthQuery is retained as a narrow test seam for callers that only
// have a width query. Production rendering always queries both terminal dimensions.
func startProgressWithWidthQuery(out io.Writer, terminal bool, stage, logPath string, heartbeat, animation time.Duration, query func(uintptr) (int, error)) *stageProgress {
	return startProgressWithSizeQuery(out, terminal, stage, logPath, heartbeat, animation, func(fd uintptr) (int, int, error) {
		width, err := query(fd)
		return width, progressDimensionsFromEnv().rows, err
	})
}

func startProgressWithSizeQuery(out io.Writer, terminal bool, stage, logPath string, heartbeat, animation time.Duration, query func(uintptr) (int, int, error)) *stageProgress {
	terminal = terminal && strings.TrimSpace(os.Getenv("TERM")) != "dumb"
	dimensions := progressDimensionsFromEnv()
	width, rows := dimensions.width, dimensions.rows
	if terminal {
		width, rows = resolveProgressSize(out, query, os.Getenv("COLUMNS"), os.Getenv("LINES"))
		terminal = width >= 40 && rows >= 6
	}
	p := &stageProgress{out: out, stage: stage, logPath: logPath, started: time.Now(), terminal: terminal, width: width, rows: rows, sizeQuery: query}
	if terminal {
		p.stop = make(chan struct{})
		p.done = make(chan struct{})
		fmt.Fprint(out, "\033[?1049h\033[?25l")
		p.render(true, "ACTIVE")
		go p.run(animation)
		return p
	}

	fmt.Fprintf(out, "Stage: %s\nElapsed: 0s\nLatest: waiting for agent output\nLog: %s\n", stage, styledLogPath(safeProgressPath(logPath, max(1, width-10)), os.Getenv("NO_COLOR") == ""))
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
		fmt.Fprint(p.out, "\033[?25h\033[?1049l")
	}
	pathWidth := min(progressLogLineLimit-50, max(1, p.width-44))
	if p.terminal {
		pathWidth = max(1, p.width-32)
	}
	summary := fmt.Sprintf("%s %s in %s; log: %s", p.stage, result, elapsed, styledLogPath(safeProgressPath(p.logPath, pathWidth), os.Getenv("NO_COLOR") == ""))
	fmt.Fprintln(p.out, truncateProgressText(summary, p.width+1))
}

func (p *stageProgress) renderHeartbeat() {
	lines := readProgressLog(p.logPath)
	activity := "waiting for log output"
	if len(lines) > 0 {
		activity = lines[len(lines)-1]
	}
	fmt.Fprintf(p.out, "Progress update\nStage: %s\nElapsed: %s\nLatest: %s\nLog: %s\n", p.stage, time.Since(p.started).Round(time.Second), activity, styledLogPath(safeProgressPath(p.logPath, max(1, p.width-10)), os.Getenv("NO_COLOR") == ""))
}

func (p *stageProgress) render(animated bool, status string, frame ...int) {
	width, rows := p.width, p.rows
	resized := false
	if p.hasRendered && p.sizeQuery != nil {
		if file, ok := p.out.(*os.File); ok {
			if w, h, err := p.sizeQuery(file.Fd()); err == nil && w > 0 && h > 0 {
				width, rows = boundedProgressWidth(w), boundedProgressRows(h)
				resized = width != p.width || rows != p.rows
				p.width, p.rows = width, rows
			}
		}
	}
	lines := renderProgressScreen(width, rows, p.stage, p.logPath, time.Since(p.started), status, animated, frame...)
	if !p.hasRendered || resized {
		fmt.Fprint(p.out, "\033[2J")
	}
	for i, line := range lines {
		if p.hasRendered && !resized && i < len(p.lastLines) && line == p.lastLines[i] {
			continue
		}
		fmt.Fprintf(p.out, "\033[%d;1H\r\033[2K%s", i+1, line)
	}
	if !resized {
		for i := len(lines); i < len(p.lastLines); i++ {
			fmt.Fprintf(p.out, "\033[%d;1H\r\033[2K", i+1)
		}
	}
	p.lastLines = lines
	p.hasRendered = true
}

func renderProgressScreen(width, rows int, stage, logPath string, elapsed time.Duration, status string, animated bool, frame ...int) []string {
	width, rows = boundedProgressWidth(width), boundedProgressRows(rows)
	noColor := os.Getenv("NO_COLOR") != ""
	spinner := "✓"
	if status == "FAILED" {
		spinner = "!"
	} else if animated {
		frames := []rune{'⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'}
		index := 0
		if len(frame) > 0 {
			index = frame[0]
		}
		spinner = string(frames[index%len(frames)])
	}
	state := "RUNNING"
	if status != "ACTIVE" {
		state = status
	}
	statusText := func(text string) string { return styledProgressText(text, progressStatusColor(status), noColor) }
	logLine := func() string {
		path := shortProgressPath(terminalSafeProgressPath(logPath), max(1, width-5))
		return "LOG  " + styledLogPath(path, !noColor)
	}

	if rows < 12 {
		stageLine := statusText(spinner + " " + strings.ToUpper(stage) + " · " + state)
		lines := []string{stageLine, fmt.Sprintf("ELAPSED  %s", elapsed.Round(time.Second)), logLine()}
		if rows == 3 {
			lines[1] = "RECENT  " + readProgressActivity(logPath)
		}
		if rows >= 8 {
			activity := readProgressLog(logPath)
			if len(activity) == 0 {
				activity = []string{"waiting for agent output"}
			}
			lines = []string{statusText("FACTORY  " + state), strings.Repeat("─", max(0, width-1)), "CURRENT OPERATION", stageLine, fmt.Sprintf("ELAPSED  %s", elapsed.Round(time.Second)), strings.Repeat("─", max(0, width-1)), "RECENT ACTIVITY"}
			activitySlots := rows - 8
			var wrapped []string
			for _, item := range activity {
				wrapped = append(wrapped, wrapProgressText(item, max(1, width-4))...)
			}
			if len(wrapped) > activitySlots {
				wrapped = wrapped[len(wrapped)-activitySlots:]
			}
			for _, item := range wrapped {
				lines = append(lines, "  "+item)
			}
			for len(lines) < rows-1 {
				lines = append(lines, "")
			}
			lines = append(lines, logLine())
		} else if rows == 7 {
			lines = []string{statusText("FACTORY  " + state), strings.Repeat("─", max(0, width-1)), stageLine, "RECENT ACTIVITY", readProgressActivity(logPath), fmt.Sprintf("ELAPSED  %s", elapsed.Round(time.Second)), logLine()}
		} else if rows == 6 {
			lines = []string{statusText("FACTORY  " + state), stageLine, "RECENT ACTIVITY", readProgressActivity(logPath), fmt.Sprintf("ELAPSED  %s", elapsed.Round(time.Second)), logLine()}
		} else if rows == 5 {
			lines = []string{statusText("FACTORY  " + state), strings.Repeat("─", max(0, width-1)), stageLine, fmt.Sprintf("ELAPSED  %s", elapsed.Round(time.Second)), logLine()}
		} else if rows == 4 {
			lines = []string{statusText("FACTORY  " + state), stageLine, fmt.Sprintf("ELAPSED  %s", elapsed.Round(time.Second)), logLine()}
		} else if rows == 2 {
			lines = []string{stageLine, logLine()}
		} else if rows == 1 {
			path := terminalSafeProgressPath(logPath)
			pathWidth := min(width/2, progressTextWidth(filepath.Base(path))+1)
			pathWidth = max(1, pathWidth)
			stageWidth := max(1, width-pathWidth-progressTextWidth(" · LOG "))
			prefix := truncateProgressText(stageLine, stageWidth+1) + " · LOG "
			lines = []string{prefix + styledLogPath(shortProgressPath(path, pathWidth), !noColor)}
		}
		for len(lines) < rows {
			lines = append(lines, "")
		}
		for i := range lines {
			lines[i] = truncateProgressText(lines[i], width+1)
		}
		return lines
	}

	activityLines := readProgressLog(logPath)
	if len(activityLines) == 0 {
		activityLines = []string{"waiting for agent output"}
	}
	content := []string{
		styledProgressText("FACTORY", "\033[1;36m", noColor) + "  " + statusText(state),
		strings.Repeat("─", max(0, width-1)),
		"CURRENT OPERATION",
		statusText(spinner + " " + strings.ToUpper(stage)),
		fmt.Sprintf("ELAPSED  %s", elapsed.Round(time.Second)),
		strings.Repeat("─", max(0, width-1)),
		"RECENT ACTIVITY",
	}
	footerRows := 1
	if rows >= 12 {
		footerRows = 2
		content = append(content, strings.Repeat("─", max(0, width-1)))
	} else if len(content) >= 2 {
		content = append(content[:5], content[6:]...)
	}
	activitySlots := max(0, rows-len(content)-footerRows)
	var wrapped []string
	for _, activity := range activityLines {
		wrapped = append(wrapped, wrapProgressText(activity, max(1, width-4))...)
	}
	if len(wrapped) > activitySlots {
		wrapped = wrapped[len(wrapped)-activitySlots:]
	}
	for len(content)+len(wrapped) < rows-footerRows {
		content = append(content, "")
	}
	for _, line := range wrapped {
		content = append(content, "  "+line)
	}
	footer := []string{logLine()}
	if footerRows == 2 {
		footer = []string{"", logLine()}
	}
	content = append(content, footer...)
	if len(content) > rows {
		content = content[:rows]
	}
	for i := range content {
		content[i] = truncateProgressText(content[i], width+1)
	}
	return content
}

func readProgressActivity(path string) string {
	lines := readProgressLog(path)
	if len(lines) == 0 {
		return "waiting for agent output"
	}
	return lines[len(lines)-1]
}

func styledLogPath(path string, enabled bool) string {
	if !enabled || path == "" {
		return path
	}
	return "\033[2m" + path + "\033[0m"
}

func styledProgressText(text, color string, noColor bool) string {
	if noColor || color == "" {
		return text
	}
	return color + text + "\033[0m"
}

func progressStatusColor(status string) string {
	switch status {
	case "COMPLETED":
		return "\033[1;32m"
	case "FAILED":
		return "\033[1;31m"
	default:
		return "\033[1;33m"
	}
}

func resolveProgressWidth(out io.Writer, query func(uintptr) (int, error), columns string) int {
	if file, ok := out.(*os.File); ok && query != nil {
		if width, err := query(file.Fd()); err == nil && width > 0 {
			return boundedProgressWidth(width)
		}
	}
	if width, err := strconv.Atoi(columns); err == nil && width > 0 && width <= progressMaxWidth {
		return width
	}
	return progressDefaultWidth
}

func resolveProgressSize(out io.Writer, query func(uintptr) (int, int, error), columns, rows string) (int, int) {
	size := progressDimensionsFromEnv()
	if width, err := strconv.Atoi(columns); err == nil && width > 0 && width <= progressMaxWidth {
		size.width = width
	}
	if height, err := strconv.Atoi(rows); err == nil && height > 0 && height <= progressMaxRows {
		size.rows = height
	}
	if file, ok := out.(*os.File); ok && query != nil {
		if width, height, err := query(file.Fd()); err == nil {
			if width > 0 {
				size.width = boundedProgressWidth(width)
			}
			if height > 0 {
				size.rows = boundedProgressRows(height)
			}
		}
	}
	return size.width, size.rows
}

type progressDimensions struct{ width, rows int }

func progressDimensionsFromEnv() progressDimensions {
	return progressDimensions{width: progressEnvDimension("COLUMNS", progressDefaultWidth, progressMaxWidth), rows: progressEnvDimension("LINES", progressDefaultRows, progressMaxRows)}
}

func progressEnvDimension(name string, fallback, maximum int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value < 1 || value > maximum {
		return fallback
	}
	return value
}

func boundedProgressWidth(width int) int { return min(max(width, 1), progressMaxWidth) }
func boundedProgressRows(rows int) int   { return min(max(rows, 1), progressMaxRows) }

func wrapProgressText(text string, width int) []string {
	if width <= 0 {
		return []string{""}
	}
	var rows []string
	var row strings.Builder
	rowWidth := 0
	flush := func() {
		rows = append(rows, row.String())
		row.Reset()
		rowWidth = 0
	}
	for _, word := range strings.Fields(text) {
		wordRunes := []rune(word)
		wordWidth := progressTextWidth(word)
		if rowWidth > 0 && rowWidth+1+wordWidth <= width {
			row.WriteByte(' ')
			rowWidth++
		} else if rowWidth > 0 {
			flush()
		}
		for _, r := range wordRunes {
			runeWidth := progressRuneWidth(r)
			if rowWidth > 0 && rowWidth+runeWidth > width {
				flush()
			}
			row.WriteRune(r)
			rowWidth += runeWidth
		}
	}
	if rowWidth > 0 || len(rows) == 0 {
		flush()
	}
	return rows
}

func safeProgressPath(path string, width int) string {
	path = terminalSafeProgressPath(path)
	return shortProgressPath(path, width)
}

func terminalSafeProgressPath(path string) string {
	path = terminalSafeText(path)
	path = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return ' '
		}
		return r
	}, path)
	return strings.TrimSpace(path)
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
	for i := 0; i < len(text); {
		if text[i] == '\x1b' {
			end := skipProgressEscape(text, i)
			out.WriteString(text[i:end])
			i = end
			continue
		}
		r, size := utf8.DecodeRuneInString(text[i:])
		runeWidth := progressRuneWidth(r)
		if used+runeWidth > limit {
			break
		}
		out.WriteRune(r)
		used += runeWidth
		i += size
	}
	out.WriteString("…")
	if strings.Contains(out.String(), "\033[") {
		out.WriteString("\033[0m")
	}
	return out.String()
}

func progressTextWidth(text string) int {
	width := 0
	for i := 0; i < len(text); {
		if text[i] == '\x1b' {
			i = skipProgressEscape(text, i)
			continue
		}
		r, size := utf8.DecodeRuneInString(text[i:])
		width += progressRuneWidth(r)
		i += size
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
		for i, b := range data {
			if b == '\r' || b == '\n' {
				if b == '\r' && i+1 < len(data) && data[i+1] == '\n' {
					data = data[i+2:]
				} else {
					data = data[i+1:]
				}
				break
			}
		}
	}
	parts := splitProgressRecords(data)
	lines := make([]string, 0, progressLogLines)
	for _, part := range parts {
		clean := sanitizeProgressLine(part)
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

func splitProgressRecords(data []byte) []string {
	var records []string
	start := 0
	for i := 0; i < len(data); i++ {
		if data[i] != '\r' && data[i] != '\n' {
			continue
		}
		records = append(records, string(data[start:i]))
		if data[i] == '\r' && i+1 < len(data) && data[i+1] == '\n' {
			i++
		}
		start = i + 1
	}
	if start < len(data) {
		records = append(records, string(data[start:]))
	}
	return records
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
