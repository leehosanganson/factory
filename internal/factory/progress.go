package factory

import (
	"fmt"
	"io"
	"time"
)

type stageProgress struct {
	out     io.Writer
	stage   string
	attempt int
	logPath string
	started time.Time
	stop    chan struct{}
	done    chan struct{}
}

func startProgress(out io.Writer, terminal bool, stage string, attempt int, logPath string) *stageProgress {
	p := &stageProgress{out: out, stage: stage, attempt: attempt, logPath: logPath, started: time.Now()}
	if !terminal {
		fmt.Fprintf(out, "Running %s attempt %d/4; log: %s\n", stage, attempt, logPath)
		return p
	}
	p.stop = make(chan struct{})
	p.done = make(chan struct{})
	frames := []rune{'⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'}
	fmt.Fprintf(out, "\r%c %s attempt %d/4 (%s)", frames[0], stage, attempt, time.Since(p.started).Round(time.Second))
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		frame := 1
		for {
			select {
			case <-p.stop:
				return
			case <-ticker.C:
				fmt.Fprintf(out, "\r%c %s attempt %d/4 (%s)", frames[frame%len(frames)], stage, attempt, time.Since(p.started).Round(time.Second))
				frame++
			}
		}
	}()
	return p
}

func (p *stageProgress) finish(err error) {
	elapsed := time.Since(p.started).Round(time.Second)
	if p.stop != nil {
		close(p.stop)
		<-p.done
		fmt.Fprint(p.out, "\r\033[2K")
	}
	result := "completed"
	if err != nil {
		result = "failed"
	}
	fmt.Fprintf(p.out, "%s attempt %d/4 %s in %s; log: %s\n", p.stage, p.attempt, result, elapsed, p.logPath)
}
