package harness

import "context"

// Runner executes a task in its assigned workspace, with optional retained state.
type Runner interface {
	Run(context.Context, Task) (Reply, error)
}

type Task struct {
	WorkDir    string
	StateDir   string
	Prompt     string
	ReplyLimit int
	Resume     bool
}

type Reply struct {
	Text      string
	Truncated bool
}
