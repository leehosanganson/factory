package factory

import "context"

func (a *cleanRecordingAgent) RunWithContext(ctx context.Context, stage, prompt, task, workdir, log string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.Run(stage, prompt, task, workdir, log)
}

func (a *cleanDestinationRaceAgent) RunWithContext(ctx context.Context, stage, prompt, task, workdir, log string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.Run(stage, prompt, task, workdir, log)
}

func (*cleanNoopAgent) RunWithContext(ctx context.Context, stage, prompt, task, workdir, log string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return (&cleanNoopAgent{}).Run(stage, prompt, task, workdir, log)
}

func (a *cleanRenameOnlyAgent) RunWithContext(ctx context.Context, stage, prompt, task, workdir, log string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.Run(stage, prompt, task, workdir, log)
}

func (a *cleanWriterAgent) RunWithContext(ctx context.Context, stage, prompt, task, workdir, log string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.Run(stage, prompt, task, workdir, log)
}
