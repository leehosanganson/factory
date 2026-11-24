package runtime

import (
	"context"
	"path/filepath"
	"time"

	"github.com/leehosanganson/factory/internal/harness"
	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
	restworkspace "github.com/leehosanganson/factory/internal/workspace"
)

func runRemoteHarnessAttempt(parent, jobCtx context.Context, cancel context.CancelFunc, heartbeat <-chan remoteHeartbeatResult, config restserver.RemoteWorkerConfig, client *remoteWorkerClient, claim remoteWorkerClaim, runner harness.Runner, root, resultsBase string) error {
	timeout, _ := time.ParseDuration(config.Limits.JobTimeout)
	ctx, stop := context.WithTimeout(jobCtx, timeout)
	defer stop()
	job := claim.Job
	thread := job.ThreadID != ""
	id := job.ID
	directory := "tasks"
	if thread {
		if !remoteWorkerJobIDPattern.MatchString(job.ThreadID) || job.ThreadPosition < 1 {
			return finishRemoteWorkerFailure(parent, cancel, heartbeat, config, client, claim)
		}
		id = job.ThreadID
		directory = "threads"
	}
	manager, err := restworkspace.New(restworkspace.Config{RepositoryRoot: root, ResultsRoot: filepath.Join(resultsBase, aliasDirectory(job.Request.Repository), directory)})
	if err != nil {
		return finishRemoteWorkerFailure(parent, cancel, heartbeat, config, client, claim)
	}
	var ws restworkspace.Workspace
	resume := thread && job.ThreadPosition > 1
	if resume {
		ws, err = manager.RetainedWorkspace(id)
	} else {
		ws, err = manager.CreateContext(ctx, id)
	}
	if err != nil {
		return finishRemoteWorkerFailure(parent, cancel, heartbeat, config, client, claim)
	}
	reply, err := runner.Run(ctx, harness.Task{
		WorkDir: ws.WorktreePath, StateDir: ws.StatePath,
		Prompt: job.Request.Task, ReplyLimit: min(config.Limits.HarnessOutputBytes, restjobs.MaxTaskReplyBytes), Resume: resume,
	})
	evidence := restjobs.VerificationEvidence{
		Checks:      []restjobs.VerificationCheck{},
		Limitations: []string{restjobs.LimitationAgentNotVerdict, restjobs.LimitationNoChecksConfigured},
	}
	if thread {
		evidence.Reply, evidence.ReplyTruncated = reply.Text, reply.Truncated
	}
	return finishRemoteExecution(parent, cancel, heartbeat, config, client, claim, evidence, err)
}
