// Package restworkspace manages private per-job Git repositories and result
// directories. The results root must be owned by the server account and
// private (0700); processes running as that same UID are trusted operators with
// filesystem control. Successful workspaces are retained until Sweep observes
// protected completion metadata older than the retention period.
package restworkspace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	Retention          = 24 * time.Hour
	markerName         = ".completion.json"
	sourceRevisionName = ".source-revision.json"
)

var jobIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// Config defines trusted server-side roots and injectable test dependencies.
type Config struct {
	RepositoryRoot string
	ResultsRoot    string
	Now            func() time.Time
	RunGit         func(context.Context, string, ...string) error
}

// Workspace contains server-derived per-job paths. Do not accept these paths
// back from a client as authority for filesystem operations.
type Workspace struct {
	JobID          string
	Generation     int64
	WorktreePath   string
	StatePath      string
	OutputPath     string
	SourceRevision SourceRevision
}

// SourceRevision identifies the exact Git source captured for a job.
type SourceRevision struct {
	Commit string
	Tree   string
}

// SweepReport summarizes one fail-closed expiry pass.
type SweepReport struct {
	Scanned  int
	Removed  int
	Retained int
	Errors   []error
}

// Manager owns a repository/results-root pair. Operations are serialized within
// this manager; filesystem ownership and permission checks guard persisted data.
type Manager struct {
	mu        sync.Mutex
	repoRoot  string
	root      string
	rootDev   uint64
	rootIno   uint64
	parentDev uint64
	parentIno uint64
	repoDev   uint64
	repoIno   uint64
	now       func() time.Time
	runGit    func(context.Context, string, ...string) error
}

type sourceRevisionRecord struct {
	Version    int    `json:"version"`
	JobID      string `json:"job_id"`
	Generation int64  `json:"generation,omitempty"`
	Commit     string `json:"commit"`
	Tree       string `json:"tree"`
}

type completion struct {
	Version       int       `json:"version"`
	JobID         string    `json:"job_id"`
	Repository    string    `json:"repository"`
	WorktreeDev   uint64    `json:"worktree_dev"`
	WorktreeInode uint64    `json:"worktree_inode"`
	CompletedAt   time.Time `json:"completed_at"`
}

// New validates the repository and creates (if needed) a protected results
// directory. Configured paths must be absolute and contain no symlink elements.
func New(config Config) (*Manager, error) {
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.RunGit == nil {
		config.RunGit = runGit
	}
	repo, err := canonicalDirectory(config.RepositoryRoot)
	if err != nil {
		return nil, fmt.Errorf("validate repository root: %w", err)
	}
	if err := gitIsRepository(repo); err != nil {
		return nil, fmt.Errorf("validate repository root: %w", err)
	}
	if !filepath.IsAbs(config.ResultsRoot) || filepath.Clean(config.ResultsRoot) != config.ResultsRoot {
		return nil, errors.New("results root must be an absolute clean path")
	}
	if err := mkdirProtected(config.ResultsRoot); err != nil {
		return nil, fmt.Errorf("create results root: %w", err)
	}
	root, err := canonicalDirectory(config.ResultsRoot)
	if err != nil {
		return nil, fmt.Errorf("validate results root: %w", err)
	}
	if err := verifyProtectedDir(filepath.Dir(root)); err != nil {
		return nil, fmt.Errorf("validate server-specific results parent %q: %w", filepath.Dir(root), err)
	}
	if err := verifyProtectedDir(root); err != nil {
		return nil, fmt.Errorf("validate results root: %w", err)
	}
	rootDev, rootInode, err := directoryIdentity(root)
	if err != nil {
		return nil, fmt.Errorf("validate results root identity: %w", err)
	}
	parentDev, parentInode, err := directoryIdentity(filepath.Dir(root))
	if err != nil {
		return nil, fmt.Errorf("validate results parent identity: %w", err)
	}
	repoDev, repoInode, err := directoryIdentity(repo)
	if err != nil {
		return nil, fmt.Errorf("validate repository identity: %w", err)
	}
	return &Manager{
		repoRoot: repo, root: root, rootDev: rootDev, rootIno: rootInode,
		parentDev: parentDev, parentIno: parentInode, repoDev: repoDev, repoIno: repoInode,
		now: config.Now, runGit: config.RunGit,
	}, nil
}

// RepositoryRoot returns the canonical trusted checkout root owned by this manager.
func (m *Manager) RepositoryRoot() string { return m.repoRoot }

// Create creates a private repository at the checkout's current source revision.
func (m *Manager) Create(jobID string) (Workspace, error) {
	return m.CreateContext(context.Background(), jobID)
}

// CreateContext captures the current checkout and materializes that revision.
func (m *Manager) CreateContext(ctx context.Context, jobID string) (Workspace, error) {
	revision, err := m.CaptureContext(ctx)
	if err != nil {
		return Workspace{}, err
	}
	return m.CreateRevisionContext(ctx, jobID, revision)
}

// CaptureContext records the current commit and tree and rejects source trees
// that need Git submodule or checkout-filter processing.
func (m *Manager) CaptureContext(ctx context.Context) (SourceRevision, error) {
	if ctx == nil {
		return SourceRevision{}, errors.New("creation context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return SourceRevision{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.verifyRepositoryContext(ctx); err != nil {
		return SourceRevision{}, err
	}
	commit, err := gitOutputContext(ctx, m.repoRoot, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil || !validGitObjectID(commit) {
		return SourceRevision{}, errors.New("capture repository source revision")
	}
	tree, err := gitOutputContext(ctx, m.repoRoot, "rev-parse", "--verify", commit+"^{tree}")
	if err != nil || !validGitObjectID(tree) {
		return SourceRevision{}, errors.New("capture repository source tree")
	}
	if err := validateSourceTree(ctx, m.repoRoot, commit); err != nil {
		return SourceRevision{}, err
	}
	if err := m.verifyRepositoryContext(ctx); err != nil {
		return SourceRevision{}, err
	}
	return SourceRevision{Commit: commit, Tree: tree}, nil
}

// CreateRevisionContext materializes a previously captured tree in a private
// repository, without a linked-worktree dependency on the configured checkout.
// Partial job directories are retained on failure for fail-closed recovery.
func (m *Manager) CreateRevisionContext(ctx context.Context, jobID string, revision SourceRevision) (Workspace, error) {
	return m.createRevisionContext(ctx, jobID, 0, revision)
}

// CreateAttemptRevisionContext creates an exclusive workspace for one positive
// execution generation. Existing attempt directories are never reused.
func (m *Manager) CreateAttemptRevisionContext(ctx context.Context, jobID string, generation int64, revision SourceRevision) (Workspace, error) {
	if generation <= 0 {
		return Workspace{}, errors.New("invalid attempt generation")
	}
	return m.createRevisionContext(ctx, jobID, generation, revision)
}

func (m *Manager) createRevisionContext(ctx context.Context, jobID string, generation int64, revision SourceRevision) (Workspace, error) {
	if ctx == nil {
		return Workspace{}, errors.New("creation context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return Workspace{}, err
	}
	if !validJobID(jobID) || generation < 0 || !validGitObjectID(revision.Commit) || !validGitObjectID(revision.Tree) {
		return Workspace{}, errors.New("invalid job, generation, or source revision")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Workspace{}, err
	}
	if err := m.verifyRepository(); err != nil {
		return Workspace{}, err
	}
	if err := m.verifyRoot(); err != nil {
		return Workspace{}, err
	}
	if err := ctx.Err(); err != nil {
		return Workspace{}, err
	}
	jobDir := filepath.Join(m.root, jobID)
	if generation == 0 {
		if err := os.Mkdir(jobDir, 0o700); err != nil {
			return Workspace{}, fmt.Errorf("create job result directory: %w", err)
		}
	} else {
		if err := mkdirProtected(jobDir); err != nil {
			return Workspace{}, fmt.Errorf("create job result directory: %w", err)
		}
		attemptsDir := filepath.Join(jobDir, "attempts")
		if err := mkdirProtected(attemptsDir); err != nil {
			return Workspace{}, fmt.Errorf("create attempt result directory: %w", err)
		}
		jobDir = filepath.Join(attemptsDir, fmt.Sprint(generation))
		if err := os.Mkdir(jobDir, 0o700); err != nil {
			return Workspace{}, fmt.Errorf("create exclusive attempt result directory: %w", err)
		}
	}
	workspace := Workspace{
		JobID: jobID, Generation: generation, WorktreePath: filepath.Join(jobDir, "worktree"), SourceRevision: revision,
		StatePath: filepath.Join(jobDir, "state"), OutputPath: filepath.Join(jobDir, "output"),
	}
	for _, path := range []string{workspace.StatePath, workspace.OutputPath} {
		if err := os.Mkdir(path, 0o700); err != nil {
			return Workspace{}, fmt.Errorf("create private job directory: %w", err)
		}
	}
	if err := verifyProtectedDir(jobDir); err != nil {
		return Workspace{}, fmt.Errorf("validate job result directory: %w", err)
	}
	actualTree, err := gitOutputContext(ctx, m.repoRoot, "rev-parse", "--verify", revision.Commit+"^{tree}")
	if err != nil || actualTree != revision.Tree {
		return Workspace{}, errors.New("source revision is unavailable")
	}
	if err := validateSourceTree(ctx, m.repoRoot, revision.Commit); err != nil {
		return Workspace{}, err
	}
	if err := m.verifyRepositoryContext(ctx); err != nil {
		return Workspace{}, err
	}
	if err := ctx.Err(); err != nil {
		return Workspace{}, err
	}
	if err := m.runGit(ctx, m.repoRoot, "init", "--quiet", "--template="+workspace.StatePath, workspace.WorktreePath); err != nil {
		return Workspace{}, fmt.Errorf("initialize private job repository: %w", err)
	}
	if err := m.runGit(ctx, workspace.WorktreePath, "-c", "protocol.file.allow=always", "fetch", "--no-tags", "--no-recurse-submodules", "--depth=1", m.repoRoot, revision.Commit); err != nil {
		return Workspace{}, fmt.Errorf("transfer captured job source: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Workspace{}, fmt.Errorf("transfer captured job source: %w", err)
	}
	if err := m.runGit(ctx, workspace.WorktreePath, "-c", "core.autocrlf=false", "-c", "core.hooksPath="+workspace.StatePath, "checkout", "--detach", "--force", "FETCH_HEAD"); err != nil {
		return Workspace{}, fmt.Errorf("materialize captured job source: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Workspace{}, fmt.Errorf("materialize captured job source: %w", err)
	}
	if err := m.verifyRepositoryContext(ctx); err != nil {
		return Workspace{}, err
	}
	if err := verifyPrivateRepository(workspace.WorktreePath); err != nil {
		return Workspace{}, fmt.Errorf("validate private job repository: %w", err)
	}
	if got, err := gitOutputContext(ctx, workspace.WorktreePath, "rev-parse", "HEAD^{tree}"); err != nil || got != revision.Tree {
		return Workspace{}, errors.New("materialized job source tree does not match captured revision")
	}
	if err := m.verifySameDevice(jobDir, workspace.StatePath, workspace.OutputPath, workspace.WorktreePath); err != nil {
		return Workspace{}, err
	}
	if err := m.writeSourceRevision(jobDir, jobID, generation, revision); err != nil {
		return Workspace{}, err
	}
	workspace.SourceRevision = revision
	return workspace, nil
}

// VerifyResultDirectory accepts only the exact direct job directory beneath the
// managed results root and validates its protected filesystem identity.
func (m *Manager) VerifyResultDirectory(path string) error {
	if !m.validResultDirectory(path) {
		return errors.New("result directory is outside the managed root")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.verifyRoot(); err != nil {
		return err
	}
	if filepath.Dir(path) != m.root {
		for _, parent := range []string{filepath.Dir(filepath.Dir(path)), filepath.Dir(path)} {
			if err := verifyProtectedDir(parent); err != nil {
				return err
			}
		}
	}
	return verifyProtectedDir(path)
}

func (m *Manager) validResultDirectory(path string) bool {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false
	}
	if filepath.Dir(path) == m.root {
		return validJobID(filepath.Base(path))
	}
	generation, err := strconv.ParseInt(filepath.Base(path), 10, 64)
	attemptsDir := filepath.Dir(path)
	jobDir := filepath.Dir(attemptsDir)
	return err == nil && generation > 0 && strconv.FormatInt(generation, 10) == filepath.Base(path) && filepath.Base(attemptsDir) == "attempts" && filepath.Dir(jobDir) == m.root && validJobID(filepath.Base(jobDir))
}

// RetainedWorkspace returns the validated private repository or legacy linked
// worktree for read-only operator reconciliation.
func (m *Manager) RetainedWorkspace(jobID string) (Workspace, error) {
	return m.retainedWorkspace(jobID, 0)
}

// RetainedWorkspaceForGeneration selects only the exact persisted attempt
// directory. It never falls back to another generation or the legacy path.
func (m *Manager) RetainedWorkspaceForGeneration(jobID string, generation int64) (Workspace, error) {
	if generation <= 0 {
		return Workspace{}, errors.New("invalid attempt generation")
	}
	return m.retainedWorkspace(jobID, generation)
}

func (m *Manager) retainedWorkspace(jobID string, generation int64) (Workspace, error) {
	if !validJobID(jobID) {
		return Workspace{}, errors.New("invalid job ID")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.verifyRepository(); err != nil {
		return Workspace{}, err
	}
	if err := m.verifyRoot(); err != nil {
		return Workspace{}, err
	}
	jobDir := filepath.Join(m.root, jobID)
	if generation > 0 {
		jobDir = filepath.Join(jobDir, "attempts", strconv.FormatInt(generation, 10))
	}
	if generation > 0 {
		for _, parent := range []string{filepath.Dir(filepath.Dir(jobDir)), filepath.Dir(jobDir)} {
			if err := verifyProtectedDir(parent); err != nil {
				return Workspace{}, errors.New("retained job workspace is unavailable")
			}
		}
	}
	if err := verifyProtectedDir(jobDir); err != nil {
		return Workspace{}, errors.New("retained job workspace is unavailable")
	}
	if err := verifyExpectedWorkspace(jobDir); err != nil {
		return Workspace{}, errors.New("retained job workspace is unavailable")
	}
	worktree := filepath.Join(jobDir, "worktree")
	if err := verifyManagedRepository(m.repoRoot, worktree); err != nil {
		return Workspace{}, errors.New("retained job worktree is unavailable")
	}
	revision, err := m.readSourceRevision(jobDir, jobID, generation, worktree)
	if err != nil {
		return Workspace{}, errors.New("retained job source identity is unavailable")
	}
	return Workspace{JobID: jobID, Generation: generation, WorktreePath: worktree, StatePath: filepath.Join(jobDir, "state"), OutputPath: filepath.Join(jobDir, "output"), SourceRevision: revision}, nil
}

// MarkSucceeded atomically records trusted completion metadata with mode 0600.
// Failed/canceled jobs must not call this method and therefore remain retained.
func (m *Manager) MarkSucceeded(jobID string) error {
	if !validJobID(jobID) {
		return errors.New("invalid job ID")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.verifyRepository(); err != nil {
		return err
	}
	if err := m.verifyRoot(); err != nil {
		return err
	}
	jobDir := filepath.Join(m.root, jobID)
	if err := verifyProtectedDir(jobDir); err != nil {
		return fmt.Errorf("validate job result directory: %w", err)
	}
	if err := verifyExpectedWorkspace(jobDir); err != nil {
		return err
	}
	worktree := filepath.Join(jobDir, "worktree")
	if err := verifyPrivateRepository(worktree); err != nil {
		return err
	}
	if err := m.verifyRepository(); err != nil {
		return err
	}
	dev, inode, err := directoryIdentity(worktree)
	if err != nil {
		return err
	}
	marker := completion{Version: 1, JobID: jobID, Repository: m.repoRoot, WorktreeDev: dev, WorktreeInode: inode, CompletedAt: m.now().UTC()}
	data, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode completion metadata: %w", err)
	}
	data = append(data, '\n')
	markerPath := filepath.Join(jobDir, markerName)
	if err := m.verifyRepository(); err != nil {
		return err
	}
	tmp, err := os.OpenFile(filepath.Join(jobDir, ".completion.tmp"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create completion metadata: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := m.verifyRepository(); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write completion metadata: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync completion metadata: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close completion metadata: %w", err)
	}
	if err := verifyProtectedDir(jobDir); err != nil {
		return fmt.Errorf("revalidate job result directory: %w", err)
	}
	if err := m.verifyRepository(); err != nil {
		return err
	}
	if err := os.Link(tmpName, markerPath); err != nil {
		return fmt.Errorf("publish completion metadata: %w", err)
	}
	if err := os.Remove(tmpName); err != nil {
		return fmt.Errorf("finalize completion metadata: %w", err)
	}
	return verifyProtectedFile(markerPath)
}

// Sweep removes only direct job directories with valid success metadata strictly
// older than 24 hours. Every uncertain, malformed, failed, canceled, orphaned,
// or unsafe item is retained and represented in the report.
func (m *Manager) Sweep() SweepReport {
	return m.SweepContext(context.Background())
}

// SweepContext removes expired workspaces until ctx is canceled. Git cleanup
// operations receive ctx; interrupted or uncertain cleanup retains the item.
func (m *Manager) SweepContext(ctx context.Context) SweepReport {
	if ctx == nil {
		return SweepReport{Errors: []error{errors.New("sweep context must not be nil")}}
	}
	if err := lockContext(ctx, &m.mu); err != nil {
		return SweepReport{Errors: []error{errors.New("workspace sweep canceled before lock")}}
	}
	defer m.mu.Unlock()
	report := SweepReport{}
	if ctx.Err() != nil {
		report.Errors = append(report.Errors, errors.New("workspace sweep canceled"))
		return report
	}
	if err := m.verifyRoot(); err != nil {
		report.Errors = append(report.Errors, errors.New("managed results root failed validation"))
		return report
	}
	entries, err := os.ReadDir(m.root)
	if err != nil {
		report.Errors = append(report.Errors, errors.New("cannot read managed results root"))
		return report
	}
	cutoff := m.now().Add(-Retention)
	for _, entry := range entries {
		if ctx.Err() != nil {
			report.Retained += len(entries) - report.Scanned
			break
		}
		report.Scanned++
		name := entry.Name()
		if !validJobID(name) || !entry.IsDir() {
			report.Retained++
			report.Errors = append(report.Errors, errors.New("retained unrecognized results-root entry"))
			continue
		}
		jobDir := filepath.Join(m.root, name)
		if _, err := os.Lstat(filepath.Join(jobDir, "attempts")); err == nil {
			report.Retained++
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			report.Retained++
			report.Errors = append(report.Errors, errors.New("retained job with invalid attempt directory"))
			continue
		}
		metadata, err := m.readCompletion(jobDir, name)
		if err != nil || !metadata.CompletedAt.Before(cutoff) {
			report.Retained++
			if err != nil {
				report.Errors = append(report.Errors, errors.New("retained job with invalid completion metadata"))
			}
			continue
		}
		if err := m.removeExpired(ctx, jobDir, name, metadata); err != nil {
			report.Retained++
			report.Errors = append(report.Errors, errors.New("retained expired job after cleanup safety check failed"))
			continue
		}
		report.Removed++
	}
	return report
}

func lockContext(ctx context.Context, mutex *sync.Mutex) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if mutex.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (m *Manager) verifySameDevice(paths ...string) error {
	rootDev, _, err := directoryIdentity(m.root)
	if err != nil {
		return errors.New("cannot verify managed filesystem device")
	}
	for _, path := range paths {
		dev, _, err := directoryIdentity(path)
		if err != nil || dev != rootDev {
			return errors.New("managed workspace crosses filesystem devices")
		}
	}
	return nil
}

func (m *Manager) verifyRepository() error {
	return m.verifyRepositoryContext(context.Background())
}

func (m *Manager) verifyRepositoryContext(ctx context.Context) error {
	if ctx.Err() != nil || !identityMatches(m.repoRoot, m.repoDev, m.repoIno) {
		return errors.New("configured repository identity changed")
	}
	if err := gitIsRepositoryContext(ctx, m.repoRoot); err != nil || ctx.Err() != nil || !identityMatches(m.repoRoot, m.repoDev, m.repoIno) {
		return errors.New("configured repository identity changed")
	}
	return nil
}

func (m *Manager) verifyRoot() error {
	if err := verifyProtectedDir(filepath.Dir(m.root)); err != nil || !identityMatches(filepath.Dir(m.root), m.parentDev, m.parentIno) {
		return errors.New("managed server-specific results parent failed validation")
	}
	if err := verifyProtectedDir(m.root); err != nil {
		return errors.New("managed results root failed validation")
	}
	resolved, err := filepath.EvalSymlinks(m.root)
	if err != nil || resolved != m.root || !identityMatches(m.root, m.rootDev, m.rootIno) {
		return errors.New("managed results root changed")
	}
	return nil
}

func (m *Manager) readCompletion(jobDir, jobID string) (completion, error) {
	if err := verifyProtectedDir(jobDir); err != nil {
		return completion{}, err
	}
	if err := verifyExpectedWorkspace(jobDir); err != nil {
		return completion{}, err
	}
	if err := m.verifySameDevice(jobDir, filepath.Join(jobDir, "state"), filepath.Join(jobDir, "output"), filepath.Join(jobDir, "worktree")); err != nil {
		return completion{}, err
	}
	return m.readMarker(jobDir, jobID)
}

func (m *Manager) writeSourceRevision(jobDir, jobID string, generation int64, revision SourceRevision) error {
	if err := verifyProtectedDir(jobDir); err != nil {
		return errors.New("validate job result directory before source identity write")
	}
	record := sourceRevisionRecord{Version: 1, JobID: jobID, Generation: generation, Commit: revision.Commit, Tree: revision.Tree}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode source identity: %w", err)
	}
	data = append(data, '\n')
	tmpPath := filepath.Join(jobDir, sourceRevisionName+".tmp")
	tmp, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("create source identity: %w", err)
	}
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write source identity: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync source identity: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close source identity: %w", err)
	}
	if err := verifyProtectedDir(jobDir); err != nil {
		return errors.New("revalidate job result directory before source identity publish")
	}
	path := filepath.Join(jobDir, sourceRevisionName)
	if err := os.Link(tmpPath, path); err != nil {
		return fmt.Errorf("publish source identity: %w", err)
	}
	if err := os.Remove(tmpPath); err != nil {
		return fmt.Errorf("finalize source identity: %w", err)
	}
	if err := verifyProtectedFile(path); err != nil {
		return err
	}
	if err := syncDirectory(jobDir); err != nil {
		return fmt.Errorf("sync source identity directory: %w", err)
	}
	return nil
}

func (m *Manager) readSourceRevision(jobDir, jobID string, generation int64, worktree string) (SourceRevision, error) {
	path := filepath.Join(jobDir, sourceRevisionName)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := verifyLegacyLinkedWorktree(m.repoRoot, worktree); err != nil {
			return SourceRevision{}, errors.New("source identity is missing from a private job repository")
		}
		return SourceRevision{}, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || !ownedByEffectiveUser(info) {
		return SourceRevision{}, errors.New("source identity file is not a private regular file")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return SourceRevision{}, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || opened.Mode().Perm() != 0o600 || !ownedByEffectiveUser(opened) || !sameFileIdentity(info, opened) {
		return SourceRevision{}, errors.New("source identity file changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil || len(data) > 4096 {
		return SourceRevision{}, errors.New("source identity file is invalid or too large")
	}
	var record sourceRevisionRecord
	if err := decodeUniqueJSON(data, &record); err != nil {
		return SourceRevision{}, err
	}
	if record.Version != 1 || record.JobID != jobID || record.Generation != generation || !validJobID(record.JobID) || !validGitObjectID(record.Commit) || !validGitObjectID(record.Tree) || len(record.Commit) != len(record.Tree) {
		return SourceRevision{}, errors.New("source identity does not match retained job")
	}
	actualTree, err := gitOutput(worktree, "rev-parse", "--verify", record.Commit+"^{tree}")
	if err != nil || actualTree != record.Tree {
		return SourceRevision{}, errors.New("source identity commit and tree do not match")
	}
	if _, err := gitOutput(worktree, "merge-base", "--is-ancestor", record.Commit, "HEAD"); err != nil {
		return SourceRevision{}, errors.New("retained worktree is not descended from source identity")
	}
	return SourceRevision{Commit: record.Commit, Tree: record.Tree}, nil
}

func decodeUniqueJSON(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return errors.New("source identity must be a JSON object")
	}
	seen := make(map[string]bool, 4)
	allowed := map[string]bool{"version": true, "job_id": true, "generation": true, "commit": true, "tree": true}
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || !allowed[key] || seen[key] {
			return errors.New("source identity has an invalid, unknown, or duplicate field")
		}
		seen[key] = true
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return errors.New("source identity has an invalid field value")
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return errors.New("source identity object is incomplete")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("source identity has trailing data")
	}
	strict := json.NewDecoder(bytes.NewReader(data))
	strict.DisallowUnknownFields()
	if err := strict.Decode(destination); err != nil {
		return errors.New("source identity has unknown or invalid fields")
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer dir.Close()
	info, err := dir.Stat()
	if err != nil || !info.IsDir() {
		return errors.New("source identity parent is not a directory")
	}
	return dir.Sync()
}

func (m *Manager) readMarker(jobDir, jobID string) (completion, error) {
	if err := verifyProtectedDir(jobDir); err != nil {
		return completion{}, err
	}
	path := filepath.Join(jobDir, markerName)
	if err := verifyProtectedFile(path); err != nil {
		return completion{}, err
	}
	data, err := readProtectedFile(path)
	if err != nil || len(data) > 4096 {
		return completion{}, errors.New("invalid completion metadata file")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var metadata completion
	if err := decoder.Decode(&metadata); err != nil {
		return completion{}, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return completion{}, errors.New("trailing completion metadata")
	}
	if metadata.Version != 1 || metadata.JobID != jobID || metadata.Repository != m.repoRoot || metadata.WorktreeDev == 0 || metadata.WorktreeInode == 0 || metadata.CompletedAt.IsZero() || metadata.CompletedAt.Location() != time.UTC {
		return completion{}, errors.New("completion metadata identity mismatch")
	}
	return metadata, nil
}

func (m *Manager) removeExpired(ctx context.Context, jobDir, jobID string, metadata completion) error {
	current, err := m.readCompletion(jobDir, jobID)
	if err != nil || !current.CompletedAt.Equal(metadata.CompletedAt) || current.Repository != metadata.Repository {
		return errors.New("completion metadata changed")
	}
	jobDev, jobInode, err := directoryIdentity(jobDir)
	if err != nil {
		return err
	}
	stateDev, stateInode, err := directoryIdentity(filepath.Join(jobDir, "state"))
	if err != nil {
		return err
	}
	outputDev, outputInode, err := directoryIdentity(filepath.Join(jobDir, "output"))
	if err != nil {
		return err
	}
	markerInfo, err := os.Lstat(filepath.Join(jobDir, markerName))
	if err != nil || verifyProtectedFile(filepath.Join(jobDir, markerName)) != nil {
		return errors.New("completion metadata changed")
	}
	worktree := filepath.Join(jobDir, "worktree")
	if err := verifyPrivateRepository(worktree); err != nil {
		return err
	}
	dev, inode, err := directoryIdentity(worktree)
	if err != nil || dev != metadata.WorktreeDev || inode != metadata.WorktreeInode {
		return errors.New("worktree identity changed")
	}
	// Revalidate the repository, root, job ID, metadata, and workspace before
	// removing the job's private repository.
	if err := m.verifyRoot(); err != nil {
		return err
	}
	if err := m.verifyRepositoryContext(ctx); err != nil {
		return err
	}
	current, err = m.readMarker(jobDir, jobID)
	if err != nil || current.JobID != jobID || current.Repository != m.repoRoot || !current.CompletedAt.Equal(metadata.CompletedAt) {
		return errors.New("job identity changed before worktree cleanup")
	}
	if !identityMatches(jobDir, jobDev, jobInode) || !samePathIdentity(filepath.Join(jobDir, markerName), markerInfo) {
		return errors.New("job identity changed before worktree cleanup")
	}
	dev, inode, err = directoryIdentity(worktree)
	if err != nil || dev != metadata.WorktreeDev || inode != metadata.WorktreeInode || verifyPrivateRepository(worktree) != nil {
		return errors.New("worktree identity changed before cleanup")
	}
	if err := m.runGit(ctx, worktree, "rev-parse", "--show-toplevel"); err != nil || ctx.Err() != nil {
		return errors.New("private job repository cleanup validation failed")
	}
	if err := removeOwnedDirectory(worktree, dev, inode); err != nil || ctx.Err() != nil {
		return errors.New("private job repository cleanup failed")
	}
	if err := m.verifyRoot(); err != nil || !identityMatches(jobDir, jobDev, jobInode) {
		return errors.New("job result directory changed after worktree cleanup")
	}
	if err := m.verifyRepositoryContext(ctx); err != nil {
		return err
	}
	if !identityMatches(filepath.Join(jobDir, "state"), stateDev, stateInode) || !identityMatches(filepath.Join(jobDir, "output"), outputDev, outputInode) {
		return errors.New("private job directory identity changed after worktree cleanup")
	}
	current, err = m.readMarker(jobDir, jobID)
	if err != nil || current.JobID != jobID || current.Repository != m.repoRoot || !current.CompletedAt.Equal(metadata.CompletedAt) || !samePathIdentity(filepath.Join(jobDir, markerName), markerInfo) {
		return errors.New("job identity changed after worktree cleanup")
	}
	if err := removeOwnedDirectory(filepath.Join(jobDir, "state"), stateDev, stateInode); err != nil {
		return errors.New("private state cleanup failed")
	}
	if err := removeOwnedDirectory(filepath.Join(jobDir, "output"), outputDev, outputInode); err != nil {
		return errors.New("private output cleanup failed")
	}
	if err := removeOwnedFile(filepath.Join(jobDir, markerName), markerInfo); err != nil {
		return errors.New("completion metadata cleanup failed")
	}
	if err := removeOwnedDirectory(jobDir, jobDev, jobInode); err != nil {
		return errors.New("job result directory is not empty or changed")
	}
	return nil
}

func identityMatches(path string, dev, inode uint64) bool {
	gotDev, gotInode, err := directoryIdentity(path)
	return err == nil && gotDev == dev && gotInode == inode
}

func samePathIdentity(path string, expected os.FileInfo) bool {
	current, err := os.Lstat(path)
	return err == nil && sameFileIdentity(current, expected)
}

func removeOwnedDirectory(path string, dev, inode uint64) error {
	if !identityMatches(path, dev, inode) {
		return errors.New("directory identity changed")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.IsDir() {
		return errors.New("opened path is not a directory")
	}
	openedDev, openedInode, err := fileIdentity(opened)
	if err != nil || openedDev != dev || openedInode != inode || !identityMatches(path, dev, inode) {
		return errors.New("directory identity changed while opening")
	}
	entries, err := file.ReadDir(-1)
	if err != nil || !identityMatches(path, dev, inode) {
		return errors.New("directory changed while enumerating")
	}
	for _, entry := range entries {
		child := filepath.Join(path, entry.Name())
		expected, err := os.Lstat(child)
		if err != nil || !identityMatches(path, dev, inode) {
			return errors.New("directory entry changed during cleanup")
		}
		if err := removeOwnedEntry(child, expected); err != nil {
			return err
		}
		if !identityMatches(path, dev, inode) {
			return errors.New("directory identity changed during cleanup")
		}
	}
	if !identityMatches(path, dev, inode) {
		return errors.New("directory identity changed before removal")
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return errors.New("directory reappeared after removal")
	}
	return nil
}

func removeOwnedEntry(path string, expected os.FileInfo) error {
	info, err := os.Lstat(path)
	if err != nil || !sameFileIdentity(info, expected) {
		return errors.New("directory entry identity changed")
	}
	if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
		dev, inode, err := fileIdentity(expected)
		if err != nil {
			return err
		}
		return removeOwnedDirectory(path, dev, inode)
	}
	if !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
		return errors.New("refusing to remove special filesystem entry")
	}
	if info.Mode()&os.ModeSymlink == 0 {
		file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return err
		}
		opened, statErr := file.Stat()
		closeErr := file.Close()
		if statErr != nil || closeErr != nil || !sameFileIdentity(info, opened) {
			return errors.New("file identity changed while opening")
		}
	}
	if !samePathIdentity(path, info) {
		return errors.New("entry identity changed before removal")
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return errors.New("entry reappeared after removal")
	}
	return nil
}

func removeOwnedFile(path string, expected os.FileInfo) error {
	if !samePathIdentity(path, expected) {
		return errors.New("file identity changed")
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return errors.New("file reappeared after removal")
	}
	return nil
}

func fileIdentity(info os.FileInfo) (uint64, uint64, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Ino == 0 {
		return 0, 0, errors.New("filesystem does not expose file identity")
	}
	return uint64(stat.Dev), uint64(stat.Ino), nil
}

func verifyExpectedWorkspace(jobDir string) error {
	for _, name := range []string{"state", "output"} {
		if err := verifyProtectedDir(filepath.Join(jobDir, name)); err != nil {
			return fmt.Errorf("invalid managed %s directory", name)
		}
	}
	if err := verifyWorktreeDir(filepath.Join(jobDir, "worktree")); err != nil {
		return errors.New("invalid managed worktree directory")
	}
	return nil
}

func validJobID(id string) bool { return jobIDPattern.MatchString(id) }

func validateSourceTree(ctx context.Context, root, commit string) error {
	listing, err := gitRawOutputContext(ctx, root, "ls-tree", "-r", "-z", "--full-tree", commit)
	if err != nil {
		return errors.New("inspect captured repository source")
	}
	paths := make([]byte, 0, len(listing))
	for _, entry := range bytes.Split(listing, []byte{0}) {
		if len(entry) == 0 {
			continue
		}
		metadata, path, ok := bytes.Cut(entry, []byte{'\t'})
		if !ok {
			return errors.New("invalid captured repository tree")
		}
		fields := strings.Fields(string(metadata))
		if len(fields) != 3 {
			return errors.New("invalid captured repository tree")
		}
		if fields[0] == "160000" {
			return errors.New("repository source contains an unsupported submodule")
		}
		paths = append(paths, path...)
		paths = append(paths, 0)
	}
	if len(paths) == 0 {
		return nil
	}
	lfsCommand := exec.CommandContext(ctx, "git", "-C", root, "grep", "--no-textconv", "-I", "-l", "-F", "-z", "-e", "version https://git-lfs.github.com/spec/v1", commit)
	lfsCommand.Env = gitEnvironment(os.Environ())
	var lfsMatches bytes.Buffer
	lfsCommand.Stdout = &lfsMatches
	lfsErr := lfsCommand.Run()
	if lfsErr != nil {
		if exitErr, ok := lfsErr.(*exec.ExitError); !ok || exitErr.ExitCode() != 1 {
			return errors.New("inspect captured repository Git LFS objects")
		}
	}
	for _, match := range bytes.Split(lfsMatches.Bytes(), []byte{0}) {
		if len(match) == 0 {
			continue
		}
		prefix := append([]byte(commit), ':')
		if !bytes.HasPrefix(match, prefix) {
			return errors.New("invalid captured Git LFS path")
		}
		path := string(match[len(prefix):])
		pointer, err := gitRawOutputContext(ctx, root, "cat-file", "blob", commit+":"+path)
		if err != nil {
			return errors.New("inspect captured repository Git LFS object")
		}
		if isGitLFSPointer(pointer) {
			return errors.New("repository source contains an unsupported Git LFS pointer")
		}
	}
	command := exec.CommandContext(ctx, "git", "-C", root, "check-attr", "--source", commit, "-z", "--stdin", "filter", "working-tree-encoding", "eol", "text", "ident")
	command.Env = gitSourceEnvironment(os.Environ())
	command.Stdin = bytes.NewReader(paths)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return errors.New("inspect captured repository checkout attributes")
	}
	attributes := bytes.Split(stdout.Bytes(), []byte{0})
	if len(attributes)%3 != 1 {
		return errors.New("invalid repository checkout attributes")
	}
	for index := 0; index+2 < len(attributes); index += 3 {
		name, value := string(attributes[index+1]), string(attributes[index+2])
		if value == "unspecified" || value == "unset" {
			continue
		}
		switch name {
		case "filter":
			return errors.New("repository source uses an unsupported checkout filter")
		case "working-tree-encoding", "eol", "text", "ident":
			return errors.New("repository source uses unsupported checkout transformations")
		default:
			return errors.New("repository source has unsupported checkout attributes")
		}
	}
	return nil
}

func isGitLFSPointer(content []byte) bool {
	if len(content) > 1024 || !bytes.HasPrefix(content, []byte("version https://git-lfs.github.com/spec/v1\n")) {
		return false
	}
	lines := strings.Split(string(content), "\n")
	if len(lines) < 4 || !strings.HasPrefix(lines[1], "oid sha256:") || !strings.HasPrefix(lines[2], "size ") || lines[len(lines)-1] != "" {
		return false
	}
	for _, extension := range lines[3 : len(lines)-1] {
		if !strings.HasPrefix(extension, "ext-") || !strings.Contains(extension, " ") {
			return false
		}
	}
	digest := strings.TrimPrefix(lines[1], "oid sha256:")
	if len(digest) != 64 || strings.Trim(digest, "0123456789abcdef") != "" {
		return false
	}
	size := strings.TrimPrefix(lines[2], "size ")
	if size == "" || strings.Trim(size, "0123456789") != "" {
		return false
	}
	return true
}

func verifyManagedRepository(repositoryRoot, path string) error {
	if err := verifyPrivateRepository(path); err == nil {
		return nil
	}
	if err := registeredWorktree(repositoryRoot, path); err != nil {
		return errors.New("job repository is not managed by the configured checkout")
	}
	return verifyWorktreeDir(path)
}

func verifyPrivateRepository(path string) error {
	if err := verifyWorktreeDir(path); err != nil {
		return err
	}
	info, err := os.Lstat(filepath.Join(path, ".git"))
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("job repository is not private")
	}
	reported, err := gitOutput(path, "rev-parse", "--show-toplevel")
	if err != nil || reported != path {
		return errors.New("job repository is not an independent checkout")
	}
	gitDir, err := gitOutput(path, "rev-parse", "--absolute-git-dir")
	if err != nil || filepath.Clean(gitDir) != filepath.Join(path, ".git") {
		return errors.New("job repository shares Git metadata")
	}
	return nil
}

func validGitObjectID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, r := range value {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

func canonicalDirectory(path string) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", errors.New("path must be absolute and clean")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() {
		return "", errors.New("path must be an existing directory")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return "", errors.New("path or an ancestor uses a symlink")
	}
	return path, nil
}

func mkdirProtected(path string) error {
	volume := filepath.VolumeName(path)
	current := volume + string(filepath.Separator)
	if filepath.Clean(path) == current {
		return errors.New("results root cannot be a filesystem root")
	}
	parts := strings.Split(strings.TrimPrefix(path, current), string(filepath.Separator))
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return errors.New("results root must be beneath a server-specific parent")
	}
	for _, part := range parts[:len(parts)-1] {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(current, 0o700); err != nil {
				return err
			}
			continue
		}
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("results root path contains a non-directory or symlink")
		}
	}
	if err := verifyProtectedDir(current); err != nil {
		return errors.New("results root parent is not a private server-specific directory")
	}
	rootInfo, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return os.Mkdir(path, 0o700)
	}
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return errors.New("results root path contains a non-directory or symlink")
	}
	return verifyProtectedDir(path)
}

func verifyProtectedDir(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || !ownedByEffectiveUser(info) {
		return errors.New("directory is not a protected owned directory")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return errors.New("directory path is not canonical")
	}
	return nil
}

func verifyWorktreeDir(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !ownedByEffectiveUser(info) {
		return errors.New("worktree is not an owned directory")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return errors.New("worktree path is not canonical")
	}
	return nil
}

func directoryIdentity(path string) (uint64, uint64, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return 0, 0, errors.New("cannot verify directory identity")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Ino == 0 {
		return 0, 0, errors.New("filesystem does not expose directory identity")
	}
	return uint64(stat.Dev), uint64(stat.Ino), nil
}

func readProtectedFile(path string) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || opened.Mode().Perm()&0o077 != 0 || !ownedByEffectiveUser(opened) {
		return nil, errors.New("opened metadata is not a protected owned regular file")
	}
	pathInfo, err := os.Lstat(path)
	if err != nil || !sameFileIdentity(opened, pathInfo) {
		return nil, errors.New("metadata path changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil || len(data) > 4096 {
		return nil, errors.New("metadata exceeds size limit or could not be read")
	}
	return data, nil
}

func verifyProtectedFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || !ownedByEffectiveUser(info) {
		return errors.New("file is not a protected owned regular file")
	}
	return nil
}

func sameFileIdentity(a, b os.FileInfo) bool {
	statA, okA := a.Sys().(*syscall.Stat_t)
	statB, okB := b.Sys().(*syscall.Stat_t)
	return okA && okB && statA.Dev == statB.Dev && statA.Ino == statB.Ino
}

func ownedByEffectiveUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func verifyLegacyLinkedWorktree(root, path string) error {
	info, err := os.Lstat(filepath.Join(path, ".git"))
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("job repository does not have linked-worktree metadata")
	}
	if err := registeredWorktree(root, path); err != nil {
		return err
	}
	commonDir, err := gitOutput(path, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		return errors.New("cannot verify linked-worktree common directory")
	}
	rootCommonDir, err := gitOutput(root, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil || filepath.Clean(commonDir) != filepath.Clean(rootCommonDir) {
		return errors.New("job repository does not share configured checkout metadata")
	}
	gitDir, err := gitOutput(path, "rev-parse", "--absolute-git-dir")
	if err != nil {
		return errors.New("cannot verify linked-worktree git directory")
	}
	relative, err := filepath.Rel(commonDir, filepath.Clean(gitDir))
	if err != nil || filepath.IsAbs(relative) || filepath.Clean(relative) == "." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || relative != filepath.Join("worktrees", filepath.Base(relative)) {
		return errors.New("job repository git directory is not registered worktree metadata")
	}
	return nil
}

func registeredWorktree(root, path string) error {
	output, err := gitOutput(root, "worktree", "list", "--porcelain")
	if err != nil {
		return errors.New("cannot verify registered worktree")
	}
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, "worktree ") && strings.TrimPrefix(line, "worktree ") == path {
			return nil
		}
	}
	return errors.New("worktree is not registered to configured repository")
}

func gitIsRepository(root string) error {
	return gitIsRepositoryContext(context.Background(), root)
}

func gitIsRepositoryContext(ctx context.Context, root string) error {
	got, err := gitOutputContext(ctx, root, "rev-parse", "--show-toplevel")
	if err != nil || got != root {
		return errors.New("path is not the exact Git worktree root")
	}
	return nil
}

func gitSourceEnvironment(source []string) []string {
	environment := make([]string, 0, len(source)+2)
	for _, entry := range source {
		name, _, ok := strings.Cut(entry, "=")
		if ok && !strings.HasPrefix(name, "GIT_") {
			environment = append(environment, entry)
		}
	}
	return append(environment, "GIT_ATTR_NOSYSTEM=1", "GIT_ATTR_NOGLOBAL=1")
}

func gitEnvironment(source []string) []string {
	environment := make([]string, 0, len(source)+2)
	for _, entry := range source {
		name, _, ok := strings.Cut(entry, "=")
		if !ok || strings.HasPrefix(name, "GIT_") {
			continue
		}
		environment = append(environment, entry)
	}
	return append(environment, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_ATTR_NOSYSTEM=1", "GIT_ATTR_NOGLOBAL=1")
}

func gitOutput(root string, args ...string) (string, error) {
	return gitOutputContext(context.Background(), root, args...)
}

func gitOutputContext(ctx context.Context, root string, args ...string) (string, error) {
	output, err := gitRawOutputContext(ctx, root, args...)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func gitRawOutputContext(ctx context.Context, root string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	command.Env = gitEnvironment(os.Environ())
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return nil, err
	}
	return stdout.Bytes(), nil
}

func runGit(ctx context.Context, root string, args ...string) error {
	command := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	command.Env = gitEnvironment(os.Environ())
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return err
	}
	return nil
}
