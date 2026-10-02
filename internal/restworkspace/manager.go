// Package restworkspace manages isolated Git worktrees and private per-job
// result directories. The results root must be owned by the server account and
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
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	Retention  = 24 * time.Hour
	markerName = ".completion.json"
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
	JobID        string
	WorktreePath string
	StatePath    string
	OutputPath   string
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

// Create creates a separate detached worktree at the repository's current
// checkout HEAD and private state/output directories. It creates no branch and
// leaves partial results in place on failure so startup recovery fails closed.
func (m *Manager) Create(jobID string) (Workspace, error) {
	return m.CreateContext(context.Background(), jobID)
}

// CreateContext creates a separate detached worktree at the repository's
// current checkout HEAD and private state/output directories. It creates no
// branch and leaves partial results in place on failure so startup recovery
// fails closed. Cancellation before provisioning starts creates no job files;
// cancellation during Git work retains any partial results for fail-closed
// recovery.
func (m *Manager) CreateContext(ctx context.Context, jobID string) (Workspace, error) {
	if ctx == nil {
		return Workspace{}, errors.New("creation context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return Workspace{}, err
	}
	if !validJobID(jobID) {
		return Workspace{}, errors.New("invalid job ID")
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
	if err := os.Mkdir(jobDir, 0o700); err != nil {
		return Workspace{}, fmt.Errorf("create job result directory: %w", err)
	}
	workspace := Workspace{
		JobID: jobID, WorktreePath: filepath.Join(jobDir, "worktree"),
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
	head, err := gitOutput(m.repoRoot, "rev-parse", "--verify", "HEAD")
	if err != nil || !validGitObjectID(head) {
		return Workspace{}, errors.New("read repository checkout HEAD")
	}
	if err := m.verifyRepository(); err != nil {
		return Workspace{}, err
	}
	if err := ctx.Err(); err != nil {
		return Workspace{}, err
	}
	if err := m.runGit(ctx, m.repoRoot, "worktree", "add", "--detach", workspace.WorktreePath, head); err != nil {
		return Workspace{}, fmt.Errorf("create detached job worktree: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Workspace{}, fmt.Errorf("create detached job worktree: %w", err)
	}
	// If the configured repository changed while Git was adding the worktree,
	// retain the result directory rather than attempting cleanup through an
	// untrusted repository path.
	if err := m.verifyRepository(); err != nil {
		return Workspace{}, err
	}
	if err := verifyWorktreeDir(workspace.WorktreePath); err != nil {
		return Workspace{}, fmt.Errorf("validate created worktree: %w", err)
	}
	if err := m.verifySameDevice(jobDir, workspace.StatePath, workspace.OutputPath, workspace.WorktreePath); err != nil {
		return Workspace{}, err
	}
	return workspace, nil
}

// VerifyResultDirectory accepts only the exact direct job directory beneath the
// managed results root and validates its protected filesystem identity.
func (m *Manager) VerifyResultDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) != m.root || !validJobID(filepath.Base(path)) {
		return errors.New("result directory is outside the managed root")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.verifyRoot(); err != nil {
		return err
	}
	return verifyProtectedDir(path)
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
	if err := registeredWorktree(m.repoRoot, worktree); err != nil {
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
	m.mu.Lock()
	defer m.mu.Unlock()
	report := SweepReport{}
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
		report.Scanned++
		name := entry.Name()
		if !validJobID(name) || !entry.IsDir() {
			report.Retained++
			report.Errors = append(report.Errors, errors.New("retained unrecognized results-root entry"))
			continue
		}
		jobDir := filepath.Join(m.root, name)
		metadata, err := m.readCompletion(jobDir, name)
		if err != nil || !metadata.CompletedAt.Before(cutoff) {
			report.Retained++
			if err != nil {
				report.Errors = append(report.Errors, errors.New("retained job with invalid completion metadata"))
			}
			continue
		}
		if err := m.removeExpired(jobDir, name, metadata); err != nil {
			report.Retained++
			report.Errors = append(report.Errors, errors.New("retained expired job after cleanup safety check failed"))
			continue
		}
		report.Removed++
	}
	return report
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
	if !identityMatches(m.repoRoot, m.repoDev, m.repoIno) {
		return errors.New("configured repository identity changed")
	}
	if err := gitIsRepository(m.repoRoot); err != nil || !identityMatches(m.repoRoot, m.repoDev, m.repoIno) {
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

func (m *Manager) removeExpired(jobDir, jobID string, metadata completion) error {
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
	if err := verifyWorktreeDir(worktree); err != nil {
		return err
	}
	dev, inode, err := directoryIdentity(worktree)
	if err != nil || dev != metadata.WorktreeDev || inode != metadata.WorktreeInode {
		return errors.New("worktree identity changed")
	}
	if err := registeredWorktree(m.repoRoot, worktree); err != nil {
		return err
	}
	// Revalidate the repository, root, job ID, metadata, and worktree at the
	// last path-based boundary before asking Git to remove the worktree.
	if err := m.verifyRoot(); err != nil {
		return err
	}
	if err := m.verifyRepository(); err != nil {
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
	if err != nil || dev != metadata.WorktreeDev || inode != metadata.WorktreeInode || registeredWorktree(m.repoRoot, worktree) != nil {
		return errors.New("worktree identity changed before cleanup")
	}
	if err := m.runGit(context.Background(), m.repoRoot, "worktree", "remove", "--force", worktree); err != nil {
		return errors.New("git worktree cleanup failed")
	}
	if _, err := os.Lstat(worktree); !errors.Is(err, os.ErrNotExist) {
		return errors.New("git worktree path remains after cleanup")
	}
	if err := m.verifyRoot(); err != nil || !identityMatches(jobDir, jobDev, jobInode) {
		return errors.New("job result directory changed after worktree cleanup")
	}
	if err := m.verifyRepository(); err != nil {
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

func gitIsRepository(root string) error {
	got, err := gitOutput(root, "rev-parse", "--show-toplevel")
	if err != nil || got != root {
		return errors.New("path is not the exact Git worktree root")
	}
	return nil
}

func gitOutput(root string, args ...string) (string, error) {
	command := exec.Command("git", append([]string{"-C", root}, args...)...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return "", err
	}
	return strings.TrimSpace(stdout.String()), nil
}

func runGit(ctx context.Context, root string, args ...string) error {
	command := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return err
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
