package restjobs

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"

	_ "modernc.org/sqlite"
)

const (
	recoveryBundleManifest   = "manifest.json"
	recoveryBundleDatabase   = "factory/rest-server/jobs.db"
	maxRecoveryBundleBytes   = int64(2 << 30)
	maxRecoveryManifestBytes = int64(1 << 20)
	maxRecoveryEntries       = 100000
)

var recoveryJobIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func validJobID(id string) bool { return recoveryJobIDPattern.MatchString(id) }

type RecoveryBundleSummary struct {
	Version int                 `json:"version"`
	Jobs    []RecoveryBundleJob `json:"jobs"`
}

type RecoveryBundleJob struct {
	ID         string `json:"id"`
	Repository string `json:"repository"`
	Status     string `json:"status"`
	Workspace  bool   `json:"workspace"`
}

type recoveryBundleManifestData struct {
	Version  int                 `json:"version"`
	Database string              `json:"database"`
	Jobs     []RecoveryBundleJob `json:"jobs"`
}

// CreateRecoveryBundle creates a stopped-server snapshot. resultsRoot is the
// configured REST results base, and repositoryDirs maps aliases to its hashed
// child directory names. The manifest intentionally excludes request payloads
// and host paths.
func CreateRecoveryBundle(ctx context.Context, database, resultsRoot string, repositoryDirs map[string]string, destination string) error {
	if ctx == nil || !filepath.IsAbs(database) || filepath.Clean(database) != database || !filepath.IsAbs(resultsRoot) || filepath.Clean(resultsRoot) != resultsRoot || !filepath.IsAbs(destination) || filepath.Clean(destination) != destination || len(repositoryDirs) == 0 {
		return errors.New("invalid recovery bundle paths")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validatePrivateDirectory(filepath.Dir(destination)); err != nil {
		return errors.New("recovery bundle destination directory must be private")
	}
	if err := validatePrivateDirectory(resultsRoot); err != nil {
		return errors.New("REST results directory must be private")
	}
	for alias, directory := range repositoryDirs {
		if strings.TrimSpace(alias) == "" || directory != recoveryAliasDirectory(alias) {
			return errors.New("repository results identity is invalid")
		}
	}
	if err := rejectPathOverlap(destination, database, resultsRoot); err != nil {
		return err
	}
	if err := rejectExistingPathAliases(destination, database, resultsRoot); err != nil {
		return err
	}
	unlock, err := acquireSQLiteServerOwnership(database)
	if err != nil {
		return errors.New("SQLite server is active or ownership cannot be verified")
	}
	defer unlock()

	jobs, err := recoveryBundleJobs(ctx, database, resultsRoot, repositoryDirs)
	if err != nil {
		return err
	}
	parent := filepath.Dir(destination)
	tmp, err := os.CreateTemp(parent, ".factory-recovery-*.partial")
	if err != nil {
		return errors.New("create temporary recovery bundle")
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return errors.New("protect temporary recovery bundle")
	}
	gz := gzip.NewWriter(tmp)
	tarWriter := tar.NewWriter(gz)
	manifest := recoveryBundleManifestData{Version: 1, Database: recoveryBundleDatabase, Jobs: jobs}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		_ = tarWriter.Close()
		_ = gz.Close()
		_ = tmp.Close()
		return errors.New("encode recovery bundle manifest")
	}
	if len(encoded) > int(maxRecoveryManifestBytes) {
		_ = tarWriter.Close()
		_ = gz.Close()
		_ = tmp.Close()
		return errors.New("recovery bundle manifest exceeds limit")
	}
	entries := 0
	bytesWritten := int64(0)
	write := func(name string, mode int64, size int64, source io.Reader) error {
		entries++
		if entries > maxRecoveryEntries || size < 0 || bytesWritten+size > maxRecoveryBundleBytes {
			return errors.New("recovery bundle exceeds limits")
		}
		header := &tar.Header{Name: name, Mode: mode & 0o700, Size: size, Typeflag: tar.TypeReg}
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}
		written, err := io.CopyN(tarWriter, source, size)
		bytesWritten += written
		if err != nil || written != size {
			return errors.New("read recovery source file")
		}
		return nil
	}
	if err := write(recoveryBundleManifest, 0o600, int64(len(encoded)), strings.NewReader(string(encoded))); err != nil {
		_ = tarWriter.Close()
		_ = gz.Close()
		_ = tmp.Close()
		return errors.New("write recovery bundle manifest")
	}
	dbTmp := tmpPath + ".db"
	if err := BackupSQLiteDatabase(ctx, database, dbTmp); err != nil {
		_ = tarWriter.Close()
		_ = gz.Close()
		_ = tmp.Close()
		_ = os.Remove(dbTmp)
		return err
	}
	defer os.Remove(dbTmp)
	if err := checkSQLiteIntegrity(dbTmp); err != nil {
		_ = tarWriter.Close()
		_ = gz.Close()
		_ = tmp.Close()
		return errors.New("SQLite snapshot failed integrity check")
	}
	if err := archiveTreeDirectoryHeaders(tarWriter, []string{"factory", "factory/rest-server"}); err != nil {
		_ = tarWriter.Close()
		_ = gz.Close()
		_ = tmp.Close()
		return errors.New("write recovery directory entries")
	}
	if err := archiveFile(tarWriter, recoveryBundleDatabase, dbTmp, write); err != nil {
		_ = tarWriter.Close()
		_ = gz.Close()
		_ = tmp.Close()
		return err
	}
	for _, job := range jobs {
		if err := ctx.Err(); err != nil {
			_ = tarWriter.Close()
			_ = gz.Close()
			_ = tmp.Close()
			return err
		}
		if !job.Workspace {
			continue
		}
		root := filepath.Join(resultsRoot, job.Repository, "results", job.ID)
		if err := archiveTree(tarWriter, "factory/rest-server/"+job.Repository+"/results/"+job.ID, root, write, &entries); err != nil {
			_ = tarWriter.Close()
			_ = gz.Close()
			_ = tmp.Close()
			return errors.New("archive retained workspace")
		}
	}
	if err := tarWriter.Close(); err != nil {
		_ = gz.Close()
		_ = tmp.Close()
		return errors.New("finish recovery archive")
	}
	if err := gz.Close(); err != nil {
		_ = tmp.Close()
		return errors.New("finish recovery compression")
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return errors.New("sync recovery bundle")
	}
	if err := tmp.Close(); err != nil {
		return errors.New("close recovery bundle")
	}
	if err := os.Link(tmpPath, destination); err != nil {
		return errors.New("publish recovery bundle atomically")
	}
	parentDir, err := os.Open(parent)
	if err != nil {
		_ = os.Remove(destination)
		return errors.New("open recovery bundle directory")
	}
	syncErr := parentDir.Sync()
	closeErr := parentDir.Close()
	if syncErr != nil || closeErr != nil {
		_ = os.Remove(destination)
		return errors.New("sync recovery bundle directory")
	}
	if err := VerifyRecoveryBundle(destination); err != nil {
		_ = os.Remove(destination)
		return errors.New("published recovery bundle failed validation")
	}
	return nil
}

// VerifyRecoveryBundle validates archive structure, restrictive permissions,
// manifest references, workspace containment, and SQLite integrity without extracting.
func VerifyRecoveryBundle(bundle string) error {
	_, err := InspectRecoveryBundle(bundle)
	return err
}

// InspectRecoveryBundle returns only bounded, secret-free manifest identities.
func InspectRecoveryBundle(bundle string) (RecoveryBundleSummary, error) {
	if !filepath.IsAbs(bundle) {
		return RecoveryBundleSummary{}, errors.New("recovery bundle path must be absolute")
	}
	info, err := os.Lstat(bundle)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || !bundleOwnedByCurrentUser(info) || info.Size() > maxRecoveryBundleBytes {
		return RecoveryBundleSummary{}, errors.New("recovery bundle must be a private regular file")
	}
	file, err := os.Open(bundle)
	if err != nil {
		return RecoveryBundleSummary{}, errors.New("open recovery bundle")
	}
	defer file.Close()
	gz, err := gzip.NewReader(io.LimitReader(file, maxRecoveryBundleBytes+1))
	if err != nil {
		return RecoveryBundleSummary{}, errors.New("invalid recovery bundle compression")
	}
	defer gz.Close()
	tr := tar.NewReader(io.LimitReader(gz, maxRecoveryBundleBytes+1))
	seen := map[string]bool{}
	var manifest recoveryBundleManifestData
	var dbData []byte
	entries := 0
	var totalBytes int64
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return RecoveryBundleSummary{}, errors.New("invalid recovery archive")
		}
		entries++
		name := strings.TrimSuffix(h.Name, "/")
		if entries > maxRecoveryEntries || (h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeDir) || h.Size < 0 || h.Mode&0o077 != 0 || filepath.IsAbs(name) || filepath.Clean(name) != name || name == "." || strings.HasPrefix(name, "../") || strings.Contains(name, "\\") || seen[name] || h.Typeflag == tar.TypeDir && h.Size != 0 || h.Typeflag == tar.TypeReg && h.Mode&0o400 == 0 {
			return RecoveryBundleSummary{}, errors.New("unsafe recovery archive entry")
		}
		if h.Size > maxRecoveryBundleBytes || totalBytes+h.Size > maxRecoveryBundleBytes {
			return RecoveryBundleSummary{}, errors.New("recovery archive entry exceeds limit")
		}
		totalBytes += h.Size
		if name != recoveryBundleManifest && name != recoveryBundleDatabase && name != "factory" && name != "factory/rest-server" && !strings.HasPrefix(name, "factory/rest-server/") {
			return RecoveryBundleSummary{}, errors.New("unexpected recovery archive entry")
		}
		seen[name] = true
		if h.Typeflag == tar.TypeDir {
			if name != "factory" && name != "factory/rest-server" && !strings.HasPrefix(name, "factory/rest-server/") {
				return RecoveryBundleSummary{}, errors.New("unexpected recovery directory")
			}
			continue
		}
		data, err := io.ReadAll(io.LimitReader(tr, h.Size+1))
		if err != nil || int64(len(data)) != h.Size {
			return RecoveryBundleSummary{}, errors.New("truncated recovery archive entry")
		}
		if name == recoveryBundleManifest {
			decoder := json.NewDecoder(strings.NewReader(string(data)))
			decoder.DisallowUnknownFields()
			if h.Size > maxRecoveryManifestBytes || decoder.Decode(&manifest) != nil || decoder.Decode(new(any)) != io.EOF {
				return RecoveryBundleSummary{}, errors.New("invalid recovery bundle manifest")
			}
		} else if name == recoveryBundleDatabase {
			dbData = data
		}
	}
	if !seen[recoveryBundleManifest] || !seen[recoveryBundleDatabase] || manifest.Version != 1 || manifest.Database != recoveryBundleDatabase {
		return RecoveryBundleSummary{}, errors.New("recovery bundle is incomplete")
	}
	if err := validateBundleReferences(manifest, seen); err != nil {
		return RecoveryBundleSummary{}, err
	}
	if err := validateBundleDatabase(dbData, manifest); err != nil {
		return RecoveryBundleSummary{}, errors.New("recovery SQLite database or manifest references are invalid")
	}
	return RecoveryBundleSummary{Version: manifest.Version, Jobs: manifest.Jobs}, nil
}

// RestoreRecoveryBundle verifies first and extracts atomically into a new private directory.
func RestoreRecoveryBundle(bundle, destination string) error {
	if !filepath.IsAbs(destination) || filepath.Clean(destination) != destination {
		return errors.New("restore destination must be an absolute clean path")
	}
	if err := VerifyRecoveryBundle(bundle); err != nil {
		return err
	}
	if _, err := os.Lstat(destination); err == nil || !errors.Is(err, os.ErrNotExist) {
		return errors.New("restore destination must not already exist")
	}
	if err := validatePrivateDirectory(filepath.Dir(destination)); err != nil {
		return errors.New("restore parent directory must be private")
	}
	tmp, err := os.MkdirTemp(filepath.Dir(destination), ".factory-restore-*")
	if err != nil {
		return errors.New("create private restore staging directory")
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(tmp)
		}
	}()
	if err := os.Chmod(tmp, 0o700); err != nil {
		return errors.New("protect restore staging directory")
	}
	if err := extractVerifiedBundle(bundle, tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, destination); err != nil {
		return errors.New("publish restored data atomically")
	}
	published = true
	parentDir, err := os.Open(filepath.Dir(destination))
	if err != nil {
		return errors.New("open restore parent directory")
	}
	syncErr := parentDir.Sync()
	closeErr := parentDir.Close()
	if syncErr != nil || closeErr != nil {
		return errors.New("sync restored data directory")
	}
	return nil
}

func recoveryBundleJobs(ctx context.Context, database, resultsRoot string, dirs map[string]string) ([]RecoveryBundleJob, error) {
	dsn, err := sqliteDSN(database)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, errors.New("open recovery SQLite database")
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT id,request_json,status FROM factory_jobs ORDER BY id`)
	if err != nil {
		return nil, errors.New("read recovery job records")
	}
	defer rows.Close()
	jobs := []RecoveryBundleJob{}
	for rows.Next() {
		var id, requestJSON, status string
		if err := rows.Scan(&id, &requestJSON, &status); err != nil {
			return nil, errors.New("read recovery job record")
		}
		var request Request
		if json.Unmarshal([]byte(requestJSON), &request) != nil {
			return nil, errors.New("invalid recovery job record")
		}
		dir, ok := dirs[request.Repository]
		if !ok || !validJobID(id) {
			return nil, errors.New("recovery job identity is invalid")
		}
		entry := RecoveryBundleJob{ID: id, Repository: dir, Status: status}
		workspace := filepath.Join(resultsRoot, dir, "results", id)
		if _, err := os.Lstat(workspace); err == nil {
			for _, directory := range []string{filepath.Dir(filepath.Dir(filepath.Dir(workspace))), filepath.Dir(filepath.Dir(workspace)), filepath.Dir(workspace), workspace} {
				if err := validatePrivateDirectory(directory); err != nil {
					return nil, errors.New("retained workspace path is unsafe")
				}
			}
			if err := verifyBundleTree(workspace); err != nil {
				return nil, errors.New("retained workspace is unsafe")
			}
			entry.Workspace = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, errors.New("cannot inspect retained workspace")
		}
		jobs = append(jobs, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("read recovery job records")
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })
	return jobs, nil
}

func validatePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || !bundleOwnedByCurrentUser(info) {
		return errors.New("directory is not private")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return errors.New("directory is not canonical")
	}
	return nil
}

func recoveryAliasDirectory(alias string) string {
	sum := sha256.Sum256([]byte(alias))
	return hex.EncodeToString(sum[:])
}

func bundleOwnedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func rejectExistingPathAliases(destination, database, root string) error {
	paths := []string{database, root, filepath.Dir(destination)}
	resolved := make([]string, 0, len(paths))
	for _, path := range paths {
		value, err := filepath.EvalSymlinks(path)
		if err != nil {
			return errors.New("recovery source path is unavailable")
		}
		resolved = append(resolved, value)
	}
	dest, err := filepath.Abs(destination)
	if err != nil {
		return errors.New("resolve recovery destination")
	}
	if _, err := os.Lstat(dest); err == nil || !errors.Is(err, os.ErrNotExist) {
		return errors.New("recovery bundle destination must not already exist")
	}
	for _, source := range resolved[:2] {
		if dest == source || pathContains(dest, source) || pathContains(source, dest) {
			return errors.New("recovery bundle destination overlaps source data")
		}
	}
	return nil
}

func rejectPathOverlap(destination, database, root string) error {
	dest, _ := filepath.Abs(destination)
	db, _ := filepath.Abs(database)
	results, _ := filepath.Abs(root)
	if dest == db || pathContains(dest, db) || pathContains(db, dest) || pathContains(dest, results) || pathContains(results, dest) {
		return errors.New("recovery bundle destination overlaps source data")
	}
	return nil
}
func pathContains(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && (rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func archiveTreeDirectoryHeaders(tw *tar.Writer, paths []string) error {
	for _, name := range paths {
		if err := tw.WriteHeader(&tar.Header{Name: name + "/", Mode: 0o700, Typeflag: tar.TypeDir}); err != nil {
			return err
		}
	}
	return nil
}

func archiveFile(tw *tar.Writer, name, path string, write func(string, int64, int64, io.Reader) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || !bundleOwnedByCurrentUser(info) {
		return errors.New("invalid recovery source file")
	}
	return write(name, int64(info.Mode().Perm()), info.Size(), f)
}

func archiveTree(tw *tar.Writer, prefix, root string, write func(string, int64, int64, io.Reader) error, count *int) error {
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("invalid retained workspace root")
	}
	*count++
	if err := tw.WriteHeader(&tar.Header{Name: prefix + "/", Mode: 0o700, Typeflag: tar.TypeDir}); err != nil {
		return err
	}
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() && !info.Mode().IsRegular() || !bundleOwnedByCurrentUser(info) || info.Mode().Perm()&0o077 != 0 {
			return errors.New("unsupported workspace entry")
		}
		name := prefix + "/" + filepath.ToSlash(rel)
		if info.IsDir() {
			*count++
			if *count > maxRecoveryEntries {
				return errors.New("too many workspace entries")
			}
			return tw.WriteHeader(&tar.Header{Name: name + "/", Mode: 0o700, Typeflag: tar.TypeDir})
		}
		return archiveFile(tw, name, path, write)
	})
}

func verifyBundleTree(root string) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() && !info.Mode().IsRegular() || !bundleOwnedByCurrentUser(info) || info.Mode().Perm()&0o077 != 0 {
			return errors.New("unsafe workspace entry")
		}
		return nil
	})
}

func validateBundleReferences(m recoveryBundleManifestData, seen map[string]bool) error {
	if len(m.Jobs) > 10000 {
		return errors.New("recovery manifest exceeds job limit")
	}
	known := map[string]bool{}
	for _, j := range m.Jobs {
		key := j.Repository + "/" + j.ID
		if !validJobID(j.ID) || !validAliasDigest(j.Repository) || !validRecoveryStatus(j.Status) || known[key] {
			return errors.New("invalid recovery job reference")
		}
		known[key] = true
		prefix := "factory/rest-server/" + j.Repository + "/results/" + j.ID
		has := seen[prefix]
		for name := range seen {
			if strings.HasPrefix(name, prefix+"/") {
				has = true
			}
		}
		if j.Workspace != has {
			return errors.New("recovery workspace reference mismatch")
		}
	}
	for name := range seen {
		if strings.HasPrefix(name, "factory/rest-server/") && name != recoveryBundleDatabase {
			parts := strings.Split(name, "/")
			if len(parts) < 3 || parts[0] != "factory" || parts[1] != "rest-server" {
				return errors.New("unreferenced recovery workspace")
			}
			if len(parts) >= 4 && parts[3] == "results" {
				if len(parts) < 5 || !known[parts[2]+"/"+parts[4]] {
					return errors.New("unreferenced recovery workspace")
				}
			} else if len(parts) > 3 {
				return errors.New("unexpected recovery workspace path")
			}
		}
	}
	return nil
}

func checkSQLiteIntegrity(path string) error {
	dsn, err := sqliteDSN(path)
	if err != nil {
		return err
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	var result string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&result); err != nil || result != "ok" {
		return errors.New("integrity check failed")
	}
	return nil
}
func validAliasDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func validRecoveryStatus(value string) bool {
	switch value {
	case "queued", "running", "succeeded", "failed", "canceled":
		return true
	default:
		return false
	}
}

func validateBundleDatabase(data []byte, manifest recoveryBundleManifestData) error {
	if err := integrityCheckBytes(data); err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "factory-recovery-manifest-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, "check.db")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return err
	}
	dsn, err := sqliteDSN(path)
	if err != nil {
		return err
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.Query(`SELECT id,request_json,status FROM factory_jobs`)
	if err != nil {
		return err
	}
	defer rows.Close()
	jobs := make(map[string]RecoveryBundleJob, len(manifest.Jobs))
	for _, job := range manifest.Jobs {
		jobs[job.ID] = job
	}
	count := 0
	for rows.Next() {
		var id, raw, status string
		if err := rows.Scan(&id, &raw, &status); err != nil {
			return err
		}
		var request Request
		if json.Unmarshal([]byte(raw), &request) != nil {
			return errors.New("invalid job request")
		}
		job, ok := jobs[id]
		if !ok || job.Status != status || job.Repository != digestRepositoryAlias(request.Repository) {
			return errors.New("job manifest does not match database")
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != len(jobs) {
		return errors.New("manifest has missing database jobs")
	}
	return nil
}

func digestRepositoryAlias(alias string) string {
	sum := sha256.Sum256([]byte(alias))
	return hex.EncodeToString(sum[:])
}

func integrityCheckBytes(data []byte) error {
	if len(data) == 0 {
		return errors.New("empty database")
	}
	dir, err := os.MkdirTemp("", "factory-recovery-check-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	name := filepath.Join(dir, "check.db")
	if err := os.WriteFile(name, data, 0o600); err != nil {
		return err
	}
	return checkSQLiteIntegrity(name)
}

func extractVerifiedBundle(bundle, destination string) error {
	f, err := os.Open(bundle)
	if err != nil {
		return errors.New("open recovery bundle")
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return errors.New("open recovery compression")
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return errors.New("read recovery archive")
		}
		name := filepath.FromSlash(h.Name)
		target := filepath.Join(destination, name)
		if !pathContains(destination, target) || name == "." {
			return errors.New("unsafe restore path")
		}
		if h.Typeflag == tar.TypeDir {
			if err := os.MkdirAll(target, 0o700); err != nil {
				return errors.New("create restored directory")
			}
			if err := os.Chmod(target, 0o700); err != nil {
				return errors.New("protect restored directory")
			}
			continue
		}
		if h.Typeflag != tar.TypeReg {
			return errors.New("unsupported restore entry")
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return errors.New("create restored parent")
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return errors.New("create restored file")
		}
		_, copyErr := io.CopyN(out, tr, h.Size)
		closeErr := out.Close()
		if copyErr != nil || closeErr != nil {
			return errors.New("write restored file")
		}
		if err := os.Chmod(target, os.FileMode(h.Mode)&0o700); err != nil {
			return errors.New("protect restored file")
		}
	}
	return nil
}

func acquireSQLiteServerOwnership(path string) (func(), error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("database path must be absolute")
	}
	lockPath, err := sqliteServerLockPath(path)
	if err != nil {
		return nil, err
	}
	pathLockPath, err := sqliteServerPathLockPath(path)
	if err != nil {
		return nil, err
	}
	locks := []*os.File{}
	for _, candidate := range []string{lockPath, pathLockPath} {
		if err := ensurePrivateSQLiteFile(candidate); err != nil {
			closeSQLiteServerLocks(locks)
			return nil, err
		}
		lock, err := os.OpenFile(candidate, os.O_RDWR, 0)
		if err != nil {
			closeSQLiteServerLocks(locks)
			return nil, err
		}
		if err := lockSQLiteServerFile(lock); err != nil {
			_ = lock.Close()
			closeSQLiteServerLocks(locks)
			return nil, err
		}
		locks = append(locks, lock)
	}
	return func() { closeSQLiteServerLocks(locks) }, nil
}
