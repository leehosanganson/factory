package factory

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	oauthStateRecordVersion = 1
	oauthStateTTL           = 10 * time.Minute
	oauthStateRecordLimit   = 4096
	oauthStateLockTimeout   = 30 * time.Second
)

var oauthStatePrincipalIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
var oauthStateRecordNamePattern = regexp.MustCompile(`^state-[0-9a-f]{64}\.json$`)
var oauthStateLockMu sync.Mutex
var oauthStateLocks = make(map[string]*sync.Mutex)
var oauthStateSyncDirectory = syncDirectory
var oauthStateRemove = os.Remove

// OAuthStateStore durably records one-time OAuth state tokens for one host.
// It stores token digests only and does not perform OAuth or HTTP operations.
type OAuthStateStore struct {
	root string
	now  func() time.Time
}

type oauthStateRecord struct {
	Version   int       `json:"version"`
	Digest    string    `json:"digest"`
	Principal string    `json:"principal_id"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// NewOAuthStateStore creates or opens a private, single-host OAuth state store.
func NewOAuthStateStore(root string) (*OAuthStateStore, error) {
	return newOAuthStateStore(root, time.Now)
}

func newOAuthStateStore(root string, now func() time.Time) (*OAuthStateStore, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("OAuth state store root must be absolute")
	}
	if now == nil {
		return nil, fmt.Errorf("OAuth state store clock must not be nil")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create OAuth state store: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("inspect OAuth state store: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("OAuth state store path is not a real directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("OAuth state store must be owned by the effective user")
	}
	root, err = resolvedPath(root)
	if err != nil {
		return nil, fmt.Errorf("resolve OAuth state store: %w", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("secure OAuth state store: %w", err)
	}
	if err := ensureRealDirectory(filepath.Dir(root), root); err != nil {
		return nil, err
	}
	info, err = os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("stat OAuth state store: %w", err)
	}
	return &OAuthStateStore{root: root, now: now}, nil
}

// Create persists a 32-byte random state token bound to principalID and returns
// its unpadded base64url representation. The raw token is never persisted.
func (s *OAuthStateStore) Create(ctx context.Context, principalID string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("OAuth state store is nil")
	}
	if !oauthStatePrincipalIDPattern.MatchString(principalID) {
		return "", fmt.Errorf("invalid OAuth state principal ID")
	}
	if err := checkOAuthStateContext(ctx); err != nil {
		return "", err
	}
	var tokenBytes [32]byte
	if _, err := rand.Read(tokenBytes[:]); err != nil {
		return "", fmt.Errorf("generate OAuth state token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes[:])
	digest := sha256.Sum256([]byte(token))
	record := oauthStateRecord{
		Version: oauthStateRecordVersion, Digest: hex.EncodeToString(digest[:]),
		Principal: principalID,
	}
	unlock, err := s.lock(ctx)
	if err != nil {
		return "", err
	}
	defer unlock()
	if err := checkOAuthStateContext(ctx); err != nil {
		return "", err
	}
	record.CreatedAt = s.now().UTC()
	record.ExpiresAt = record.CreatedAt.Add(oauthStateTTL)
	if err := s.removeExpiredRecords(record.CreatedAt); err != nil {
		return "", fmt.Errorf("clean expired OAuth state: %w", err)
	}
	path := s.recordPath(record.Digest)
	if err := ensureRegularIfExists(path); err != nil {
		return "", err
	}
	if _, err := os.Lstat(path); err == nil {
		return "", fmt.Errorf("OAuth state digest collision")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect OAuth state record: %w", err)
	}
	if err := writeJSONAtomic(s.root, filepath.Base(path), record); err != nil {
		return "", fmt.Errorf("persist OAuth state: %w", err)
	}
	return token, nil
}

// Consume durably removes a matching, unexpired state token before returning
// its principal. Unknown, expired, malformed, and reused tokens fail.
func (s *OAuthStateStore) Consume(ctx context.Context, token string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("OAuth state store is nil")
	}
	if err := checkOAuthStateContext(ctx); err != nil {
		return "", err
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != 32 || base64.RawURLEncoding.EncodeToString(raw) != token {
		return "", fmt.Errorf("invalid or unavailable OAuth state")
	}
	digest := sha256.Sum256([]byte(token))
	digestText := hex.EncodeToString(digest[:])
	unlock, err := s.lock(ctx)
	if err != nil {
		return "", err
	}
	defer unlock()
	if err := checkOAuthStateContext(ctx); err != nil {
		return "", err
	}
	path := s.recordPath(digestText)
	record, err := readOAuthStateRecord(path)
	if err != nil {
		return "", fmt.Errorf("OAuth state is unavailable: %w", err)
	}
	if subtle.ConstantTimeCompare([]byte(record.Digest), []byte(digestText)) != 1 {
		return "", fmt.Errorf("invalid or unavailable OAuth state")
	}
	now := s.now().UTC()
	if now.Before(record.CreatedAt) || !now.Before(record.ExpiresAt) {
		return "", fmt.Errorf("invalid or unavailable OAuth state")
	}
	if err := checkOAuthStateContext(ctx); err != nil {
		return "", err
	}
	if err := removeOAuthStateRecord(path, s.root); err != nil {
		return "", fmt.Errorf("consume OAuth state: %w", err)
	}
	if err := checkOAuthStateContext(ctx); err != nil {
		return "", err
	}
	return record.Principal, nil
}

func removeOAuthStateRecord(path, directory string) error {
	if err := oauthStateRemove(path); err != nil {
		return err
	}
	if err := oauthStateSyncDirectory(directory); err != nil {
		return fmt.Errorf("sync OAuth state directory after removal: %w", err)
	}
	return nil
}

func (s *OAuthStateStore) removeExpiredRecords(now time.Time) error {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		if name == "oauth-state.lock" || (strings.HasPrefix(name, ".record-") && strings.HasSuffix(name, ".tmp")) {
			if err := ensureRegularIfExists(filepath.Join(s.root, name)); err != nil {
				return err
			}
			continue
		}
		if !oauthStateRecordNamePattern.MatchString(name) {
			return fmt.Errorf("unexpected OAuth state store entry %q", name)
		}
		path := filepath.Join(s.root, name)
		record, err := readOAuthStateRecord(path)
		if err != nil {
			return fmt.Errorf("inspect OAuth state record %s: %w", name, err)
		}
		if s.recordPath(record.Digest) != path {
			return fmt.Errorf("OAuth state record digest does not match its filename")
		}
		if !now.Before(record.ExpiresAt) {
			if err := oauthStateRemove(path); err != nil {
				return err
			}
			removed = true
		}
	}
	if removed {
		if err := oauthStateSyncDirectory(s.root); err != nil {
			return fmt.Errorf("sync OAuth state directory after expiry cleanup: %w", err)
		}
	}
	return nil
}

func (s *OAuthStateStore) lock(ctx context.Context) (func(), error) {
	if err := ensureRealDirectory(filepath.Dir(s.root), s.root); err != nil {
		return nil, err
	}
	path := filepath.Join(s.root, "oauth-state.lock")
	if err := ensureRegularIfExists(path); err != nil {
		return nil, err
	}
	oauthStateLockMu.Lock()
	local := oauthStateLocks[path]
	if local == nil {
		local = &sync.Mutex{}
		oauthStateLocks[path] = local
	}
	oauthStateLockMu.Unlock()
	waitCtx, cancel := context.WithTimeout(ctx, oauthStateLockTimeout)
	defer cancel()
	if err := acquireOAuthStateLocalLock(waitCtx, local); err != nil {
		return nil, fmt.Errorf("wait for OAuth state lock: %w", err)
	}
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		local.Unlock()
		return nil, fmt.Errorf("open OAuth state lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		local.Unlock()
		return nil, fmt.Errorf("stat OAuth state lock: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&^0o600 != 0 || info.Mode().Perm()&0o600 != 0o600 {
		_ = file.Close()
		local.Unlock()
		return nil, fmt.Errorf("OAuth state lock must be a private regular file owned by the effective user")
	}
	if err := acquireOAuthStateFileLock(waitCtx, fd); err != nil {
		_ = file.Close()
		local.Unlock()
		return nil, fmt.Errorf("lock OAuth state store: %w", err)
	}
	if err := waitCtx.Err(); err != nil {
		_ = syscall.Flock(fd, syscall.LOCK_UN)
		_ = file.Close()
		local.Unlock()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(fd, syscall.LOCK_UN)
		_ = file.Close()
		local.Unlock()
	}, nil
}

func acquireOAuthStateLocalLock(ctx context.Context, lock *sync.Mutex) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if lock.TryLock() {
			if err := ctx.Err(); err != nil {
				lock.Unlock()
				return err
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func acquireOAuthStateFileLock(ctx context.Context, fd int) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				_ = syscall.Flock(fd, syscall.LOCK_UN)
				return ctxErr
			}
			return nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) && !errors.Is(err, syscall.EINTR) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *OAuthStateStore) recordPath(digest string) string {
	return filepath.Join(s.root, "state-"+digest+".json")
}

func readOAuthStateRecord(path string) (oauthStateRecord, error) {
	var record oauthStateRecord
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return record, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return record, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&^0o600 != 0 || info.Mode().Perm()&0o400 == 0 || info.Size() > oauthStateRecordLimit {
		return record, fmt.Errorf("OAuth state record must be a private, bounded regular file owned by the effective user")
	}
	data, err := io.ReadAll(io.LimitReader(file, oauthStateRecordLimit+1))
	if err != nil {
		return record, err
	}
	if len(data) > oauthStateRecordLimit {
		return record, fmt.Errorf("OAuth state record exceeds size limit")
	}
	if err := rejectOAuthStateJSONFields(data); err != nil {
		return record, fmt.Errorf("malformed OAuth state record")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return record, fmt.Errorf("malformed OAuth state record")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return record, fmt.Errorf("malformed OAuth state record")
	}
	if record.Version != oauthStateRecordVersion || len(record.Digest) != 64 || !principalBearerDigestPattern.MatchString(record.Digest) || !oauthStatePrincipalIDPattern.MatchString(record.Principal) || record.CreatedAt.IsZero() || record.ExpiresAt.IsZero() || !record.ExpiresAt.Equal(record.CreatedAt.Add(oauthStateTTL)) {
		return record, fmt.Errorf("invalid OAuth state record")
	}
	return record, nil
}

func rejectOAuthStateJSONFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := consumeJSONValue(decoder, oauthStateJSONFields("")); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("expected one JSON value")
	}
	return nil
}

func oauthStateJSONFields(parent string) map[string]struct{} {
	if parent == "" {
		return map[string]struct{}{"version": {}, "digest": {}, "principal_id": {}, "created_at": {}, "expires_at": {}}
	}
	return nil
}

func checkOAuthStateContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("OAuth state context must not be nil")
	}
	return ctx.Err()
}
