//go:build darwin

package restjobs

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func sqliteServerLockPath(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	identity, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", errors.New("SQLite file identity is unavailable")
	}
	cacheDir, err := os.UserCacheDir()
	if err != nil || strings.TrimSpace(cacheDir) == "" {
		return "", errors.New("private cache directory is unavailable")
	}
	lockDir := filepath.Join(cacheDir, "factory", "sqlite-locks")
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		return "", err
	}
	if err := os.Chmod(lockDir, 0o700); err != nil {
		return "", err
	}
	var key [16]byte
	binary.BigEndian.PutUint64(key[:8], uint64(identity.Dev))
	binary.BigEndian.PutUint64(key[8:], uint64(identity.Ino))
	return filepath.Join(lockDir, hex.EncodeToString(key[:])+".lock"), nil
}
