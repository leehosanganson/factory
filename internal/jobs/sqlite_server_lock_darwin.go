//go:build darwin

package restjobs

import (
	"os"
	"syscall"
)

func serverSQLiteLockPath(path string) (string, error)     { return sqliteServerLockPath(path) }
func serverSQLitePathLockPath(path string) (string, error) { return sqliteServerPathLockPath(path) }
func flockSQLiteServerFile(file *os.File) error            { return lockSQLiteServerFile(file) }
func unflockSQLiteServerFile(file *os.File) error          { return unlockSQLiteServerFile(file) }

func lockSQLiteServerFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func unlockSQLiteServerFile(file *os.File) error {
	err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	return closeErr
}
