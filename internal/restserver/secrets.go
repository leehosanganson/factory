package restserver

import (
	"fmt"
	"io"
	"os"
	"syscall"
)

const secretFileLimit = 1 << 20

// Secrets holds startup-loaded API and GitHub credentials. It is intentionally
// separate from Config, and callers must not pass PAT to Pi or serialize it.
type Secrets struct {
	APIKey []byte `json:"-"`
	PAT    []byte `json:"-"`
}

// LoadSecrets reads each secret from a private file owned by the effective
// user. Files must be regular, non-symlink files, owner-readable, and have no
// group/other permissions.
func LoadSecrets(cfg Config) (Secrets, error) {
	if err := cfg.Validate(); err != nil {
		return Secrets{}, fmt.Errorf("validate REST server config: %w", err)
	}
	apiKey, err := readProtectedSecret(cfg.APIKeyFile, "API key")
	if err != nil {
		return Secrets{}, err
	}
	pat, err := readProtectedSecret(cfg.PATFile, "GitHub PAT")
	if err != nil {
		clear(apiKey)
		return Secrets{}, err
	}
	if len(apiKey) == 0 || len(pat) == 0 {
		clear(apiKey)
		clear(pat)
		return Secrets{}, fmt.Errorf("API key and GitHub PAT files must not be empty")
	}
	return Secrets{APIKey: apiKey, PAT: pat}, nil
}

func readProtectedSecret(path, label string) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open %s secret file: %w", label, err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat %s secret file: %w", label, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s secret file must be a regular file", label)
	}
	if info.Mode().Perm()&^0o600 != 0 || info.Mode().Perm()&0o400 == 0 {
		return nil, fmt.Errorf("%s secret file must be owner-readable and inaccessible to group and others", label)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("%s secret file must be owned by the effective user", label)
	}
	if info.Size() > secretFileLimit {
		return nil, fmt.Errorf("%s secret file exceeds %d bytes", label, secretFileLimit)
	}
	data, err := io.ReadAll(io.LimitReader(file, secretFileLimit+1))
	if err != nil {
		return nil, fmt.Errorf("read %s secret file: %w", label, err)
	}
	if len(data) > secretFileLimit {
		clear(data)
		return nil, fmt.Errorf("%s secret file exceeds %d bytes", label, secretFileLimit)
	}
	return data, nil
}
