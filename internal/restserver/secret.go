package restserver

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"io"
	"os"
	"syscall"
	"unicode"
	"unicode/utf8"
)

const apiKeyFileLimit = 4096

// APIKey is an immutable in-memory shared API key. Its value is never
// included in formatting or JSON output. Load a new value to apply rotation.
type APIKey struct {
	value string
}

// LoadAPIKey reads a single shared API key from a protected regular file.
// File updates do not affect an already loaded value; reload or restart to rotate.
func LoadAPIKey(path string) (APIKey, error) {
	if path == "" {
		return APIKey{}, fmt.Errorf("API key file path must not be empty")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return APIKey{}, fmt.Errorf("open API key file: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return APIKey{}, fmt.Errorf("stat API key file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return APIKey{}, fmt.Errorf("API key file must be a regular file")
	}
	if info.Mode().Perm()&^0o600 != 0 || info.Mode().Perm()&0o400 == 0 {
		return APIKey{}, fmt.Errorf("API key file must be owner-readable and inaccessible to group and others")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return APIKey{}, fmt.Errorf("API key file must be owned by the effective user")
	}
	if info.Size() > apiKeyFileLimit {
		return APIKey{}, fmt.Errorf("API key file exceeds %d bytes", apiKeyFileLimit)
	}
	data, err := io.ReadAll(io.LimitReader(file, apiKeyFileLimit+1))
	if err != nil {
		return APIKey{}, fmt.Errorf("read API key file: %w", err)
	}
	if len(data) > apiKeyFileLimit {
		return APIKey{}, fmt.Errorf("API key file exceeds %d bytes", apiKeyFileLimit)
	}
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
		if len(data) > 0 && data[len(data)-1] == '\r' {
			data = data[:len(data)-1]
		}
	}
	if !validAPIKey(data) {
		return APIKey{}, fmt.Errorf("API key file must contain one nonempty token without whitespace or line breaks")
	}
	return APIKey{value: string(data)}, nil
}

// Authenticate reports whether token matches this shared key.
func (key APIKey) Authenticate(token string) bool {
	if key.value == "" || token == "" {
		return false
	}
	want, got := sha256.Sum256([]byte(key.value)), sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(want[:], got[:]) == 1
}

// String prevents accidental disclosure through ordinary formatting.
func (APIKey) String() string { return "[REDACTED]" }

// GoString prevents accidental disclosure through Go-syntax formatting.
func (APIKey) GoString() string { return "[REDACTED]" }

func validAPIKey(data []byte) bool {
	if len(data) == 0 || !utf8.Valid(data) {
		return false
	}
	for len(data) > 0 {
		r, size := utf8.DecodeRune(data)
		if r == 0 || unicode.IsSpace(r) {
			return false
		}
		data = data[size:]
	}
	return true
}
