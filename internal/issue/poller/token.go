package poller

import (
	"errors"
	"io"
	"os"
	"syscall"
)

const tokenLimit = 4096

func loadToken(path string) (string, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", errors.New("GitHub token file is unavailable")
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", errors.New("GitHub token file is unavailable")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || info.Mode().Perm()&^0o600 != 0 || info.Mode().Perm()&0o400 == 0 || !ok || stat.Uid != uint32(os.Geteuid()) || info.Size() > tokenLimit {
		return "", errors.New("GitHub token file must be private and owner-readable")
	}
	data, err := io.ReadAll(io.LimitReader(file, tokenLimit+1))
	if err != nil || len(data) > tokenLimit {
		return "", errors.New("GitHub token file is invalid")
	}
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
		if len(data) > 0 && data[len(data)-1] == '\r' {
			data = data[:len(data)-1]
		}
	}
	for _, value := range data {
		if value <= 0x20 || value == 0x7f {
			return "", errors.New("GitHub token file must contain one token")
		}
	}
	if len(data) == 0 {
		return "", errors.New("GitHub token file must contain one token")
	}
	return string(data), nil
}
