//go:build darwin

package main

import (
	"syscall"
	"time"
)

func waitReadable(fd uintptr, interval time.Duration) (bool, error) {
	var set syscall.FdSet
	word := int(fd) / 32
	if word >= len(set.Bits) {
		return false, syscall.EINVAL
	}
	set.Bits[word] |= 1 << (uint(fd) % 32)
	timeout := syscall.NsecToTimeval(interval.Nanoseconds())
	err := syscall.Select(int(fd)+1, &set, nil, nil, &timeout)
	return err == nil && set.Bits[word]&(1<<(uint(fd)%32)) != 0, err
}
