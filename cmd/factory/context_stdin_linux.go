//go:build linux

package main

import (
	"syscall"
	"time"
	"unsafe"
)

func waitReadable(fd uintptr, interval time.Duration) (bool, error) {
	var set syscall.FdSet
	bitsPerWord := 8 * int(unsafe.Sizeof(set.Bits[0]))
	word := int(fd) / bitsPerWord
	if word >= len(set.Bits) {
		return false, syscall.EINVAL
	}
	set.Bits[word] |= 1 << (uint(fd) % uint(bitsPerWord))
	timeout := syscall.NsecToTimeval(interval.Nanoseconds())
	n, err := syscall.Select(int(fd)+1, &set, nil, nil, &timeout)
	return n > 0, err
}
