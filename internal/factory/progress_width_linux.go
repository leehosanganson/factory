//go:build linux

package factory

import (
	"syscall"
	"unsafe"
)

type terminalWindowSize struct {
	rows    uint16
	columns uint16
	x       uint16
	y       uint16
}

func queryTerminalWidth(fd uintptr) (int, error) {
	var size terminalWindowSize
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(&size)))
	if errno != 0 {
		return 0, errno
	}
	if size.columns == 0 {
		return 0, syscall.EINVAL
	}
	return int(size.columns), nil
}
