//go:build linux

package main

import (
	"os"
	"syscall"
	"unsafe"
)

func isTerminal(file *os.File) bool {
	var termios syscall.Termios
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, file.Fd(), uintptr(syscall.TCGETS), uintptr(unsafe.Pointer(&termios)))
	return errno == 0
}

func terminalWidth(file *os.File) (int, error) {
	var size terminalWindowSize
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, file.Fd(), uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(&size)))
	if errno != 0 {
		return 0, errno
	}
	if size.columns == 0 {
		return 0, syscall.EINVAL
	}
	return int(size.columns), nil
}

type terminalWindowSize struct {
	rows    uint16
	columns uint16
	x       uint16
	y       uint16
}
