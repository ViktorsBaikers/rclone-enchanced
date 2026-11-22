//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package vfscache

import (
	"fmt"
	"os"
	"syscall"
)

// fileOwner reports the OS owner of a file. Unsupported platforms refuse.
func fileOwner(info os.FileInfo) (uint32, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return stat.Uid, true
}

// deviceOf reports the filesystem device of a file.
func deviceOf(info os.FileInfo) (uint64, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint64(stat.Dev), true
}

// lockQuarantineRoot takes an exclusive non-blocking lock on the root
// directory, proving no other offline process holds it.
func lockQuarantineRoot(path string) (func(), error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = dir.Close()
		return nil, fmt.Errorf("%w: %v", ErrQuarantineNotQuiesced, err)
	}
	return func() {
		_ = syscall.Flock(int(dir.Fd()), syscall.LOCK_UN)
		_ = dir.Close()
	}, nil
}
