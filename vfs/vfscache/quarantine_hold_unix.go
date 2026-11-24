//go:build unix

package vfscache

import (
	"fmt"
	"os"
	"syscall"
)

// HoldCacheRoot takes a shared lock on the cache root for as long as the
// process serves writes from it.
//
// Offline quarantine takes the exclusive form of this same lock, so a running
// client makes quarantine refuse instead of separating bytes out from under it.
// The shared lock is advisory: a client that cannot take it still starts, and
// quarantine remains gated on the operator's decision to stop that client.
func HoldCacheRoot(root string) (func(), error) {
	dir, err := os.Open(root)
	if err != nil {
		return nil, fmt.Errorf("hold cache root %q: %w", root, err)
	}
	if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		_ = dir.Close()
		return nil, fmt.Errorf("hold cache root %q: %w", root, err)
	}
	return func() {
		_ = syscall.Flock(int(dir.Fd()), syscall.LOCK_UN)
		_ = dir.Close()
	}, nil
}
