//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly

package vfscache

import "os"

// fileOwner is unavailable, so quarantine always refuses on this platform.
func fileOwner(os.FileInfo) (uint32, bool) { return 0, false }

// deviceOf is unavailable, so quarantine always refuses on this platform.
func deviceOf(os.FileInfo) (uint64, bool) { return 0, false }

// lockQuarantineRoot cannot prove quiescence on this platform.
func lockQuarantineRoot(string) (func(), error) { return nil, ErrQuarantineUnsupported }
