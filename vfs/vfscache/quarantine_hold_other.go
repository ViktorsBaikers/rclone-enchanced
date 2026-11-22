//go:build !unix

package vfscache

// HoldCacheRoot is unavailable on this platform, so a running client cannot
// prove quiescence to offline quarantine. Quarantine stays refused here.
func HoldCacheRoot(root string) (func(), error) {
	return func() {}, nil
}
