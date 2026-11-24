package vfscache

// FIXME need to test async writeback here

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rclone/rclone/backend/teldrive"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/lib/random"
	"github.com/rclone/rclone/lib/ranges"
	"github.com/rclone/rclone/lib/readers"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var zeroes = string(make([]byte, 100))

func newItemTestCache(t *testing.T) (r *fstest.Run, c *Cache) {
	opt := vfscommon.Opt

	// Disable the cache cleaner as it interferes with these tests
	opt.CachePollInterval = 0

	// Disable synchronous write
	opt.WriteBack = 0

	// Disable handle caching so existing tests get immediate close behavior
	opt.HandleCaching = 0

	return newTestCacheOpt(t, opt)
}

// Check the object has contents
func checkObject(t *testing.T, r *fstest.Run, remote string, contents string) {
	obj, err := r.Fremote.NewObject(context.Background(), remote)
	require.NoError(t, err)
	in, err := obj.Open(context.Background())
	require.NoError(t, err)
	buf, err := io.ReadAll(in)
	require.NoError(t, err)
	require.NoError(t, in.Close())
	assert.Equal(t, contents, string(buf))
}

func newFileLength(t *testing.T, r *fstest.Run, c *Cache, remote string, length int) (contents string, obj fs.Object, item *Item) {
	contents = random.String(length)
	r.WriteObject(context.Background(), remote, contents, time.Now())
	item, _ = c.get(remote)
	obj, err := r.Fremote.NewObject(context.Background(), remote)
	require.NoError(t, err)
	return
}

func newFile(t *testing.T, r *fstest.Run, c *Cache, remote string) (contents string, obj fs.Object, item *Item) {
	return newFileLength(t, r, c, remote, 100)
}

func TestItemExists(t *testing.T) {
	_, c := newItemTestCache(t)
	item, _ := c.get("potato")

	assert.False(t, item.Exists())
	require.NoError(t, item.Open(nil))
	assert.True(t, item.Exists())
	require.NoError(t, item.Close(nil))
	assert.True(t, item.Exists())
	item.remove("test")
	assert.False(t, item.Exists())
}

func TestItemGetSize(t *testing.T) {
	r, c := newItemTestCache(t)
	item, _ := c.get("potato")
	require.NoError(t, item.Open(nil))

	size, err := item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(0), size)

	n, err := item.WriteAt([]byte("hello"), 0)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	size, err = item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(5), size)

	require.NoError(t, item.Close(nil))
	checkObject(t, r, "potato", "hello")
}

func TestItemDirty(t *testing.T) {
	r, c := newItemTestCache(t)
	item, _ := c.get("potato")
	require.NoError(t, item.Open(nil))

	assert.Equal(t, false, item.IsDirty())

	n, err := item.WriteAt([]byte("hello"), 0)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	assert.Equal(t, true, item.IsDirty())

	require.NoError(t, item.Close(nil))

	// Sync writeback so expect clean here
	assert.Equal(t, false, item.IsDirty())

	item.Dirty()

	assert.Equal(t, true, item.IsDirty())
	checkObject(t, r, "potato", "hello")
}

func TestItemSync(t *testing.T) {
	_, c := newItemTestCache(t)
	item, _ := c.get("potato")

	require.Error(t, item.Sync())

	require.NoError(t, item.Open(nil))

	require.NoError(t, item.Sync())

	require.NoError(t, item.Close(nil))
}

func TestItemTruncateNew(t *testing.T) {
	r, c := newItemTestCache(t)
	item, _ := c.get("potato")

	require.Error(t, item.Truncate(0))

	require.NoError(t, item.Open(nil))

	require.NoError(t, item.Truncate(100))

	size, err := item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(100), size)

	// Check the Close callback works
	callbackCalled := false
	callback := func(o fs.Object) {
		callbackCalled = true
		assert.Equal(t, "potato", o.Remote())
		assert.Equal(t, int64(100), o.Size())
	}
	require.NoError(t, item.Close(callback))
	assert.True(t, callbackCalled)

	checkObject(t, r, "potato", zeroes)
}

func TestItemTruncateExisting(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")

	require.Error(t, item.Truncate(40))
	checkObject(t, r, "existing", contents)

	require.NoError(t, item.Open(obj))

	require.NoError(t, item.Truncate(40))

	require.NoError(t, item.Truncate(60))

	require.NoError(t, item.Close(nil))

	checkObject(t, r, "existing", contents[:40]+zeroes[:20])
}

func TestItemTruncateShrinkUncached(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")

	// Shrink a file which hasn't been read so the downloader has to
	// fetch the kept part of the file on close.
	require.NoError(t, item.Open(obj))
	require.NoError(t, item.Truncate(40))
	require.NoError(t, item.Close(nil))

	checkObject(t, r, "existing", contents[:40])
}

func TestItemReadAt(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")
	buf := make([]byte, 10)

	_, err := item.ReadAt(buf, 10)
	require.Error(t, err)

	require.NoError(t, item.Open(obj))

	n, err := item.ReadAt(buf, 10)
	assert.Equal(t, 10, n)
	require.NoError(t, err)
	assert.Equal(t, contents[10:20], string(buf[:n]))

	n, err = item.ReadAt(buf, 95)
	assert.Equal(t, 5, n)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, contents[95:], string(buf[:n]))

	n, err = item.ReadAt(buf, 1000)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, contents[:0], string(buf[:n]))

	n, err = item.ReadAt(buf, -1)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, contents[:0], string(buf[:n]))

	require.NoError(t, item.Close(nil))
}

func TestItemWriteAtNew(t *testing.T) {
	r, c := newItemTestCache(t)
	item, _ := c.get("potato")
	buf := make([]byte, 10)

	_, err := item.WriteAt(buf, 10)
	require.Error(t, err)

	require.NoError(t, item.Open(nil))

	assert.Equal(t, int64(0), item.getDiskSize())

	n, err := item.WriteAt([]byte("HELLO"), 10)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	// FIXME we account for the sparse data we've "written" to
	// disk here so this is actually 5 bytes higher than expected
	assert.Equal(t, int64(15), item.getDiskSize())

	n, err = item.WriteAt([]byte("THEND"), 20)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	assert.Equal(t, int64(25), item.getDiskSize())

	require.NoError(t, item.Close(nil))

	checkObject(t, r, "potato", zeroes[:10]+"HELLO"+zeroes[:5]+"THEND")
}

func TestItemWriteAtExisting(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")

	require.NoError(t, item.Open(obj))

	n, err := item.WriteAt([]byte("HELLO"), 10)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	n, err = item.WriteAt([]byte("THEND"), 95)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	n, err = item.WriteAt([]byte("THEVERYEND"), 120)
	require.NoError(t, err)
	assert.Equal(t, 10, n)

	require.NoError(t, item.Close(nil))

	checkObject(t, r, "existing", contents[:10]+"HELLO"+contents[15:95]+"THEND"+zeroes[:20]+"THEVERYEND")
}

func TestItemLoadMeta(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")
	_ = contents

	// Open the object to create metadata for it
	require.NoError(t, item.Open(obj))
	require.NoError(t, item.Close(nil))
	info := item.info

	// Remove the item from the cache
	c.mu.Lock()
	delete(c.item, item.name)
	c.mu.Unlock()

	// Reload the item so we have to load the metadata
	item2, _ := c._get("existing")
	require.NoError(t, item2.Open(obj))
	info2 := item.info
	require.NoError(t, item2.Close(nil))

	// Check that the item is different
	assert.NotEqual(t, item, item2)
	// ... but the info is the same
	assert.Equal(t, info, info2)
}

func TestItemReload(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")
	_ = contents

	// Open the object to create metadata for it
	require.NoError(t, item.Open(obj))

	// Make it dirty
	n, err := item.WriteAt([]byte("THEENDMYFRIEND"), 95)
	require.NoError(t, err)
	assert.Equal(t, 14, n)
	assert.True(t, item.IsDirty())

	// Close the file to pacify Windows, but don't call item.Close()
	item.mu.Lock()
	require.NoError(t, item.fd.Close())
	item.fd = nil
	item.mu.Unlock()

	// Remove the item from the cache
	c.mu.Lock()
	delete(c.item, item.name)
	c.mu.Unlock()

	// Reload the item so we have to load the metadata and restart
	// the transfer
	item2, _ := c._get("existing")
	require.NoError(t, item2.reload(context.Background()))
	assert.False(t, item2.IsDirty())

	// Check that the item is different
	assert.NotEqual(t, item, item2)

	// And check the contents got written back to the remote
	checkObject(t, r, "existing", contents[:95]+"THEENDMYFRIEND")

	// And check that AddVirtual was called
	assert.Equal(t, []avInfo{
		{Remote: "existing", Size: 109, IsDir: false},
	}, avInfos)
}

func TestItemReloadRemoteGone(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")
	_ = contents

	// Open the object to create metadata for it
	require.NoError(t, item.Open(obj))

	size, err := item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(100), size)

	// Read something to instantiate the cache file
	buf := make([]byte, 10)
	_, err = item.ReadAt(buf, 10)
	require.NoError(t, err)

	// Test cache file present
	_, err = os.Stat(item.c.toOSPath(item.name))
	require.NoError(t, err)

	require.NoError(t, item.Close(nil))

	// Remove the remote object
	require.NoError(t, obj.Remove(context.Background()))

	// Re-open with no object
	require.NoError(t, item.Open(nil))

	// Check size is now 0
	size, err = item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(0), size)

	// Test cache file is now empty
	fi, err := os.Stat(item.c.toOSPath(item.name))
	require.NoError(t, err)
	assert.Equal(t, int64(0), fi.Size())

	require.NoError(t, item.Close(nil))
}

func TestItemReloadCacheStale(t *testing.T) {
	r, c := newItemTestCache(t)

	contents, obj, item := newFile(t, r, c, "existing")

	// Open the object to create metadata for it
	require.NoError(t, item.Open(obj))

	size, err := item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(100), size)

	// Read something to instantiate the cache file
	buf := make([]byte, 10)
	_, err = item.ReadAt(buf, 10)
	require.NoError(t, err)

	// Test cache file present
	_, err = os.Stat(item.c.toOSPath(item.name))
	require.NoError(t, err)

	require.NoError(t, item.Close(nil))

	// Update the remote to something different
	contents2, obj, item := newFileLength(t, r, c, "existing", 110)
	assert.NotEqual(t, contents, contents2)

	// Re-open with updated object
	oldFingerprint := item.info.Fingerprint
	assert.NotEqual(t, "", oldFingerprint)
	require.NoError(t, item.Open(obj))

	// Make sure fingerprint was updated
	assert.NotEqual(t, oldFingerprint, item.info.Fingerprint)
	assert.NotEqual(t, "", item.info.Fingerprint)

	// Check size is now 110
	size, err = item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(110), size)

	// Test cache file is now correct size
	fi, err := os.Stat(item.c.toOSPath(item.name))
	require.NoError(t, err)
	assert.Equal(t, int64(110), fi.Size())

	// Write to the file to make it dirty
	// This checks we aren't reusing stale data
	n, err := item.WriteAt([]byte("HELLO"), 0)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, true, item.IsDirty())

	require.NoError(t, item.Close(nil))

	// Now check with all that swizzling stuff around that the
	// object is correct

	checkObject(t, r, "existing", "HELLO"+contents2[5:])
}

// TestItemReloadDirtyBeyondRemote checks that reloading a dirty cache
// item whose cache file has grown larger than the remote object
// recovers what it can from the cache file instead of failing with
// "invalid seek position".
func TestItemReloadDirtyBeyondRemote(t *testing.T) {
	r, c := newItemTestCache(t)

	// Small remote object (100 bytes)
	_, obj, item := newFile(t, r, c, "existing")

	// Open it and write over and past the end of the remote object,
	// growing the cache file to 200 bytes and making it dirty.
	require.NoError(t, item.Open(obj))
	newContents := random.String(200)
	n, err := item.WriteAt([]byte(newContents), 0)
	require.NoError(t, err)
	assert.Equal(t, 200, n)
	assert.True(t, item.IsDirty())

	size, err := item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(200), size)

	// Simulate an unclean shutdown: the cache file on disk is fully grown
	// but the final range-metadata update was never persisted, so Rs
	// under-reports the parts of the file that are present. Here the
	// remaining present range [0,150) ends beyond the 100 byte remote
	// object, so the missing range [150,200) starts past the end of the
	// remote object.
	item.mu.Lock()
	item.info.Rs = item.info.Rs.Intersection(ranges.Range{Pos: 0, Size: 150})
	require.NoError(t, item._save())
	require.NoError(t, item.fd.Close())
	item.fd = nil
	item.mu.Unlock()

	// Drop the item so reload has to reload the metadata from disk
	c.mu.Lock()
	delete(c.item, item.name)
	c.mu.Unlock()

	// Reload the dirty item. Before the fix this failed trying to download
	// the missing [150,200) range from the 100 byte remote object with
	// "invalid seek position".
	item2, _ := c._get("existing")
	require.NoError(t, item2.reload(context.Background()))
	assert.False(t, item2.IsDirty())

	// The grown contents are recovered from the cache file and written
	// back to the remote.
	checkObject(t, r, "existing", newContents)
}

func TestItemReadWrite(t *testing.T) {
	r, c := newItemTestCache(t)
	const (
		size     = 50*1024*1024 + 123
		fileName = "large"
	)

	item, _ := c.get(fileName)
	require.NoError(t, item.Open(nil))

	// Create the test file
	in := readers.NewPatternReader(size)
	buf := make([]byte, 1024*1024)
	buf2 := make([]byte, 1024*1024)
	offset := int64(0)
	for {
		n, err := in.Read(buf)
		n2, err2 := item.WriteAt(buf[:n], offset)
		offset += int64(n2)
		require.NoError(t, err2)
		require.Equal(t, n, n2)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	// Check it is the right size
	readSize, err := item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(size), readSize)

	require.NoError(t, item.Close(nil))

	assert.False(t, item.remove(fileName))

	obj, err := r.Fremote.NewObject(context.Background(), fileName)
	require.NoError(t, err)
	assert.Equal(t, int64(size), obj.Size())

	// read and check a block of size N at offset
	// It returns eof true if the end of file has been reached
	readCheckBuf := func(t *testing.T, in io.ReadSeeker, buf, buf2 []byte, item *Item, offset int64, N int) (n int, eof bool) {
		what := fmt.Sprintf("buf=%p, buf2=%p, item=%p, offset=%d, N=%d", buf, buf2, item, offset, N)
		n, err := item.ReadAt(buf, offset)

		_, err2 := in.Seek(offset, io.SeekStart)
		require.NoError(t, err2, what)
		n2, err2 := in.Read(buf2[:n])
		require.Equal(t, n, n2, what)
		assert.Equal(t, buf[:n], buf2[:n2], what)
		assert.Equal(t, buf[:n], buf2[:n2], what)

		if err == io.EOF {
			return n, true
		}
		require.NoError(t, err, what)
		require.NoError(t, err2, what)
		return n, false
	}
	readCheck := func(t *testing.T, item *Item, offset int64, N int) (n int, eof bool) {
		return readCheckBuf(t, in, buf, buf2, item, offset, N)
	}

	// Read it back sequentially
	t.Run("Sequential", func(t *testing.T) {
		require.NoError(t, item.Open(obj))
		assert.False(t, item.present())
		offset := int64(0)
		for {
			n, eof := readCheck(t, item, offset, len(buf))
			offset += int64(n)
			if eof {
				break
			}
		}
		assert.Equal(t, int64(size), offset)
		require.NoError(t, item.Close(nil))
		assert.False(t, item.remove(fileName))
	})

	// Read it back randomly
	t.Run("Random", func(t *testing.T) {
		require.NoError(t, item.Open(obj))
		assert.False(t, item.present())
		for !item.present() {
			blockSize := rand.Intn(len(buf))
			offset := max(rand.Int63n(size+2*int64(blockSize))-int64(blockSize), 0)
			_, _ = readCheck(t, item, offset, blockSize)
		}
		require.NoError(t, item.Close(nil))
		assert.False(t, item.remove(fileName))
	})

	// Read it back randomly concurrently
	t.Run("RandomConcurrent", func(t *testing.T) {
		require.NoError(t, item.Open(obj))
		assert.False(t, item.present())
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				in := readers.NewPatternReader(size)
				buf := make([]byte, 1024*1024)
				buf2 := make([]byte, 1024*1024)
				for !item.present() {
					blockSize := rand.Intn(len(buf))
					offset := max(rand.Int63n(size+2*int64(blockSize))-int64(blockSize), 0)
					_, _ = readCheckBuf(t, in, buf, buf2, item, offset, blockSize)
				}
			})
		}
		wg.Wait()
		require.NoError(t, item.Close(nil))
		assert.False(t, item.remove(fileName))
	})

	// Read it back in reverse which creates the maximum number of
	// downloaders
	t.Run("Reverse", func(t *testing.T) {
		require.NoError(t, item.Open(obj))
		assert.False(t, item.present())
		offset := int64(size)
		for {
			blockSize := len(buf)
			offset -= int64(blockSize)
			if offset < 0 {
				offset = 0
				blockSize += int(offset)
			}
			_, _ = readCheck(t, item, offset, blockSize)
			if offset == 0 {
				break
			}
		}
		require.NoError(t, item.Close(nil))
		assert.False(t, item.remove(fileName))
	})
}

func newItemTestCacheHandleCaching(t *testing.T, handleCaching time.Duration) (r *fstest.Run, c *Cache) {
	opt := vfscommon.Opt

	// Disable the cache cleaner as it interferes with these tests
	opt.CachePollInterval = 0

	// Disable synchronous write
	opt.WriteBack = 0

	// Set handle caching grace period
	opt.HandleCaching = fs.Duration(handleCaching)

	return newTestCacheOpt(t, opt)
}

func TestItemHandleCaching(t *testing.T) {
	r, c := newItemTestCacheHandleCaching(t, 1*time.Second)

	contents, obj, item := newFile(t, r, c, "existing")

	// Open, read, and close the item
	require.NoError(t, item.Open(obj))

	buf := make([]byte, 10)
	n, err := item.ReadAt(buf, 0)
	assert.Equal(t, 10, n)
	require.NoError(t, err)
	assert.Equal(t, contents[:10], string(buf[:n]))

	require.NoError(t, item.Close(nil))

	// After close, grace period should keep fd and downloaders alive
	item.mu.Lock()
	assert.NotNil(t, item.fd, "fd should still be open during grace period")
	assert.NotNil(t, item.downloaders, "downloaders should still be alive during grace period")
	assert.NotNil(t, item.graceTimer, "grace timer should be set")
	item.mu.Unlock()

	// Re-open the item - should reuse existing fd and downloaders
	require.NoError(t, item.Open(obj))

	// Read data to verify it works
	n, err = item.ReadAt(buf, 10)
	assert.Equal(t, 10, n)
	require.NoError(t, err)
	assert.Equal(t, contents[10:20], string(buf[:n]))

	// Close again
	require.NoError(t, item.Close(nil))

	// Wait for grace period to expire
	time.Sleep(1500 * time.Millisecond)

	// After grace period, fd and downloaders should be cleaned up
	item.mu.Lock()
	assert.Nil(t, item.fd, "fd should be closed after grace period")
	assert.Nil(t, item.downloaders, "downloaders should be closed after grace period")
	assert.Nil(t, item.graceTimer, "grace timer should be nil after expiry")
	item.mu.Unlock()
}

func TestItemHandleCachingDisabled(t *testing.T) {
	r, c := newItemTestCacheHandleCaching(t, 0)

	contents, obj, item := newFile(t, r, c, "existing")
	_ = contents

	// Open and close the item
	require.NoError(t, item.Open(obj))
	require.NoError(t, item.Close(nil))

	// With handle caching disabled, fd and downloaders should be immediately closed
	item.mu.Lock()
	assert.Nil(t, item.fd, "fd should be closed immediately when handle caching disabled")
	assert.Nil(t, item.downloaders, "downloaders should be closed immediately when handle caching disabled")
	assert.Nil(t, item.graceTimer, "grace timer should not be set when handle caching disabled")
	item.mu.Unlock()
}

func TestItemHandleCachingReset(t *testing.T) {
	r, c := newItemTestCacheHandleCaching(t, 1*time.Second)

	_, obj, item := newFile(t, r, c, "existing")

	// Open, read (to instantiate cache), and close the item
	require.NoError(t, item.Open(obj))

	buf := make([]byte, 10)
	_, err := item.ReadAt(buf, 0)
	require.NoError(t, err)

	require.NoError(t, item.Close(nil))

	// Grace timer should be active
	item.mu.Lock()
	assert.NotNil(t, item.graceTimer, "grace timer should be set")
	item.mu.Unlock()

	// Reset should skip the item during grace period
	rr, _, err := item.Reset()
	require.NoError(t, err)
	assert.Equal(t, SkippedGrace, rr)

	// Grace timer should still be active
	item.mu.Lock()
	assert.NotNil(t, item.graceTimer, "grace timer should still be set after skipped reset")
	item.mu.Unlock()

	// Wait for grace period to expire then reset should remove the item
	time.Sleep(1500 * time.Millisecond)

	rr, _, err = item.Reset()
	require.NoError(t, err)
	assert.Equal(t, RemovedNotInUse, rr)
}

// TestItemHandleCachingReopenDuringGraceClose reproduces a race between
// reopening an item and the grace-period close firing for it.
//
// closeAfterGrace clears the grace timer and then runs the actual close,
// which temporarily drops item.mu while it tears down the downloaders -
// at that point the file handle is still open. A reopen landing in that
// window used to see no grace timer and a live fd and fail _createFile
// with "internal error: didn't Close file".
func TestItemHandleCachingReopenDuringGraceClose(t *testing.T) {
	r, c := newItemTestCacheHandleCaching(t, 10*time.Second)

	_, obj, item := newFile(t, r, c, "existing")
	buf := make([]byte, 1)

	const iterations = 50
	for i := range iterations {
		// Open, read (to create a downloader) and close so a grace
		// timer is pending with the fd and downloaders still alive.
		require.NoError(t, item.Open(obj))
		_, err := item.ReadAt(buf, 0)
		require.NoError(t, err)
		require.NoError(t, item.Close(nil))

		// Drive the grace close ourselves so we can race it against a
		// reopen. Hold item.mu and park both the close (A) and the
		// reopen (B) on the lock with A queued first. When we release,
		// A runs the close, which drops item.mu to tear down the
		// downloaders, and B - already waiting - grabs it in that
		// window and observes the still-open fd.
		item.mu.Lock()
		require.NotNil(t, item.graceTimer, "grace timer should be set after close")
		item.graceTimer.Stop()

		var openErr error
		var wg sync.WaitGroup
		wg.Add(2)
		startA := make(chan struct{})
		startB := make(chan struct{})
		go func() {
			defer wg.Done()
			<-startA
			item.closeAfterGrace()
		}()
		go func() {
			defer wg.Done()
			<-startB
			openErr = item.Open(obj)
		}()
		close(startA)
		time.Sleep(time.Millisecond) // let A park on item.mu first
		close(startB)
		time.Sleep(time.Millisecond) // let B park on item.mu
		item.mu.Unlock()
		wg.Wait()

		require.NoError(t, openErr, "reopen racing a grace-period close failed on iteration %d", i)

		// Drop the handle from the successful reopen so the next
		// iteration starts from a closed item.
		require.NoError(t, item.Close(nil))
	}

	// Stop the grace timer left pending by the final close.
	item.mu.Lock()
	if item.graceTimer != nil {
		item.graceTimer.Stop()
	}
	item.mu.Unlock()
}

// Reopening in the grace period after a remote modtime change must
// read the remote data, not zeros from an unfetched recreated file.
func TestItemHandleCachingStaleReopenReadsRemote(t *testing.T) {
	r, c := newItemTestCacheHandleCaching(t, 10*time.Second)
	ctx := context.Background()

	contents, obj, item := newFile(t, r, c, "existing")

	require.NoError(t, item.Open(obj))
	buf := make([]byte, len(contents))
	n, err := item.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, contents, string(buf[:n]))
	require.NoError(t, item.Close(nil))

	require.NoError(t, obj.SetModTime(ctx, time.Now().Add(time.Hour)))
	obj, err = r.Fremote.NewObject(ctx, "existing")
	require.NoError(t, err)

	require.NoError(t, item.Open(obj))
	defer func() { require.NoError(t, item.Close(nil)) }()

	size, err := item.GetSize()
	require.NoError(t, err)
	assert.Equal(t, int64(len(contents)), size)

	buf = make([]byte, len(contents))
	n, err = item.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, contents, string(buf[:n]))
}

func TestItemReloadNewDirtyFileReadsLocalData(t *testing.T) {
	_, c := newItemTestCache(t)
	c.opt.WriteBack = fs.Duration(time.Hour)
	item, _ := c.get("new-local")
	require.NoError(t, item.Open(nil))
	want := []byte("local data survives a restart")
	_, err := item.WriteAt(want, 0)
	require.NoError(t, err)
	// Leave only the initial dirty marker on disk, as a crash during writes does.
	item.mu.Lock()
	item.info.Rs = nil
	item.info.Size = 0
	require.NoError(t, item._save())
	require.NoError(t, item.fd.Close())
	item.fd = nil
	item.mu.Unlock()
	c.mu.Lock()
	delete(c.item, item.name)
	c.mu.Unlock()
	recovered, _ := c.get("new-local")
	require.NoError(t, recovered.reload(context.Background()))
	require.NoError(t, recovered.Open(nil))
	got := make([]byte, len(want))
	_, err = recovered.ReadAt(got, 0)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.NoError(t, recovered.Close(nil))
}

func TestB2VFSJournalCAS(t *testing.T) {
	_, c := newItemTestCache(t)
	c.opt.WriteBack = fs.Duration(24 * time.Hour)
	item, _ := c.get("journal")
	require.NoError(t, item.Open(nil))
	_, err := item.WriteAt([]byte("original"), 0)
	require.NoError(t, err)
	store := item.admissionStore()
	before, err := store.LoadUploadAdmission(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, before.Generation)
	require.NoError(t, item.Close(nil))
	require.NoError(t, item.Open(nil))
	unchanged, err := item.admissionStore().LoadUploadAdmission(context.Background())
	require.NoError(t, err)
	require.Equal(t, before, unchanged)
	rr, _, err := item.Reset()
	require.NoError(t, err)
	require.Equal(t, SkippedDirty, rr)
	_, err = item.WriteAt([]byte("changed!"), 0)
	require.NoError(t, err)
	after, err := item.admissionStore().LoadUploadAdmission(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, before.Generation, after.Generation)
	require.Error(t, store.StoreUploadAdmission(context.Background(), before))
	require.NoError(t, item.Close(nil))
}

// The parent owns the server and cache directory across two independent processes.
type b2CacheOracle struct {
	mu                                         sync.Mutex
	mode                                       string
	cacheDir                                   string
	request                                    api.UploadCreateRequest
	key                                        string
	session                                    api.UploadSession
	keys                                       map[string]bool
	creates, folders, puts, publications, gets int
	restored                                   bool
	completed                                  bool
	allowSuccess                               bool
	completeStarted                            chan struct{}
	releaseComplete                            chan struct{}
	stored                                     []byte
	original                                   []byte
	downloads                                  int
}

func (x *b2CacheOracle) serve(w http.ResponseWriter, r *http.Request) {
	x.mu.Lock()
	defer x.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	allowSuccess := x.allowSuccess || x.mode == "completed_result_control" ||
		(x.mode == "new_content_control" && x.restored && x.creates > 1) ||
		(x.restored && (x.mode == "transient_selected_recovery" || x.mode == "lost_create_recovery" || x.mode == "result_replaced_restart"))
	refuse := func() {
		if !x.restored && (x.mode == "transient_selected_recovery" || x.mode == "lost_create_recovery" || x.mode == "result_replaced_restart") {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":{"code":"temporary","message":"retry selected session"}}`)
			return
		}
		w.WriteHeader(http.StatusConflict)
		if strings.Contains(x.mode, "truncated") || strings.HasPrefix(x.mode, "discovery_") {
			_, _ = io.WriteString(w, `{"error":`)
		} else if x.mode == "legacy_unknown_restart" {
			_, _ = io.WriteString(w, `{"error":{"code":"conflict","message":"legacy destination unknown"}}`)
		} else {
			_, _ = io.WriteString(w, `{"error":{"code":"upload_parent_unavailable","message":"destination unavailable"}}`)
		}
	}
	switch {
	case r.Method == "GET" && r.URL.Path == "/api/v1/me":
		_ = json.NewEncoder(w).Encode(api.UserProfile{UserId: 42})
	case r.Method == "GET" && (r.URL.Path == "/api/v1/files/old-target/content" || r.URL.Path == "/api/v1/files/winner/content"):
		x.downloads++
		body := x.original
		if r.URL.Path == "/api/v1/files/winner/content" {
			body = x.stored
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, "file.txt", time.Unix(1700000000, 0), bytes.NewReader(body))
	case r.Method == "GET" && r.URL.Path == "/api/v1/files":
		if x.completed && x.mode != "discovery_equal_restored_restart" {
			id := "winner"
			if x.mode == "result_replaced_restart" {
				id = "replacement"
			}
			_ = json.NewEncoder(w).Encode(api.ReadMetadataResponse{Files: []api.FileInfo{{Id: id, Kind: "file", Name: x.request.Name, Size: int64(len(x.stored)), ModTime: x.request.ModTime, Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString(x.stored)}}}})
			return
		}
		if x.restored && x.mode == "discovery_equal_restored_restart" {
			_ = json.NewEncoder(w).Encode(api.ReadMetadataResponse{Files: []api.FileInfo{{
				Id: "old-target", Kind: "file", Name: "file.txt", Size: 4, ModTime: x.request.ModTime,
				Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString([]byte("DATA"))},
			}}})
			return
		}
		if x.original != nil {
			_ = json.NewEncoder(w).Encode(api.ReadMetadataResponse{Files: []api.FileInfo{{
				Id: "old-target", Kind: "file", Name: "file.txt", Size: int64(len(x.original)),
				ModTime: time.Unix(1700000000, 0),
				Hash:    &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString(x.original)},
			}}})
			return
		}
		_ = json.NewEncoder(w).Encode(api.ReadMetadataResponse{})
	case r.Method == "GET" && r.URL.Path == "/api/v1/uploads":
		_ = json.NewEncoder(w).Encode(api.UploadSessionPage{Items: []api.UploadSession{}})
	case r.Method == "POST" && r.URL.Path == "/api/v1/uploads":
		var request api.UploadCreateRequest
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			w.WriteHeader(400)
			return
		}
		key := r.Header.Get("Idempotency-Key")
		// Observe the real Item metadata before acknowledging remote admission.
		raw, err := os.ReadFile(filepath.Join(x.cacheDir, "vfsMeta", "b2-vfs", "file.txt"))
		var info Info
		var state fs.UploadAdmissionState
		if err != nil || json.Unmarshal(raw, &info) != nil || json.Unmarshal(info.UploadAdmission, &state) != nil ||
			state.RequestKey != key || len(state.Request) == 0 || state.Generation == "" {
			w.WriteHeader(500)
			return
		}
		if !x.keys[key] {
			x.keys[key] = true
			x.creates++
			x.request, x.key = request, key
			x.session = api.UploadSession{ID: uuid.NewString(), Name: request.Name, ParentId: request.ParentId,
				ExpectedSize: request.Size, MimeType: request.MimeType, ModTime: request.ModTime,
				Encryption: request.Encryption, ConflictPolicy: request.ConflictPolicy, PartSize: tdhash.BlockSize,
				State: "open", RequiresCompletionManifest: true, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
		} else if request != x.request {
			w.WriteHeader(409)
			return
		}
		if strings.Contains(x.mode, "lost_create") {
			if !x.restored {
				_, _ = io.WriteString(w, `{"id":`)
				return
			}
			if !allowSuccess {
				refuse()
				return
			}
		}
		_ = json.NewEncoder(w).Encode(x.session)
	case r.Method == "GET" && r.URL.Path == "/api/v1/uploads/"+x.session.ID:
		x.gets++
		if x.restored && x.mode == "discovery_unavailable_proof" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":{"code":"not_found","message":"selected session unavailable"}}`)
			return
		}
		if x.completed {
			session := x.session
			session.State, session.FileId = "completed", "winner"
			_ = json.NewEncoder(w).Encode(session)
			return
		}
		if allowSuccess {
			_ = json.NewEncoder(w).Encode(x.session)
			return
		}
		refuse()
	case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/parts"):
		page := api.UploadPartPage{Items: []api.UploadPart{}}
		if x.completed {
			page.Items = []api.UploadPart{{UploadId: x.session.ID, PartNo: 1, State: "stored",
				PlainSize: int64(len(x.stored)), Checksum: tdhash.SumString(x.stored)}}
		}
		_ = json.NewEncoder(w).Encode(page)
	case r.Method == "PUT" && strings.Contains(r.URL.Path, "/parts/"):
		x.puts++
		raw, err := os.ReadFile(filepath.Join(x.cacheDir, "vfsMeta", "b2-vfs", "file.txt"))
		var info Info
		var selected fs.UploadAdmissionState
		if err != nil || json.Unmarshal(raw, &info) != nil || json.Unmarshal(info.UploadAdmission, &selected) != nil ||
			selected.UploadID != x.session.ID {
			w.WriteHeader(500)
			return
		}
		if !allowSuccess {
			refuse()
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(400)
			return
		}
		x.stored = append([]byte(nil), body...)
		_ = json.NewEncoder(w).Encode(api.UploadPart{UploadId: x.session.ID, PartNo: 1, State: "stored",
			PlainSize: int64(len(body)), Checksum: tdhash.SumString(body)})
	case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/complete") && (x.completed || allowSuccess):
		var manifest api.UploadCompleteRequest
		if json.NewDecoder(r.Body).Decode(&manifest) != nil {
			w.WriteHeader(400)
			return
		}
		if x.completeStarted != nil {
			close(x.completeStarted)
			select {
			case <-x.releaseComplete:
			case <-r.Context().Done():
				return
			}
		}
		if !x.completed {
			x.publications++
			x.completed = true
			if x.mode == "completed_result_control" && !x.restored {
				_, _ = io.WriteString(w, `{"id":`)
				return
			}
		}
		_ = json.NewEncoder(w).Encode(api.FileInfo{Id: "winner", Kind: "file", Name: x.request.Name,
			Size: manifest.Size, Hash: manifest.Hash, ModTime: x.request.ModTime})
	case r.URL.Path == "/api/v1/folders":
		x.folders++
		w.WriteHeader(409)
	default:
		w.WriteHeader(400)
	}
}

func b2OwnedCache(t *testing.T, root, endpoint string) (context.Context, *Cache) {
	t.Helper()
	previous := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(root))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(previous)) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	ctx, ci := fs.AddConfig(ctx)
	ci.LowLevelRetries, ci.Transfers = 1, 2
	ci.MultiThreadStreams = 0
	f, err := teldrive.NewFs(ctx, "b2-vfs", "", configmap.Simple{"api_host": endpoint, "api_key": "synthetic-key", "chunk_size": "16Mi"})
	require.NoError(t, err)
	opt := vfscommon.Opt
	opt.WriteBack, opt.CachePollInterval, opt.HandleCaching = fs.Duration(24*time.Hour), 0, 0
	c, err := New(ctx, f, &opt, func(string, int64, bool) error { return nil })
	require.NoError(t, err)
	return ctx, c
}

func b2CacheChild(t *testing.T) {
	ctx, c := b2OwnedCache(t, os.Getenv("B2_VFS_CACHE"), os.Getenv("B2_VFS_ENDPOINT"))
	item, _ := c.get("file.txt")
	stage, mode := os.Getenv("B2_VFS_STAGE"), os.Getenv("B2_VFS_MODE")
	if strings.HasPrefix(mode, "existing_remote_") {
		if stage == "first" {
			obj, err := c.fremote.NewObject(ctx, item.name)
			require.NoError(t, err)
			require.NoError(t, item.Open(obj))
			tail := make([]byte, 8)
			n, err := item.ReadAt(tail, obj.Size()-int64(len(tail)))
			require.NoError(t, err)
			require.Equal(t, len(tail), n)
			require.Equal(t, "e-tail--", string(tail))
			require.False(t, item.present())
			require.False(t, item.HasRange(ranges.Range{Pos: 8, Size: 5}))
			n, err = item.WriteAt([]byte("FIRST"), 8)
			require.NoError(t, err)
			require.Equal(t, 5, n)
			generation := item.info.UploadGeneration
			require.NotEmpty(t, generation)
			require.False(t, item.HasRange(ranges.Range{Pos: 24, Size: 4}))
			n, err = item.WriteAt([]byte("NEXT"), 24)
			require.NoError(t, err)
			require.Equal(t, 4, n)
			if mode == "existing_remote_sparse_write" {
				n, err = item.WriteAt([]byte("TAIL"), obj.Size()+9)
				require.NoError(t, err)
				require.Equal(t, 4, n)
			}
			if mode == "existing_remote_truncate" {
				require.NoError(t, item.Truncate(40))
				require.NoError(t, item.Truncate(56))
			}
			require.Equal(t, generation, item.info.UploadGeneration)
			// Keep the file open: neither Close nor Sync may repair the journal.
		} else {
			require.True(t, item.IsDirty())
			rr, _, err := item.Reset()
			require.NoError(t, err)
			require.Equal(t, SkippedDirty, rr)
			require.NotEmpty(t, c.writeback.Queue())
			require.NoError(t, c.QueueSetExpiry(item.writeBackID, time.Now(), 0))
			require.Eventually(t, func() bool { return !item.IsDirty() }, 5*time.Second, time.Millisecond)
			got, err := os.ReadFile(c.toOSPath(item.name))
			require.NoError(t, err)
			require.Equal(t, os.Getenv("B2_VFS_EXPECTED_SHA256"), fmt.Sprintf("%x", sha256.Sum256(got)))
			obj, err := c.fremote.NewObject(ctx, item.name)
			require.NoError(t, err)
			in, err := obj.Open(ctx)
			require.NoError(t, err)
			uploaded, err := io.ReadAll(in)
			require.NoError(t, err)
			require.NoError(t, in.Close())
			require.Equal(t, os.Getenv("B2_VFS_EXPECTED_SHA256"), fmt.Sprintf("%x", sha256.Sum256(uploaded)))
		}
		require.Equal(t, int64(24*time.Hour), int64(c.opt.WriteBack))
		fmt.Println("B2_VFS_CHILD_COMMITTED")
		os.Exit(0)
	}
	if stage == "first" {
		require.NoError(t, item.Open(nil))
		_, err := item.WriteAt([]byte("data"), 0)
		require.NoError(t, err)
		require.NoError(t, item.Close(nil))
		if mode == "legacy_dirty_fail_closed" {
			item.mu.Lock()
			item.info.UploadGeneration, item.info.UploadAdmission = "", nil
			require.NoError(t, item._save())
			item.mu.Unlock()
		}
	}
	before := item.info.UploadGeneration
	if stage == "restart" && strings.HasPrefix(mode, "discovery_") {
		fingerprint := item.info.Fingerprint
		journal, err := os.ReadFile(c.toOSPath("file.txt"))
		require.NoError(t, err)
		require.Equal(t, sha256.Sum256([]byte("data")), sha256.Sum256(journal))
		source, err := c.fcache.NewObject(ctx, item.name)
		require.NoError(t, err)
		if mode == "discovery_unavailable_proof" {
			_, ordinaryErr := c.fremote.NewObject(ctx, item.name)
			require.ErrorIs(t, ordinaryErr, fs.ErrorObjectNotFound)
			require.False(t, fs.IsUploadAdmissionError(ordinaryErr))
		}
		proofCtx := fs.WithUploadAdmissionStore(ctx, item.admissionStore())
		proofCtx = fs.WithUploadAdmissionIntent(proofCtx, fs.UploadAdmissionIntent{
			Source: source, Destination: c.fremote, DestinationRemote: item.name,
		})
		remote := source.Remote()
		proofCtx = fs.WithUploadAdmissionDiscovery(proofCtx, source.Fs(), &remote)
		_, err = c.fremote.NewObject(proofCtx, item.name)
		require.Error(t, err)
		require.True(t, fs.IsUploadAdmissionError(err))
		if mode == "discovery_unavailable_proof" {
			require.ErrorIs(t, err, fs.ErrorObjectNotFound)
		}
		proofCtx = fs.WithUploadAdmissionDiscovery(proofCtx, source.Fs(), nil)
		callbacks := 0
		callback := func(fs.DirEntries) error { callbacks++; return nil }
		require.Error(t, c.fremote.Features().ListP(proofCtx, "", callback))
		require.Error(t, c.fremote.Features().ListR(proofCtx, "", callback))
		require.Zero(t, callbacks)
		if mode == "discovery_partial_cache" {
			require.Error(t, item.reload(ctx))
			require.Empty(t, c.writeback.Queue())
		} else {
			require.NoError(t, item.reload(ctx))
			require.NotEmpty(t, c.writeback.Queue())
			require.NoError(t, c.QueueSetExpiry(item.writeBackID, time.Now(), 0))
			require.Eventually(t, func() bool {
				for _, q := range c.writeback.Queue() {
					if q.ID == item.writeBackID && q.Tries > 0 && !q.Uploading {
						return true
					}
				}
				return false
			}, 5*time.Second, time.Millisecond)
		}
		stores := 0
		require.Error(t, item.store(ctx, func(fs.Object) { stores++ }))
		require.Zero(t, stores)
		require.True(t, item.IsDirty())
		require.Equal(t, fingerprint, item.info.Fingerprint)
		after, err := os.ReadFile(c.toOSPath("file.txt"))
		require.NoError(t, err)
		require.Equal(t, sha256.Sum256(journal), sha256.Sum256(after))
		fmt.Println("B2_VFS_CHILD_COMMITTED")
		os.Exit(0)
	}
	if stage == "new-content" {
		require.NoError(t, item.Open(nil))
		_, err := item.WriteAt([]byte("DATA"), 0)
		require.NoError(t, err)
		require.NoError(t, item.Close(nil))
		require.NotEqual(t, before, item.info.UploadGeneration)
	}
	rr, _, err := item.Reset()
	require.NoError(t, err)
	require.Equal(t, SkippedDirty, rr)
	// The real public queue expiry boundary drives the unchanged scheduler.
	if len(c.writeback.Queue()) != 0 {
		require.NoError(t, c.QueueSetExpiry(item.writeBackID, time.Now(), 0))
		require.Eventually(t, func() bool {
			for _, q := range c.writeback.Queue() {
				if q.ID == item.writeBackID && q.Tries > 0 && !q.Uploading {
					return true
				}
			}
			return !item.IsDirty()
		}, 5*time.Second, time.Millisecond)
	} else {
		err := item.store(ctx, nil)
		if stage == "completed" || stage == "new-content" || stage == "recovered" {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
	if stage == "completed" || stage == "new-content" || stage == "recovered" {
		require.False(t, item.IsDirty())
	} else {
		require.True(t, item.IsDirty())
	}
	got, err := os.ReadFile(c.toOSPath("file.txt"))
	require.NoError(t, err)
	want := "data"
	if stage == "new-content" {
		want = "DATA"
	}
	require.Equal(t, sha256.Sum256([]byte(want)), sha256.Sum256(got))
	require.Equal(t, int64(24*time.Hour), int64(c.opt.WriteBack))
	if stage == "first" && mode == "discovery_partial_cache" {
		item.mu.Lock()
		item.info.Rs = ranges.Ranges{{Pos: 0, Size: 2}}
		item.info.Fingerprint = "original-partial-source"
		require.NoError(t, item._save())
		item.mu.Unlock()
	}
	// Exit without cache cleanup or deferred queue cancellation, like a process loss.
	fmt.Println("B2_VFS_CHILD_COMMITTED")
	os.Exit(0)
}

func TestB2VFSAdmissionRestart(t *testing.T) {
	if os.Getenv("B2_VFS_CHILD") == "1" {
		b2CacheChild(t)
		return
	}
	for _, mode := range []string{"lost_create_ack_restart", "truncated_terminal_restart", "legacy_dirty_fail_closed",
		"metadata_save_failure", "stale_generation", "explicit_retry_reset", "new_content_control", "completed_result_control", "legacy_unknown_restart",
		"discovery_equal_restored_restart", "discovery_unavailable_proof", "discovery_partial_cache", "transient_selected_recovery", "lost_create_recovery", "result_replaced_restart",
		"existing_remote_write", "existing_remote_sparse_write", "existing_remote_truncate"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "legacy_dirty_fail_closed" {
				t.Run("opaque_invalid_retains_dirty", b2CacheOpaqueAdmission)
			}
			if mode == "explicit_retry_reset" {
				t.Run("rename_and_noop_controls", b2CacheRenameTransitions)
			}
			if mode == "new_content_control" {
				t.Run("open_admitted_write", b2CacheOpenWriteTransition)
			}
			if mode == "metadata_save_failure" {
				t.Run("before_send", func(t *testing.T) { b2CacheJournalFailure(t, mode) })
				t.Run("after_result", b2CacheJournalAfterResultFailure)
				t.Run("before_local_write", b2CacheWritePersistenceFailure)
				return
			}
			if mode == "stale_generation" {
				b2CacheJournalFailure(t, mode)
				return
			}
			root := t.TempDir()
			x := &b2CacheOracle{mode: mode, cacheDir: root, keys: make(map[string]bool), allowSuccess: strings.HasPrefix(mode, "existing_remote_")}
			var expected []byte
			if strings.HasPrefix(mode, "existing_remote_") {
				x.original = []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ--old-remote-tail--")
				expected = append([]byte(nil), x.original...)
				copy(expected[8:13], "FIRST")
				copy(expected[24:28], "NEXT")
				if mode == "existing_remote_sparse_write" {
					expected = append(expected, make([]byte, 9)...)
					expected = append(expected, "TAIL"...)
				}
				if mode == "existing_remote_truncate" {
					expected = append(expected[:40], make([]byte, 16)...)
				}
			}
			server := httptest.NewServer(http.HandlerFunc(x.serve))
			defer server.Close()
			runChild := func(stage string) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestB2VFSAdmissionRestart$", "-test.timeout=12s")
				cmd.Env = append(os.Environ(), "B2_VFS_CHILD=1", "B2_VFS_CACHE="+root, "B2_VFS_ENDPOINT="+server.URL,
					"B2_VFS_MODE="+mode, "B2_VFS_STAGE="+stage,
					fmt.Sprintf("B2_VFS_EXPECTED_SHA256=%x", sha256.Sum256(expected)))
				out, err := cmd.CombinedOutput()
				require.NoError(t, err, "%s", out)
				require.Contains(t, string(out), "B2_VFS_CHILD_COMMITTED")
			}
			runChild("first")
			readState := func() (Info, fs.UploadAdmissionState) {
				body, err := os.ReadFile(filepath.Join(root, "vfsMeta", "b2-vfs", "file.txt"))
				require.NoError(t, err)
				var info Info
				require.NoError(t, json.Unmarshal(body, &info))
				var state fs.UploadAdmissionState
				if len(info.UploadAdmission) != 0 {
					require.NoError(t, json.Unmarshal(info.UploadAdmission, &state))
				}
				return info, state
			}
			info, original := readState()
			require.True(t, info.Dirty)
			if strings.HasPrefix(mode, "existing_remote_") {
				x.mu.Lock()
				require.Zero(t, x.creates)
				require.Zero(t, x.publications)
				downloads := x.downloads
				require.Positive(t, downloads)
				x.mu.Unlock()
				runChild("restart")
				require.NotEmpty(t, info.Fingerprint)
				require.NotEmpty(t, info.UploadGeneration)
				require.Equal(t, int64(len(expected)), info.Size)
				require.True(t, info.Rs.Present(ranges.Range{Pos: 8, Size: 5}))
				require.True(t, info.Rs.Present(ranges.Range{Pos: 24, Size: 4}))
				if mode == "existing_remote_sparse_write" {
					require.True(t, info.Rs.Present(ranges.Range{Pos: int64(len(x.original)), Size: 13}))
				}
				if mode == "existing_remote_truncate" {
					require.True(t, info.Rs.Present(ranges.Range{Pos: 40, Size: 16}))
					for _, r := range info.Rs {
						require.LessOrEqual(t, r.End(), info.Size)
					}
				}
				afterInfo, _ := readState()
				require.False(t, afterInfo.Dirty)
				require.Equal(t, info.UploadGeneration, afterInfo.UploadGeneration)
				x.mu.Lock()
				defer x.mu.Unlock()
				require.Equal(t, 1, x.creates)
				require.Equal(t, 1, x.puts)
				require.Equal(t, 1, x.publications)
				require.Zero(t, x.folders)
				require.Greater(t, x.downloads, downloads)
				require.Equal(t, expected, x.stored)
				require.Equal(t, sha256.Sum256(expected), sha256.Sum256(x.stored))
				return
			}
			x.mu.Lock()
			x.restored = true
			if mode == "completed_result_control" {
				require.True(t, x.completed)
				require.Equal(t, sha256.Sum256([]byte("data")), sha256.Sum256(x.stored))
			}
			created, puts := x.creates, x.puts
			x.mu.Unlock()
			stage := "restart"
			if mode == "new_content_control" {
				stage = "new-content"
			}
			if mode == "completed_result_control" {
				stage = "completed"
			}
			if mode == "transient_selected_recovery" || mode == "lost_create_recovery" {
				stage = "recovered"
			}
			runChild(stage)
			afterInfo, after := readState()
			x.mu.Lock()
			defer x.mu.Unlock()
			require.Zero(t, x.folders)
			if mode == "new_content_control" || mode == "completed_result_control" || mode == "transient_selected_recovery" || mode == "lost_create_recovery" || mode == "result_replaced_restart" {
				require.Equal(t, 1, x.publications)
				want := "data"
				if mode == "new_content_control" {
					want = "DATA"
				}
				require.Equal(t, sha256.Sum256([]byte(want)), sha256.Sum256(x.stored))
			} else {
				require.Zero(t, x.publications)
			}
			if mode == "new_content_control" {
				require.NotEqual(t, original.Generation, after.Generation)
				require.NotEqual(t, original.RequestKey, after.RequestKey)
				require.Equal(t, created+1, x.creates)
			} else if mode == "transient_selected_recovery" || mode == "lost_create_recovery" || mode == "result_replaced_restart" {
				require.Equal(t, original.Generation, after.Generation)
				require.Equal(t, original.RequestKey, after.RequestKey)
				require.Equal(t, original.Request, after.Request)
				require.Equal(t, original.IntentFingerprint, after.IntentFingerprint)
				require.Equal(t, x.session.ID, after.UploadID)
				require.Equal(t, created, x.creates)
				require.Equal(t, puts+1, x.puts)
			} else {
				require.Equal(t, original, after)
				require.Equal(t, created, x.creates)
				require.Equal(t, puts, x.puts)
				if strings.HasPrefix(mode, "discovery_") {
					require.Equal(t, info.Fingerprint, afterInfo.Fingerprint)
					require.NotEmpty(t, after.UploadID)
				}
			}
			require.Equal(t, mode != "completed_result_control" && mode != "new_content_control" && mode != "transient_selected_recovery" && mode != "lost_create_recovery", afterInfo.Dirty)
		})
	}
}

func b2CacheJournalFailure(t *testing.T, mode string) {
	root := t.TempDir()
	x := &b2CacheOracle{mode: mode, cacheDir: root, keys: make(map[string]bool)}
	server := httptest.NewServer(http.HandlerFunc(x.serve))
	defer server.Close()
	ctx, c := b2OwnedCache(t, root, server.URL)
	item, _ := c.get("file.txt")
	require.NoError(t, item.Open(nil))
	_, err := item.WriteAt([]byte("data"), 0)
	require.NoError(t, err)
	require.NoError(t, item.Close(nil))
	c.writeback.Remove(item.writeBackID)
	store := item.admissionStore()
	state, err := store.LoadUploadAdmission(ctx)
	require.NoError(t, err)
	if mode == "metadata_save_failure" {
		meta := c.toOSPathMeta("file.txt")
		before, err := os.ReadFile(meta)
		require.NoError(t, err)
		require.NoError(t, os.Rename(meta, meta+".retained"))
		require.NoError(t, os.Mkdir(meta, 0700))
		require.Error(t, item.store(ctx, nil))
		after, err := os.ReadFile(meta + ".retained")
		require.NoError(t, err)
		require.Equal(t, before, after)
	} else {
		x.allowSuccess = true
		x.completeStarted, x.releaseComplete = make(chan struct{}), make(chan struct{})
		result := make(chan error, 1)
		callback := make(chan struct{}, 1)
		go func() { result <- item.store(ctx, func(fs.Object) { callback <- struct{}{} }) }()
		select {
		case <-x.completeStarted:
		case <-ctx.Done():
			t.Fatal("original store never reached completion")
		}
		require.NoError(t, item.Open(nil))
		_, err := item.WriteAt([]byte("DATA"), 0)
		require.NoError(t, err)
		require.Error(t, store.StoreUploadAdmission(ctx, state))
		close(x.releaseComplete)
		select {
		case err := <-result:
			require.ErrorIs(t, err, context.Canceled)
		case <-ctx.Done():
			t.Fatal("stale store did not return")
		}
		require.Empty(t, callback)
		require.Nil(t, item.o)
		require.NoError(t, item.Close(nil))
	}
	require.True(t, item.IsDirty())
	bytes, err := os.ReadFile(c.toOSPath("file.txt"))
	require.NoError(t, err)
	want := "data"
	if mode == "stale_generation" {
		want = "DATA"
	}
	require.Equal(t, sha256.Sum256([]byte(want)), sha256.Sum256(bytes))
	x.mu.Lock()
	defer x.mu.Unlock()
	if mode == "metadata_save_failure" {
		require.Zero(t, x.creates)
	} else {
		require.Equal(t, 1, x.creates)
	}
	require.Zero(t, x.folders)
}

func b2CacheOpaqueAdmission(t *testing.T) {
	for _, corrupt := range []string{`{"Generation":[]}`, `{"Generation":`, "unknown-field"} {
		t.Run(fmt.Sprint(len(corrupt)), func(t *testing.T) {
			root := t.TempDir()
			x := &b2CacheOracle{cacheDir: root, keys: make(map[string]bool)}
			server := httptest.NewServer(http.HandlerFunc(x.serve))
			defer server.Close()
			ctx, c := b2OwnedCache(t, root, server.URL)
			item, _ := c.get("file.txt")
			require.NoError(t, item.Open(nil))
			_, err := item.WriteAt([]byte("data"), 0)
			require.NoError(t, err)
			require.NoError(t, item.Close(nil))
			c.writeback.Remove(item.writeBackID)
			meta := c.toOSPathMeta("file.txt")
			if corrupt == "unknown-field" {
				corrupt = fmt.Sprintf(`{"Generation":%q,"request_key":"unexpected"}`, item.info.UploadGeneration)
			}
			if json.Valid([]byte(corrupt)) {
				item.mu.Lock()
				item.info.UploadAdmission = json.RawMessage(corrupt)
				require.NoError(t, item._save())
				item.mu.Unlock()
			} else {
				require.NoError(t, os.WriteFile(meta, []byte(corrupt), 0600))
			}
			_, recovered := b2OwnedCache(t, root, server.URL)
			loaded, _ := recovered.get("file.txt")
			require.Error(t, loaded.store(ctx, nil))
			require.True(t, loaded.IsDirty())
			got, err := os.ReadFile(recovered.toOSPath("file.txt"))
			require.NoError(t, err)
			require.Equal(t, sha256.Sum256([]byte("data")), sha256.Sum256(got))
			_, err = os.Stat(meta)
			require.NoError(t, err)
			x.mu.Lock()
			require.Zero(t, x.creates)
			require.Zero(t, x.folders)
			x.mu.Unlock()
		})
	}
}

func b2CacheRenameTransitions(t *testing.T) {
	root := t.TempDir()
	x := &b2CacheOracle{cacheDir: root, keys: make(map[string]bool)}
	server := httptest.NewServer(http.HandlerFunc(x.serve))
	defer server.Close()
	ctx, c := b2OwnedCache(t, root, server.URL)
	item, _ := c.get("folder/file.txt")
	require.NoError(t, item.Open(nil))
	_, err := item.WriteAt([]byte("data"), 0)
	require.NoError(t, err)
	require.NoError(t, item.Close(nil))
	before, err := item.admissionStore().LoadUploadAdmission(ctx)
	require.NoError(t, err)
	require.NoError(t, c.Rename("folder/file.txt", "folder/file.txt", nil))
	require.NoError(t, item.Open(nil))
	_, err = item.WriteAt(nil, 100)
	require.NoError(t, err)
	require.NoError(t, item.Truncate(-1))
	require.NoError(t, item.Close(nil))
	unchanged, err := item.admissionStore().LoadUploadAdmission(ctx)
	require.NoError(t, err)
	require.Equal(t, before, unchanged)
	require.NoError(t, c.DirRename("folder", "renamed"))
	renamed, err := item.admissionStore().LoadUploadAdmission(ctx)
	require.NoError(t, err)
	require.NotEqual(t, before.Generation, renamed.Generation)
	got, err := os.ReadFile(c.toOSPath("renamed/file.txt"))
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256([]byte("data")), sha256.Sum256(got))
	rr, _, err := item.Reset()
	require.NoError(t, err)
	require.Equal(t, SkippedDirty, rr)
	require.NoError(t, c.Rename("renamed/file.txt", "renamed/other.txt", nil))
	again, err := item.admissionStore().LoadUploadAdmission(ctx)
	require.NoError(t, err)
	require.NotEqual(t, renamed.Generation, again.Generation)
}

func b2CacheJournalAfterResultFailure(t *testing.T) {
	root := t.TempDir()
	x := &b2CacheOracle{cacheDir: root, keys: make(map[string]bool), allowSuccess: true,
		completeStarted: make(chan struct{}), releaseComplete: make(chan struct{})}
	server := httptest.NewServer(http.HandlerFunc(x.serve))
	defer server.Close()
	ctx, c := b2OwnedCache(t, root, server.URL)
	item, _ := c.get("file.txt")
	require.NoError(t, item.Open(nil))
	_, err := item.WriteAt([]byte("data"), 0)
	require.NoError(t, err)
	require.NoError(t, item.Close(nil))
	c.writeback.Remove(item.writeBackID)
	result, callback := make(chan error, 1), make(chan struct{}, 1)
	go func() { result <- item.store(ctx, func(fs.Object) { callback <- struct{}{} }) }()
	select {
	case <-x.completeStarted:
	case <-ctx.Done():
		t.Fatal("original store never reached completion")
	}
	meta := c.toOSPathMeta("file.txt")
	require.NoError(t, os.Rename(meta, meta+".retained"))
	require.NoError(t, os.Mkdir(meta, 0700))
	close(x.releaseComplete)
	select {
	case err := <-result:
		require.ErrorContains(t, err, "clean metadata")
	case <-ctx.Done():
		t.Fatal("metadata failure did not finish")
	}
	require.Empty(t, callback)
	require.True(t, item.IsDirty())
	state, err := os.ReadFile(meta + ".retained")
	require.NoError(t, err)
	var retained Info
	require.NoError(t, json.Unmarshal(state, &retained))
	require.True(t, retained.Dirty)
	bytes, err := os.ReadFile(c.toOSPath("file.txt"))
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256([]byte("data")), sha256.Sum256(bytes))
}

func b2CacheOpenWriteTransition(t *testing.T) {
	root := t.TempDir()
	x := &b2CacheOracle{mode: "new_content_control", cacheDir: root, keys: make(map[string]bool)}
	server := httptest.NewServer(http.HandlerFunc(x.serve))
	defer server.Close()
	ctx, c := b2OwnedCache(t, root, server.URL)
	item, _ := c.get("file.txt")
	require.NoError(t, item.Open(nil))
	_, err := item.WriteAt([]byte("data"), 0)
	require.NoError(t, err)
	require.NoError(t, item.Sync())
	require.Error(t, item.store(ctx, nil))
	before, err := item.admissionStore().LoadUploadAdmission(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, before.UploadID)
	_, err = item.WriteAt([]byte("DATA"), 0)
	require.NoError(t, err)
	after, err := item.admissionStore().LoadUploadAdmission(ctx)
	require.NoError(t, err)
	require.NotEqual(t, before.Generation, after.Generation)
	require.Empty(t, after.RequestKey)
	x.mu.Lock()
	x.restored = true
	x.mu.Unlock()
	require.NoError(t, item.Close(nil))
	require.NoError(t, c.QueueSetExpiry(item.writeBackID, time.Now(), 0))
	require.Eventually(t, func() bool { return !item.IsDirty() }, 5*time.Second, time.Millisecond)
	got, err := os.ReadFile(c.toOSPath("file.txt"))
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256([]byte("DATA")), sha256.Sum256(got))
	x.mu.Lock()
	defer x.mu.Unlock()
	require.Equal(t, 2, x.creates)
	require.Equal(t, 1, x.publications)
}

func b2CacheWritePersistenceFailure(t *testing.T) {
	root := t.TempDir()
	x := &b2CacheOracle{cacheDir: root, keys: make(map[string]bool), allowSuccess: true}
	server := httptest.NewServer(http.HandlerFunc(x.serve))
	defer server.Close()
	ctx, c := b2OwnedCache(t, root, server.URL)
	item, _ := c.get("file.txt")
	require.NoError(t, item.Open(nil))
	_, err := item.WriteAt([]byte("data"), 0)
	require.NoError(t, err)
	require.NoError(t, item.Sync())
	require.NoError(t, item.store(ctx, nil))
	require.False(t, item.IsDirty())
	before := item.info.UploadGeneration
	meta := c.toOSPathMeta("file.txt")
	require.NoError(t, os.Rename(meta, meta+".retained"))
	require.NoError(t, os.Mkdir(meta, 0700))
	n, err := item.WriteAt([]byte("DATA"), 0)
	require.Error(t, err)
	require.Zero(t, n)
	require.Equal(t, before, item.info.UploadGeneration)
	got, err := os.ReadFile(c.toOSPath("file.txt"))
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256([]byte("data")), sha256.Sum256(got))
	require.Error(t, item.Close(nil))
}

func TestB2VFSWriteJournalPersistenceFailure(t *testing.T) {
	for _, mode := range []string{"write", "truncate"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			x := &b2CacheOracle{cacheDir: root, keys: make(map[string]bool)}
			server := httptest.NewServer(http.HandlerFunc(x.serve))
			defer server.Close()
			_, c := b2OwnedCache(t, root, server.URL)
			item, _ := c.get("file.txt")
			require.NoError(t, item.Open(nil))
			n, err := item.WriteAt([]byte("data"), 0)
			require.NoError(t, err)
			require.Equal(t, 4, n)
			generation := item.info.UploadGeneration
			meta := c.toOSPathMeta(item.name)
			before, err := os.ReadFile(meta)
			require.NoError(t, err)
			require.NoError(t, os.Rename(meta, meta+".retained"))
			require.NoError(t, os.Mkdir(meta, 0700))
			want := []byte("data\x00\x00\x00\x00DATA")
			if mode == "write" {
				n, err = item.WriteAt([]byte("DATA"), 8)
				require.Equal(t, 4, n)
			} else {
				err = item.Truncate(12)
				want = []byte("data\x00\x00\x00\x00\x00\x00\x00\x00")
			}
			require.ErrorContains(t, err, "failed to replace metadata")
			require.True(t, item.IsDirty())
			require.Equal(t, generation, item.info.UploadGeneration)
			require.True(t, item.HasRange(ranges.Range{Pos: 0, Size: 12}))
			got, err := os.ReadFile(c.toOSPath(item.name))
			require.NoError(t, err)
			require.Equal(t, want, got)
			retained, err := os.ReadFile(meta + ".retained")
			require.NoError(t, err)
			require.Equal(t, before, retained)
			n, err = item.WriteAt([]byte("NO"), 0)
			require.Error(t, err)
			require.Zero(t, n)
			got, err = os.ReadFile(c.toOSPath(item.name))
			require.NoError(t, err)
			require.Equal(t, want, got)
			require.NoError(t, os.Remove(meta))
			require.NoError(t, os.Rename(meta+".retained", meta))
			n, err = item.WriteAt([]byte("d"), 0)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			require.Equal(t, generation, item.info.UploadGeneration)
			body, err := os.ReadFile(meta)
			require.NoError(t, err)
			var saved Info
			require.NoError(t, json.Unmarshal(body, &saved))
			require.True(t, saved.Dirty)
			require.Equal(t, int64(len(want)), saved.Size)
			require.Equal(t, generation, saved.UploadGeneration)
			require.True(t, saved.Rs.Present(ranges.Range{Pos: 0, Size: int64(len(want))}))
			require.NoError(t, item.Close(nil))
			x.mu.Lock()
			defer x.mu.Unlock()
			require.Zero(t, x.creates)
			require.Zero(t, x.puts)
		})
	}
}
