package vfscache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/teldrive"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/vfs/vfscache/writeback"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/require"
)

const b3Recovery = "11111111-1111-4111-8111-111111111111"

type b3GuardFixture struct {
	mu        sync.Mutex
	file      *api.FileInfo
	content   []byte
	failList  bool
	failHash  bool
	creates   int
	gets      int
	parts     int
	completes int
	legacy    int
}

func (f *b3GuardFixture) setFile(size int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	digest := tdhash.SumString(f.content)
	f.file = &api.FileInfo{
		Id: "orig", ParentId: "", Name: "file.txt", Kind: "file", Status: "active",
		Size: size, ModTime: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Generation: 1,
		Hash: &api.FileHash{Algorithm: "blake3", Value: digest},
	}
}

func (f *b3GuardFixture) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/api/v1/me":
		_, _ = io.WriteString(w, `{"userId":1}`)
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files":
		f.mu.Lock()
		fail := f.failList
		file := f.file
		f.mu.Unlock()
		if fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":{"code":"fixture_list","message":"list failed"}}`)
			return
		}
		response := api.ReadMetadataResponse{}
		if file != nil && r.URL.Query().Get("parentId") == "" {
			response.Files = []api.FileInfo{*file}
		}
		_ = json.NewEncoder(w).Encode(response)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/files/") && strings.HasSuffix(r.URL.Path, "/content"):
		f.serveContent(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/files/"):
		f.mu.Lock()
		file, failHash := f.file, f.failHash
		f.mu.Unlock()
		if file == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		copy := *file
		if failHash {
			copy.Hash = nil
		}
		_ = json.NewEncoder(w).Encode(copy)
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded":
		w.Header().Set("X-TelDrive-Recovery-Incarnation", b3Recovery)
		_, _ = io.WriteString(w, `{}`)
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/guarded":
		f.mu.Lock()
		f.creates++
		f.mu.Unlock()
		w.WriteHeader(http.StatusPreconditionFailed)
		_, _ = io.WriteString(w, `{"error":{"code":"upload_recovery_changed","message":"fixture refused"}}`)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/uploads/guarded/"):
		f.mu.Lock()
		f.gets++
		f.mu.Unlock()
		w.Header().Set("X-TelDrive-Upload-Guard", "1")
		w.WriteHeader(http.StatusNotFound)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v1/uploads"):
		f.mu.Lock()
		if strings.Contains(r.URL.Path, "/guarded/") {
			f.completes++
		} else {
			f.legacy++
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusBadRequest)
	case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/parts/"):
		f.mu.Lock()
		f.parts++
		f.mu.Unlock()
		w.WriteHeader(http.StatusBadRequest)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (f *b3GuardFixture) serveContent(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	content := append([]byte(nil), f.content...)
	f.mu.Unlock()
	start, end := int64(0), int64(len(content))-1
	if header := r.Header.Get("Range"); strings.HasPrefix(header, "bytes=") {
		spec := strings.TrimSuffix(strings.TrimPrefix(header, "bytes="), "-")
		parts := strings.SplitN(spec, "-", 2)
		if len(parts) == 2 {
			if parts[0] != "" {
				start, _ = strconv.ParseInt(parts[0], 10, 64)
			}
			if parts[1] != "" {
				end, _ = strconv.ParseInt(parts[1], 10, 64)
			}
		}
		if start < 0 {
			start = 0
		}
		if end >= int64(len(content)) {
			end = int64(len(content)) - 1
		}
		if start > end || start >= int64(len(content)) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(content[start : end+1])
		return
	}
	_, _ = w.Write(content)
}

func b3GuardCache(t *testing.T, root, endpoint string) (context.Context, *Cache) {
	t.Helper()
	previous := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(root))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(previous)) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	ctx, ci := fs.AddConfig(ctx)
	ci.LowLevelRetries, ci.Transfers = 1, 2
	ci.MultiThreadStreams = 0
	f, err := teldrive.NewFs(ctx, "b3-guard", "", configmap.Simple{
		"api_host": endpoint, "api_key": "synthetic-key", "chunk_size": "16Mi", "guarded_upload": "true",
	})
	require.NoError(t, err)
	opt := vfscommon.Opt
	opt.WriteBack, opt.CachePollInterval, opt.HandleCaching = fs.Duration(24*time.Hour), 0, 0
	c, err := New(ctx, f, &opt, func(string, int64, bool) error { return nil })
	require.NoError(t, err)
	return ctx, c
}

func b3GuardServer(t *testing.T, fixture *b3GuardFixture) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(fixture.serve))
	t.Cleanup(server.Close)
	return server.URL
}

func b3GuardForce(t *testing.T, c *Cache, name string) {
	t.Helper()
	for _, info := range c.Queue()["queue"].([]writeback.QueueInfo) {
		if info.Name == name {
			require.NoError(t, c.QueueSetExpiry(info.ID, time.Now(), 0))
			return
		}
	}
	t.Fatalf("no queued writeback for %q", name)
}

// TestB3DirtyQueueHold proves a definitive guarded refusal becomes a durable
// non-runnable disposition across queue attempts, edit and reload.
func TestB3DirtyQueueHold(t *testing.T) {
	fixture := new(b3GuardFixture)
	endpoint := b3GuardServer(t, fixture)
	root := t.TempDir()
	_, c := b3GuardCache(t, root, endpoint)
	item, _ := c.get("pending.txt")
	require.NoError(t, item.Open(nil))
	const payload = "independent pending cache bytes"
	n, err := item.WriteAt([]byte(payload), 0)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.NoError(t, item.Close(nil))

	item.mu.Lock()
	require.NotNil(t, item.guardJournal, "journal must exist before the first dirty byte")
	require.Empty(t, item.info.MutationMarker)
	generation := item.info.UploadGeneration
	item.mu.Unlock()

	b3GuardForce(t, c, "pending.txt")
	require.Eventually(t, func() bool { return item.HeldReason() != "" }, 5*time.Second, time.Millisecond)
	require.True(t, item.IsDirty())
	require.Equal(t, generation, func() string { item.mu.Lock(); defer item.mu.Unlock(); return item.info.UploadGeneration }())
	_, hasProof := item.GuardedReceiptProof()
	require.False(t, hasProof, "held item has no exact receipt")

	fixture.mu.Lock()
	require.Equal(t, 1, fixture.creates)
	require.Zero(t, fixture.parts)
	require.Zero(t, fixture.completes)
	require.Zero(t, fixture.legacy)
	fixture.mu.Unlock()

	backing, err := os.ReadFile(c.toOSPath("pending.txt"))
	require.NoError(t, err)
	require.Equal(t, payload, string(backing))

	// Edits stay non-runnable and never change the protected bytes.
	require.NoError(t, item.Open(nil))
	n, err = item.WriteAt([]byte("EDIT"), 4)
	require.ErrorIs(t, err, writeback.ErrHeld)
	require.Zero(t, n)
	require.NoError(t, item.Close(nil))
	require.NoError(t, item.Open(nil))
	held := make([]byte, len(payload))
	_, err = item.ReadAt(held, 0)
	require.NoError(t, err)
	require.Equal(t, payload, string(held))
	require.NoError(t, item.Close(nil))

	rr, _, err := item.Reset()
	require.NoError(t, err)
	require.Equal(t, SkippedDirty, rr)
	removed, _ := item.RemoveNotInUse(0, false)
	require.False(t, removed)

	b3GuardForce(t, c, "pending.txt")
	time.Sleep(50 * time.Millisecond)
	fixture.mu.Lock()
	require.Equal(t, 1, fixture.creates, "a held generation sends nothing on retry")
	fixture.mu.Unlock()

	// A fresh cache reload restores the hold instead of a runnable intent.
	reloadCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reloaded, err := New(reloadCtx, c.fremote, c.opt, func(string, int64, bool) error { return nil })
	require.NoError(t, err)
	loaded, found := reloaded.get("pending.txt")
	require.True(t, found)
	require.NotEmpty(t, loaded.HeldReason())
	removed, _ = loaded.RemoveNotInUse(0, false)
	require.False(t, removed)
}

// TestB3DirtyJournalFailure proves the mutation marker is durable before any
// byte changes, and a marker-save failure leaves the bytes untouched.
func TestB3DirtyJournalFailure(t *testing.T) {
	fixture := new(b3GuardFixture)
	endpoint := b3GuardServer(t, fixture)
	root := t.TempDir()
	_, c := b3GuardCache(t, root, endpoint)
	item, _ := c.get("pending.txt")
	require.NoError(t, item.Open(nil))
	_, err := item.WriteAt([]byte("base"), 0)
	require.NoError(t, err)
	item.mu.Lock()
	generation := item.info.UploadGeneration
	item.mu.Unlock()

	meta := c.toOSPathMeta(item.name)
	require.NoError(t, os.Rename(meta, meta+".retained"))
	require.NoError(t, os.Mkdir(meta, 0o700))
	n, err := item.WriteAt([]byte("OVER"), 0)
	require.ErrorContains(t, err, "failed to persist local mutation marker")
	require.Zero(t, n)
	got, readErr := os.ReadFile(c.toOSPath(item.name))
	require.NoError(t, readErr)
	require.Equal(t, "base", string(got))
	item.mu.Lock()
	require.Equal(t, generation, item.info.UploadGeneration)
	require.Empty(t, item.info.MutationMarker)
	item.mu.Unlock()

	require.NoError(t, os.Remove(meta))
	require.NoError(t, os.Rename(meta+".retained", meta))
	n, err = item.WriteAt([]byte("OVER"), 0)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	item.mu.Lock()
	require.Equal(t, generation, item.info.UploadGeneration)
	require.Empty(t, item.info.MutationMarker)
	require.NoError(t, item._saveGuardLocked())
	item.mu.Unlock()
}

// TestB3DirtySparse proves a partial edit stages and verifies the whole
// original, and a shorter external replacement never truncates dirty bytes.
func TestB3DirtySparse(t *testing.T) {
	fixture := &b3GuardFixture{content: []byte("0123456789abcdefghijklmnopqrstuv")}
	fixture.setFile(int64(len(fixture.content)))
	endpoint := b3GuardServer(t, fixture)
	root := t.TempDir()
	ctx, c := b3GuardCache(t, root, endpoint)
	obj, err := c.fremote.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	item, _ := c.get("file.txt")
	require.NoError(t, item.Open(obj))
	require.False(t, item.present())
	n, err := item.WriteAt([]byte("XY"), 8)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.True(t, item.present(), "the complete original must be staged before a partial edit")

	// Remote bytes did not change.
	fixture.mu.Lock()
	require.Equal(t, "0123456789abcdefghijklmnopqrstuv", string(fixture.content))
	fixture.mu.Unlock()

	// A shorter external replacement must not truncate or rebind dirty bytes.
	shorter := &api.FileInfo{Id: "orig", ParentId: "", Name: "file.txt", Kind: "file", Status: "active",
		Size: 4, ModTime: time.Now(), Generation: 2, Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString([]byte("0123"))}}
	fixture.mu.Lock()
	fixture.file = shorter
	fixture.mu.Unlock()
	shorterObj, err := c.fremote.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	require.NoError(t, item.Open(shorterObj))
	require.Equal(t, int64(len(fixture.content)), item.info.Size)
	require.True(t, item.IsDirty())
	body, err := os.ReadFile(c.toOSPath(item.name))
	require.NoError(t, err)
	require.Equal(t, "01234567XYabcdefghijklmnopqrstuv", string(body))

	// A missing authoritative digest refuses before any byte or generation.
	missing := &b3GuardFixture{content: []byte("0123456789abcdef")}
	missing.setFile(int64(len(missing.content)))
	missingEndpoint := b3GuardServer(t, missing)
	missingRoot := t.TempDir()
	missingCtx, missingCache := b3GuardCache(t, missingRoot, missingEndpoint)
	missingObj, err := missingCache.fremote.NewObject(missingCtx, "file.txt")
	require.NoError(t, err)
	missing.mu.Lock()
	missing.failHash = true
	missing.mu.Unlock()
	missingItem, _ := missingCache.get("file.txt")
	require.NoError(t, missingItem.Open(missingObj))
	missingItem.mu.Lock()
	beforeGen := missingItem.info.UploadGeneration
	beforeSize := missingItem.info.Size
	missingItem.mu.Unlock()
	n, err = missingItem.WriteAt([]byte("XX"), 0)
	require.ErrorIs(t, err, fs.ErrGuardedProofUnavailable)
	require.Zero(t, n)
	missingItem.mu.Lock()
	require.Equal(t, beforeGen, missingItem.info.UploadGeneration)
	require.Equal(t, beforeSize, missingItem.info.Size)
	require.False(t, missingItem.info.Dirty)
	missingItem.mu.Unlock()
}

// TestB3DirtyCachedHandle proves a dirty guarded handle keeps its local bytes
// while a resync sees the replacement, and a true list error stays an error.
func TestB3DirtyCachedHandle(t *testing.T) {
	fixture := &b3GuardFixture{content: []byte("0123456789abcdef")}
	fixture.setFile(int64(len(fixture.content)))
	endpoint := b3GuardServer(t, fixture)
	root := t.TempDir()
	ctx, c := b3GuardCache(t, root, endpoint)
	obj, err := c.fremote.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	item, _ := c.get("file.txt")
	require.NoError(t, item.Open(obj))
	_, err = item.WriteAt([]byte("XX"), 0)
	require.NoError(t, err)
	kept := item.o

	fixture.mu.Lock()
	fixture.file = &api.FileInfo{Id: "replacement", ParentId: "", Name: "file.txt", Kind: "file", Status: "active",
		Size: 2, ModTime: time.Now(), Generation: 7, Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString([]byte("zz"))}}
	fixture.mu.Unlock()
	replacement, err := c.fremote.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	require.NoError(t, item.Open(replacement))
	require.Same(t, kept, item.o, "a dirty guarded handle must not silently retarget")
	body, err := os.ReadFile(c.toOSPath(item.name))
	require.NoError(t, err)
	require.Equal(t, "XX23456789abcdef", string(body))

	fixture.mu.Lock()
	fixture.failList = true
	fixture.mu.Unlock()
	_, err = c.fremote.List(ctx, "")
	require.Error(t, err, "a true backend list error must stay an error")
	require.Equal(t, "XX23456789abcdef", string(mustReadFile(t, c.toOSPath(item.name))))
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	return body
}
