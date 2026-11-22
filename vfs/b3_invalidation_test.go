package vfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/teldrive"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/require"
)

const (
	b3VFSTTL = 6 * time.Hour
	b3A      = "00000000-0000-4000-8000-000000000001"
	b3B      = "00000000-0000-4000-8000-000000000002"
	b3OldA   = "00000000-0000-4000-8000-000000000003"
	b3NewA   = "00000000-0000-4000-8000-000000000004"
	b3OldB   = "00000000-0000-4000-8000-000000000005"
	b3NewB   = "00000000-0000-4000-8000-000000000006"
	b3Mount  = "00000000-0000-4000-8000-000000000007"
	b3Aside  = "00000000-0000-4000-8000-000000000008"
	b3Moved  = "00000000-0000-4000-8000-000000000009"
	b3Child  = "00000000-0000-4000-8000-000000000010"
	b3Other  = "00000000-0000-4000-8000-000000000011"
	b3Ghost  = "00000000-0000-4000-8000-000000000012"
)

type b3VFSCatalog struct {
	mu         sync.Mutex
	rows       []api.FileInfo
	calls      []string
	failParent string
	failStatus int
	gate       *b3VFSListGate
}

func (c *b3VFSCatalog) replace(rows ...api.FileInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows = rows
}

func (c *b3VFSCatalog) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

func (c *b3VFSCatalog) fail(parent string, status int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failParent, c.failStatus = parent, status
}

func (c *b3VFSCatalog) requested(parent string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, call := range c.calls {
		if call == parent {
			count++
		}
	}
	return count
}

type b3VFSListGate struct {
	parent    string
	recursive bool
	entered   chan struct{}
	release   chan struct{}
}

func b3VFSGate(t *testing.T, parent string, recursive bool) (*b3VFSListGate, func()) {
	t.Helper()
	gate := &b3VFSListGate{parent, recursive, make(chan struct{}), make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	t.Cleanup(release)
	return gate, release
}

func (g *b3VFSListGate) hold(ctx context.Context) {
	close(g.entered)
	select {
	case <-g.release:
	case <-ctx.Done():
	}
}

func (c *b3VFSCatalog) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/api/v1/files" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	q := r.URL.Query()
	parent, recursive := q.Get("parentId"), q.Get("scope") == "recursive"
	c.mu.Lock()
	c.calls = append(c.calls, parent)
	status := 0
	if parent == c.failParent {
		status = c.failStatus
	}
	var gate *b3VFSListGate
	if c.gate != nil && c.gate.parent == parent && c.gate.recursive == recursive {
		gate, c.gate = c.gate, nil
	}
	var rows []api.FileInfo
	parents := map[string]bool{parent: true}
	if recursive {
		for changed := true; changed; {
			changed = false
			for _, row := range c.rows {
				if row.Kind == "folder" && parents[row.ParentId] && !parents[row.Id] {
					parents[row.Id] = true
					changed = true
				}
			}
		}
	}
	for _, row := range c.rows {
		if parents[row.ParentId] {
			rows = append(rows, row)
		}
	}
	c.mu.Unlock()
	if gate != nil {
		gate.hold(r.Context())
	}
	if status != 0 {
		w.WriteHeader(status)
		_, _ = io.WriteString(w, `{"error":{"code":"fixture_catalog_error","message":"fixture list failed"}}`)
		return
	}
	_ = json.NewEncoder(w).Encode(api.ReadMetadataResponse{Files: rows})
}

func b3VFSRow(id, parent, name, kind string) api.FileInfo {
	return api.FileInfo{Id: id, ParentId: parent, Name: name, Kind: kind, Status: "active", ModTime: t1, Generation: 1}
}

type b3VFSStream struct {
	ctx      context.Context
	mode     string
	cursor   string
	commands chan b3VFSFrame
}

type b3VFSFrame struct {
	text string
	done chan error
}

func b3VFSReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for fixture observation")
		var zero T
		return zero
	}
}

func (s b3VFSStream) send(t *testing.T, text string) {
	t.Helper()
	done := make(chan error, 1)
	select {
	case s.commands <- b3VFSFrame{text, done}:
	case <-s.ctx.Done():
		t.Fatal("fixture stream closed before send")
	case <-time.After(5 * time.Second):
		t.Fatal("fixture stream did not accept frame")
	}
	require.NoError(t, b3VFSReceive(t, done))
}

func b3VFSBackend(t *testing.T, root string, catalog http.HandlerFunc) (fs.Fs, <-chan b3VFSStream) {
	t.Helper()
	streams := make(chan b3VFSStream, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") != "vfs-fixture-key" || r.Method != http.MethodGet {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/api/v1/me":
			_, _ = io.WriteString(w, `{"userId":1}`)
		case "/api/v1/events":
			s := b3VFSStream{r.Context(), r.URL.Query().Get("mode"), r.Header.Get("Last-Event-ID"), make(chan b3VFSFrame)}
			select {
			case streams <- s:
			case <-s.ctx.Done():
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("X-TelDrive-Events", "ordered")
			w.Header().Set("X-TelDrive-Heartbeat-Ms", "15000")
			for {
				select {
				case frame, ok := <-s.commands:
					if !ok {
						return
					}
					_, err := io.WriteString(w, frame.text)
					if err == nil {
						err = http.NewResponseController(w).Flush()
					}
					frame.done <- err
				case <-s.ctx.Done():
					return
				}
			}
		default:
			catalog(w, r)
		}
	}))
	t.Cleanup(server.Close)
	ctx, ci := fs.AddConfig(t.Context())
	ci.LowLevelRetries = 2
	f, err := teldrive.NewFs(ctx, server.Listener.Addr().String(), root, configmap.Simple{
		"api_host": server.URL, "api_key": "vfs-fixture-key", "encoding": "Slash,BackSlash,LeftSpace,RightSpace,InvalidUtf8",
	})
	require.NoError(t, err)
	return f, streams
}

func b3VFSNew(t *testing.T, f fs.Fs, mode vfscommon.CacheMode) *VFS {
	t.Helper()
	opt := vfscommon.Opt
	opt.CacheMode = mode
	opt.DirCacheTime = fs.Duration(b3VFSTTL)
	opt.PollInterval = fs.Duration(time.Second)
	opt.WriteBack = fs.Duration(24 * time.Hour)
	ctx, ci := fs.AddConfig(t.Context())
	ci.BufferSize = 0
	ci.LowLevelRetries = 2
	opt.ChunkStreams = 0
	opt.NoChecksum = false
	v := New(ctx, f, &opt)
	t.Cleanup(v.Shutdown)
	return v
}

func b3VFSCursor(position int) string {
	return fmt.Sprintf("11111111-1111-4111-8111-111111111111.22222222-2222-4222-8222-222222222222.%d", position)
}

func b3VFSReady(mode string, position int) string {
	return fmt.Sprintf("event: sync.ready\ndata: {\"mode\":%q,\"cursor\":%q}\n\n", mode, b3VFSCursor(position))
}

const b3VFSRenew = "event: sync.renew\ndata: {}\n\n"

func b3VFSNext(t *testing.T, streams <-chan b3VFSStream, cursor string) b3VFSStream {
	t.Helper()
	s := b3VFSReceive(t, streams)
	require.Equal(t, "ordered", s.mode)
	require.Equal(t, cursor, s.cursor)
	return s
}

func b3VFSStart(t *testing.T, streams <-chan b3VFSStream) b3VFSStream {
	t.Helper()
	initial := b3VFSNext(t, streams, "")
	initial.send(t, b3VFSReady("baseline", 10)+b3VFSRenew)
	return b3VFSNext(t, streams, b3VFSCursor(10))
}

func b3VFSID(t *testing.T, v *VFS, name, id string) {
	t.Helper()
	node, err := v.Stat(name)
	require.NoError(t, err, "%s", name)
	require.Equal(t, id, node.DirEntry().(interface{ ID() string }).ID(), "%s", name)
}

func b3VFSAbsent(t *testing.T, v *VFS, name string) {
	t.Helper()
	_, err := v.Stat(name)
	require.ErrorIs(t, err, ENOENT, "%s", name)
}

func b3VFSNestedRows(fresh bool) []api.FileInfo {
	rows := []api.FileInfo{b3VFSRow(b3A, "", "a", "folder"), b3VFSRow(b3B, b3A, "b", "folder")}
	if fresh {
		return append(rows, b3VFSRow(b3NewA, b3A, "new-a", "file"), b3VFSRow(b3NewB, b3B, "new-b", "file"))
	}
	return append(rows, b3VFSRow(b3OldA, b3A, "old-a", "file"), b3VFSRow(b3OldB, b3B, "old-b", "file"))
}

type b3VFSOrdinaryBackend struct {
	fs.Fs
	features fs.Features
}

func (f *b3VFSOrdinaryBackend) Features() *fs.Features { return &f.features }

func TestB3VFSNestedResyncTracer(t *testing.T) {
	for _, leaf := range []string{"two_levels_no_prewalk", "dirty_sibling", "long_ttl_outage"} {
		t.Run(leaf, func(t *testing.T) {
			c := new(b3VFSCatalog)
			c.replace(b3VFSNestedRows(false)...)
			f, streams := b3VFSBackend(t, "", c.serve)
			mode := vfscommon.CacheModeOff
			if leaf == "dirty_sibling" {
				mode = vfscommon.CacheModeWrites
			}
			v := b3VFSNew(t, f, mode)
			resume := b3VFSStart(t, streams)
			resume.send(t, b3VFSReady("replay", 10))
			b3VFSID(t, v, "a/old-a", b3OldA)
			b3VFSID(t, v, "a/b/old-b", b3OldB)

			const dirtyPath, dirtyBytes = "a/dirty", "independent pending cache bytes"
			if leaf == "dirty_sibling" {
				h, err := v.Create(dirtyPath)
				require.NoError(t, err)
				_, err = h.WriteString(dirtyBytes)
				require.NoError(t, err)
				require.NoError(t, h.Close())
				require.NotNil(t, v.cache.DirtyItem(dirtyPath))
			}

			close(resume.commands)
			resume = b3VFSNext(t, streams, b3VFSCursor(10))
			c.replace(b3VFSNestedRows(true)...)
			calls := c.count()
			b3VFSID(t, v, "a/old-a", b3OldA)
			b3VFSID(t, v, "a/b/old-b", b3OldB)
			require.Equal(t, calls, c.count(), "outage forced an origin read under the ordinary TTL")
			require.EqualValues(t, b3VFSTTL, v.Opt.DirCacheTime)
			if leaf == "long_ttl_outage" {
				return
			}

			resume.send(t, "event: sync.required\ndata: {\"reason\":\"cursor_expired\"}\n\n")
			fresh := b3VFSNext(t, streams, "")
			fresh.send(t, b3VFSReady("baseline", 20)+b3VFSRenew)
			b3VFSNext(t, streams, b3VFSCursor(20)) // The next request proves baseline ACK.
			require.Equal(t, calls, c.count(), "baseline walked origin")
			b3VFSID(t, v, "a/new-a", b3NewA)
			b3VFSID(t, v, "a/b/new-b", b3NewB)
			b3VFSAbsent(t, v, "a/old-a")
			b3VFSAbsent(t, v, "a/b/old-b")
			require.Equal(t, calls+3, c.count(), "first access must refresh root and both nested lists once")
			if leaf == "dirty_sibling" {
				require.NotNil(t, v.cache.DirtyItem(dirtyPath))
				h, err := v.Open(dirtyPath)
				require.NoError(t, err)
				contents, err := io.ReadAll(h)
				require.NoError(t, err)
				require.NoError(t, h.Close())
				require.Equal(t, dirtyBytes, string(contents))
				require.EqualValues(t, 24*time.Hour, v.Opt.WriteBack)
			}
		})
	}
	t.Run("ordinary_backend_control", func(t *testing.T) {
		c := new(b3VFSCatalog)
		c.replace(b3VFSNestedRows(false)...)
		f, _ := b3VFSBackend(t, "", c.serve)
		ordinary := &b3VFSOrdinaryBackend{Fs: f, features: *f.Features()}
		ordinary.features.ScopedChangeNotify = nil
		var notify func(string, fs.EntryType)
		polled := make(chan time.Duration, 1)
		ordinary.features.ChangeNotify = func(ctx context.Context, callback func(string, fs.EntryType), poll <-chan time.Duration) {
			notify = callback
			go func() {
				select {
				case interval := <-poll:
					polled <- interval
				case <-ctx.Done():
				}
			}()
		}
		v := b3VFSNew(t, ordinary, vfscommon.CacheModeOff)
		require.Equal(t, time.Second, b3VFSReceive(t, polled))
		b3VFSID(t, v, "a/old-a", b3OldA)
		b3VFSID(t, v, "a/b/old-b", b3OldB)
		c.replace(b3VFSNestedRows(true)...)
		calls := c.count()
		notify("a/old-a", fs.EntryObject)
		require.Equal(t, calls, c.count())
		b3VFSID(t, v, "a/new-a", b3NewA)
		b3VFSID(t, v, "a/b/old-b", b3OldB)
		require.Equal(t, calls+1, c.count(), "ordinary backend notification must not stale the subtree")
	})
}

func b3VFSLocation(parent, name string) string {
	id := "null"
	if parent != "" {
		id = fmt.Sprintf("%q", parent)
	}
	return fmt.Sprintf(`{"parentId":%s,"name":%q}`, id, name)
}

func b3VFSChange(position int, event, id, kind, before, after string) string {
	payload := fmt.Sprintf(`"kind":%q`, kind)
	if before != "" {
		payload += `,"before":` + before
	}
	if after != "" {
		payload += `,"after":` + after
	}
	return fmt.Sprintf("id: %s\nevent: %s\ndata: {\"version\":1,\"occurredAt\":\"2026-01-01T00:00:00Z\",\"resourceType\":\"file\",\"resourceId\":%q,\"payload\":{%s}}\n\n",
		b3VFSCursor(position), event, id, payload)
}

func b3VFSApply(t *testing.T, s b3VFSStream, streams <-chan b3VFSStream, position int, frame string) b3VFSStream {
	t.Helper()
	s.send(t, frame+b3VFSRenew)
	return b3VFSNext(t, streams, b3VFSCursor(position))
}

func TestB3VFSMoveScopes(t *testing.T) {
	for _, kind := range []string{"file", "folder"} {
		for _, op := range []string{"move", "rename"} {
			for _, sides := range []string{"both", "old_only", "new_only", "neither"} {
				t.Run(kind+"_"+op+"_"+sides, func(t *testing.T) {
					b3VFSMove(t, kind, op, sides)
				})
			}
		}
	}
	t.Run("unknown_scope_root_case", b3VFSUnknownScope)
	t.Run("in_flight_stale_list", func(t *testing.T) {
		t.Run("readdir", func(t *testing.T) { b3VFSBlockedList(t, false, true) })
		t.Run("tree", func(t *testing.T) { b3VFSBlockedList(t, true, true) })
	})
	t.Run("blocked_readdir_barrier_and_pause", func(t *testing.T) { b3VFSBlockedList(t, false, false) })
	t.Run("blocked_read_dir_tree_barrier_and_pause", func(t *testing.T) { b3VFSBlockedList(t, true, false) })
}

func b3VFSMove(t *testing.T, kind, op, sides string) {
	t.Helper()
	oldCached := sides == "both" || sides == "old_only"
	newCached := sides == "both" || sides == "new_only"
	base := []api.FileInfo{
		b3VFSRow(b3A, "", "a", "folder"), b3VFSRow(b3B, "", "b", "folder"),
		b3VFSRow(b3Aside, "", "a-ish", "folder"), b3VFSRow(b3OldA, b3Aside, "unrelated-old", "file"),
	}
	const oldWire, newWire = "old＼leaf", "new＼leaf"
	const oldName, newName = `old\leaf`, `new\leaf`
	oldParent, newParent, newPath := b3A, b3B, "b/"+newName
	if op == "rename" {
		newParent, newPath = oldParent, "a/"+newName
	}
	oldPath := "a/" + oldName
	rows := append([]api.FileInfo(nil), base...)
	if oldCached || op == "move" {
		rows = append(rows, b3VFSRow(b3Moved, oldParent, oldWire, kind))
		if kind == "folder" {
			rows = append(rows, b3VFSRow(b3OldB, b3Moved, "old-child", "file"))
		}
	}
	if newCached {
		rows = append(rows, b3VFSRow(b3Other, newParent, newWire, kind))
		if kind == "folder" {
			rows = append(rows, b3VFSRow(b3Ghost, b3Other, "retired-child", "file"))
		}
	}
	c := new(b3VFSCatalog)
	c.replace(rows...)
	f, streams := b3VFSBackend(t, "", c.serve)
	v := b3VFSNew(t, f, vfscommon.CacheModeOff)
	s := b3VFSStart(t, streams)
	s.send(t, b3VFSReady("replay", 10))
	b3VFSID(t, v, "a-ish/unrelated-old", b3OldA)
	if oldCached {
		b3VFSID(t, v, oldPath, b3Moved)
		if kind == "folder" {
			b3VFSID(t, v, oldPath+"/old-child", b3OldB)
		}
	}
	if newCached {
		b3VFSID(t, v, newPath, b3Other)
		if kind == "folder" {
			b3VFSID(t, v, newPath+"/retired-child", b3Ghost)
		}
	}
	if op == "move" {
		require.Equal(t, oldCached, c.requested(oldParent) != 0)
		require.Equal(t, newCached, c.requested(newParent) != 0)
	} else {
		require.Equal(t, oldCached || newCached, c.requested(oldParent) != 0)
	}

	base[3] = b3VFSRow(b3NewA, b3Aside, "unrelated-new", "file")
	next := append(base, b3VFSRow(b3Moved, newParent, newWire, kind))
	if kind == "folder" {
		next = append(next, b3VFSRow(b3Child, b3Moved, "new-child", "file"))
	}
	c.replace(next...)
	before := c.count()
	b3VFSApply(t, s, streams, 11, b3VFSChange(11, "file.updated", b3Moved, kind,
		b3VFSLocation(oldParent, oldWire), b3VFSLocation(newParent, newWire)))
	require.Equal(t, before, c.count(), "move notification visited the origin")
	if kind == "file" || !newCached {
		b3VFSID(t, v, "a-ish/unrelated-old", b3OldA)
		require.Equal(t, before, c.count(), "precise scope crossed into an unrelated sibling")
	} else {
		// A known conflicting folder alias must broaden the scope.
		b3VFSID(t, v, "a-ish/unrelated-new", b3NewA)
	}
	b3VFSAbsent(t, v, oldPath)
	b3VFSID(t, v, newPath, b3Moved)
	if kind == "folder" {
		b3VFSID(t, v, newPath+"/new-child", b3Child)
		b3VFSAbsent(t, v, newPath+"/retired-child")
		b3VFSAbsent(t, v, oldPath+"/old-child")
	}
}

func b3VFSUnknownScope(t *testing.T) {
	c := new(b3VFSCatalog)
	base := []api.FileInfo{
		b3VFSRow(b3Mount, "", "mount", "folder"), b3VFSRow(b3A, b3Mount, "a", "folder"),
	}
	c.replace(append(base, b3VFSRow(b3OldA, b3A, "old-a", "file"), b3VFSRow(b3Moved, "", "outside", "file"))...)
	f, streams := b3VFSBackend(t, "mount", c.serve)
	v := b3VFSNew(t, f, vfscommon.CacheModeOff)
	s := b3VFSStart(t, streams)
	s.send(t, b3VFSReady("replay", 10))
	b3VFSID(t, v, "a/old-a", b3OldA)

	c.replace(append(base, b3VFSRow(b3NewA, b3A, "new-a", "file"), b3VFSRow(b3Moved, b3Mount, "inside", "file"))...)
	count := c.count()
	outside := c.requested("")
	s = b3VFSApply(t, s, streams, 11, b3VFSChange(11, "file.updated", b3Moved, "file",
		b3VFSLocation("", "outside"), b3VFSLocation(b3Mount, "inside")))
	require.Equal(t, count, c.count())
	b3VFSID(t, v, "inside", b3Moved)
	b3VFSID(t, v, "a/old-a", b3OldA)
	require.Equal(t, count+1, c.count(), "proven outside parent caused a mount-wide invalidation")
	require.Equal(t, outside, c.requested(""))

	s.send(t, b3VFSReady("replay", 11))
	count = c.count()
	b3VFSApply(t, s, streams, 12, b3VFSChange(12, "file.updated", b3Other, "file",
		b3VFSLocation(b3Ghost, "unknown"), `{"name":"missing-parent"}`))
	require.Equal(t, count, c.count(), "unknown scope walked origin")
	b3VFSID(t, v, "a/new-a", b3NewA)
	b3VFSAbsent(t, v, "a/old-a")
	require.Equal(t, outside, c.requested(""), "unknown fallback escaped the mounted root")
}

type b3VFSBlockedPublish struct {
	fs.Fs
	features fs.Features
	gate     *b3VFSListGate
	used     sync.Once
}

func (f *b3VFSBlockedPublish) Features() *fs.Features { return &f.features }

func (f *b3VFSBlockedPublish) hold(ctx context.Context, dir string, recursive bool) {
	if dir != f.gate.parent || recursive != f.gate.recursive {
		return
	}
	block := false
	f.used.Do(func() { block = true })
	if block {
		f.gate.hold(ctx)
	}
}

func (f *b3VFSBlockedPublish) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	items, err := f.Fs.List(ctx, dir)
	if err == nil {
		f.hold(ctx, dir, false)
	}
	return items, err
}

func b3VFSBlockedList(t *testing.T, recursive, postOrigin bool) {
	t.Helper()
	rootRows := []api.FileInfo{b3VFSRow(b3A, "", "a", "folder")}
	c := new(b3VFSCatalog)
	c.replace(append(rootRows, b3VFSRow(b3Ghost, b3A, "stale", "folder"))...)
	origin, streams := b3VFSBackend(t, "", c.serve)
	f := origin
	parent := b3A
	if postOrigin {
		parent = "a"
	}
	gate, release := b3VFSGate(t, parent, recursive)
	defer release()
	if postOrigin {
		wrapped := &b3VFSBlockedPublish{Fs: origin, features: *origin.Features(), gate: gate}
		wrapped.features.ListR = func(ctx context.Context, dir string, cb fs.ListRCallback) error {
			err := origin.Features().ListR(ctx, dir, cb)
			if err == nil {
				wrapped.hold(ctx, dir, true)
			}
			return err
		}
		f = wrapped
	} else {
		c.mu.Lock()
		c.gate = gate
		c.mu.Unlock()
	}
	v := b3VFSNew(t, f, vfscommon.CacheModeOff)
	s := b3VFSStart(t, streams)
	s.send(t, b3VFSReady("replay", 10))
	node, err := v.Stat("a")
	require.NoError(t, err)
	dir := node.(*Dir)
	old := make(chan error, 1)
	go func() {
		if recursive {
			old <- dir.readDirTree()
		} else {
			_, err := v.ReadDir("a")
			old <- err
		}
	}()
	b3VFSReceive(t, gate.entered)
	c.replace(append(rootRows, b3VFSRow(b3NewA, b3A, "fresh", "file"))...)
	count := c.count()
	s.send(t, "event: sync.required\ndata: {\"reason\":\"cursor_expired\"}\n\n")
	baseline := b3VFSNext(t, streams, "")
	s = b3VFSApply(t, baseline, streams, 20, b3VFSReady("baseline", 20))
	s.send(t, b3VFSReady("replay", 20))
	timedOut, err := setPollInterval(v, 0, 3*time.Second)
	require.NoError(t, err)
	require.False(t, timedOut)
	b3VFSReceive(t, s.ctx.Done())
	require.Equal(t, count, c.count(), "baseline or pause attempted an origin read")

	type lookup struct {
		node Node
		err  error
	}
	latest := make(chan lookup, 1)
	go func() {
		n, err := v.Stat("a/fresh")
		latest <- lookup{n, err}
	}()
	got := b3VFSReceive(t, latest)
	require.NoError(t, got.err, "new lookup waited on or observed the blocked old response")
	require.Equal(t, b3NewA, got.node.DirEntry().(fs.IDer).ID())
	select {
	case err := <-old:
		t.Fatalf("old read returned before release: %v", err)
	default:
	}
	release()
	wantErr := dircache.ErrNamespaceChanged
	if postOrigin {
		wantErr = errDirReadInvalidated
	}
	require.ErrorIs(t, b3VFSReceive(t, old), wantErr)
	count = c.count()
	b3VFSID(t, v, "a/fresh", b3NewA)
	b3VFSAbsent(t, v, "a/stale")
	require.Equal(t, count, c.count(), "old read disturbed the newer published list")
	_, err = origin.List(t.Context(), "a/stale")
	require.ErrorIs(t, err, fs.ErrorDirNotFound)
	require.Zero(t, c.requested(b3Ghost), "old list reinstalled the stale folder alias")
}

func TestB3VFSRereadFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
	}{
		{"permission_error", http.StatusForbidden},
		{"transient_error", http.StatusServiceUnavailable},
		{"authoritative_missing", http.StatusNotFound},
		{"successful_retry", http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := new(b3VFSCatalog)
			c.replace(b3VFSNestedRows(false)...)
			f, streams := b3VFSBackend(t, "", c.serve)
			v := b3VFSNew(t, f, vfscommon.CacheModeOff)
			s := b3VFSStart(t, streams)
			s.send(t, b3VFSReady("replay", 10))
			b3VFSID(t, v, "a/old-a", b3OldA)
			c.replace(b3VFSNestedRows(true)...)
			c.fail(b3A, tc.status)
			count := c.count()
			b3VFSApply(t, s, streams, 11, b3VFSChange(11, "file.purged", b3OldA, "file", b3VFSLocation(b3A, "old-a"), ""))
			require.Equal(t, count, c.count())
			_, err := v.Stat("a/old-a")
			switch tc.status {
			case http.StatusNotFound:
				require.ErrorIs(t, err, ENOENT)
				count = c.count()
				b3VFSAbsent(t, v, "a/old-a")
				require.Equal(t, count, c.count(), "authoritative absence was not cached")
				return
			case http.StatusForbidden:
				require.ErrorIs(t, err, fs.ErrorPermissionDenied)
			default:
				require.ErrorContains(t, err, "TelDrive API error 503 (fixture_catalog_error)")
			}
			require.Greater(t, c.count(), count)
			count = c.count()
			if tc.name == "successful_retry" {
				c.fail("", 0)
				b3VFSID(t, v, "a/new-a", b3NewA)
				count = c.count()
				b3VFSAbsent(t, v, "a/old-a")
				require.Equal(t, count, c.count(), "successful retry did not publish freshness")
			} else {
				_, again := v.Stat("a/old-a")
				require.EqualError(t, again, err.Error())
				require.Greater(t, c.count(), count, "failed refresh marked the old list fresh")
			}
		})
	}
}

type b3VFSContentRequest struct {
	id, bytesRange string
}

type b3VFSContent struct {
	mu       sync.Mutex
	requests []b3VFSContentRequest
	data     map[string][]byte
	cutSeek  bool
}

func (c *b3VFSContent) serve(w http.ResponseWriter, r *http.Request, id string) {
	c.mu.Lock()
	data := c.data[id]
	bytesRange := r.Header.Get("Range")
	c.requests = append(c.requests, b3VFSContentRequest{id, bytesRange})
	cut := c.cutSeek && id == b3OldA && strings.HasPrefix(bytesRange, "bytes=1-")
	if cut {
		c.cutSeek = false
	}
	c.mu.Unlock()
	if data == nil {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":{"code":"fixture_content_missing","message":"original object gone"}}`)
		return
	}
	if cut {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 1-%d/%d", len(data)-1, len(data)))
		w.Header().Set("Content-Length", fmt.Sprint(len(data)-1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[1:2])
		return
	}
	http.ServeContent(w, r, "fixture.bin", t1, bytes.NewReader(data))
}

func (c *b3VFSContent) observed() []b3VFSContentRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]b3VFSContentRequest(nil), c.requests...)
}

func b3VFSFile(id, name string, data []byte) api.FileInfo {
	row := b3VFSRow(id, "", name, "file")
	row.Size = int64(len(data))
	row.Hash = &api.FileHash{Value: tdhash.SumString(data)}
	return row
}

func TestB3VFSOpenIdentity(t *testing.T) {
	for _, leaf := range []string{"active_stream", "lazy_unstarted", "seek_reopen", "removed_source_error", "new_lookup_changed"} {
		t.Run(leaf, func(t *testing.T) {
			const slot = "slot"
			original := []byte("original bytes: 0123456789")
			replacement := []byte("replacement payload has a different size")
			oldSHA, newSHA := sha256.Sum256(original), sha256.Sum256(replacement)
			require.NotEqual(t, oldSHA, newSHA)
			catalog := new(b3VFSCatalog)
			catalog.replace(b3VFSFile(b3OldA, slot, original))
			content := &b3VFSContent{
				data:    map[string][]byte{b3OldA: original, b3NewA: replacement},
				cutSeek: leaf == "seek_reopen",
			}
			f, streams := b3VFSBackend(t, "", func(w http.ResponseWriter, r *http.Request) {
				rest, found := strings.CutPrefix(r.URL.Path, "/api/v1/files/")
				if id, ok := strings.CutSuffix(rest, "/content"); found && ok {
					content.serve(w, r, id)
				} else {
					catalog.serve(w, r)
				}
			})
			v := b3VFSNew(t, f, vfscommon.CacheModeOff)
			s := b3VFSStart(t, streams)
			s.send(t, b3VFSReady("replay", 10))
			h, err := v.Open(slot)
			require.NoError(t, err)
			defer h.Release()
			require.IsType(t, &ReadFileHandle{}, h)
			require.Empty(t, content.observed(), "allocating a direct handle must remain lazy")
			var head []byte
			if leaf == "active_stream" || leaf == "seek_reopen" {
				head = make([]byte, 5)
				_, err := io.ReadFull(h, head)
				require.NoError(t, err)
				require.Equal(t, original[:5], head)
				require.Len(t, content.observed(), 1)
			}

			rows := []api.FileInfo{b3VFSFile(b3NewA, slot, replacement)}
			oldLocation := b3VFSLocation("", slot)
			frame := b3VFSChange(11, "file.updated", b3OldA, "file", oldLocation, b3VFSLocation("", "archived"))
			if leaf == "removed_source_error" {
				content.mu.Lock()
				delete(content.data, b3OldA)
				content.mu.Unlock()
				frame = b3VFSChange(11, "file.purged", b3OldA, "file", oldLocation, "")
			} else {
				rows = append(rows, b3VFSFile(b3OldA, "archived", original))
			}
			catalog.replace(rows...)
			count, reads := catalog.count(), len(content.observed())
			frame += b3VFSChange(12, "file.created", b3NewA, "file", "", b3VFSLocation("", slot))
			b3VFSApply(t, s, streams, 12, frame)
			require.Equal(t, count, catalog.count(), "notification listed origin")
			require.Len(t, content.observed(), reads, "notification opened content")
			b3VFSID(t, v, slot, b3NewA)

			wantCalls := 1
			switch leaf {
			case "removed_source_error":
				got, err := io.ReadAll(h)
				require.ErrorIs(t, err, fs.ErrorObjectNotFound)
				require.Empty(t, got)
			case "seek_reopen":
				pos, err := h.Seek(1, io.SeekStart)
				require.NoError(t, err)
				require.EqualValues(t, 1, pos)
				got, err := io.ReadAll(h)
				require.NoError(t, err)
				require.Equal(t, sha256.Sum256(original[1:]), sha256.Sum256(got))
				wantCalls = 3 // initial, ranged seek with a short body, and reopened range
			default:
				tail, err := io.ReadAll(h)
				require.NoError(t, err)
				require.Equal(t, oldSHA, sha256.Sum256(append(head, tail...)))
			}
			require.NoError(t, h.Close(), "invalidation must not close the original handle or swap its checksum source")
			calls := content.observed()
			require.Len(t, calls, wantCalls)
			for _, call := range calls {
				require.Equal(t, b3OldA, call.id, "old handle requested the replacement")
			}
			if leaf == "seek_reopen" {
				require.True(t, strings.HasPrefix(calls[1].bytesRange, "bytes=1-"))
				require.Equal(t, calls[1].bytesRange, calls[2].bytesRange)
			}
			got, err := v.ReadFile(slot)
			require.NoError(t, err)
			require.Len(t, got, len(replacement))
			require.Equal(t, newSHA, sha256.Sum256(got))
			calls = content.observed()
			require.Len(t, calls, wantCalls+1)
			require.Equal(t, b3NewA, calls[wantCalls].id)
		})
	}
}
