package vfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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
	"github.com/rclone/rclone/vfs/vfscache"
	"github.com/rclone/rclone/vfs/vfscache/writeback"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/require"
)

const b3DirtyRecovery = "11111111-1111-4111-8111-111111111111"

type b3DirtyFixture struct {
	mu        sync.Mutex
	creates   int
	parts     int
	completes int
	legacy    int
	mkdirs    int
	folders   map[string]string // folder id -> display name
}

func (f *b3DirtyFixture) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/api/v1/me":
		_, _ = io.WriteString(w, `{"userId":1}`)
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files":
		_, _ = io.WriteString(w, `{"items":[]}`)
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/folders":
		var req api.FolderCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.mkdirs++
		if f.folders == nil {
			f.folders = map[string]string{}
		}
		f.folders["folder-"+req.Name] = req.Name
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(api.FileInfo{
			Id: "folder-" + req.Name, ParentId: req.ParentId, Name: req.Name, Kind: "folder", Status: "active", Generation: 1,
		})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/files/"):
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/files/")
		f.mu.Lock()
		name, ok := f.folders[id]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(api.FileInfo{
			Id: id, Name: name, Kind: "folder", Status: "active", Generation: 1,
		})
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded":
		w.Header().Set("X-TelDrive-Recovery-Incarnation", b3DirtyRecovery)
		_, _ = io.WriteString(w, `{}`)
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/guarded":
		f.mu.Lock()
		f.creates++
		f.mu.Unlock()
		w.WriteHeader(http.StatusPreconditionFailed)
		_, _ = io.WriteString(w, `{"error":{"code":"upload_recovery_changed","message":"fixture refused"}}`)
	case strings.HasPrefix(r.URL.Path, "/api/v1/uploads/guarded"):
		w.Header().Set("X-TelDrive-Upload-Guard", "1")
		f.mu.Lock()
		if strings.Contains(r.URL.Path, "/parts/") {
			f.parts++
		} else if strings.HasSuffix(r.URL.Path, "/complete") {
			f.completes++
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusBadRequest)
	case strings.HasPrefix(r.URL.Path, "/api/v1/uploads"):
		f.mu.Lock()
		f.legacy++
		f.mu.Unlock()
		w.WriteHeader(http.StatusBadRequest)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

// b3GuardedBackend is not needed: the fixture server has no ordered stream.
func TestB3DirtyHoldTracer(t *testing.T) {
	previous := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(previous)) })

	fixture := new(b3DirtyFixture)
	server := httptest.NewServer(http.HandlerFunc(fixture.serve))
	t.Cleanup(server.Close)

	ctx, ci := fs.AddConfig(t.Context())
	ci.LowLevelRetries, ci.BufferSize = 2, 0
	remote, err := teldrive.NewFs(ctx, "b3-dirty", "", configmap.Simple{
		"api_host": server.URL, "api_key": "fixture-key", "guarded_upload": "true",
		"encoding": "Slash,BackSlash,LeftSpace,RightSpace,InvalidUtf8",
	})
	require.NoError(t, err)
	remote.Features().ScopedChangeNotify = nil

	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeWrites
	opt.WriteBack = fs.Duration(24 * time.Hour)
	opt.HandleCaching = 0
	v := New(ctx, remote, &opt)
	t.Cleanup(v.Shutdown)

	const name = "pending.txt"
	const payload = "independent pending cache bytes"
	h, err := v.Create(name)
	require.NoError(t, err)
	_, err = h.WriteString(payload)
	require.NoError(t, err)
	require.NoError(t, h.Close())
	require.NotNil(t, v.cache.DirtyItem(name))

	forceWriteback(t, v, name)
	item := v.cache.DirtyItem(name)
	require.NotNil(t, item)
	require.Eventually(t, func() bool { return item.HeldReason() != "" }, 5*time.Second, time.Millisecond)

	fixture.mu.Lock()
	require.Equal(t, 1, fixture.creates)
	require.Zero(t, fixture.parts)
	require.Zero(t, fixture.completes)
	require.Zero(t, fixture.legacy)
	fixture.mu.Unlock()

	// The held generation stays locally readable.
	read, err := v.Open(name)
	require.NoError(t, err)
	got, err := io.ReadAll(read)
	require.NoError(t, err)
	require.NoError(t, read.Close())
	require.Equal(t, payload, string(got))

	// Mounted unlink and rename may not remove or rehome held data.
	require.ErrorIs(t, v.Remove(name), EPERM)
	require.ErrorIs(t, v.Rename(name, "moved.txt"), EPERM)

	// A second queue attempt still sends nothing.
	forceWriteback(t, v, name)
	time.Sleep(50 * time.Millisecond)
	fixture.mu.Lock()
	require.Equal(t, 1, fixture.creates)
	fixture.mu.Unlock()
	require.Equal(t, fs.Duration(24*time.Hour), v.Opt.WriteBack)
}

func forceWriteback(t *testing.T, v *VFS, name string) {
	t.Helper()
	for _, info := range v.cache.Queue()["queue"].([]writeback.QueueInfo) {
		if info.Name == name {
			require.NoError(t, v.cache.QueueSetExpiry(info.ID, time.Now(), 0))
			return
		}
	}
	t.Fatalf("no queued writeback for %q", name)
}

// TestB3HeldDirectoryRefusal proves a directory holding a held generation
// cannot be removed or rehomed, while the held bytes stay readable.
func TestB3HeldDirectoryRefusal(t *testing.T) {
	previous := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(previous)) })

	fixture := new(b3DirtyFixture)
	server := httptest.NewServer(http.HandlerFunc(fixture.serve))
	t.Cleanup(server.Close)

	ctx, ci := fs.AddConfig(t.Context())
	ci.LowLevelRetries, ci.BufferSize = 2, 0
	remote, err := teldrive.NewFs(ctx, "b3-dir", "", configmap.Simple{
		"api_host": server.URL, "api_key": "fixture-key", "guarded_upload": "true",
		"encoding": "Slash,BackSlash,LeftSpace,RightSpace,InvalidUtf8",
	})
	require.NoError(t, err)
	remote.Features().ScopedChangeNotify = nil

	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeWrites
	opt.WriteBack = fs.Duration(24 * time.Hour)
	opt.HandleCaching = 0
	v := New(ctx, remote, &opt)
	t.Cleanup(v.Shutdown)

	root, err := v.Root()
	require.NoError(t, err)
	held, err := root.Mkdir("hold")
	require.NoError(t, err)

	const name = "hold/pending.txt"
	const payload = "held bytes inside a protected directory"
	h, err := v.Create(name)
	require.NoError(t, err)
	_, err = h.WriteString(payload)
	require.NoError(t, err)
	require.NoError(t, h.Close())
	require.NotNil(t, v.cache.DirtyItem(name))
	forceWriteback(t, v, name)
	require.Eventually(t, func() bool { return v.cache.HeldItem(name) != "" }, 5*time.Second, time.Millisecond)
	require.True(t, v.cache.HeldInTree("hold"))
	require.Equal(t, payload, string(b3OpenBytes(t, v, name)))

	heldNode, err := root.Stat("hold")
	require.NoError(t, err)
	held, ok := heldNode.(*Dir)
	require.True(t, ok, "hold must resolve to a directory")

	require.ErrorIs(t, held.Remove(), EPERM)
	require.ErrorIs(t, v.Rename("hold", "moved"), EPERM)
	require.ErrorIs(t, v.Remove(name), EPERM)

	// Nothing sent, nothing recreated.
	fixture.mu.Lock()
	require.Equal(t, 1, fixture.creates)
	require.Zero(t, fixture.parts)
	require.Zero(t, fixture.completes)
	fixture.mu.Unlock()
}

// TestB3QuarantineRefusesRunningClient proves a live client holds the shared
// root lock, so offline quarantine refuses instead of separating bytes out
// from under a running mount.
func TestB3QuarantineRefusesRunningClient(t *testing.T) {
	cacheBase := t.TempDir()
	require.NoError(t, os.Chmod(cacheBase, 0o700))
	require.NoError(t, config.SetCacheDir(cacheBase))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir("")) })

	fixture := new(b3DirtyFixture)
	server := httptest.NewServer(http.HandlerFunc(fixture.serve))
	t.Cleanup(server.Close)

	ctx, ci := fs.AddConfig(t.Context())
	ci.LowLevelRetries, ci.BufferSize = 2, 0
	remote, err := teldrive.NewFs(ctx, "b3-lock", "", configmap.Simple{
		"api_host": server.URL, "api_key": "fixture-key", "guarded_upload": "true",
	})
	require.NoError(t, err)
	remote.Features().ScopedChangeNotify = nil

	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeWrites
	opt.HandleCaching = 0
	v := New(ctx, remote, &opt)
	t.Cleanup(v.Shutdown)
	require.NotNil(t, v.cache)

	target := filepath.Join(t.TempDir(), "quarantine")
	_, err = vfscache.QuarantineHeldCache(vfscache.QuarantineRequest{
		CacheRoot: v.cache.DedicatedRootOSPath(), Target: target, Publication: fs.GuardedPublicationAbsent,
	})
	require.ErrorIs(t, err, vfscache.ErrQuarantineNotQuiesced)
	_, statErr := os.Stat(target)
	require.True(t, os.IsNotExist(statErr), "quarantine must not create its target while a client runs")
}

// b3Upload models one guarded session the fixture serves. A completed session
// carries the exact receipt the server would publish.
type b3Upload struct {
	id        string
	admission string
	parentID  string
	name      string
	mimeType  string
	size      int64
	modTime   time.Time
	content   []byte
	completed bool
	revealed  bool
	segments  []string
	wrongHash bool
	missing   bool
}

// b3ReconcileFixture is a synthetic server for restart reconciliation. It
// records every mutating call so a restarted mount can be proven send-free.
type b3ReconcileFixture struct {
	mu           sync.Mutex
	entries      map[string]*api.FileInfo
	uploads      map[string]*b3Upload
	uploadOrder  []string
	order        []string
	nextUpload   int
	failComplete bool
	failLists    bool
	statusHook   func()
	creates      int
	parts        int
	completes    int
	status       int
	legacy       int
	mkdirs       int
}

func newB3ReconcileFixture() *b3ReconcileFixture {
	return &b3ReconcileFixture{entries: map[string]*api.FileInfo{}, uploads: map[string]*b3Upload{}}
}

func (f *b3ReconcileFixture) addFolder(id, name, parent string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[id] = &api.FileInfo{
		Id: id, ParentId: parent, Name: name, Kind: "folder", Status: "active",
		ModTime: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
	f.order = append(f.order, id)
}

func (f *b3ReconcileFixture) setEntry(id, name, kind, parent string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, ok := f.entries[id]
	if !ok {
		entry = &api.FileInfo{Id: id, ModTime: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
		f.entries[id] = entry
		f.order = append(f.order, id)
	}
	entry.Name, entry.Kind, entry.Status, entry.ParentId = name, kind, "active", parent
}

func (f *b3ReconcileFixture) counts() (creates, parts, completes, legacy, mkdirs int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.creates, f.parts, f.completes, f.legacy, f.mkdirs
}

func (f *b3ReconcileFixture) uploadContent(id string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.uploads[id].content...)
}

func (f *b3ReconcileFixture) uploadID(t *testing.T) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	require.NotEmpty(t, f.uploads)
	for id, upload := range f.uploads {
		if len(upload.content) > 0 {
			return id
		}
	}
	for id := range f.uploads {
		return id
	}
	return ""
}

// reveal publishes every completed session, as the server would after the
// completing request was committed and its response lost.
func (f *b3ReconcileFixture) reveal() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, upload := range f.uploads {
		upload.revealed = true
		id := "file-" + upload.id
		if f.entries[id] == nil {
			f.entries[id] = &api.FileInfo{
				Id: id, ParentId: upload.parentID, Name: upload.name, Kind: "file", Status: "active",
				Size: upload.size, Generation: 1, ModTime: upload.modTime,
				Hash: &api.FileHash{Algorithm: "blake3", Value: f.resultHashLocked(upload)},
			}
			f.order = append(f.order, id)
		}
	}
}

func (f *b3ReconcileFixture) uploadIDForParent(t *testing.T, parent string) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.uploadOrder) - 1; i >= 0; i-- {
		upload := f.uploads[f.uploadOrder[i]]
		if upload != nil && upload.parentID == parent && len(upload.content) > 0 {
			return upload.id
		}
	}
	for i := len(f.uploadOrder) - 1; i >= 0; i-- {
		upload := f.uploads[f.uploadOrder[i]]
		if upload != nil && upload.parentID == parent {
			return upload.id
		}
	}
	t.Fatalf("no upload for parent %q", parent)
	return ""
}

func (f *b3ReconcileFixture) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/api/v1/me":
		_, _ = io.WriteString(w, `{"userId":1}`)
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files":
		f.serveList(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/files":
		f.mu.Lock()
		f.mkdirs++
		f.mu.Unlock()
		w.WriteHeader(http.StatusBadRequest)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/files/") && strings.HasSuffix(r.URL.Path, "/content"):
		f.serveContent(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/files/"):
		f.mu.Lock()
		entry, ok := f.entries[strings.TrimPrefix(r.URL.Path, "/api/v1/files/")]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(entry)
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded":
		w.Header().Set("X-TelDrive-Recovery-Incarnation", b3DirtyRecovery)
		_, _ = io.WriteString(w, `{}`)
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/guarded":
		f.serveCreate(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/uploads/guarded/"):
		f.serveStatus(w, r)
	case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/parts/"):
		f.servePart(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete"):
		f.serveComplete(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/parts"):
		f.serveLegacyParts(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/uploads/"):
		f.serveLegacyStatus(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/v1/uploads"):
		f.mu.Lock()
		f.legacy++
		f.mu.Unlock()
		w.WriteHeader(http.StatusBadRequest)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

// serveContent returns the exact bytes a completed session published.
func (f *b3ReconcileFixture) serveContent(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/files/"), "/content")
	f.mu.Lock()
	var content []byte
	for _, upload := range f.uploads {
		if "file-"+upload.id == id {
			content = append([]byte(nil), upload.content...)
		}
	}
	f.mu.Unlock()
	if content == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	_, _ = w.Write(content)
}

func (f *b3ReconcileFixture) serveList(w http.ResponseWriter, r *http.Request) {
	parent := r.URL.Query().Get("parentId")
	if parent == "root" {
		parent = ""
	}
	f.mu.Lock()
	fail := f.failLists
	f.mu.Unlock()
	if fail {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":{"code":"fixture_list","message":"list failed"}}`)
		return
	}
	f.mu.Lock()
	response := api.ReadMetadataResponse{}
	for _, id := range f.order {
		entry := f.entries[id]
		if entry.ParentId == parent {
			response.Files = append(response.Files, *entry)
		}
	}
	f.mu.Unlock()
	_ = json.NewEncoder(w).Encode(response)
}

func (f *b3ReconcileFixture) serveCreate(w http.ResponseWriter, r *http.Request) {
	var request api.GuardedUploadCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	parent := ""
	if request.Destination.ParentID != nil {
		parent = *request.Destination.ParentID
	}
	f.mu.Lock()
	f.creates++
	f.nextUpload++
	upload := &b3Upload{
		id: uuid.NewString(), admission: r.Header.Get("Idempotency-Key"),
		parentID: parent, name: request.Name, mimeType: request.MimeType,
		size: request.Size, modTime: request.ModTime,
	}
	f.uploads[upload.id] = upload
	f.uploadOrder = append(f.uploadOrder, upload.id)
	body := f.sessionBodyLocked(upload)
	f.mu.Unlock()
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(body)
}

func (f *b3ReconcileFixture) sessionBodyLocked(upload *b3Upload) map[string]any {
	body := map[string]any{
		"id": upload.id, "name": upload.name, "parentId": upload.parentID,
		"expectedSize": upload.size, "mimeType": upload.mimeType,
		"modTime": upload.modTime.UTC().Format(time.RFC3339Nano), "encryption": false,
		"conflictPolicy": "replace", "partSize": tdhash.BlockSize,
		"requiresCompletionManifest": true, "state": "open",
		"expiresAt": "2036-01-01T00:00:00Z", "createdAt": "2026-01-01T00:00:00Z",
	}
	if !upload.completed || !upload.revealed {
		return body
	}
	body["state"] = "completed"
	body["fileId"] = "file-" + upload.id
	body["completedReceipt"] = map[string]any{
		"admissionKey": upload.admission, "uploadId": upload.id, "size": upload.size,
		"hash":   map[string]any{"algorithm": "blake3", "value": f.resultHashLocked(upload)},
		"result": f.resultEntryLocked(upload),
	}
	return body
}

func (f *b3ReconcileFixture) resultHashLocked(upload *b3Upload) string {
	if upload.wrongHash {
		return tdhash.SumString([]byte("independent external bytes"))
	}
	return tdhash.SumString(upload.content)
}

func (f *b3ReconcileFixture) resultEntryLocked(upload *b3Upload) map[string]any {
	return map[string]any{
		"id": "file-" + upload.id, "name": upload.name, "kind": "file", "status": "active",
		"parentId": upload.parentID, "size": upload.size, "generation": 1,
		"hash":           map[string]any{"algorithm": "blake3", "value": f.resultHashLocked(upload)},
		"modTime":        upload.modTime.UTC().Format(time.RFC3339Nano),
		"parentSegments": upload.segments,
	}
}

// serveLegacyStatus answers the ordinary retained-attempt status probe. It
// never reveals a completion before the restart reconciliation can.
func (f *b3ReconcileFixture) serveLegacyStatus(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/uploads/")
	f.mu.Lock()
	upload, ok := f.uploads[id]
	var body map[string]any
	if ok {
		body = f.sessionBodyLocked(upload)
	}
	f.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	_ = json.NewEncoder(w).Encode(body)
}

func (f *b3ReconcileFixture) serveLegacyParts(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(strings.TrimSuffix(r.URL.Path, "/parts"), "/api/v1/uploads/")
	f.mu.Lock()
	upload, ok := f.uploads[id]
	items := []any{}
	if ok && len(upload.content) > 0 {
		items = append(items, map[string]any{
			"uploadId": id, "partNo": 1, "state": "stored", "plainSize": len(upload.content),
			"checksum": tdhash.SumString(upload.content),
		})
	}
	f.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "nextCursor": ""})
}

func (f *b3ReconcileFixture) serveStatus(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/uploads/guarded/")
	f.mu.Lock()
	f.status++
	hook := f.statusHook
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	f.mu.Lock()
	upload, ok := f.uploads[id]
	var body map[string]any
	if ok {
		body = f.sessionBodyLocked(upload)
	}
	f.mu.Unlock()
	w.Header().Set("X-TelDrive-Upload-Guard", "1")
	w.Header().Set("X-TelDrive-Recovery-Incarnation", b3DirtyRecovery)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	_ = json.NewEncoder(w).Encode(body)
}

func (f *b3ReconcileFixture) servePart(w http.ResponseWriter, r *http.Request) {
	content, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	path := strings.TrimSuffix(r.URL.Path, "/")
	id := strings.SplitN(strings.TrimPrefix(path, "/api/v1/uploads/guarded/"), "/", 2)[0]
	partNo, _ := strconv.Atoi(path[strings.LastIndex(path, "/")+1:])
	f.mu.Lock()
	f.parts++
	upload := f.uploads[id]
	if upload != nil {
		// These fixtures upload a single part; a replay replaces it instead of
		// duplicating the bytes.
		upload.content = append(upload.content[:0], content...)
	}
	f.mu.Unlock()
	if upload == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	_ = json.NewEncoder(w).Encode(api.UploadPart{
		UploadId: id, PartNo: partNo, State: "stored",
		PlainSize: int64(len(content)), Checksum: tdhash.SumString(content),
	})
}

func (f *b3ReconcileFixture) serveComplete(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(strings.TrimSuffix(r.URL.Path, "/complete"), "/api/v1/uploads/guarded/")
	var request api.UploadCompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.completes++
	upload := f.uploads[id]
	if upload != nil {
		upload.completed = true
	}
	fail := f.failComplete
	var body map[string]any
	if upload != nil {
		body = f.resultEntryLocked(upload)
	}
	f.mu.Unlock()
	if upload == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if fail {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":{"code":"fixture_unavailable","message":"response lost"}}`)
		return
	}
	w.Header().Set("Etag", `"1"`)
	w.Header().Set("Location", "/files/file-"+id)
	_ = json.NewEncoder(w).Encode(body)
}

// b3RestartMount is a mount session whose cache and VFS are recreated from
// disk by restart. The backend client is shared so the namespace fence stays
// observable.
type b3RestartMount struct {
	ctx      context.Context
	remote   fs.Fs
	endpoint string
	root     string
}

func b3NewRestartMount(t *testing.T) *b3RestartMount {
	t.Helper()
	previous := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(previous)) })
	ctx, ci := fs.AddConfig(t.Context())
	ci.LowLevelRetries, ci.BufferSize, ci.Transfers = 2, 0, 1
	return &b3RestartMount{ctx: ctx, root: config.GetCacheDir()}
}

func (m *b3RestartMount) client(t *testing.T) fs.Fs {
	t.Helper()
	remote, err := teldrive.NewFs(m.ctx, "b3-dirty", "", configmap.Simple{
		"api_host": m.endpoint, "api_key": "fixture-key", "guarded_upload": "true",
		"encoding": "Slash,BackSlash,LeftSpace,RightSpace,InvalidUtf8",
	})
	require.NoError(t, err)
	remote.Features().ScopedChangeNotify = nil
	return remote
}

func (m *b3RestartMount) start(t *testing.T, fixture *b3ReconcileFixture) *VFS {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(fixture.serve))
	t.Cleanup(server.Close)
	m.endpoint = server.URL
	m.remote = m.client(t)
	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeWrites
	opt.WriteBack = fs.Duration(24 * time.Hour)
	opt.HandleCaching = 0
	return New(m.ctx, m.remote, &opt)
}

func (m *b3RestartMount) restart(t *testing.T) *VFS {
	t.Helper()
	// A restart reuses the cache and metadata on disk but not the client.
	m.remote = m.client(t)
	return m.restartClient(t)
}

func (m *b3RestartMount) restartClient(t *testing.T) *VFS {
	t.Helper()
	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeWrites
	opt.WriteBack = fs.Duration(24 * time.Hour)
	opt.HandleCaching = 0
	v := New(m.ctx, m.remote, &opt)
	require.Equal(t, fs.Duration(24*time.Hour), v.Opt.WriteBack)
	return v
}

// writePending stores payload at name through the mount and waits for the
// guarded attempt to reach the fixture.
func b3WritePending(t *testing.T, v *VFS, name, payload string) {
	t.Helper()
	h, err := v.Create(name)
	require.NoError(t, err)
	_, err = h.WriteString(payload)
	require.NoError(t, err)
	require.NoError(t, h.Close())
	require.NotNil(t, v.cache.DirtyItem(name))
}

func b3WaitAttempt(t *testing.T, m *b3RestartMount, sessionID string) {
	t.Helper()
	// The persisted JSON is indented, so a field separator may carry spaces.
	needle := regexp.MustCompile(`"sessionId"\s*:\s*"` + regexp.QuoteMeta(sessionID) + `"`)
	require.Eventually(t, func() bool {
		found := false
		_ = filepath.WalkDir(m.root, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			data, readErr := os.ReadFile(path)
			if readErr == nil && needle.Match(data) {
				found = true
				return filepath.SkipAll
			}
			return nil
		})
		return found
	}, 15*time.Second, 5*time.Millisecond)
}

func b3OpenBytes(t *testing.T, v *VFS, name string) []byte {
	t.Helper()
	read, err := v.Open(name)
	require.NoError(t, err)
	got, err := io.ReadAll(read)
	require.NoError(t, err)
	require.NoError(t, read.Close())
	return got
}

func b3Digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// TestB3DirtyOverlay proves a held generation stays readable through a
// restarted mount when its ancestor vanished and is now a file. The overlay is
// local only: no remote create is attempted and directory reads keep failing.
func TestB3DirtyOverlay(t *testing.T) {
	m := b3NewRestartMount(t)
	fixture := newB3ReconcileFixture()
	fixture.addFolder("folder-1", "folder", "")
	fixture.failComplete = true
	v := m.start(t, fixture)
	t.Cleanup(v.Shutdown)

	const name = "folder/pending.txt"
	const payload = "held bytes behind a vanished ancestor"
	b3WritePending(t, v, name, payload)
	forceWriteback(t, v, name)
	require.Eventually(t, func() bool {
		creates, _, completes, _, _ := fixture.counts()
		return creates == 1 && completes > 0
	}, 5*time.Second, time.Millisecond)
	b3WaitAttempt(t, m, fixture.uploadID(t))
	v.Shutdown()

	// The ancestor is gone, a file now occupies its name, and every origin
	// listing fails from here on.
	fixture.mu.Lock()
	delete(fixture.entries, "folder-1")
	fixture.failLists = true
	fixture.mu.Unlock()
	fixture.setEntry("folder-1", "folder", "file", "")

	v = m.restart(t)
	t.Cleanup(v.Shutdown)

	require.NotEmpty(t, v.cache.HeldItem(name))
	require.NotNil(t, v.cache.DirtyItem(name))
	got := b3OpenBytes(t, v, name)
	require.Equal(t, payload, string(got))
	require.Equal(t, b3Digest([]byte(payload)), b3Digest(got))

	// An ordinary directory read still returns the backend error and the
	// listing never becomes a successful complete one.
	beforeCreates, beforeParts, beforeCompletes, beforeLegacy, _ := fixture.counts()
	_, readDirErr := v.ReadDir("folder")
	require.Error(t, readDirErr)
	require.True(t, v.root.cachedDir("folder").read.IsZero())

	creates, parts, completes, legacy, mkdirs := fixture.counts()
	require.Equal(t, beforeCreates, creates, "restart must not create another session")
	require.Equal(t, beforeParts, parts)
	require.Equal(t, beforeCompletes, completes)
	require.Equal(t, beforeLegacy, legacy)
	require.Zero(t, mkdirs, "no remote directory may be created")
	require.Equal(t, fs.Duration(24*time.Hour), v.Opt.WriteBack)
}

// TestB3DirtyRestartReconcile proves restart adoption is read-only and exact.
func TestB3DirtyRestartReconcile(t *testing.T) {
	const payload = "reconciled pending bytes"

	t.Run("adopts exact completed result", func(t *testing.T) {
		m := b3NewRestartMount(t)
		fixture := newB3ReconcileFixture()
		fixture.addFolder("folder-1", "folder", "")
		fixture.failComplete = true
		v := m.start(t, fixture)
		t.Cleanup(v.Shutdown)

		const name = "folder/pending.txt"
		b3WritePending(t, v, name, payload)
		forceWriteback(t, v, name)
		require.Eventually(t, func() bool {
			_, _, completes, _, _ := fixture.counts()
			return completes > 0
		}, 5*time.Second, time.Millisecond)
		session := fixture.uploadID(t)
		b3WaitAttempt(t, m, session)

		creates, parts, completes, legacy, _ := fixture.counts()
		require.NotZero(t, creates)
		require.NotZero(t, parts)
		require.NotNil(t, v.cache.DirtyItem(name))
		require.Equal(t, payload, string(fixture.uploadContent(session)))
		v.Shutdown()
		// The server committed the result; only its response was lost.
		fixture.reveal()
		creates, parts, completes, legacy, _ = fixture.counts()

		v = m.restart(t)
		t.Cleanup(v.Shutdown)

		created, sentParts, sentCompletes, sentLegacy, mkdirs := fixture.counts()
		require.Equal(t, creates, created)
		require.Equal(t, parts, sentParts)
		require.Equal(t, completes, sentCompletes)
		require.Equal(t, legacy, sentLegacy)
		require.Zero(t, mkdirs)
		require.Zero(t, v.cache.HeldItem(name), "an exact result installs a clean alias")
		require.Nil(t, v.cache.DirtyItem(name))
		got := b3OpenBytes(t, v, name)
		require.Equal(t, payload, string(got))
		require.Equal(t, b3Digest([]byte(payload)), b3Digest(got))
	})

	t.Run("refuses a result that is not the local proof", func(t *testing.T) {
		m := b3NewRestartMount(t)
		fixture := newB3ReconcileFixture()
		fixture.addFolder("folder-1", "folder", "")
		fixture.failComplete = true
		v := m.start(t, fixture)
		t.Cleanup(v.Shutdown)

		const name = "folder/pending.txt"
		b3WritePending(t, v, name, payload)
		forceWriteback(t, v, name)
		require.Eventually(t, func() bool {
			_, _, completes, _, _ := fixture.counts()
			return completes > 0
		}, 5*time.Second, time.Millisecond)
		session := fixture.uploadID(t)
		b3WaitAttempt(t, m, session)
		v.Shutdown()

		fixture.mu.Lock()
		fixture.uploads[session].wrongHash = true
		fixture.uploads[session].revealed = true
		fixture.mu.Unlock()

		v = m.restart(t)
		t.Cleanup(v.Shutdown)

		require.NotEmpty(t, v.cache.HeldItem(name))
		require.NotNil(t, v.cache.DirtyItem(name))
		got := b3OpenBytes(t, v, name)
		require.Equal(t, payload, string(got))
		require.Equal(t, b3Digest([]byte(payload)), b3Digest(got))
		require.Equal(t, b3Digest([]byte(payload)), b3Digest(fixture.uploadContent(session)))
	})
}

// TestB3DirtyFolderFollow proves same-UUID follow, refusal and collision rules
// for an authoritative folder move across restart.
func TestB3DirtyFolderFollow(t *testing.T) {
	const payload = "pending bytes that follow a folder"

	t.Run("follows the same UUID", func(t *testing.T) {
		m := b3NewRestartMount(t)
		fixture := newB3ReconcileFixture()
		fixture.addFolder("folder-1", "left", "")
		fixture.failComplete = true
		v := m.start(t, fixture)
		t.Cleanup(v.Shutdown)

		b3WritePending(t, v, "left/pending.txt", payload)
		forceWriteback(t, v, "left/pending.txt")
		require.Eventually(t, func() bool {
			_, _, completes, _, _ := fixture.counts()
			return completes > 0
		}, 5*time.Second, time.Millisecond)
		session := fixture.uploadID(t)
		b3WaitAttempt(t, m, session)
		v.Shutdown()

		// The folder keeps its UUID but moves to a new name.
		fixture.setEntry("folder-1", "renamed", "folder", "")
		fixture.mu.Lock()
		fixture.uploads[session].segments = []string{"renamed"}
		fixture.uploads[session].revealed = true
		fixture.mu.Unlock()

		v = m.restart(t)
		t.Cleanup(v.Shutdown)

		require.Zero(t, v.cache.HeldItem("renamed/pending.txt"))
		require.Nil(t, v.cache.DirtyItem("renamed/pending.txt"))
		require.Nil(t, v.cache.DirtyItem("left/pending.txt"), "the old alias must not survive the move")
		item := v.cache.Item("renamed/pending.txt")
		require.NotNil(t, item)
		require.NoError(t, item.Open(nil))
		got := make([]byte, len(payload))
		_, err := item.ReadAt(got, 0)
		require.NoError(t, err)
		require.NoError(t, item.Close(nil))
		require.Equal(t, payload, string(got))
		require.Equal(t, b3Digest([]byte(payload)), b3Digest(got))
	})

	t.Run("refuses a different folder identity", func(t *testing.T) {
		m := b3NewRestartMount(t)
		fixture := newB3ReconcileFixture()
		fixture.addFolder("folder-1", "old", "")
		fixture.failComplete = true
		v := m.start(t, fixture)
		t.Cleanup(v.Shutdown)

		b3WritePending(t, v, "old/pending.txt", payload)
		forceWriteback(t, v, "old/pending.txt")
		require.Eventually(t, func() bool {
			_, _, completes, _, _ := fixture.counts()
			return completes > 0
		}, 5*time.Second, time.Millisecond)
		session := fixture.uploadID(t)
		b3WaitAttempt(t, m, session)
		v.Shutdown()

		// The destination is now an inactive file, not the captured folder.
		fixture.setEntry("folder-1", "old", "file", "")
		fixture.mu.Lock()
		fixture.uploads[session].segments = []string{"old"}
		fixture.mu.Unlock()

		v = m.restart(t)
		t.Cleanup(v.Shutdown)

		require.NotEmpty(t, v.cache.HeldItem("old/pending.txt"))
		require.Equal(t, payload, string(b3OpenBytes(t, v, "old/pending.txt")))
		_, _, _, _, mkdirs := fixture.counts()
		require.Zero(t, mkdirs, "the old directory is never recreated")
	})

	t.Run("refuses an occupied alias", func(t *testing.T) {
		m := b3NewRestartMount(t)
		fixture := newB3ReconcileFixture()
		fixture.addFolder("left-1", "left", "")
		fixture.addFolder("right-1", "right", "")
		fixture.failComplete = true
		v := m.start(t, fixture)
		t.Cleanup(v.Shutdown)

		const payloadLeft = "left pending bytes"
		b3WritePending(t, v, "left/f.txt", payloadLeft)
		forceWriteback(t, v, "left/f.txt")
		require.Eventually(t, func() bool {
			_, _, completes, _, _ := fixture.counts()
			return completes > 0
		}, 5*time.Second, time.Millisecond)
		leftSession := fixture.uploadIDForParent(t, "left-1")
		b3WaitAttempt(t, m, leftSession)
		v.Shutdown()

		v = m.restart(t)
		t.Cleanup(v.Shutdown)
		const payloadRight = "right pending bytes"
		b3WritePending(t, v, "right/f.txt", payloadRight)
		forceWriteback(t, v, "right/f.txt")
		require.Eventually(t, func() bool {
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			for _, upload := range fixture.uploads {
				if upload.parentID == "right-1" {
					return true
				}
			}
			return false
		}, 10*time.Second, time.Millisecond)
		rightSession := fixture.uploadIDForParent(t, "right-1")
		b3WaitAttempt(t, m, rightSession)
		v.Shutdown()

		// Left moves onto the path right still owns; right's parent becomes a
		// file so right cannot follow.
		fixture.setEntry("left-1", "right", "folder", "")
		fixture.setEntry("right-1", "right", "file", "")
		fixture.mu.Lock()
		fixture.uploads[leftSession].segments = []string{"right"}
		fixture.uploads[leftSession].revealed = true
		fixture.mu.Unlock()

		v = m.restart(t)
		t.Cleanup(v.Shutdown)

		require.NotEmpty(t, v.cache.HeldItem("left/f.txt"),
			"an occupied alias keeps the evicted generation held")
		require.Equal(t, payloadLeft, string(b3OpenBytes(t, v, "left/f.txt")))
	})

	t.Run("refuses a stale namespace epoch", func(t *testing.T) {
		m := b3NewRestartMount(t)
		fixture := newB3ReconcileFixture()
		fixture.addFolder("folder-1", "folder", "")
		fixture.failComplete = true
		v := m.start(t, fixture)
		t.Cleanup(v.Shutdown)

		const name = "folder/pending.txt"
		b3WritePending(t, v, name, payload)
		forceWriteback(t, v, name)
		require.Eventually(t, func() bool {
			_, _, completes, _, _ := fixture.counts()
			return completes > 0
		}, 5*time.Second, time.Millisecond)
		session := fixture.uploadID(t)
		b3WaitAttempt(t, m, session)
		v.Shutdown()

		// Advance the epoch once so the fence is published, reveal the exact
		// result, then invalidate the namespace while the reconciling status
		// request is in flight.
		m.remote.(fs.DirCacheFlusher).DirCacheFlush()
		fixture.reveal()
		fixture.statusHook = func() {
			m.remote.(fs.DirCacheFlusher).DirCacheFlush()
		}

		v = m.restartClient(t)
		t.Cleanup(v.Shutdown)

		require.NotEmpty(t, v.cache.HeldItem(name))
		require.NotNil(t, v.cache.DirtyItem(name))
		require.Equal(t, payload, string(b3OpenBytes(t, v, name)))
	})
}
