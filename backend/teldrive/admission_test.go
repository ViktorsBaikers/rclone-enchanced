package teldrive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/fs/rc/jobs"
	syncops "github.com/rclone/rclone/fs/sync"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/transform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This protocol oracle retains the original identity independently of backend objects.
// SQL publication, generated routes and real cmd.Run are separate root-owned proofs.
type b2AdmissionFixture struct {
	mu                                                                   sync.Mutex
	o                                                                    *Object
	request                                                              api.UploadCreateRequest
	key                                                                  string
	session                                                              api.UploadSession
	sessions                                                             map[string]api.UploadSession
	creates, lists, gets, parts, folders, completes                      int
	terminal, truncate, loseCreate, unknown, missing, expired, completed bool
	denied                                                               bool
	sessionID                                                            string
	genericUnknown                                                       bool
	file                                                                 *api.FileInfo
}

func newB2AdmissionFixture(t *testing.T, ctx context.Context) *b2AdmissionFixture {
	x := &b2AdmissionFixture{sessions: make(map[string]api.UploadSession)}
	x.o = newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		x.mu.Lock()
		defer x.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		unavailable := func() {
			w.WriteHeader(http.StatusConflict)
			if x.truncate {
				_, _ = io.WriteString(w, `{"error":`)
				return
			}
			if x.genericUnknown {
				_, _ = io.WriteString(w, `{"error":{"code":"conflict","message":"legacy destination unknown"}}`)
				return
			}
			_, _ = io.WriteString(w, `{"error":{"code":"upload_parent_unavailable","message":"upload destination folder is unavailable"}}`)
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads":
			x.lists++
			page := api.UploadSessionPage{Items: []api.UploadSession{}}
			if x.unknown && x.session.ID != "" {
				page.Items = []api.UploadSession{x.session}
			}
			_ = json.NewEncoder(w).Encode(page)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files":
			page := api.ReadMetadataResponse{}
			if x.file != nil {
				page.Files = []api.FileInfo{*x.file}
			}
			_ = json.NewEncoder(w).Encode(page)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads":
			x.creates++
			var request api.UploadCreateRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			key := r.Header.Get("Idempotency-Key")
			require.NotEmpty(t, key)
			if x.key == "" {
				x.key, x.request = key, request
				id := "original"
				if _, exists := x.sessions[id]; exists {
					id = key
				}
				if x.sessionID != "" {
					id = x.sessionID
				}
				x.session = api.UploadSession{ID: id, ParentId: request.ParentId, Name: request.Name,
					ExpectedSize: request.Size, MimeType: request.MimeType, ModTime: request.ModTime,
					Encryption: request.Encryption, ConflictPolicy: request.ConflictPolicy,
					State: "open", ExpiresAt: time.Now().Add(time.Hour), PartSize: tdhash.BlockSize,
					RequiresCompletionManifest: true}
				x.sessions[id] = x.session
			} else {
				assert.Equal(t, x.key, key)
				assert.Equal(t, x.request, request)
			}
			if x.loseCreate && x.creates == 1 {
				x.terminal = true
				_, _ = io.WriteString(w, `{"id":`)
				return
			}
			if x.terminal {
				unavailable()
				return
			}
			_ = json.NewEncoder(w).Encode(x.session)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/"+x.session.ID:
			x.gets++
			if x.terminal || x.unknown {
				unavailable()
				return
			}
			if x.denied {
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, `{}`)
				return
			}
			if x.missing {
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, `{}`)
				return
			}
			if x.expired {
				w.WriteHeader(http.StatusGone)
				_, _ = io.WriteString(w, `{}`)
				return
			}
			session := x.session
			if x.completed {
				session.State, session.FileId = "completed", "winner"
			}
			_ = json.NewEncoder(w).Encode(session)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/"+x.session.ID+"/parts":
			x.parts++
			page := api.UploadPartPage{Items: []api.UploadPart{}}
			if x.completed {
				page.Items = []api.UploadPart{{UploadId: x.session.ID, PartNo: 1, State: "stored", PlainSize: 4, Checksum: tdhash.SumString([]byte("data"))}}
			}
			_ = json.NewEncoder(w).Encode(page)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/"+x.session.ID+"/complete":
			x.completes++
			var manifest api.UploadCompleteRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&manifest))
			require.NotNil(t, manifest.Hash)
			assert.Equal(t, int64(4), manifest.Size)
			assert.Equal(t, tdhash.SumString([]byte("data")), manifest.Hash.Value)
			x.file = &api.FileInfo{Id: "winner", Kind: "file", Name: x.request.Name,
				ModTime: x.request.ModTime, Size: manifest.Size, Hash: manifest.Hash}
			_ = json.NewEncoder(w).Encode(x.file)
		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/parts/"):
			if x.terminal || x.unknown {
				unavailable()
				return
			}
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			_ = json.NewEncoder(w).Encode(api.UploadPart{UploadId: x.session.ID, PartNo: 1, State: "stored",
				PlainSize: int64(len(body)), Checksum: tdhash.SumString(body)})
		case r.URL.Path == "/api/v1/folders":
			x.folders++
			t.Error("selected attempt must not recreate folders")
			w.WriteHeader(http.StatusBadRequest)
		default:
			t.Errorf("unexpected admission request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	x.o.fs.name = "b2-admission"
	x.o.fs.opt.ApiHost = t.Name()
	x.o.fs.opt.APIKey = "synthetic-key"
	x.o.fs.userId = 42
	x.o.fs.pacer = fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond)))
	return x
}

func b2CommandContext(t *testing.T) context.Context {
	t.Helper()
	parent, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	ctx, ci := fs.AddConfig(parent)
	ci.CommandRunID = fs.NewCommandRunID()
	ci.LowLevelRetries = 2
	return ctx
}

func b2AdmissionSource() fs.ObjectInfo {
	return object.NewStaticObjectInfo("source.txt", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), 4, true, nil, nil).WithMimeType("text/plain")
}

func b2FreshAdmissionObject(o *Object, root, remote string) *Object {
	f := *o.fs
	f.root = root
	f.dirCache = dircache.New(root, rootID, &f)
	return &Object{fs: &f, remote: remote}
}

func TestB2CommandAdmission(t *testing.T) {
	for _, mode := range []string{"lost_create_ack_low_level", "truncated_terminal_outer", "fresh_fs_objects", "mixed_retry_before_fatal", "fingerprint_mismatch", "explicit_new_invocation", "legacy_unknown_selected"} {
		t.Run(mode, func(t *testing.T) {
			ctx := b2CommandContext(t)
			x := newB2AdmissionFixture(t, ctx)
			source := b2AdmissionSource()
			remote := "file.txt"
			if mode == "fresh_fs_objects" {
				remote = "parent/file.txt"
				require.NoError(t, x.o.fs.dirCache.FindRoot(ctx, false))
				x.o.fs.dirCache.Put("parent", "parent")
			}
			if mode == "lost_create_ack_low_level" {
				x.loseCreate = true
			}
			if mode == "legacy_unknown_selected" {
				x.unknown = true
				x.session = api.UploadSession{ID: "original", Name: "file.txt", ExpectedSize: 4, MimeType: "text/plain",
					ModTime: source.ModTime(ctx), ConflictPolicy: "replace", State: "open", ExpiresAt: time.Now().Add(time.Hour),
					PartSize: tdhash.BlockSize, RequiresCompletionManifest: true}
				x.sessions[x.session.ID] = x.session
			}
			_, err := x.o.prepareUpload(ctx, remote, source)
			if x.loseCreate || x.unknown {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			x.mu.Lock()
			x.terminal = true
			x.truncate = mode == "truncated_terminal_outer" || mode == "legacy_unknown_selected"
			creates, lists, parts := x.creates, x.lists, x.parts
			x.mu.Unlock()
			fresh := b2FreshAdmissionObject(x.o, "", "file.txt")
			if mode == "fresh_fs_objects" {
				fresh = b2FreshAdmissionObject(x.o, "parent", "file.txt")
			}
			if mode == "mixed_retry_before_fatal" {
				ctx = accounting.WithStatsGroup(ctx, t.Name())
				accounting.Stats(ctx).Error(fserrors.RetryError(errors.New("independent file")))
			}
			if mode == "fingerprint_mismatch" {
				source = object.NewStaticObjectInfo("source.txt", source.ModTime(ctx), 5, true, nil, nil).WithMimeType("text/plain")
			}
			if mode == "explicit_new_invocation" {
				ctx = b2CommandContext(t)
				x.mu.Lock()
				previousKey, previousSession := x.key, x.session
				x.key, x.terminal = "", false
				x.mu.Unlock()
				_, err = fresh.prepareUpload(ctx, "file.txt", source)
				require.NoError(t, err)
				assert.Equal(t, creates+1, x.creates)
				assert.NotEqual(t, previousKey, x.key)
				assert.NotEqual(t, previousSession.ID, x.session.ID)
				assert.Equal(t, previousSession, x.sessions[previousSession.ID])
				return
			}
			_, err = fresh.prepareUpload(ctx, "file.txt", source)
			require.Error(t, err)
			if mode == "mixed_retry_before_fatal" {
				accounting.Stats(ctx).Error(err)
				assert.True(t, accounting.Stats(ctx).HadFatalError())
			}
			if mode == "lost_create_ack_low_level" {
				assert.Greater(t, x.creates, creates, "unknown create outcome replays the same frozen POST")
			} else {
				assert.Equal(t, creates, x.creates)
			}
			assert.Equal(t, lists, x.lists)
			assert.Equal(t, parts, x.parts)
			if mode == "fresh_fs_objects" || mode == "legacy_unknown_selected" {
				require.Error(t, fresh.fs.Mkdir(ctx, ""))
				leaf := "file.txt"
				if mode == "fresh_fs_objects" {
					leaf = "parent"
				}
				_, err = fresh.fs.CreateDir(ctx, rootID, leaf)
				require.Error(t, err)
				assert.Zero(t, x.folders)
			}
			if mode == "fresh_fs_objects" {
				t.Run("duplicate_cancellation", func(t *testing.T) {
					admission, err := fresh.fs.selectUploadAdmission(ctx, "upload", source, "file.txt", "file.txt", [32]byte{})
					require.NoError(t, err)
					release, err := admission.acquire(ctx)
					require.NoError(t, err)
					defer release()
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					_, err = fresh.prepareUpload(cancelled, "file.txt", source)
					require.ErrorIs(t, err, context.Canceled)
				})
				t.Run("backend_shortcuts_before_cache", func(t *testing.T) {
					sourceFs := *x.o.fs
					sourceFs.root, sourceFs.dirCache = "source-tree", nil
					fresh.fs.dirCache = nil
					sourceObject := &Object{fs: &sourceFs, remote: "piece.txt", id: "source"}
					_, err := fresh.fs.Move(ctx, sourceObject, "file.txt")
					require.Error(t, err)
					assert.NotErrorIs(t, err, fs.ErrorCantMove)
					err = fresh.fs.DirMove(ctx, &sourceFs, "", "")
					require.Error(t, err)
					assert.NotErrorIs(t, err, fs.ErrorCantDirMove)
					assert.NotErrorIs(t, err, fs.ErrorDirExists)
				})
			}
			if mode == "fingerprint_mismatch" {
				original := b2AdmissionSource()
				fresh.fs.opt.ChunkSize += tdhash.BlockSize
				_, err = fresh.prepareUpload(ctx, "file.txt", original)
				require.ErrorIs(t, err, errAdmissionConflict)
				assert.Equal(t, creates, x.creates)
				fresh.fs.opt.ChunkSize = x.o.fs.opt.ChunkSize
				x.mu.Lock()
				x.terminal = false
				x.session.PartSize *= 2
				x.mu.Unlock()
				_, err = fresh.prepareUpload(ctx, "file.txt", original)
				require.ErrorIs(t, err, errAdmissionConflict)
				assert.Equal(t, parts, x.parts, "negotiated geometry cannot change before replay")
			}
			if mode == "legacy_unknown_selected" {
				assert.Zero(t, x.creates)
				t.Run("new_explicit_root", func(t *testing.T) {
					newContext := b2CommandContext(t)
					x.mu.Lock()
					legacy := x.session
					x.key, x.terminal = "", false
					x.mu.Unlock()
					_, err := fresh.prepareUpload(newContext, "new-root.txt", b2AdmissionSource())
					require.NoError(t, err)
					assert.Empty(t, x.request.ParentId)
					assert.NotEqual(t, legacy.ID, x.session.ID)
					assert.Equal(t, legacy, x.sessions[legacy.ID])
					assert.Zero(t, x.folders)
				})
			}
		})
	}
}

func TestB2SelectedUploadProof(t *testing.T) {
	for _, mode := range []string{"missing", "expired", "completed_equal", "completed_relocated", "completed_denied", "completed_different", "completed_short", "completed_long", "independent_rc", "independent_source"} {
		t.Run(mode, func(t *testing.T) {
			ctx := b2CommandContext(t)
			x := newB2AdmissionFixture(t, ctx)
			source := b2AdmissionSource()
			remote := "file.txt"
			if mode == "completed_relocated" {
				remote = "parent/file.txt"
				require.NoError(t, x.o.fs.dirCache.FindRoot(ctx, false))
				x.o.fs.dirCache.Put("parent", "parent")
			}
			_, err := x.o.prepareUpload(ctx, remote, source)
			require.NoError(t, err)
			x.missing, x.expired, x.completed = mode == "missing", mode == "expired", strings.HasPrefix(mode, "completed")
			x.denied = mode == "completed_denied"
			if mode == "completed_relocated" {
				x.session.ParentId = ""
			}
			if mode == "independent_rc" || mode == "independent_source" {
				x.key = ""
				if mode == "independent_rc" {
					ctx = fs.WithRCRequest(ctx)
				} else {
					source = object.NewStaticObjectInfo("other.txt", source.ModTime(ctx), 4, true, nil, nil).WithMimeType("text/plain")
				}
				_, err = x.o.prepareUpload(ctx, "file.txt", source)
				require.NoError(t, err)
				assert.Equal(t, 2, x.creates)
				return
			}
			body := "data"
			if mode == "completed_different" {
				body = "DATA"
			}
			if mode == "completed_short" {
				body = "dat"
			}
			if mode == "completed_long" {
				body += "x"
			}
			info, err := x.o.uploadMultipart(ctx, remote, strings.NewReader(body), source)
			if mode != "completed_equal" && mode != "completed_relocated" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				_, err = x.o.completeUpload(ctx, info)
				require.NoError(t, err)
				assert.Equal(t, 1, x.completes)
			}
			assert.Equal(t, 1, x.creates)
			assert.Equal(t, 1, x.lists)
		})
	}
}

type b2AdmissionJournal struct {
	mu    sync.Mutex
	state fs.UploadAdmissionState
	err   error
}

func (s *b2AdmissionJournal) LoadUploadAdmission(ctx context.Context) (fs.UploadAdmissionState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return fs.UploadAdmissionState{}, err
	}
	return s.state, s.err
}

func (s *b2AdmissionJournal) StoreUploadAdmission(ctx context.Context, state fs.UploadAdmissionState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.err != nil {
		return s.err
	}
	if state.Generation != s.state.Generation {
		return errors.New("stale generation")
	}
	s.state = state
	return nil
}

func TestB2DurableAdmissionTransition(t *testing.T) {
	ctx := b2CommandContext(t)
	x := newB2AdmissionFixture(t, ctx)
	store := &b2AdmissionJournal{state: fs.UploadAdmissionState{Generation: "f9f8e89b-154d-4246-a105-d78873d105e6"}}
	ctx = fs.WithUploadAdmissionStore(ctx, store)
	source := b2AdmissionSource()
	a, err := x.o.fs.selectUploadAdmission(ctx, "upload", source, "file.txt", "file.txt", [32]byte{})
	require.NoError(t, err)
	request := api.UploadCreateRequest{Name: "file.txt", Size: 4, ModTime: source.ModTime(ctx), MimeType: "text/plain",
		ConflictPolicy: "replace", PreferredPartSize: int64(x.o.fs.opt.ChunkSize)}
	store.err = errors.New("metadata sync failed")
	require.Error(t, a.FreezeRequest(ctx, request))
	require.Nil(t, a.snapshot().request)
	store.err = nil
	require.NoError(t, a.FreezeRequest(ctx, request))
	require.NotEmpty(t, store.state.RequestKey)
	require.NotEmpty(t, store.state.Request)
	store.err = errors.New("metadata rename failed")
	require.Error(t, a.SelectUploadID(ctx, "ea7528ac-84f4-440b-9b6f-663c3269a5bb"))
	require.Empty(t, a.snapshot().uploadID)
	store.err = nil
	require.NoError(t, a.SelectUploadID(ctx, "ea7528ac-84f4-440b-9b6f-663c3269a5bb"))
	again, err := b2FreshAdmissionObject(x.o, "", "file.txt").fs.selectUploadAdmission(ctx, "upload", source, "file.txt", "file.txt", [32]byte{})
	require.NoError(t, err)
	require.Equal(t, a.snapshot().key, again.snapshot().key)
	require.Equal(t, a.snapshot().request, again.snapshot().request)
	require.Equal(t, a.snapshot().uploadID, again.snapshot().uploadID)
}

func b2WaitAdmissionBarrier(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("RC admission barrier did not open")
	}
}

func b2AsyncAdmissionJob(t *testing.T, parent context.Context) (*jobs.Job, context.Context, func()) {
	t.Helper()
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	finished := make(chan struct{})
	job, _, err := jobs.NewJob(parent, func(ctx context.Context, _ rc.Params) (rc.Params, error) {
		started <- ctx
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil
	}, rc.Params{"_async": true, "_group": "same-admission-group"})
	require.NoError(t, err)
	_, err = jobs.OnFinish(job.ID, func() { close(finished) })
	require.NoError(t, err)
	var once sync.Once
	finish := func() {
		once.Do(func() { close(release) })
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("RC job did not finish")
		}
	}
	t.Cleanup(finish)
	select {
	case ctx := <-started:
		return job, ctx, finish
	case <-time.After(time.Second):
		t.Fatal("RC job did not start")
		return nil, nil, nil
	}
}

func TestB2RCAdmissionExactJobLiveness(t *testing.T) {
	parent := b2CommandContext(t)
	older, olderCtx, finishOlder := b2AsyncAdmissionJob(t, parent)
	newer, newerCtx, finishNewer := b2AsyncAdmissionJob(t, parent)
	require.Less(t, older.ID, newer.ID)
	require.Equal(t, older.Group, newer.Group)
	require.True(t, cache.JobIsRunning(older.ID))
	olderCopy := fs.CopyConfig(context.Background(), fs.CopyConfig(context.Background(), olderCtx))
	newerCopy := fs.CopyConfig(context.Background(), fs.CopyConfig(context.Background(), newerCtx))
	require.Equal(t, olderCtx, fs.UploadAdmissionJobContext(olderCopy))

	newerFixture := newB2AdmissionFixture(t, parent)
	_, err := newerFixture.o.prepareUpload(newerCtx, "file.txt", b2AdmissionSource())
	require.NoError(t, err)
	finishNewer()
	require.False(t, cache.JobIsRunning(newer.ID))
	require.Eventually(t, func() bool {
		commandAdmissions.Lock()
		defer commandAdmissions.Unlock()
		return commandAdmissions.scopes[admissionScopeKey{"job", uint64(newer.ID)}] == nil
	}, time.Second, time.Millisecond)

	olderFixture := newB2AdmissionFixture(t, parent)
	_, err = olderFixture.o.prepareUpload(olderCopy, "file.txt", b2AdmissionSource())
	require.NoError(t, err, "an older live job may first touch TelDrive after a newer job finishes")
	require.NotEqual(t, newerFixture.key, olderFixture.key)
	_, err = olderFixture.o.prepareUpload(olderCtx, "file.txt", b2AdmissionSource())
	require.NoError(t, err)
	require.Equal(t, 1, olderFixture.creates)
	finishOlder()
	require.Eventually(t, func() bool {
		commandAdmissions.Lock()
		defer commandAdmissions.Unlock()
		return commandAdmissions.scopes[admissionScopeKey{"job", uint64(older.ID)}] == nil
	}, time.Second, time.Millisecond)
	for _, closed := range []struct {
		ctx context.Context
		x   *b2AdmissionFixture
	}{{olderCtx, olderFixture}, {olderCopy, olderFixture}, {newerCtx, newerFixture}, {newerCopy, newerFixture}} {
		closed.x.mu.Lock()
		creates, lists, gets, parts := closed.x.creates, closed.x.lists, closed.x.gets, closed.x.parts
		closed.x.mu.Unlock()
		_, err := b2FreshAdmissionObject(closed.x.o, "", "file.txt").prepareUpload(closed.ctx, "file.txt", b2AdmissionSource())
		require.ErrorIs(t, err, errAdmissionUnresolved)
		closed.x.mu.Lock()
		require.Equal(t, []int{creates, lists, gets, parts},
			[]int{closed.x.creates, closed.x.lists, closed.x.gets, closed.x.parts})
		closed.x.mu.Unlock()
	}
	commandAdmissions.Lock()
	olderScope := commandAdmissions.scopes[admissionScopeKey{"job", uint64(older.ID)}]
	newerScope := commandAdmissions.scopes[admissionScopeKey{"job", uint64(newer.ID)}]
	commandAdmissions.Unlock()
	require.Nil(t, olderScope)
	require.Nil(t, newerScope)
	require.NoError(t, olderCtx.Err(), "job completion must not become generic context cancellation")
	require.NoError(t, olderCopy.Err())
}

func TestB2RCAdmissionOwnershipHooks(t *testing.T) {
	for _, mode := range []string{"missing_getter", "missing_on_finish", "missing_liveness", "canceled", "finished_before_first_touch"} {
		t.Run(mode, func(t *testing.T) {
			parent := b2CommandContext(t)
			job, jobCtx, finish := b2AsyncAdmissionJob(t, parent)
			ctx, release := fs.WithUploadAdmissionTransientScope(jobCtx)
			defer release()
			ctx = fs.CopyConfig(context.Background(), fs.CopyConfig(context.Background(), ctx))
			x := newB2AdmissionFixture(t, parent)
			getID, onFinish, isRunning := cache.JobGetJobID, cache.JobOnFinish, cache.JobIsRunning
			t.Cleanup(func() {
				cache.JobGetJobID, cache.JobOnFinish, cache.JobIsRunning = getID, onFinish, isRunning
			})
			expected := errAdmissionUnresolved
			switch mode {
			case "missing_getter":
				cache.JobGetJobID = nil
			case "missing_on_finish":
				cache.JobOnFinish = nil
			case "missing_liveness":
				cache.JobIsRunning = nil
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				expected = context.Canceled
			case "finished_before_first_touch":
				finish()
			}
			_, err := x.o.prepareUpload(ctx, "file.txt", b2AdmissionSource())
			require.ErrorIs(t, err, expected)
			require.Zero(t, x.creates)
			require.Zero(t, x.lists)
			require.Zero(t, x.gets)
			commandAdmissions.Lock()
			scope := commandAdmissions.scopes[admissionScopeKey{"job", uint64(job.ID)}]
			commandAdmissions.Unlock()
			require.Nil(t, scope, "actual job failures must not create replacement ownership")
		})
	}
	t.Run("marker_only_jobless", func(t *testing.T) {
		ctx := b2CommandContext(t)
		commandScope, err := admissionScopeForContext(ctx)
		require.NoError(t, err)
		onFinish, isRunning := cache.JobOnFinish, cache.JobIsRunning
		cache.JobOnFinish, cache.JobIsRunning = nil, nil
		t.Cleanup(func() { cache.JobOnFinish, cache.JobIsRunning = onFinish, isRunning })
		ctx = fs.WithRCRequest(ctx)
		_, actual := cache.JobGetJobID(fs.UploadAdmissionJobContext(ctx))
		require.False(t, actual)
		scope, err := admissionScopeForContext(ctx)
		require.NoError(t, err)
		require.Nil(t, scope, "an RC marker must not inherit the enclosing command")
		child, cancel := fs.WithUploadAdmissionTransientScope(ctx)
		defer cancel()
		child = fs.CopyConfig(context.Background(), fs.CopyConfig(context.Background(), child))
		scope, err = admissionScopeForContext(child)
		require.NoError(t, err)
		require.NotNil(t, scope)
		require.NotSame(t, commandScope, scope)
		again, err := admissionScopeForContext(child)
		require.NoError(t, err)
		require.Same(t, scope, again, "jobless retry ownership must remain stable")
		x := newB2AdmissionFixture(t, ctx)
		_, err = x.o.prepareUpload(child, "file.txt", b2AdmissionSource())
		require.NoError(t, err)
		_, err = x.o.prepareUpload(child, "file.txt", b2AdmissionSource())
		require.NoError(t, err)
		require.Equal(t, 1, x.creates)
	})
}

func TestB2RCAdmissionCompletionRaces(t *testing.T) {
	for _, mode := range []string{"during_registration", "new_final_probe", "existing_final_probe", "ready_waiter"} {
		t.Run(mode, func(t *testing.T) {
			parent := b2CommandContext(t)
			job, jobCtx, finish := b2AsyncAdmissionJob(t, parent)
			ctx := fs.CopyConfig(context.Background(), fs.CopyConfig(context.Background(), jobCtx))
			x := newB2AdmissionFixture(t, parent)
			onFinish, isRunning := cache.JobOnFinish, cache.JobIsRunning
			callbackRelease := make(chan struct{})
			callbackEntered := make(chan struct{})
			callbackDone := make(chan struct{})
			registrationEntered := make(chan struct{})
			registrationRelease := make(chan struct{})
			var releaseOnce, registrationOnce sync.Once
			releaseCallback := func() { releaseOnce.Do(func() { close(callbackRelease) }) }
			releaseRegistration := func() { registrationOnce.Do(func() { close(registrationRelease) }) }
			t.Cleanup(func() {
				releaseRegistration()
				releaseCallback()
				cache.JobOnFinish, cache.JobIsRunning = onFinish, isRunning
			})
			cache.JobOnFinish = func(id int64, fn func()) (func(), error) {
				if mode == "during_registration" {
					finish()
				}
				cancel, err := onFinish(id, func() {
					close(callbackEntered)
					<-callbackRelease
					fn()
					close(callbackDone)
				})
				if mode == "during_registration" && err == nil {
					b2WaitAdmissionBarrier(t, callbackEntered)
				}
				if mode == "ready_waiter" {
					close(registrationEntered)
					<-registrationRelease
				}
				return cancel, err
			}
			if mode == "existing_final_probe" {
				_, err := x.o.prepareUpload(ctx, "file.txt", b2AdmissionSource())
				require.NoError(t, err)
			}
			var probes atomic.Int32
			waiterProbed := make(chan struct{})
			cache.JobIsRunning = func(id int64) bool {
				probe := probes.Add(1)
				if mode == "ready_waiter" && probe == 2 {
					close(waiterProbed)
				}
				if (mode == "new_final_probe" && probe == 3) ||
					(mode == "existing_final_probe" && probe == 2) ||
					(mode == "ready_waiter" && probe == 4) {
					finish()
				}
				return isRunning(id)
			}
			x.mu.Lock()
			before := []int{x.creates, x.lists, x.gets, x.parts}
			x.mu.Unlock()
			if mode == "ready_waiter" {
				results := make(chan error, 2)
				prepare := func() {
					_, err := b2FreshAdmissionObject(x.o, "", "file.txt").prepareUpload(ctx, "file.txt", b2AdmissionSource())
					results <- err
				}
				go prepare()
				b2WaitAdmissionBarrier(t, registrationEntered)
				go prepare()
				b2WaitAdmissionBarrier(t, waiterProbed)
				releaseRegistration()
				for range 2 {
					select {
					case err := <-results:
						require.ErrorIs(t, err, errAdmissionUnresolved)
					case <-time.After(time.Second):
						t.Fatal("RC admission waiter did not return")
					}
				}
				require.GreaterOrEqual(t, probes.Load(), int32(5), "both ready scopes need a final liveness probe")
			} else {
				_, err := b2FreshAdmissionObject(x.o, "", "file.txt").prepareUpload(ctx, "file.txt", b2AdmissionSource())
				require.ErrorIs(t, err, errAdmissionUnresolved)
			}
			require.False(t, isRunning(job.ID))
			x.mu.Lock()
			require.Equal(t, before, []int{x.creates, x.lists, x.gets, x.parts},
				"completion before admission must not send requests")
			x.mu.Unlock()
			commandAdmissions.Lock()
			beforeScope := commandAdmissions.scopes[admissionScopeKey{"job", uint64(job.ID)}]
			commandAdmissions.Unlock()
			for _, retained := range []context.Context{jobCtx, ctx} {
				_, err := x.o.prepareUpload(retained, "file.txt", b2AdmissionSource())
				require.ErrorIs(t, err, errAdmissionUnresolved)
			}
			commandAdmissions.Lock()
			afterScope := commandAdmissions.scopes[admissionScopeKey{"job", uint64(job.ID)}]
			commandAdmissions.Unlock()
			require.True(t, beforeScope == afterScope, "a delayed callback must not allow replacement ownership")
			releaseCallback()
			select {
			case <-callbackDone:
			case <-time.After(time.Second):
				t.Fatal("admission finish callback did not prune")
			}
			for _, retained := range []context.Context{jobCtx, ctx} {
				_, err := x.o.prepareUpload(retained, "file.txt", b2AdmissionSource())
				require.ErrorIs(t, err, errAdmissionUnresolved)
			}
			x.mu.Lock()
			after := []int{x.creates, x.lists, x.gets, x.parts}
			x.mu.Unlock()
			require.Equal(t, before, after, "retained contexts must not create replacement requests")
			commandAdmissions.Lock()
			scope := commandAdmissions.scopes[admissionScopeKey{"job", uint64(job.ID)}]
			commandAdmissions.Unlock()
			require.Nil(t, scope)
		})
	}
}

func TestB2RCAdmissionLifetime(t *testing.T) {
	for _, mode := range []string{"same_job_fresh_fs", "separate_jobs_same_group", "copied_context",
		"closed_scope_pruning", "cli_vfs_scope_separation", "legacy_unknown_scope"} {
		t.Run(mode, func(t *testing.T) {
			ctx := b2CommandContext(t)
			fs.GetConfig(ctx).IgnoreTimes = true
			x := newB2AdmissionFixture(t, ctx)
			root := t.TempDir()
			path := filepath.Join(root, "source.txt")
			require.NoError(t, os.WriteFile(path, []byte("data"), 0600))
			source, err := local.NewFs(ctx, "local", root, configmap.Simple{})
			require.NoError(t, err)
			targetName := "b2-rc-" + uuid.NewString()
			x.o.fs.name = targetName
			destination := targetName + ":"
			fresh := func() {
				f := *x.o.fs
				f.dirCache = dircache.New("", rootID, &f)
				f.features = (&fs.Features{}).Fill(ctx, &f)
				cache.Put(destination, &f)
			}
			params := rc.Params{"srcFs": root, "srcRemote": "source.txt", "dstFs": destination, "dstRemote": "file.txt"}
			cache.Put(root, source)
			call := rc.Calls.Get("operations/copyfile")
			require.NotNil(t, call)
			invoke := func(jobCtx context.Context) error {
				fresh()
				_, err := call.Fn(jobCtx, params)
				return err
			}
			var retained context.Context
			var firstKey string
			var firstJob int64
			run := func(fn rc.Func) *jobs.Job {
				job, _, err := jobs.NewJob(ctx, fn, rc.Params{"_group": "same-caller-group"})
				require.NoError(t, err)
				return job
			}
			if mode == "separate_jobs_same_group" {
				one := run(func(jobCtx context.Context, _ rc.Params) (rc.Params, error) {
					require.NoError(t, invoke(jobCtx))
					firstKey = x.key
					return nil, nil
				})
				x.key, x.sessionID = "", uuid.NewString()
				two := run(func(jobCtx context.Context, _ rc.Params) (rc.Params, error) {
					require.NoError(t, invoke(jobCtx))
					require.NotEqual(t, firstKey, x.key)
					return nil, nil
				})
				require.NotEqual(t, one.ID, two.ID)
				require.Equal(t, one.Group, two.Group)
				return
			}
			var journal *b2AdmissionJournal
			if mode == "cli_vfs_scope_separation" || mode == "legacy_unknown_scope" {
				journal = &b2AdmissionJournal{state: fs.UploadAdmissionState{Generation: uuid.NewString()}}
				ctx = fs.WithUploadAdmissionStore(ctx, journal)
				x.sessionID = uuid.NewString()
				x.unknown = true
				x.genericUnknown = mode == "legacy_unknown_scope"
				require.Error(t, invoke(ctx))
				firstKey = x.key
			} else {
				x.loseCreate = true
			}
			job := run(func(jobCtx context.Context, _ rc.Params) (rc.Params, error) {
				firstJob, _ = cache.JobGetJobID(jobCtx)
				retained = fs.CopyConfig(context.Background(), fs.CopyConfig(context.Background(), jobCtx))
				require.Equal(t, jobCtx, fs.UploadAdmissionJobContext(retained))
				if journal != nil {
					selected, ok := fs.UploadAdmissionStoreFromContext(retained)
					require.True(t, ok)
					require.Same(t, journal, selected)
				}
				require.Error(t, invoke(jobCtx))
				if firstKey == "" {
					firstKey = x.key
				}
				request := x.request
				if mode == "copied_context" {
					origin, cancelOrigin := context.WithCancel(jobCtx)
					detached := fs.CopyConfig(context.Background(), fs.CopyConfig(context.Background(), origin))
					cancelOrigin()
					require.NoError(t, detached.Err())
					err := invoke(detached)
					require.Error(t, err)
					require.NotErrorIs(t, err, context.Canceled)
				}
				if mode == "closed_scope_pruning" {
					b2RCClosedScopeControls(t, jobCtx, root, targetName+"-nested")
					commandAdmissions.Lock()
					live := commandAdmissions.scopes[admissionScopeKey{"job", uint64(firstJob)}]
					retainsLive := live != nil && len(live.entries) != 0
					commandAdmissions.Unlock()
					require.True(t, retainsLive)
				}
				require.Error(t, invoke(retained))
				require.Equal(t, firstKey, x.key)
				require.Equal(t, request, x.request)
				canceled, cancel := context.WithCancel(retained)
				cancel()
				before := x.creates
				require.ErrorIs(t, invoke(canceled), context.Canceled)
				require.Equal(t, before, x.creates)
				return nil, nil
			})
			require.Equal(t, job.ID, firstJob)
			require.Eventually(t, func() bool {
				commandAdmissions.Lock()
				defer commandAdmissions.Unlock()
				_, exists := commandAdmissions.scopes[admissionScopeKey{"job", uint64(job.ID)}]
				return !exists
			}, time.Second, time.Millisecond)
			if journal == nil {
				_, err := x.o.fs.selectUploadAdmission(retained, "upload", b2AdmissionSource(), "file.txt", "file.txt", [32]byte{})
				require.Error(t, err)
			} else {
				// A new actual job still cannot override the recovered VFS generation.
				run(func(jobCtx context.Context, _ rc.Params) (rc.Params, error) {
					require.Error(t, invoke(jobCtx))
					require.Equal(t, firstKey, x.key)
					return nil, nil
				})
			}
			require.Len(t, x.sessions, 1)
			require.Zero(t, x.folders)
			got, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, []byte("data"), got)
			// Explicit independent work remains useful after the retained failure.
			x.key, x.terminal, x.unknown, x.loseCreate, x.sessionID = "", false, false, false, uuid.NewString()
			x.genericUnknown = false
			independent, ci := fs.AddConfig(context.Background())
			ci.LowLevelRetries = 1
			ci.IgnoreTimes = true
			_, _, err = jobs.NewJob(independent, func(jobCtx context.Context, _ rc.Params) (rc.Params, error) {
				require.NoError(t, invoke(jobCtx))
				require.NotEqual(t, firstKey, x.key)
				return nil, nil
			}, rc.Params{"_group": "same-caller-group"})
			require.NoError(t, err)
		})
	}
}

func b2RCClosedScopeControls(t *testing.T, parent context.Context, sourceRoot, targetName string) {
	t.Helper()
	x := newB2AdmissionFixture(t, parent)
	x.o.fs.name = targetName
	x.o.fs.features = (&fs.Features{}).Fill(parent, x.o.fs)
	cache.Put(targetName+":", x.o.fs)
	call := rc.Calls.Get("operations/copyfile")
	for range 4 {
		x.key, x.sessionID = "", uuid.NewString()
		job, _, err := jobs.NewJob(parent, func(ctx context.Context, _ rc.Params) (rc.Params, error) {
			return call.Fn(ctx, rc.Params{"srcFs": sourceRoot, "srcRemote": "source.txt",
				"dstFs": targetName + ":", "dstRemote": "file.txt"})
		}, rc.Params{"_group": "same-caller-group"})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			commandAdmissions.Lock()
			defer commandAdmissions.Unlock()
			_, exists := commandAdmissions.scopes[admissionScopeKey{"job", uint64(job.ID)}]
			return !exists
		}, time.Second, time.Millisecond)
	}
}

// Exercise real operation entry points against a byte-retaining protocol oracle.
// The service's SQL/authorization/CLI proof remains a separate root-owned gate.
func TestB2DiscoveryAdmission(t *testing.T) {
	for _, mode := range []string{
		"single_move_equal_restored", "adopted_resume_equal_restored", "frozen_create_unknown",
		"completed_unverified", "missing_proof", "unavailable_proof", "fingerprint_mismatch", "unchanged_move_success",
		"copy_dest_first_present_parent_loss", "copy_dest_first_present_truncated_reply",
		"copy_dest_first_present_fresh_fs_outer_retry", "copy_dest_original_transport_identity",
		"copy_dest_unadmitted_error_fallback_control", "no_check_dest_compare_dest_terminal",
		"no_check_dest_copy_dest_terminal", "compare_dest_no_admission_control", "copy_dest_no_admission_control",
		"directory_list_equal_restored", "directory_listr_equal_restored", "directory_no_traverse_equal_restored",
		"directory_check_first_equal_restored", "rc_movefile_equal_restored", "rc_separate_job_same_group",
		"source_role_read_control", "unrelated_subtree_control", "copied_discovery_context",
		"ordinary_transformed_upload_parent_loss", "ordinary_transformed_upload_fresh_fs_retry",
		"ordinary_unadmitted_cant_copy_manual_fallback", "ordinary_stream_put_fallback", "ordinary_stream_put_stream",
		"ordinary_unchanged_transient_controls", "ordinary_chunk_writer_identity", "ordinary_update_override_identity",
		"directory_no_check_dest_compare_terminal", "directory_no_check_dest_copy_terminal", "directory_comparison_selected_siblings",
		"raw_rcat_buffered_terminal_retry", "raw_rcat_spooled_terminal_retry", "raw_rcat_independent_calls",
		"raw_rcat_transient_control", "raw_rcat_cancellation", "raw_rcat_stream_oneshot_control",
	} {
		t.Run(mode, func(t *testing.T) { b2DiscoveryCase(t, mode) })
	}
}

func TestB2MixedSourceDiscoveryAdmission(t *testing.T) {
	ctx := accounting.WithStatsGroup(b2CommandContext(t), t.Name())
	ci := fs.GetConfig(ctx)
	ci.MultiThreadStreams, ci.LowLevelRetries = 0, 1
	x := newB2DiscoveryOracle(t, ctx)
	x.truncate, x.failNextOpenDiscovery = true, true
	x.f.pacer.SetRetries(1)
	root := t.TempDir()
	modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	originalBytes := []byte("data")
	originalSHA := sha256.Sum256(originalBytes)
	for _, name := range []string{"a-transient", "z-target"} {
		filename := filepath.Join(root, name)
		require.NoError(t, os.WriteFile(filename, originalBytes, 0600))
		require.NoError(t, os.Chtimes(filename, modTime, modTime))
	}
	sourceFs, err := local.NewFs(ctx, "local", root, configmap.Simple{})
	require.NoError(t, err)
	a, err := sourceFs.NewObject(ctx, "a-transient")
	require.NoError(t, err)
	z, err := sourceFs.NewObject(ctx, "z-target")
	require.NoError(t, err)
	scope, err := admissionScopeForContext(ctx)
	require.NoError(t, err)
	entryFor := func(source fs.Object) *uploadAdmission {
		identity := admissionIdentity{x.f.admissionRemoteIdentity(), admissionSourceIdentity(sourceFs),
			admissionSourcePath(source), canonicalAdmissionPath(x.f.root, source.Remote())}
		commandAdmissions.Lock()
		entry := scope.entries[identity]
		commandAdmissions.Unlock()
		require.NotNil(t, entry)
		return entry
	}

	// Both attempts share one command, source filesystem and destination parent.
	require.Error(t, operations.MoveFile(ctx, x.f, sourceFs, a.Remote(), a.Remote()))
	require.False(t, x.failNextOpenDiscovery)
	unfrozen := entryFor(a).snapshot()
	require.Nil(t, unfrozen.request)
	require.Empty(t, unfrozen.uploadID)
	require.Zero(t, x.creates)
	require.Zero(t, x.copies)
	require.Zero(t, x.puts)
	require.Zero(t, x.publications)
	require.Zero(t, x.deletes)
	require.Empty(t, x.sessions)
	require.NoError(t, operations.CheckUploadAdmission(ctx, x.f, a, a.Remote()))

	require.Error(t, operations.MoveFile(ctx, x.f, sourceFs, z.Remote(), z.Remote()))
	selected := entryFor(z)
	original := selected.snapshot()
	require.NotNil(t, original.request)
	originalRequest := *original.request
	require.NotEmpty(t, original.key)
	require.NotEmpty(t, original.uploadID)
	require.Equal(t, original.uploadID, x.keys[original.key])
	require.Equal(t, originalRequest, x.requests[original.key])
	require.Equal(t, z.Remote(), originalRequest.Name)
	require.Empty(t, originalRequest.ParentId)
	require.Equal(t, "aborted", x.sessions[original.uploadID].State)
	require.True(t, x.terminal)
	require.True(t, x.restored)
	x.seed(z.Remote(), "restored-z", []byte("DATA"), modTime)
	trap, err := x.f.NewObject(ctx, z.Remote())
	require.NoError(t, err)
	require.Equal(t, z.Size(), trap.Size())
	require.True(t, z.ModTime(ctx).Equal(trap.ModTime(ctx)))
	require.True(t, operations.Equal(ctx, z, trap))
	require.NotEqual(t, originalSHA, sha256.Sum256(x.data["restored-z"]))
	beforeCreates, beforePuts := x.creates, x.puts
	require.Equal(t, 1, beforeCreates)
	require.Equal(t, 1, beforePuts)

	// findObject lists the shared parent before filtering by name.
	// An exact unselected source must still reach that metadata RPC.
	aRemote, zRemote := a.Remote(), z.Remote()
	discoveryA := fs.WithUploadAdmissionDiscovery(ctx, sourceFs, &aRemote)
	beforeMeta, beforeSessions := x.metadata, len(x.sessionReads)
	_, err = x.f.NewObject(discoveryA, a.Remote())
	require.ErrorIs(t, err, fs.ErrorObjectNotFound)
	require.Greater(t, x.metadata, beforeMeta)
	require.Len(t, x.sessionReads, beforeSessions)
	require.NoError(t, operations.CheckUploadAdmission(ctx, x.f, a, a.Remote()))

	discoveryZ := fs.WithUploadAdmissionDiscovery(ctx, sourceFs, &zRemote)
	beforeMeta = x.metadata
	assertReconciled := func(before int) {
		require.Greater(t, len(x.sessionReads), before)
		for _, id := range x.sessionReads[before:] {
			require.Equal(t, original.uploadID, id)
		}
		require.Equal(t, beforeMeta, x.metadata)
	}
	beforeSessions = len(x.sessionReads)
	_, err = x.f.NewObject(discoveryZ, z.Remote())
	require.Error(t, err)
	require.True(t, fs.IsUploadAdmissionError(err))
	require.NotErrorIs(t, err, fs.ErrorObjectNotFound)
	assertReconciled(beforeSessions)
	beforeSessions = len(x.sessionReads)
	err = operations.CheckUploadAdmission(ctx, x.f, z, z.Remote())
	require.Error(t, err)
	require.True(t, fs.IsUploadAdmissionError(err))
	assertReconciled(beforeSessions)

	// Real comparison and move entry points must stop before equality,
	// comparison lookup, fallback copying or source removal.
	comparison := x.fresh(ctx, "comparison")
	x.seed("comparison/"+z.Remote(), "comparison-z", originalBytes, modTime)
	for _, copyDest := range []bool{false, true} {
		ci.CompareDest, ci.CopyDest = nil, nil
		if copyDest {
			ci.CopyDest = []string{"comparison"}
		} else {
			ci.CompareDest = []string{"comparison"}
		}
		beforeSessions = len(x.sessionReads)
		noTransfer, compareErr := operations.CompareOrCopyDest(ctx, x.f, trap, z, []fs.Fs{comparison}, nil, z.Remote())
		require.Error(t, compareErr)
		require.True(t, fs.IsUploadAdmissionError(compareErr))
		require.False(t, noTransfer)
		assertReconciled(beforeSessions)
	}
	ci.CompareDest, ci.CopyDest = nil, nil
	equalityCalls := 0
	moveCtx := operations.WithEqualFn(ctx, func(ctx context.Context, source fs.ObjectInfo, destination fs.Object) bool {
		equalityCalls++
		return operations.Equal(ctx, source, destination)
	})
	beforeSessions = len(x.sessionReads)
	err = operations.MoveFile(moveCtx, x.f, sourceFs, z.Remote(), z.Remote())
	require.Error(t, err)
	require.True(t, fs.IsUploadAdmissionError(err))
	require.Zero(t, equalityCalls)
	assertReconciled(beforeSessions)

	require.Equal(t, original.key, selected.snapshot().key)
	require.Equal(t, original.uploadID, selected.snapshot().uploadID)
	require.Equal(t, originalRequest, *selected.snapshot().request)
	require.Equal(t, original.uploadID, x.keys[original.key])
	require.Equal(t, originalRequest, x.requests[original.key])
	require.Len(t, x.keys, 1)
	require.Len(t, x.sessions, 1)
	require.Equal(t, beforeCreates, x.creates)
	require.Equal(t, beforePuts, x.puts)
	require.Zero(t, x.copies)
	require.Zero(t, x.publications)
	require.Zero(t, x.deletes)
	require.Zero(t, x.folders)
	require.Equal(t, unfrozen.key, entryFor(a).snapshot().key)
	require.Nil(t, entryFor(a).snapshot().request)
	require.Empty(t, entryFor(a).snapshot().uploadID)
	require.Equal(t, []byte("DATA"), x.data["restored-z"])
	require.Equal(t, originalBytes, x.data["comparison-z"])
	for _, source := range []fs.Object{a, z} {
		got, readErr := os.ReadFile(filepath.Join(root, source.Remote()))
		require.NoError(t, readErr)
		require.Equal(t, originalBytes, got)
		require.Equal(t, originalSHA, sha256.Sum256(got))
	}
}

type b2DiscoveryOracle struct {
	mu                                                                                         sync.Mutex
	f                                                                                          *Fs
	files                                                                                      map[string]api.FileInfo
	data                                                                                       map[string][]byte
	sessions                                                                                   map[string]api.UploadSession
	requests                                                                                   map[string]api.UploadCreateRequest
	keys                                                                                       map[string]string
	partData                                                                                   map[string][]byte
	copyRequests                                                                               map[string]api.FileCopy
	copySources                                                                                map[string]string
	creates, copies, publications, deletes, folders, metadata, reads, puts                     int
	terminal, restored, truncate, loseCreate, adopt, complete, missing, unavailable, transient bool
	cancel                                                                                     context.CancelFunc
	observedScopes                                                                             map[admissionScopeKey]*admissionScope
	failNextOpenDiscovery                                                                      bool
	sessionReads                                                                               []string
}

func newB2DiscoveryOracle(t *testing.T, ctx context.Context) *b2DiscoveryOracle {
	t.Helper()
	x := &b2DiscoveryOracle{files: make(map[string]api.FileInfo), data: make(map[string][]byte),
		sessions: make(map[string]api.UploadSession), requests: make(map[string]api.UploadCreateRequest),
		keys: make(map[string]string), partData: make(map[string][]byte),
		copyRequests: make(map[string]api.FileCopy), copySources: make(map[string]string),
		observedScopes: make(map[admissionScopeKey]*admissionScope)}
	o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		x.mu.Lock()
		defer x.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		observeKey := func(key string) {
			commandAdmissions.Lock()
			defer commandAdmissions.Unlock()
			for owner, scope := range commandAdmissions.scopes {
				for _, entry := range scope.entries {
					if entry.snapshot().key == key {
						x.observedScopes[owner] = scope
					}
				}
			}
		}
		refuse := func() {
			if x.cancel != nil {
				x.cancel()
			}
			w.WriteHeader(http.StatusConflict)
			if x.truncate {
				_, _ = io.WriteString(w, `{"error":`)
				return
			}
			_, _ = io.WriteString(w, `{"error":{"code":"upload_parent_unavailable","message":"destination unavailable"}}`)
		}
		file := func(name string, data []byte, id string, modTime time.Time, parent string) api.FileInfo {
			info := api.FileInfo{Id: id, Name: name, ParentId: parent, Kind: "file", Size: int64(len(data)), ModTime: modTime,
				MimeType: "text/plain", Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString(data)}}
			x.files[path.Join(parent, name)], x.data[id] = info, append([]byte(nil), data...)
			return info
		}
		switch {
		case r.Method == "GET" && r.URL.Path == "/api/v1/files":
			x.metadata++
			page := api.ReadMetadataResponse{}
			for _, info := range x.files {
				if info.ParentId == r.URL.Query().Get("parentId") || r.URL.Query().Get("scope") == "recursive" {
					page.Files = append(page.Files, info)
				}
			}
			_ = json.NewEncoder(w).Encode(page)
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/content"):
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/files/"), "/content")
			body, ok := x.data[id]
			if !ok {
				w.WriteHeader(404)
				return
			}
			x.reads++
			_, _ = w.Write(body)
		case r.Method == "GET" && r.URL.Path == "/api/v1/uploads":
			if x.failNextOpenDiscovery {
				require.Equal(t, "open", r.URL.Query().Get("state"))
				x.failNextOpenDiscovery = false
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			page := api.UploadSessionPage{Items: []api.UploadSession{}}
			if x.adopt {
				for _, s := range x.sessions {
					page.Items = append(page.Items, s)
				}
			}
			_ = json.NewEncoder(w).Encode(page)
		case r.Method == "POST" && r.URL.Path == "/api/v1/uploads":
			x.creates++
			key := r.Header.Get("Idempotency-Key")
			require.NotEmpty(t, key)
			observeKey(key)
			var request api.UploadCreateRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			id, exists := x.keys[key]
			if !exists {
				id = uuid.NewString()
				x.keys[key], x.requests[key] = id, request
				x.sessions[id] = api.UploadSession{ID: id, Name: request.Name, ParentId: request.ParentId,
					ExpectedSize: request.Size, MimeType: request.MimeType, ModTime: request.ModTime,
					Encryption: request.Encryption, ConflictPolicy: request.ConflictPolicy, State: "open",
					PartSize: tdhash.BlockSize, RequiresCompletionManifest: true, ExpiresAt: time.Now().Add(time.Hour)}
			} else {
				assert.Equal(t, x.requests[key], request)
			}
			if x.loseCreate {
				x.terminal, x.restored = true, true
				s := x.sessions[id]
				s.State = "aborted"
				x.sessions[id] = s
				_, _ = io.WriteString(w, `{"id":`)
				return
			}
			if x.terminal {
				refuse()
				return
			}
			_ = json.NewEncoder(w).Encode(x.sessions[id])
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/parts"):
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/uploads/"), "/parts")
			page := api.UploadPartPage{Items: []api.UploadPart{}}
			if body, ok := x.partData[id]; ok {
				page.Items = []api.UploadPart{{UploadId: id, PartNo: 1,
					State: "stored", PlainSize: int64(len(body)), Checksum: tdhash.SumString(body)}}
			}
			_ = json.NewEncoder(w).Encode(page)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/v1/uploads/"):
			id := strings.TrimPrefix(r.URL.Path, "/api/v1/uploads/")
			x.sessionReads = append(x.sessionReads, id)
			if x.missing {
				w.WriteHeader(404)
				return
			}
			if x.unavailable {
				w.WriteHeader(503)
				return
			}
			if x.terminal {
				refuse()
				return
			}
			s, exists := x.sessions[id]
			if !exists {
				w.WriteHeader(404)
				return
			}
			_ = json.NewEncoder(w).Encode(s)
		case r.Method == "PUT" && strings.Contains(r.URL.Path, "/parts/"):
			x.puts++
			if x.transient {
				x.transient = false
				w.WriteHeader(503)
				return
			}
			if !x.complete {
				x.terminal, x.restored = true, true
				for id, s := range x.sessions {
					s.State = "aborted"
					x.sessions[id] = s
				}
				refuse()
				return
			}
			id := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/uploads/"), "/")[0]
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			x.partData[id] = append([]byte(nil), body...)
			_ = json.NewEncoder(w).Encode(api.UploadPart{UploadId: id, PartNo: 1, State: "stored",
				PlainSize: int64(len(body)), Checksum: tdhash.SumString(body)})
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/complete"):
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/uploads/"), "/complete")
			s := x.sessions[id]
			var manifest api.UploadCompleteRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&manifest))
			require.Equal(t, int64(len(x.partData[id])), manifest.Size)
			require.NotNil(t, manifest.Hash)
			require.Equal(t, tdhash.SumString(x.partData[id]), manifest.Hash.Value)
			info := file(s.Name, x.partData[id], "result-"+id, s.ModTime, s.ParentId)
			s.State, s.FileId = "completed", info.Id
			x.sessions[id] = s
			x.publications++
			_ = json.NewEncoder(w).Encode(info)
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/copy"):
			x.copies++
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/files/"), "/copy")
			key := r.Header.Get("Idempotency-Key")
			observeKey(key)
			var request api.FileCopy
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			if previous, exists := x.copyRequests[key]; exists {
				require.Equal(t, previous, request)
				require.Equal(t, x.copySources[key], id)
			} else {
				x.copyRequests[key], x.copySources[key] = request, id
			}
			if !x.complete {
				x.terminal, x.restored = true, true
				refuse()
				return
			}
			var source api.FileInfo
			for _, info := range x.files {
				if info.Id == id {
					source = info
				}
			}
			info := file(request.Name, x.data[id], "copy-"+key, source.ModTime, request.ParentId)
			x.publications++
			_ = json.NewEncoder(w).Encode(info)
		case r.Method == "DELETE":
			x.deletes++
			w.WriteHeader(204)
		case r.URL.Path == "/api/v1/folders":
			x.folders++
			w.WriteHeader(409)
		default:
			t.Errorf("unexpected discovery request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(400)
		}
	})
	x.f = o.fs
	x.f.name, x.f.opt.ApiHost, x.f.opt.APIKey, x.f.userId = "b2-discovery-"+uuid.NewString(), t.Name(), "synthetic-key", 42
	x.f.opt.ChunkSize = tdhash.BlockSize
	x.f.features = (&fs.Features{}).Fill(ctx, x.f)
	x.f.features.Move, x.f.features.DirMove = nil, nil
	return x
}

func (x *b2DiscoveryOracle) seed(name, id string, data []byte, modTime time.Time) {
	x.mu.Lock()
	defer x.mu.Unlock()
	parent, leaf := path.Split(name)
	parent = strings.TrimSuffix(parent, "/")
	x.files[name] = api.FileInfo{Id: id, Name: leaf, ParentId: parent, Kind: "file", Size: int64(len(data)),
		ModTime: modTime, MimeType: "text/plain", Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString(data)}}
	x.data[id] = append([]byte(nil), data...)
}

func (x *b2DiscoveryOracle) fresh(ctx context.Context, root string) *Fs {
	f := *x.f
	f.root = root
	f.dirCache = dircache.New(root, rootID, &f)
	if root != "" {
		f.dirCache.Put(root, root)
	}
	f.features = (&fs.Features{}).Fill(ctx, &f)
	f.features.Move, f.features.DirMove = nil, nil
	return &f
}

type b2UnknownDiscoveryObject struct{ fs.Object }

func (b2UnknownDiscoveryObject) Size() int64 { return -1 }

func b2DiscoveryCase(t *testing.T, mode string) {
	if strings.HasPrefix(mode, "raw_rcat_") {
		b2RawDiscoveryCase(t, mode)
		return
	}
	ctx := b2CommandContext(t)
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	ci := fs.GetConfig(ctx)
	ci.MultiThreadStreams, ci.LowLevelRetries, ci.Checkers, ci.Transfers = 0, 2, 2, 2
	ci.NoUpdateDirModTime = true
	x := newB2DiscoveryOracle(t, ctx)
	x.truncate = mode != "copy_dest_first_present_parent_loss"
	x.f.pacer.SetRetries(1)
	root := t.TempDir()
	sourceName, target := "source.txt", "b"
	directory := strings.HasPrefix(mode, "directory_")
	if directory {
		target = sourceName
	}
	sourcePath := filepath.Join(root, sourceName)
	modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	require.NoError(t, os.WriteFile(sourcePath, []byte("data"), 0600))
	require.NoError(t, os.Chtimes(sourcePath, modTime, modTime))
	sourceFs, err := local.NewFs(ctx, "local", root, configmap.Simple{})
	require.NoError(t, err)
	source, err := sourceFs.NewObject(ctx, sourceName)
	require.NoError(t, err)
	sourceSHA := sha256.Sum256([]byte("data"))
	assertSource := func() {
		got, err := os.ReadFile(sourcePath)
		require.NoError(t, err)
		require.Equal(t, sourceSHA, sha256.Sum256(got))
	}
	success := func(remote string) {
		_, err := os.Stat(sourcePath)
		require.ErrorIs(t, err, os.ErrNotExist)
		o, err := x.f.NewObject(ctx, remote)
		require.NoError(t, err)
		in, err := o.Open(ctx)
		require.NoError(t, err)
		data, err := io.ReadAll(in)
		require.NoError(t, err)
		require.NoError(t, in.Close())
		require.Equal(t, sourceSHA, sha256.Sum256(data))
	}
	copyComparison := strings.Contains(mode, "copy_dest") || strings.Contains(mode, "copy_terminal") ||
		mode == "directory_comparison_selected_siblings"
	compareComparison := strings.Contains(mode, "compare_dest") || strings.Contains(mode, "compare_terminal")
	var comparison *Fs
	comparisonName := x.f.name + ":comparison"
	comparisonRoot := t.TempDir()
	if copyComparison {
		comparison = x.fresh(ctx, "comparison")
		cache.Put(comparisonName, comparison)
		ci.CopyDest = []string{comparisonName}
	} else if compareComparison {
		ci.CompareDest = []string{comparisonRoot}
	}
	addComparison := func(data string) {
		if copyComparison {
			x.seed("comparison/"+sourceName, "comparison-source", []byte(data), modTime)
		} else {
			file := filepath.Join(comparisonRoot, sourceName)
			require.NoError(t, os.WriteFile(file, []byte(data), 0600))
			require.NoError(t, os.Chtimes(file, modTime, modTime))
		}
	}
	firstPresent := strings.HasPrefix(mode, "copy_dest_first_present") || mode == "copy_dest_original_transport_identity"
	positive := mode == "unchanged_move_success" || strings.HasSuffix(mode, "no_admission_control") ||
		mode == "copy_dest_unadmitted_error_fallback_control" || mode == "ordinary_unadmitted_cant_copy_manual_fallback" ||
		mode == "ordinary_stream_put_fallback" || mode == "ordinary_stream_put_stream" ||
		mode == "ordinary_unchanged_transient_controls" || mode == "ordinary_chunk_writer_identity" ||
		mode == "ordinary_update_override_identity"
	if firstPresent || (positive && (copyComparison || compareComparison)) {
		addComparison("data")
	}
	if strings.Contains(mode, "no_check_dest") || mode == "directory_comparison_selected_siblings" {
		ci.NoCheckDest = true
	}
	if mode == "directory_listr_equal_restored" {
		ci.UseListR = true
	}
	if mode == "directory_no_traverse_equal_restored" {
		ci.NoTraverse = true
	}
	if mode == "directory_check_first_equal_restored" || mode == "directory_comparison_selected_siblings" {
		ci.CheckFirst, ci.OrderBy = true, "name,ascending"
	}
	if mode == "directory_comparison_selected_siblings" {
		require.NoError(t, os.WriteFile(filepath.Join(root, "excluded.txt"), []byte("keep"), 0600))
		var fi *filter.Filter
		ctx, fi = filter.AddConfig(ctx)
		require.NoError(t, fi.Add(false, "/excluded.txt"))
		ci.NoTraverse = true
		require.NoError(t, transform.SetOptions(ctx, "all,uppercase"))
	}
	if strings.HasPrefix(mode, "ordinary_transformed_") {
		require.NoError(t, transform.SetOptions(ctx, "all,uppercase"))
	}
	if mode == "ordinary_chunk_writer_identity" {
		ci.MultiThreadStreams, ci.MultiThreadCutoff, ci.MultiThreadSet = 2, 1, true
	}
	if mode == "ordinary_update_override_identity" {
		x.seed(target, "old-target", []byte("DATA"), modTime.Add(-time.Hour))
	}
	if mode == "unchanged_move_success" {
		x.seed(target, "unchanged", []byte("data"), modTime)
	}
	if mode == "ordinary_unadmitted_cant_copy_manual_fallback" || mode == "copy_dest_unadmitted_error_fallback_control" {
		x.f.features.Copy = func(context.Context, fs.Object, string) (fs.Object, error) { return nil, fs.ErrorCantCopy }
	}
	if positive {
		x.complete = true
	}
	if mode == "ordinary_unchanged_transient_controls" {
		x.f.pacer.SetRetries(2)
		ci.LowLevelRetries = 2
	}
	if mode == "ordinary_unchanged_transient_controls" {
		x.transient = true
	}
	if mode == "adopted_resume_equal_restored" {
		x.adopt = true
		s := api.UploadSession{ID: uuid.NewString(), Name: target, ExpectedSize: source.Size(),
			ModTime: source.ModTime(ctx), MimeType: fs.MimeType(ctx, fs.NewOverrideRemote(source, target)), ConflictPolicy: "replace",
			State: "open", PartSize: tdhash.BlockSize, RequiresCompletionManifest: true, ExpiresAt: time.Now().Add(time.Hour)}
		x.sessions[s.ID] = s
	}
	if mode == "frozen_create_unknown" {
		x.loseCreate = true
	}
	run := func(operationCtx context.Context, destination *Fs) error {
		if directory {
			return syncops.MoveDir(operationCtx, destination, sourceFs, false, false)
		}
		if strings.HasPrefix(mode, "ordinary_") {
			var current fs.Object = source
			if mode == "ordinary_stream_put_fallback" || mode == "ordinary_stream_put_stream" {
				current = b2UnknownDiscoveryObject{object.NewMemoryObject(sourceName, modTime, []byte("data"))}
				ci.StreamingUploadCutoff = 1
				if mode == "ordinary_stream_put_fallback" {
					destination.features.PutStream = nil
				}
			}
			var existing fs.Object
			if mode == "ordinary_update_override_identity" {
				existing, err = destination.NewObject(operationCtx, target)
				require.NoError(t, err)
			}
			_, copyErr := operations.Copy(operationCtx, destination, existing, target, current)
			return copyErr
		}
		return operations.MoveFile(operationCtx, destination, sourceFs, target, sourceName)
	}
	if mode == "rc_movefile_equal_restored" || mode == "rc_separate_job_same_group" {
		cache.Put(root, sourceFs)
		cache.Put(x.f.name+":", x.f)
		call := rc.Calls.Get("operations/movefile")
		require.NotNil(t, call)
		params := rc.Params{"srcFs": root, "srcRemote": sourceName, "dstFs": x.f.name + ":", "dstRemote": target}
		one, _, jobErr := jobs.NewJob(ctx, func(jobCtx context.Context, _ rc.Params) (rc.Params, error) {
			_, err := call.Fn(jobCtx, params)
			require.Error(t, err)
			x.seed(target, "old-target", []byte("DATA"), modTime)
			before := x.metadata
			cache.Put(x.f.name+":", x.fresh(jobCtx, ""))
			_, err = call.Fn(jobCtx, params)
			require.Error(t, err)
			require.True(t, fs.IsUploadAdmissionError(err))
			require.Equal(t, before, x.metadata)
			assertSource()
			return nil, nil
		}, rc.Params{"_group": "same-discovery-group"})
		require.NoError(t, jobErr)
		if mode == "rc_separate_job_same_group" {
			x.terminal, x.complete = false, true
			ci.IgnoreTimes = true
			two, _, err := jobs.NewJob(ctx, func(jobCtx context.Context, _ rc.Params) (rc.Params, error) {
				return call.Fn(jobCtx, params)
			}, rc.Params{"_group": "same-discovery-group"})
			require.NoError(t, err)
			require.NotEqual(t, one.ID, two.ID)
			require.Equal(t, one.Group, two.Group)
			success(target)
		}
		return
	}
	err = run(ctx, x.f)
	if positive {
		require.NoError(t, err)
		if strings.HasPrefix(mode, "ordinary_") {
			assertSource()
			o, err := x.f.NewObject(ctx, target)
			require.NoError(t, err)
			in, err := o.Open(ctx)
			require.NoError(t, err)
			got, err := io.ReadAll(in)
			require.NoError(t, err)
			require.NoError(t, in.Close())
			require.Equal(t, sourceSHA, sha256.Sum256(got))
		} else if mode == "compare_dest_no_admission_control" {
			_, err := os.Stat(sourcePath)
			require.ErrorIs(t, err, os.ErrNotExist)
			require.Zero(t, x.publications)
		} else {
			success(target)
		}
		if mode == "ordinary_stream_put_stream" {
			for _, request := range x.requests {
				require.Equal(t, int64(-1), request.Size)
			}
		}
		if strings.HasPrefix(mode, "ordinary_") {
			logical := source
			if mode == "ordinary_stream_put_fallback" || mode == "ordinary_stream_put_stream" {
				logical = b2UnknownDiscoveryObject{object.NewMemoryObject(sourceName, modTime, []byte("data"))}
			}
			scope, err := admissionScopeForContext(ctx)
			require.NoError(t, err)
			id := admissionIdentity{x.f.admissionRemoteIdentity(), admissionSourceIdentity(logical.Fs()),
				admissionSourcePath(logical), target}
			commandAdmissions.Lock()
			entry := scope.entries[id]
			commandAdmissions.Unlock()
			require.NotNil(t, entry)
			snapshot := entry.snapshot()
			require.Equal(t, target, snapshot.request.Name)
			require.Equal(t, x.keys[snapshot.key], snapshot.uploadID)
			if mode == "ordinary_stream_put_fallback" {
				require.Equal(t, int64(-1), entry.logicalFingerprint.Size)
				require.Equal(t, int64(4), snapshot.source.Size)
			}
			require.NotZero(t, x.publications)
		}
		if mode == "ordinary_unadmitted_cant_copy_manual_fallback" {
			t.Run("post_selection_cant_copy_without_wider_scope", b2UnscopedCopyFailure)
			t.Run("manual_upload_failure_without_wider_scope", b2UnscopedUploadFailure)
		}
		if mode == "ordinary_stream_put_fallback" || mode == "ordinary_stream_put_stream" {
			t.Run("background_manual_to_rcat", func(t *testing.T) {
				b2UnscopedStreamControl(t, mode == "ordinary_stream_put_stream")
			})
		}
		return
	}
	require.Error(t, err)
	assertSource()
	if firstPresent {
		require.NotEmpty(t, x.copyRequests)
		require.Zero(t, x.creates)
		require.Zero(t, x.puts)
		for key, id := range x.copySources {
			require.NotEmpty(t, key)
			require.Equal(t, "comparison-source", id)
		}
	}
	scope, err := admissionScopeForContext(ctx)
	require.NoError(t, err)
	require.NotNil(t, scope)
	identity := admissionIdentity{x.f.admissionRemoteIdentity(), admissionSourceIdentity(sourceFs),
		admissionSourcePath(source), canonicalAdmissionPath(x.f.root, target)}
	commandAdmissions.Lock()
	selected := scope.entries[identity]
	requestedIndex := len(scope.destinations[admissionDestination{identity.remote, identity.source, identity.requestedTarget}])
	commandAdmissions.Unlock()
	require.NotNil(t, selected)
	require.NotZero(t, requestedIndex)
	original := selected.snapshot()
	if firstPresent {
		require.Contains(t, x.copyRequests, original.key)
	}
	if original.uploadID != "" {
		require.Equal(t, "aborted", x.sessions[original.uploadID].State)
	}
	if mode == "adopted_resume_equal_restored" {
		require.Zero(t, x.creates)
		require.NotEmpty(t, original.uploadID)
	}
	if strings.HasPrefix(mode, "ordinary_transformed_") {
		require.Equal(t, "B", original.request.Name)
		require.Equal(t, "b", identity.requestedTarget)
		require.Equal(t, "B", selected.effectiveTarget)
	}
	x.seed(target, "old-target", []byte("DATA"), modTime)
	if strings.HasPrefix(mode, "ordinary_transformed_") {
		x.seed("B", "old-target-B", []byte("DATA"), modTime)
	}
	if strings.HasPrefix(mode, "ordinary_transformed_") {
		primaryCtx := fs.WithUploadAdmissionIntent(ctx, fs.UploadAdmissionIntent{Source: source, Destination: x.f, DestinationRemote: target})
		require.Error(t, operations.CheckUploadAdmission(primaryCtx, x.f, source, target))
		sourceRemote := source.Remote()
		discovery := fs.WithUploadAdmissionDiscovery(fs.CopyConfig(context.Background(), primaryCtx), source.Fs(), &sourceRemote)
		before := x.metadata
		for _, name := range []string{"b", "B"} {
			_, err := x.fresh(discovery, "").NewObject(discovery, name)
			require.Error(t, err)
			require.True(t, fs.IsUploadAdmissionError(err))
		}
		require.Equal(t, before, x.metadata)
	}
	if mode == "directory_comparison_selected_siblings" {
		require.Equal(t, "SOURCE.TXT", selected.effectiveTarget)
		x.seed("SOURCE.TXT", "old-transformed-target", []byte("DATA"), modTime)
	}
	if (compareComparison || copyComparison) && !firstPresent {
		addComparison("DATA")
	}
	if mode == "missing_proof" {
		x.missing = true
	}
	if mode == "unavailable_proof" {
		x.unavailable = true
	}
	if mode == "completed_unverified" {
		x.terminal = false
		for id, session := range x.sessions {
			session.State, session.FileId = "completed", "old-target"
			x.sessions[id] = session
		}
	}
	if mode == "fingerprint_mismatch" {
		require.NoError(t, os.WriteFile(sourcePath, []byte("other"), 0600))
		sourceSHA = sha256.Sum256([]byte("other"))
	}
	beforeCreates, beforeCopies, beforePuts, beforeMeta := x.creates, x.copies, x.puts, x.metadata
	if mode == "source_role_read_control" {
		o, err := x.f.NewObject(ctx, target)
		require.NoError(t, err)
		in, err := o.Open(ctx)
		require.NoError(t, err)
		got, err := io.ReadAll(in)
		require.NoError(t, err)
		require.NoError(t, in.Close())
		require.Equal(t, sha256.Sum256([]byte("DATA")), sha256.Sum256(got))
		_, err = x.f.List(ctx, "")
		require.NoError(t, err)
		require.NoError(t, x.f.ListR(ctx, "", func(fs.DirEntries) error { return nil }))
		assertSource()
		return
	}
	if mode == "unrelated_subtree_control" {
		exact := "unrelated-source.txt"
		discovery := fs.WithUploadAdmissionDiscovery(ctx, sourceFs, &exact)
		_, err := x.f.NewObject(discovery, target)
		require.NoError(t, err)
		x.f.dirCache.Put("unrelated", "unrelated")
		require.NoError(t, x.f.ListP(fs.WithUploadAdmissionDiscovery(ctx, sourceFs, nil), "unrelated", func(fs.DirEntries) error { return nil }))
		for _, transformed := range []bool{false, true} {
			selectedCtx, fi := filter.AddConfig(ctx)
			require.NoError(t, fi.Add(false, "/"+sourceName))
			if transformed {
				require.NoError(t, transform.SetOptions(selectedCtx, "all,uppercase"))
			}
			require.NoError(t, x.f.ListP(fs.WithUploadAdmissionDiscovery(selectedCtx, sourceFs, nil), "", func(fs.DirEntries) error { return nil }))
		}
		b2FinishedSourceDiscoveryControl(t, ctx, x, sourceFs)
		assertSource()
		return
	}
	if mode == "copied_discovery_context" {
		exact := source.Remote()
		discovery := fs.WithUploadAdmissionIntent(ctx, fs.UploadAdmissionIntent{Source: source, Destination: x.f, DestinationRemote: target})
		discovery = fs.WithUploadAdmissionDiscovery(discovery, sourceFs, &exact)
		transient, release := fs.WithUploadAdmissionTransientScope(discovery)
		defer release()
		copied := fs.CopyConfig(context.Background(), fs.CopyConfig(context.Background(), transient))
		tr := accounting.Stats(copied).NewTransfer(source, x.f)
		account := tr.Account(copied, nil)
		copied, _ = account.NewServerSideCopyAccounter(copied)
		defer func() { _ = account.Close(); tr.Done(copied, nil) }()
		sourceInfo, gotRemote, marked := fs.UploadAdmissionDiscoveryFromContext(copied)
		require.True(t, marked)
		require.Same(t, sourceFs, sourceInfo)
		require.Equal(t, exact, *gotRemote)
		gotIntent, present := fs.UploadAdmissionIntentFromContext(copied)
		require.True(t, present)
		require.Same(t, source, gotIntent.Source)
		require.Same(t, x.f, gotIntent.Destination)
		id, lifetime, present := fs.UploadAdmissionTransientScopeFromContext(copied)
		require.True(t, present)
		require.NotZero(t, id)
		// The command outranks the transient child.
		gotScope, err := admissionScopeForContext(copied)
		require.NoError(t, err)
		require.Same(t, scope, gotScope)
		_, err = x.f.NewObject(copied, target)
		require.Error(t, err)
		_, _, err = jobs.NewJob(ctx, func(jobCtx context.Context, _ rc.Params) (rc.Params, error) {
			jobScope, err := admissionScopeForContext(jobCtx)
			require.NoError(t, err)
			child, cancelChild := fs.WithUploadAdmissionTransientScope(jobCtx)
			defer cancelChild()
			child = fs.CopyConfig(context.Background(), fs.CopyConfig(context.Background(), child))
			sameJob, err := admissionScopeForContext(child)
			require.NoError(t, err)
			require.Same(t, jobScope, sameJob)
			require.NotSame(t, scope, jobScope)
			store := &b2AdmissionJournal{state: fs.UploadAdmissionState{Generation: uuid.NewString()}}
			durable := fs.WithUploadAdmissionStore(child, store)
			durable = fs.WithUploadAdmissionIntent(durable, fs.UploadAdmissionIntent{Source: source, Destination: x.f, DestinationRemote: target})
			entry, err := x.f.selectUploadAdmission(durable, "upload", source, target, target, [32]byte{})
			require.NoError(t, err)
			require.Same(t, store, entry.store)
			require.Empty(t, jobScope.entries)
			return nil, nil
		}, rc.Params{"_group": "context-precedence"})
		require.NoError(t, err)
		release()
		require.ErrorIs(t, lifetime.Err(), context.Canceled)
		assertSource()
		return
	}
	if mode == "copy_dest_original_transport_identity" {
		x.mu.Lock()
		changed := x.files["comparison/"+sourceName]
		changed.Id = "changed-comparison"
		x.files["comparison/"+sourceName] = changed
		x.data[changed.Id] = []byte("DATA")
		x.mu.Unlock()
		transport, err := comparison.NewObject(ctx, sourceName)
		require.NoError(t, err)
		intentCtx := fs.WithUploadAdmissionIntent(ctx, fs.UploadAdmissionIntent{Source: source, Destination: x.f, DestinationRemote: target})
		_, err = x.f.Copy(intentCtx, transport, target)
		require.ErrorIs(t, err, errAdmissionConflict)
	}
	destination := x.f
	if strings.Contains(mode, "fresh_fs") {
		destination = x.fresh(ctx, "")
	}
	retryCtx := filter.CopyConfig(fs.CopyConfig(context.Background(), ctx), ctx)
	err = run(retryCtx, destination)
	require.Error(t, err)
	require.True(t, fs.IsUploadAdmissionError(err))
	require.Equal(t, original.key, selected.snapshot().key)
	require.Equal(t, original.uploadID, selected.snapshot().uploadID)
	require.Equal(t, beforeCreates, x.creates)
	require.Equal(t, beforeCopies, x.copies)
	require.Equal(t, beforePuts, x.puts)
	if ci.NoCheckDest {
		require.Equal(t, beforeMeta, x.metadata)
	}
	require.Zero(t, x.publications)
	require.Zero(t, x.deletes)
	if directory && mode == "directory_comparison_selected_siblings" {
		excluded, err := os.ReadFile(filepath.Join(root, "excluded.txt"))
		require.NoError(t, err)
		require.Equal(t, sha256.Sum256([]byte("keep")), sha256.Sum256(excluded))
	}
	require.Zero(t, x.folders)
	require.Equal(t, sha256.Sum256([]byte("DATA")), sha256.Sum256(x.data["old-target"]))
	if copyComparison {
		want := "DATA"
		if firstPresent {
			want = "data"
		}
		require.Equal(t, sha256.Sum256([]byte(want)), sha256.Sum256(x.data["comparison-source"]))
	}
	assertSource()
}

type b2RawDiscoveryFs struct {
	*Fs
	calls   int
	scopeID uint64
	scope   *admissionScope
}

func (f *b2RawDiscoveryFs) Put(ctx context.Context, in io.Reader, source fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	f.calls++
	id, _, ok := fs.UploadAdmissionTransientScopeFromContext(ctx)
	if !ok {
		return nil, errors.New("rewindable Rcat has no retry owner")
	}
	if _, ok := fs.UploadAdmissionIntentFromContext(ctx); ok {
		return nil, errors.New("raw Rcat invented original source intent")
	}
	if _, ok := fs.UploadAdmissionStoreFromContext(ctx); ok {
		return nil, errors.New("raw Rcat invented durable source evidence")
	}
	if fs.GetConfig(ctx).CommandRunID != 0 {
		return nil, errors.New("raw Rcat requires no command identity")
	}
	if cache.JobGetJobID != nil {
		if _, ok := cache.JobGetJobID(fs.UploadAdmissionJobContext(ctx)); ok {
			return nil, errors.New("raw Rcat requires no job identity")
		}
	}
	if f.scopeID != 0 && f.scopeID != id {
		return nil, errors.New("Rcat changed retry owner")
	}
	f.scopeID = id
	scope, err := admissionScopeForContext(ctx)
	if err != nil {
		return nil, err
	}
	f.scope = scope
	return f.Fs.Put(ctx, in, source, options...)
}

func b2RawDiscoveryCase(t *testing.T, mode string) {
	parent, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cancelCtx, cancelCause := context.WithCancelCause(parent)
	defer cancelCause(nil)
	cancellation := errors.New("owned raw Rcat canceled")
	ctx, ci := fs.AddConfig(cancelCtx)
	ci.CommandRunID, ci.LowLevelRetries, ci.MultiThreadStreams, ci.StreamingUploadCutoff = 0, 3, 0, 64
	x := newB2DiscoveryOracle(t, ctx)
	x.f.pacer.SetRetries(1)
	x.truncate = true
	f := &b2RawDiscoveryFs{Fs: x.f}
	data := bytes.Repeat([]byte("owned Rcat bytes\n"), 16)
	cutoffSmall := mode != "raw_rcat_spooled_terminal_retry" && mode != "raw_rcat_stream_oneshot_control"
	if cutoffSmall {
		ci.StreamingUploadCutoff = fs.SizeSuffix(len(data) + 1)
	} else {
		ci.StreamingUploadCutoff = 1
	}
	if mode == "raw_rcat_spooled_terminal_retry" {
		f.features.PutStream = nil
	}
	if mode == "raw_rcat_transient_control" {
		x.complete, x.transient = true, true
	}
	if mode == "raw_rcat_independent_calls" {
		x.complete = true
	}
	if mode == "raw_rcat_cancellation" {
		x.cancel = func() { cancelCause(cancellation) }
	}
	streamCalls := 0
	if mode == "raw_rcat_stream_oneshot_control" {
		f.features.PutStream = func(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
			streamCalls++
			_, _, present := fs.UploadAdmissionTransientScopeFromContext(ctx)
			require.False(t, present)
			return x.f.PutStream(ctx, in, src, options...)
		}
	}
	modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	run := func() (fs.Object, error) {
		return operations.RcatSize(ctx, f, "raw.txt", io.NopCloser(bytes.NewReader(data)), -1, modTime, nil)
	}
	o, err := run()
	if mode == "raw_rcat_transient_control" || mode == "raw_rcat_independent_calls" {
		require.NoError(t, err)
		in, err := o.Open(ctx)
		require.NoError(t, err)
		got, err := io.ReadAll(in)
		require.NoError(t, err)
		require.NoError(t, in.Close())
		require.Equal(t, sha256.Sum256(data), sha256.Sum256(got))
		if mode == "raw_rcat_transient_control" {
			require.GreaterOrEqual(t, f.calls, 2, "must cross the actual inner Retry boundary")
			require.Len(t, x.keys, 1)
		} else {
			firstID, firstKeys := f.scopeID, len(x.keys)
			firstScope := f.scope
			require.Eventually(t, func() bool {
				commandAdmissions.Lock()
				defer commandAdmissions.Unlock()
				_, retained := commandAdmissions.scopes[admissionScopeKey{"transient", firstID}]
				return !retained && firstScope.closed && len(firstScope.destinations) == 0
			}, time.Second, time.Millisecond)
			f.scopeID = 0
			_, err := run()
			require.NoError(t, err)
			require.NotEqual(t, firstID, f.scopeID)
			require.Equal(t, firstKeys+1, len(x.keys))
		}
	} else {
		require.Error(t, err)
		require.Zero(t, x.publications)
		require.Zero(t, x.deletes)
		require.Zero(t, x.folders)
		require.Len(t, x.sessions, 1)
		require.Len(t, x.keys, 1)
		if mode == "raw_rcat_cancellation" {
			require.ErrorIs(t, err, cancellation)
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			require.ErrorIs(t, context.Cause(ctx), cancellation)
			require.Equal(t, 1, f.calls)
		} else if mode == "raw_rcat_stream_oneshot_control" {
			require.Equal(t, 1, streamCalls)
			require.Zero(t, f.calls)
			return
		} else {
			require.GreaterOrEqual(t, f.calls, 2, "must cross the actual inner Retry boundary")
			require.True(t, x.restored)
			require.Equal(t, 1, x.creates)
			require.Equal(t, 1, x.puts)
		}
	}
	require.Eventually(t, func() bool {
		commandAdmissions.Lock()
		defer commandAdmissions.Unlock()
		_, retained := commandAdmissions.scopes[admissionScopeKey{"transient", f.scopeID}]
		return !retained && f.scope != nil && f.scope.closed && len(f.scope.entries) == 0 && len(f.scope.destinations) == 0
	}, time.Second, time.Millisecond)
}

func b2UnscopedCopyFailure(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.CommandRunID, ci.LowLevelRetries, ci.MultiThreadStreams = 0, 2, 0
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	x := newB2DiscoveryOracle(t, ctx)
	x.f.pacer.SetRetries(1)
	root := t.TempDir()
	file := filepath.Join(root, "original.txt")
	require.NoError(t, os.WriteFile(file, []byte("data"), 0600))
	sourceFs, err := local.NewFs(ctx, "local", root, configmap.Simple{})
	require.NoError(t, err)
	source, err := sourceFs.NewObject(ctx, "original.txt")
	require.NoError(t, err)
	x.seed("comparison/original.txt", "comparison", []byte("data"), source.ModTime(ctx))
	comparison, err := x.fresh(ctx, "comparison").NewObject(ctx, "original.txt")
	require.NoError(t, err)
	ctx = fs.WithUploadAdmissionIntent(ctx, fs.UploadAdmissionIntent{Source: source, Destination: x.f, DestinationRemote: "b"})
	x.f.features.Copy = func(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
		result, err := x.f.Copy(ctx, src, remote)
		if err != nil {
			return result, errors.Join(fs.ErrorCantCopy, err)
		}
		return result, nil
	}
	_, err = operations.Copy(ctx, x.f, nil, "b", comparison)
	require.Error(t, err)
	require.True(t, fs.IsUploadAdmissionError(err))
	require.ErrorIs(t, err, fs.ErrorCantCopy)
	require.True(t, fserrors.IsFatalError(err))
	require.Equal(t, 1, x.copies)
	require.Zero(t, x.creates)
	require.Zero(t, x.puts)
	require.Zero(t, x.publications)
	require.Zero(t, x.deletes)
	require.Len(t, x.observedScopes, 1)
	for key, scope := range x.observedScopes {
		require.Equal(t, "transient", key.kind)
		require.Eventually(t, func() bool {
			commandAdmissions.Lock()
			defer commandAdmissions.Unlock()
			return scope.closed && len(scope.entries) == 0 && len(scope.destinations) == 0
		}, time.Second, time.Millisecond)
	}
	body, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256([]byte("data")), sha256.Sum256(body))
}

func b2UnscopedUploadFailure(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.CommandRunID, ci.LowLevelRetries, ci.MultiThreadStreams = 0, 3, 0
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	x := newB2DiscoveryOracle(t, ctx)
	x.truncate = true
	x.f.pacer.SetRetries(1)
	root := t.TempDir()
	file := filepath.Join(root, "source.txt")
	require.NoError(t, os.WriteFile(file, []byte("data"), 0600))
	sourceFs, err := local.NewFs(ctx, "local", root, configmap.Simple{})
	require.NoError(t, err)
	source, err := sourceFs.NewObject(ctx, "source.txt")
	require.NoError(t, err)
	_, err = operations.Copy(ctx, x.f, nil, "b", source)
	require.Error(t, err)
	require.True(t, fs.IsUploadAdmissionError(err))
	require.Equal(t, 1, x.creates)
	require.Equal(t, 1, x.puts)
	require.Zero(t, x.publications)
	require.Zero(t, x.deletes)
	require.Len(t, x.observedScopes, 1)
	for key, scope := range x.observedScopes {
		require.Equal(t, "transient", key.kind)
		require.Eventually(t, func() bool {
			commandAdmissions.Lock()
			defer commandAdmissions.Unlock()
			return scope.closed && len(scope.entries) == 0 && len(scope.destinations) == 0
		}, time.Second, time.Millisecond)
	}
	body, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256([]byte("data")), sha256.Sum256(body))
}

func b2UnscopedStreamControl(t *testing.T, stream bool) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.CommandRunID, ci.LowLevelRetries, ci.MultiThreadStreams, ci.StreamingUploadCutoff = 0, 2, 0, 1
	ctx = accounting.WithStatsGroup(ctx, t.Name())
	x := newB2DiscoveryOracle(t, ctx)
	x.complete = true
	x.f.pacer.SetRetries(1)
	if !stream {
		x.f.features.PutStream = nil
	}
	modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	source := b2UnknownDiscoveryObject{object.NewMemoryObject("original", modTime, []byte("data"))}
	for attempt := 1; attempt <= 2; attempt++ {
		result, err := operations.Copy(ctx, x.f, nil, "b", source)
		require.NoError(t, err)
		in, err := result.Open(ctx)
		require.NoError(t, err)
		body, err := io.ReadAll(in)
		require.NoError(t, err)
		require.NoError(t, in.Close())
		require.Equal(t, sha256.Sum256([]byte("data")), sha256.Sum256(body))
		require.Len(t, x.keys, attempt)
		require.Len(t, x.observedScopes, attempt)
		for key, scope := range x.observedScopes {
			require.Equal(t, "transient", key.kind)
			require.Eventually(t, func() bool {
				commandAdmissions.Lock()
				defer commandAdmissions.Unlock()
				_, retained := commandAdmissions.scopes[key]
				return !retained && scope.closed && len(scope.entries) == 0 && len(scope.destinations) == 0
			}, time.Second, time.Millisecond)
		}
	}
	require.Equal(t, 2, x.publications)
	require.Zero(t, x.deletes)
}

func b2FinishedSourceDiscoveryControl(t *testing.T, ctx context.Context, x *b2DiscoveryOracle, source fs.Fs) {
	t.Helper()
	localPath := filepath.Join(source.Root(), "finished.txt")
	require.NoError(t, os.WriteFile(localPath, []byte("done"), 0600))
	x.mu.Lock()
	x.complete, x.terminal = true, false
	x.mu.Unlock()
	require.NoError(t, operations.MoveFile(ctx, x.f, source, "finished.txt", "finished.txt"))
	_, err := os.Stat(localPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	x.mu.Lock()
	x.terminal = true
	x.mu.Unlock()
	selectedCtx, fi := filter.AddConfig(ctx)
	require.NoError(t, fi.Add(false, "/source.txt"))
	callbacks := 0
	require.NoError(t, x.f.ListP(fs.WithUploadAdmissionDiscovery(selectedCtx, source, nil), "", func(fs.DirEntries) error {
		callbacks++
		return nil
	}))
	require.NotZero(t, callbacks)
}
