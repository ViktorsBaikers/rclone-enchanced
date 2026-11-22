package teldrive

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/fs/rc/jobs"
	syncops "github.com/rclone/rclone/fs/sync"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/rest"
	"github.com/rclone/rclone/lib/transform"
	"github.com/stretchr/testify/require"
)

// Extend 03-18's byte-retaining HTTP oracle, not its retry or admission policy.
// The barrier models committed parent loss/restore; generated HTTP/SQL proof is separate.
type b2SyncOracle struct {
	*b2DiscoveryOracle
	arrived        chan string
	release        chan struct{}
	barrier        sync.Once
	releaseOnce    sync.Once
	block          bool
	denied         bool
	moves, patches int
	trace          []string
}

func newB2SyncOracle(t *testing.T, ctx context.Context) *b2SyncOracle {
	t.Helper()
	x := &b2SyncOracle{b2DiscoveryOracle: newB2DiscoveryOracle(t, ctx),
		arrived: make(chan string, 1), release: make(chan struct{})}
	original := x.f.srv
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		x.mu.Lock()
		x.trace = append(x.trace, r.Method+" "+r.URL.RequestURI())
		block := x.block && r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/copy")
		denied := x.denied
		x.mu.Unlock()
		if block {
			x.barrier.Do(func() {
				x.arrived <- r.Header.Get("Idempotency-Key")
				select {
				case <-x.release:
				case <-r.Context().Done():
				}
			})
		}
		if denied {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error":{"code":"forbidden","message":"current access denied"}}`)
			return
		}
		if r.Method == http.MethodPatch || (r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/move")) {
			x.mu.Lock()
			defer x.mu.Unlock()
			id := strings.TrimPrefix(strings.TrimSuffix(r.URL.Path, "/move"), "/api/v1/files/")
			var found api.FileInfo
			for _, info := range x.files {
				if info.Id == id {
					found = info
					break
				}
			}
			if found.Id == "" {
				w.WriteHeader(404)
				return
			}
			if r.Method == http.MethodPatch {
				x.patches++
				var update api.UpdateFileInformation
				if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				found.Name = update.Name
			} else {
				x.moves++
				var move api.MoveFileRequest
				if err := json.NewDecoder(r.Body).Decode(&move); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				found.ParentId = move.ParentId
			}
			byID := make(map[string]api.FileInfo, len(x.files))
			for _, info := range x.files {
				byID[info.Id] = info
			}
			byID[id] = found
			var fullPath func(api.FileInfo) string
			fullPath = func(info api.FileInfo) string {
				if parent, ok := byID[info.ParentId]; ok {
					return path.Join(fullPath(parent), info.Name)
				}
				return path.Join(info.ParentId, info.Name)
			}
			x.files = make(map[string]api.FileInfo, len(byID))
			for _, info := range byID {
				x.files[fullPath(info)] = info
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method == http.MethodDelete {
			x.mu.Lock()
			x.deletes++
			id := strings.TrimPrefix(r.URL.Path, "/api/v1/files/")
			for name, info := range x.files {
				if info.Id == id {
					delete(x.files, name)
				}
			}
			x.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		headers := make(map[string]string)
		for name := range r.Header {
			headers[name] = r.Header.Get(name)
		}
		response, err := original.Call(r.Context(), &rest.Opts{Method: r.Method, Path: r.URL.Path,
			Parameters: r.URL.Query(), Body: r.Body, ExtraHeaders: headers, IgnoreStatus: true})
		if err != nil {
			t.Error(err)
			w.WriteHeader(502)
			return
		}
		defer response.Body.Close()
		for name, values := range response.Header {
			w.Header()[name] = values
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { x.releaseOnce.Do(func() { close(x.release) }) })
	x.f.srv = rest.NewClient(server.Client()).SetRoot(server.URL).SetErrorHandler(errorHandler)
	x.f.pacer.SetRetries(1)
	return x
}

func (x *b2SyncOracle) folder(name string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	parent, leaf := path.Split(name)
	x.files[name] = api.FileInfo{Id: name, Name: leaf, ParentId: strings.TrimSuffix(parent, "/"), Kind: "folder"}
}

func (x *b2SyncOracle) freshMove(ctx context.Context, root string) *Fs {
	f := x.fresh(ctx, root)
	f.dirCache = dircache.New(root, rootID, f)
	f.features.DirMove = f.DirMove
	f.features.Move = nil
	return f
}

func b2SyncContext(t *testing.T) context.Context {
	t.Helper()
	ctx := accounting.WithStatsGroup(b2CommandContext(t), t.Name())
	ci := fs.GetConfig(ctx)
	ci.LowLevelRetries, ci.MultiThreadStreams, ci.Checkers, ci.Transfers = 1, 0, 1, 1
	ci.NoUpdateDirModTime = true
	return ctx
}

var b2SyncModTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func b2SyncLocal(t *testing.T, ctx context.Context) (fs.Fs, fs.Object) {
	t.Helper()
	root := t.TempDir()
	file := filepath.Join(root, "source.txt")
	require.NoError(t, os.WriteFile(file, []byte("data"), 0600))
	require.NoError(t, os.Chtimes(file, b2SyncModTime, b2SyncModTime))
	f, err := local.NewFs(ctx, "local", root, configmap.Simple{})
	require.NoError(t, err)
	o, err := f.NewObject(ctx, "source.txt")
	require.NoError(t, err)
	return f, o
}

func b2SyncSHA(t *testing.T, ctx context.Context, o fs.Object, want string) {
	t.Helper()
	in, err := o.Open(ctx)
	require.NoError(t, err)
	body, err := io.ReadAll(in)
	require.NoError(t, err)
	require.NoError(t, in.Close())
	require.Equal(t, int64(len(want)), int64(len(body)))
	require.Equal(t, sha256.Sum256([]byte(want)), sha256.Sum256(body))
}

func (x *b2SyncOracle) bytes(t *testing.T, ctx context.Context, id, want string) {
	t.Helper()
	// Download by independent retained receipt, not the oracle's backing byte map.
	b2SyncSHA(t, ctx, &Object{fs: x.f, id: id, size: int64(len(want))}, want)
}

func (x *b2SyncOracle) selected(t *testing.T, ctx context.Context, source fs.Object, destination *Fs, remote string) *uploadAdmission {
	t.Helper()
	scope, err := admissionScopeForContext(ctx)
	require.NoError(t, err)
	require.NotNil(t, scope)
	identity := admissionIdentity{destination.admissionRemoteIdentity(), admissionSourceIdentity(source.Fs()),
		admissionSourcePath(source), canonicalAdmissionPath(destination.root, remote)}
	commandAdmissions.Lock()
	entry := scope.entries[identity]
	count := len(scope.entries)
	commandAdmissions.Unlock()
	require.NotNil(t, entry, "first selection must use original logical source and primary requested target")
	require.Equal(t, 1, count)
	return entry
}

func (x *b2SyncOracle) loseCopy(t *testing.T, ctx context.Context, source fs.Object, destination *Fs, remote string, run func() error) (*uploadAdmission, error) {
	t.Helper()
	admitted, err := destination.CheckUploadAdmission(ctx, source, remote)
	require.NoError(t, err)
	require.False(t, admitted, "entry must precede the first admission")
	x.block = true
	result := make(chan error, 1)
	go func() { result <- run() }()
	var key string
	select {
	case key = <-x.arrived:
	case err := <-result:
		t.Fatalf("operation returned before real admitted Copy HTTP barrier: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	entry := x.selected(t, ctx, source, destination, remote)
	snapshot := entry.snapshot()
	require.NotNil(t, snapshot.copyRequest)
	require.Equal(t, key, snapshot.key)
	require.Equal(t, "comparison-source", snapshot.copySourceID)
	x.mu.Lock()
	x.terminal, x.restored = true, true
	x.mu.Unlock()
	x.releaseOnce.Do(func() { close(x.release) })
	select {
	case err = <-result:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Error(t, err)
	require.True(t, fs.IsUploadAdmissionError(err), "%v", err)
	require.Len(t, x.copyRequests, 1)
	require.Equal(t, "comparison-source", x.copySources[key])
	require.Zero(t, x.creates)
	require.Zero(t, x.puts)
	require.Zero(t, x.publications)
	require.Zero(t, x.deletes)
	return entry, err
}

func TestB2AlternateCopySyncAdmission(t *testing.T) {
	for _, mode := range []string{
		"pair_checker_first_present_parent_loss", "src_only_first_present_parent_loss",
		"pair_checker_first_present_truncated_fresh_fs", "src_only_first_present_truncated_fresh_fs",
		"selected_siblings_transformed_ordered", "unadmitted_comparison_fallback_control",
		"rc_movefile_first_present_original_intent", "NoCheckDestCompareDestCopyDest",
		"dirmove_terminal_fresh_fs", "dirmove_unresolved_proof", "dirmove_wrong_logical_source",
		"dirmove_current_authorization", "dirmove_no_admission_control", "dirmove_unrelated_subtree_control",
		"dirmove_separate_jobs_same_group", "move_shortcut_retained_admission", "move_shortcut_no_admission_control",
	} {
		t.Run(mode, func(t *testing.T) {
			switch {
			case strings.HasPrefix(mode, "dirmove_"):
				b2SyncDirMoveCase(t, mode)
			case strings.HasPrefix(mode, "move_shortcut_"):
				b2SyncMoveCase(t, mode)
			case mode == "NoCheckDestCompareDestCopyDest":
				b2SyncFlagControls(t)
			case mode == "selected_siblings_transformed_ordered":
				for _, noTraverse := range []bool{false, true} {
					name := "default"
					if noTraverse {
						name = "check_first_ordered_no_traverse"
					}
					t.Run(name, func(t *testing.T) { b2SyncAlternateCase(t, mode, noTraverse) })
				}
			default:
				b2SyncAlternateCase(t, mode, false)
			}
		})
	}
}

func b2SyncAlternateCase(t *testing.T, mode string, ordered bool) {
	ctx := b2SyncContext(t)
	ci := fs.GetConfig(ctx)
	x := newB2SyncOracle(t, ctx)
	sourceFs, source := b2SyncLocal(t, ctx)
	comparisonName := x.f.name + ":comparison"
	x.folder("comparison")
	x.seed("comparison/source.txt", "comparison-source", []byte("data"), b2SyncModTime)
	cache.Put(comparisonName, x.fresh(ctx, "comparison"))
	ci.CopyDest = []string{comparisonName}
	pair := strings.HasPrefix(mode, "pair_checker_")
	oldName := "preserved.txt"
	if pair {
		oldName = "source.txt"
	}
	x.seed(oldName, "old-target", []byte("OLD!"), b2SyncModTime.Add(-time.Hour))
	x.seed("foreign.txt", "foreign", []byte("foreign"), b2SyncModTime)
	if strings.HasPrefix(mode, "src_only_") {
		ci.NoCheckDest = true
	}
	if strings.Contains(mode, "truncated") || mode == "rc_movefile_first_present_original_intent" {
		x.truncate = true
	}
	if mode == "selected_siblings_transformed_ordered" {
		ci.CheckFirst, ci.OrderBy, ci.NoTraverse = ordered, "", ordered
		if ordered {
			ci.OrderBy = "name,ascending"
		}
		require.NoError(t, transform.SetOptions(ctx, "all,uppercase"))
		var fi *filter.Filter
		ctx, fi = filter.AddConfig(ctx)
		require.NoError(t, fi.Add(false, "/excluded.txt"))
		require.NoError(t, os.WriteFile(filepath.Join(sourceFs.Root(), "excluded.txt"), []byte("keep"), 0600))
	}
	run := func(operationCtx context.Context, destination *Fs) error {
		return syncops.MoveDir(operationCtx, destination, sourceFs, false, false)
	}
	if mode == "unadmitted_comparison_fallback_control" {
		x.complete = true
		x.f.features.Copy = func(context.Context, fs.Object, string) (fs.Object, error) {
			return nil, errors.New("ordinary unadmitted comparison error")
		}
		require.NoError(t, run(ctx, x.f))
		require.Zero(t, x.copies)
		require.Equal(t, 1, x.creates)
		require.Equal(t, 1, x.publications)
		_, err := os.Stat(filepath.Join(sourceFs.Root(), source.Remote()))
		require.ErrorIs(t, err, os.ErrNotExist)
		o, err := x.f.NewObject(ctx, source.Remote())
		require.NoError(t, err)
		b2SyncSHA(t, ctx, o, "data")
		x.bytes(t, ctx, "comparison-source", "data")
		x.bytes(t, ctx, "old-target", "OLD!")
		return
	}
	verify := func(operationCtx context.Context, destination *Fs, invoke func() error) {
		entry, copyErr := x.loseCopy(t, operationCtx, source, destination, source.Remote(), invoke)
		original := entry.snapshot()
		var apiErr *apiError
		if !x.truncate {
			require.ErrorAs(t, copyErr, &apiErr)
			require.Equal(t, "upload_parent_unavailable", apiErr.Code)
		}
		b2SyncSHA(t, operationCtx, source, "data")
		x.bytes(t, operationCtx, "comparison-source", "data")
		x.bytes(t, operationCtx, "old-target", "OLD!")
		x.bytes(t, operationCtx, "foreign", "foreign")
		x.seed("source.txt", "restored-primary", []byte("DATA"), b2SyncModTime)
		if mode == "selected_siblings_transformed_ordered" {
			require.Equal(t, "SOURCE.TXT", entry.effectiveTarget)
			x.seed("SOURCE.TXT", "restored-effective", []byte("DATA"), b2SyncModTime)
		}
		beforeMetadata, beforeCopies := x.metadata, x.copies
		fresh := x.fresh(operationCtx, "")
		var retryErr error
		if mode == "rc_movefile_first_present_original_intent" {
			cache.Put(x.f.name+":", fresh)
			_, retryErr = rc.Calls.Get("operations/movefile").Fn(operationCtx, rc.Params{
				"srcFs": sourceFs.Root(), "srcRemote": source.Remote(), "dstFs": x.f.name + ":", "dstRemote": source.Remote()})
		} else {
			retryErr = run(operationCtx, fresh)
		}
		require.Error(t, retryErr)
		require.True(t, fs.IsUploadAdmissionError(retryErr))
		require.Equal(t, original.key, entry.snapshot().key)
		require.Equal(t, original.copySourceID, entry.snapshot().copySourceID)
		require.Equal(t, beforeCopies, x.copies)
		if ci.NoCheckDest {
			require.Equal(t, beforeMetadata, x.metadata)
		}
		require.Len(t, x.copyRequests, 1)
		require.Zero(t, x.creates)
		require.Zero(t, x.puts)
		require.Zero(t, x.publications)
		require.Zero(t, x.deletes)
		b2SyncSHA(t, operationCtx, source, "data")
		x.bytes(t, operationCtx, "restored-primary", "DATA")
		if mode == "selected_siblings_transformed_ordered" {
			excluded, err := os.ReadFile(filepath.Join(sourceFs.Root(), "excluded.txt"))
			require.NoError(t, err)
			require.Equal(t, sha256.Sum256([]byte("keep")), sha256.Sum256(excluded))
			// Same directory, same command, unchanged filters: select an unaffected sibling alone.
			selectedCtx, fi := filter.AddConfig(operationCtx)
			require.NoError(t, fi.Add(false, "/source.txt"))
			require.NoError(t, fi.Add(false, "/excluded.txt"))
			require.NoError(t, os.WriteFile(filepath.Join(sourceFs.Root(), "sibling.txt"), []byte("sibling"), 0600))
			x.complete, x.terminal = true, false
			require.NoError(t, run(selectedCtx, x.fresh(selectedCtx, "")))
			o, err := x.f.NewObject(selectedCtx, "SIBLING.TXT")
			require.NoError(t, err)
			b2SyncSHA(t, selectedCtx, o, "sibling")
			b2SyncSHA(t, selectedCtx, source, "data")
		}
	}
	if mode == "rc_movefile_first_present_original_intent" {
		cache.Put(sourceFs.Root(), sourceFs)
		cache.Put(x.f.name+":", x.f)
		call := rc.Calls.Get("operations/movefile")
		require.NotNil(t, call)
		_, _, err := jobs.NewJob(ctx, func(jobCtx context.Context, _ rc.Params) (rc.Params, error) {
			verify(jobCtx, x.f, func() error {
				_, err := call.Fn(jobCtx, rc.Params{"srcFs": sourceFs.Root(), "srcRemote": source.Remote(),
					"dstFs": x.f.name + ":", "dstRemote": source.Remote()})
				return err
			})
			return nil, nil
		}, rc.Params{"_group": "same-sync-group"})
		require.NoError(t, err)
		return
	}
	verify(ctx, x.f, func() error { return run(ctx, x.f) })
}

func b2SyncFlagControls(t *testing.T) {
	for _, mode := range []string{"no_check_dest", "no_check_dest_compare_dest", "no_check_dest_copy_dest"} {
		t.Run(mode, func(t *testing.T) {
			for _, present := range []bool{false, true} {
				name := "comparison_absent"
				if present {
					name = "comparison_present"
				}
				t.Run(name, func(t *testing.T) {
					ctx := b2SyncContext(t)
					ci := fs.GetConfig(ctx)
					ci.NoCheckDest = true
					x := newB2SyncOracle(t, ctx)
					x.complete = true
					sourceFs, source := b2SyncLocal(t, ctx)
					x.folder("comparison")
					if present {
						x.seed("comparison/source.txt", "comparison-source", []byte("data"), b2SyncModTime)
					}
					comparisonName := x.f.name + ":comparison"
					cache.Put(comparisonName, x.fresh(ctx, "comparison"))
					switch mode {
					case "no_check_dest_compare_dest":
						ci.CompareDest = []string{comparisonName}
					case "no_check_dest_copy_dest":
						ci.CopyDest = []string{comparisonName}
					}
					require.NoError(t, syncops.MoveDir(ctx, x.f, sourceFs, false, false))
					if present && mode == "no_check_dest_compare_dest" {
						// SrcOnly's compare-match path skips without authorizing source deletion.
						b2SyncSHA(t, ctx, source, "data")
						require.Zero(t, x.publications)
						require.Zero(t, x.creates)
						require.Zero(t, x.copies)
					} else {
						// A successful alternate copy also keeps SrcOnly's existing source-retention route.
						if present && mode == "no_check_dest_copy_dest" {
							b2SyncSHA(t, ctx, source, "data")
							require.Equal(t, 1, x.copies)
							require.Zero(t, x.creates)
						} else {
							_, err := os.Stat(filepath.Join(sourceFs.Root(), source.Remote()))
							require.ErrorIs(t, err, os.ErrNotExist)
							require.Equal(t, 1, x.creates)
						}
						o, err := x.f.NewObject(ctx, source.Remote())
						require.NoError(t, err)
						b2SyncSHA(t, ctx, o, "data")
					}
				})
			}
		})
	}
}

type b2SyncManifestEntry struct {
	Size   int64
	SHA256 [32]byte
}

func b2SyncManifest(t *testing.T, ctx context.Context, f fs.Fs) map[string]b2SyncManifestEntry {
	t.Helper()
	result := make(map[string]b2SyncManifestEntry)
	var walk func(string)
	walk = func(dir string) {
		entries, err := f.List(ctx, dir)
		require.NoError(t, err)
		for _, entry := range entries {
			if o, ok := entry.(fs.Object); ok {
				in, err := o.Open(ctx)
				require.NoError(t, err)
				body, err := io.ReadAll(in)
				require.NoError(t, err)
				require.NoError(t, in.Close())
				result[o.Remote()] = b2SyncManifestEntry{int64(len(body)), sha256.Sum256(body)}
			} else {
				walk(entry.Remote())
			}
		}
	}
	walk("")
	return result
}

func b2SyncDirectorySetup(t *testing.T, ctx context.Context) (*b2SyncOracle, *Fs, *Fs, fs.Object) {
	t.Helper()
	x := newB2SyncOracle(t, ctx)
	x.folder("src")
	x.folder("dst")
	x.folder("comparison")
	x.seed("src/source.txt", "original-source", []byte("data"), b2SyncModTime)
	x.seed("dst/source.txt", "old-target", []byte("OLD!"), b2SyncModTime.Add(-time.Hour))
	x.seed("comparison/source.txt", "comparison-source", []byte("data"), b2SyncModTime)
	x.seed("foreign.txt", "foreign", []byte("foreign"), b2SyncModTime)
	comparisonName := x.f.name + ":comparison"
	cache.Put(comparisonName, x.fresh(ctx, "comparison"))
	fs.GetConfig(ctx).CopyDest = []string{comparisonName}
	sourceFs, destination := x.freshMove(ctx, "src"), x.freshMove(ctx, "dst")
	source, err := sourceFs.NewObject(ctx, "source.txt")
	require.NoError(t, err)
	return x, sourceFs, destination, source
}

func (x *b2SyncOracle) removeDestination() {
	x.mu.Lock()
	defer x.mu.Unlock()
	for name := range x.files {
		if name == "dst" || strings.HasPrefix(name, "dst/") {
			delete(x.files, name)
		}
	}
}

func b2SyncDirMoveCase(t *testing.T, mode string) {
	ctx := b2SyncContext(t)
	if mode == "dirmove_no_admission_control" {
		x := newB2SyncOracle(t, ctx)
		x.folder("src")
		x.folder("src/nested")
		x.seed("src/source.txt", "source", []byte("data"), b2SyncModTime)
		x.seed("src/nested/deep.txt", "deep", []byte("deep"), b2SyncModTime)
		source, destination := x.freshMove(ctx, "src"), x.freshMove(ctx, "dst")
		require.True(t, filter.GetConfig(ctx).InActive())
		require.Nil(t, destination.Features().Move)
		require.NotNil(t, destination.Features().DirMove)
		require.NotNil(t, destination.Features().Copy)
		require.NoError(t, syncops.MoveDir(ctx, destination, source, false, false))
		require.NotZero(t, x.patches+x.moves)
		require.Zero(t, x.copies)
		require.Zero(t, x.creates)
		require.Equal(t, map[string]b2SyncManifestEntry{
			"source.txt":      {4, sha256.Sum256([]byte("data"))},
			"nested/deep.txt": {4, sha256.Sum256([]byte("deep"))},
		}, b2SyncManifest(t, ctx, x.freshMove(ctx, "dst")))
		_, err := x.freshMove(ctx, "src").List(ctx, "")
		require.Error(t, err)
		t.Run("explicit_subdirectory_no_admission", func(t *testing.T) {
			x.folder("fresh")
			require.NoError(t, operations.DirMove(ctx, x.freshMove(ctx, ""), "dst/nested", "fresh/deep"))
			require.NotZero(t, x.moves)
			require.Equal(t, map[string]b2SyncManifestEntry{"deep.txt": {4, sha256.Sum256([]byte("deep"))}},
				b2SyncManifest(t, ctx, x.freshMove(ctx, "fresh/deep")))
			require.Equal(t, map[string]b2SyncManifestEntry{"source.txt": {4, sha256.Sum256([]byte("data"))}},
				b2SyncManifest(t, ctx, x.freshMove(ctx, "dst")))
		})
		return
	}
	x, sourceFs, destination, source := b2SyncDirectorySetup(t, ctx)
	x.truncate = mode == "dirmove_unresolved_proof"
	expected := map[string]b2SyncManifestEntry{"source.txt": {4, sha256.Sum256([]byte("data"))}}
	require.Equal(t, expected, b2SyncManifest(t, ctx, sourceFs))
	exercise := func(operationCtx context.Context, invoke func() error) {
		require.True(t, filter.GetConfig(operationCtx).InActive(), "actual sync.MoveDir must reach its shortcut")
		require.Nil(t, destination.Features().Move)
		require.NotNil(t, destination.Features().Copy)
		actualDirMove := destination.Features().DirMove
		var firstDirMove error
		destination.features.DirMove = func(ctx context.Context, f fs.Fs, src, dst string) error {
			firstDirMove = actualDirMove(ctx, f, src, dst)
			return firstDirMove
		}
		entry, _ := x.loseCopy(t, operationCtx, source, destination, "source.txt", invoke)
		require.ErrorIs(t, firstDirMove, fs.ErrorDirExists, "existing target must force actual DirMove then real March/Copy fallback")
		original := entry.snapshot()
		x.removeDestination()
		if mode == "dirmove_current_authorization" {
			x.denied = true
		}
		if mode == "dirmove_wrong_logical_source" {
			x.folder("wrong")
			x.seed("wrong/source.txt", "wrong-source", []byte("data"), b2SyncModTime)
		}
		retrySource := x.freshMove(operationCtx, "src")
		if mode == "dirmove_wrong_logical_source" {
			retrySource = x.freshMove(operationCtx, "wrong")
		}
		retryDestination := x.freshMove(operationCtx, "dst")
		require.False(t, retrySource.dirCache.FoundRoot())
		require.False(t, retryDestination.dirCache.FoundRoot())
		beforeTrace, beforeCopies := len(x.trace), x.copies
		err := syncops.MoveDir(operationCtx, retryDestination, retrySource, false, false)
		require.Error(t, err)
		require.True(t, fs.IsUploadAdmissionError(err))
		require.NotErrorIs(t, err, fs.ErrorCantDirMove)
		require.NotErrorIs(t, err, fs.ErrorDirExists)
		require.Equal(t, beforeTrace, len(x.trace), "retained copy has no read-only outcome route; refuse before path/cache/RPC")
		require.False(t, retrySource.dirCache.FoundRoot())
		require.False(t, retryDestination.dirCache.FoundRoot())
		require.Equal(t, beforeCopies, x.copies)
		require.Equal(t, original.key, entry.snapshot().key)
		require.Zero(t, x.moves)
		require.Zero(t, x.patches)
		require.Zero(t, x.folders)
		require.Zero(t, x.deletes)
		require.Zero(t, x.creates)
		require.Zero(t, x.puts)
		require.Zero(t, x.publications)
		require.Empty(t, x.sessions)
		require.Len(t, x.copyRequests, 1)
		require.Same(t, entry, x.selected(t, operationCtx, source, destination, "source.txt"))
		// Explicit subdirectory remotes and a fresh drive-root Fs bypass March entirely.
		for _, route := range []string{"operations_dir_move", "direct_backend_fresh_root"} {
			t.Run(route, func(t *testing.T) {
				freshRoot := x.freshMove(operationCtx, "")
				before := len(x.trace)
				var err error
				if route == "operations_dir_move" {
					err = operations.DirMove(operationCtx, freshRoot, "src", "dst")
				} else {
					err = freshRoot.DirMove(operationCtx, x.freshMove(operationCtx, ""), "src", "dst")
				}
				require.Error(t, err)
				require.True(t, fs.IsUploadAdmissionError(err))
				require.Equal(t, before, len(x.trace))
				require.False(t, freshRoot.dirCache.FoundRoot())
			})
		}
		x.denied = false
		require.Equal(t, expected, b2SyncManifest(t, operationCtx, x.freshMove(operationCtx, "src")))
		x.bytes(t, operationCtx, "old-target", "OLD!")
		x.bytes(t, operationCtx, "comparison-source", "data")
		x.bytes(t, operationCtx, "foreign", "foreign")
		if mode == "dirmove_wrong_logical_source" {
			x.bytes(t, operationCtx, "wrong-source", "data")
		}
		if mode == "dirmove_unrelated_subtree_control" {
			x.folder("other-src")
			x.seed("other-src/unrelated.txt", "unrelated", []byte("unrelated"), b2SyncModTime)
			require.NoError(t, syncops.MoveDir(operationCtx, x.freshMove(operationCtx, "other-dst"),
				x.freshMove(operationCtx, "other-src"), false, false))
			require.Equal(t, map[string]b2SyncManifestEntry{"unrelated.txt": {9, sha256.Sum256([]byte("unrelated"))}},
				b2SyncManifest(t, operationCtx, x.freshMove(operationCtx, "other-dst")))
			require.Equal(t, expected, b2SyncManifest(t, operationCtx, x.freshMove(operationCtx, "src")))
		}
	}
	if mode == "dirmove_separate_jobs_same_group" {
		call := rc.Calls.Get("sync/move")
		require.NotNil(t, call)
		params := rc.Params{"srcFs": x.f.name + ":src", "dstFs": x.f.name + ":dst"}
		cache.Put(x.f.name+":src", sourceFs)
		cache.Put(x.f.name+":dst", destination)
		first, _, err := jobs.NewJob(ctx, func(jobCtx context.Context, _ rc.Params) (rc.Params, error) {
			exercise(jobCtx, func() error { _, err := call.Fn(jobCtx, params); return err })
			return nil, nil
		}, rc.Params{"_group": "same-dirmove-group"})
		require.NoError(t, err)
		x.complete, x.terminal = true, false
		cache.Put(x.f.name+":src", x.freshMove(ctx, "src"))
		cache.Put(x.f.name+":dst", x.freshMove(ctx, "dst"))
		second, _, err := jobs.NewJob(ctx, call.Fn, rc.Params{"srcFs": x.f.name + ":src",
			"dstFs": x.f.name + ":dst", "_group": "same-dirmove-group"})
		require.NoError(t, err)
		require.NotEqual(t, first.ID, second.ID)
		require.Equal(t, first.Group, second.Group)
		require.NotZero(t, x.patches+x.moves)
		require.Equal(t, expected, b2SyncManifest(t, ctx, x.freshMove(ctx, "dst")))
		return
	}
	exercise(ctx, func() error { return syncops.MoveDir(ctx, destination, sourceFs, false, false) })
	if mode == "dirmove_current_authorization" {
		t.Run("selected_upload_current_denial", func(t *testing.T) {
			authCtx := b2SyncContext(t)
			authOracle, authSourceFs, authDestination, authSource := b2SyncDirectorySetup(t, authCtx)
			// Force real Copy's manual branch so the movement guard must GET a selected session.
			authDestination.features.Copy = nil
			_, err := operations.Copy(authCtx, authDestination, nil, "source.txt", authSource)
			require.Error(t, err)
			require.True(t, fs.IsUploadAdmissionError(err))
			selection := authOracle.selected(t, authCtx, authSource, authDestination, "source.txt").snapshot()
			require.NotEmpty(t, selection.uploadID)
			require.Len(t, authOracle.sessions, 1)
			authOracle.mu.Lock()
			authOracle.denied = true
			authOracle.mu.Unlock()
			freshSource := authOracle.freshMove(authCtx, authSourceFs.Root())
			freshDestination := authOracle.freshMove(authCtx, authDestination.Root())
			beforeMetadata, beforeTrace := authOracle.metadata, len(authOracle.trace)
			err = syncops.MoveDir(authCtx, freshDestination, freshSource, false, false)
			require.ErrorIs(t, err, fs.ErrorPermissionDenied)
			require.True(t, fs.IsUploadAdmissionError(err))
			require.Equal(t, beforeMetadata, authOracle.metadata)
			require.Equal(t, beforeTrace+1, len(authOracle.trace))
			require.Contains(t, authOracle.trace[len(authOracle.trace)-1], "GET /api/v1/uploads/"+selection.uploadID)
			require.False(t, freshSource.dirCache.FoundRoot())
			require.False(t, freshDestination.dirCache.FoundRoot())
			require.Zero(t, authOracle.moves)
			require.Zero(t, authOracle.patches)
			require.Zero(t, authOracle.deletes)
			require.Zero(t, authOracle.publications)
			authOracle.mu.Lock()
			authOracle.denied = false
			authOracle.mu.Unlock()
			require.Equal(t, expected, b2SyncManifest(t, authCtx, authOracle.freshMove(authCtx, "src")))
			authOracle.bytes(t, authCtx, "old-target", "OLD!")
		})
	}
}

func b2SyncMoveCase(t *testing.T, mode string) {
	for _, route := range []string{"move", "move_transfer", "backend_move", "backup", "case_change", "transformed"} {
		t.Run(route, func(t *testing.T) {
			if mode == "move_shortcut_no_admission_control" {
				b2SyncMoveControl(t, route)
				return
			}
			for _, truncate := range []bool{false, true} {
				name := "terminal"
				if truncate {
					name = "unresolved"
				}
				t.Run(name, func(t *testing.T) {
					ctx := b2SyncContext(t)
					x, sourceFs, destination, source := b2SyncDirectorySetup(t, ctx)
					x.truncate = truncate
					entry, _ := x.loseCopy(t, ctx, source, destination, "source.txt", func() error {
						return syncops.MoveDir(ctx, destination, sourceFs, false, false)
					})
					original := entry.snapshot()
					freshSourceFs, freshDestination := x.freshMove(ctx, "src"), x.freshMove(ctx, "dst")
					freshDestination.features.Move = freshDestination.Move
					freshSource := *source.(*Object)
					freshSource.fs = freshSourceFs
					old := x.files["dst/source.txt"]
					dst, err := freshDestination.newObjectWithInfo(ctx, "source.txt", &old)
					require.NoError(t, err)
					ownedCtx := fs.WithUploadAdmissionIntent(ctx, fs.UploadAdmissionIntent{
						Source: &freshSource, Destination: freshDestination, DestinationRemote: "source.txt"})
					if route == "transformed" {
						require.NoError(t, transform.SetOptions(ownedCtx, "all,uppercase"))
					}
					backup := x.freshMove(ctx, "backup")
					backup.features.Move = backup.Move
					x.folder("backup")
					fs.GetConfig(ctx).Suffix = ".bak"
					before := len(x.trace)
					switch route {
					case "move", "transformed":
						_, err = operations.Move(ownedCtx, freshDestination, dst, "source.txt", &freshSource)
					case "move_transfer":
						_, err = operations.MoveTransfer(ownedCtx, freshDestination, dst, "source.txt", &freshSource)
					case "backend_move":
						_, err = freshDestination.Move(ownedCtx, &freshSource, "source.txt")
					case "backup":
						err = operations.MoveBackupDir(ownedCtx, backup, dst)
					case "case_change":
						_, err = operations.MoveCaseInsensitive(ownedCtx, freshDestination, freshSourceFs,
							"SOURCE.TXT", "source.txt", false, &freshSource)
					}
					require.Error(t, err)
					require.True(t, fs.IsUploadAdmissionError(err))
					require.Equal(t, before, len(x.trace), "guard must precede destination deletion, path creation and temporary/backup lookup")
					require.False(t, freshSourceFs.dirCache.FoundRoot())
					require.False(t, freshDestination.dirCache.FoundRoot())
					require.False(t, backup.dirCache.FoundRoot())
					require.Equal(t, original.key, entry.snapshot().key)
					require.Zero(t, x.deletes)
					require.Zero(t, x.moves)
					require.Zero(t, x.patches)
					require.Zero(t, x.creates)
					require.Zero(t, x.publications)
					require.Zero(t, x.puts)
					require.Len(t, x.copyRequests, 1)
					require.Same(t, entry, x.selected(t, ctx, source, destination, "source.txt"))
					require.Equal(t, map[string]b2SyncManifestEntry{"source.txt": {4, sha256.Sum256([]byte("data"))}},
						b2SyncManifest(t, ctx, x.freshMove(ctx, "src")))
					x.bytes(t, ctx, "old-target", "OLD!")
					x.bytes(t, ctx, "comparison-source", "data")
				})
			}
		})
	}
}

func b2SyncMoveControl(t *testing.T, route string) {
	ctx := b2SyncContext(t)
	x := newB2SyncOracle(t, ctx)
	x.folder("src")
	x.folder("dst")
	x.folder("backup")
	x.seed("src/source.txt", "source", []byte("data"), b2SyncModTime)
	x.seed("dst/source.txt", "old-target", []byte("OLD!"), b2SyncModTime)
	sourceFs, destination := x.freshMove(ctx, "src"), x.freshMove(ctx, "dst")
	destination.features.Move = destination.Move
	source, err := sourceFs.NewObject(ctx, "source.txt")
	require.NoError(t, err)
	dst, err := destination.NewObject(ctx, "source.txt")
	require.NoError(t, err)
	target := "source.txt"
	switch route {
	case "move":
		_, err = operations.Move(ctx, destination, dst, target, source)
	case "move_transfer":
		_, err = operations.MoveTransfer(ctx, destination, dst, target, source)
	case "backend_move":
		target = "direct.txt"
		_, err = destination.Move(ctx, source, target)
	case "backup":
		fs.GetConfig(ctx).Suffix = ".bak"
		backup := x.freshMove(ctx, "backup")
		backup.features.Move = backup.Move
		err = operations.MoveBackupDir(ctx, backup, source)
		destination, target = backup, "source.txt.bak"
	case "case_change":
		sourceFs.features.Move = sourceFs.Move
		_, err = operations.MoveCaseInsensitive(ctx, sourceFs, sourceFs, "SOURCE.TXT", "source.txt", false, source)
		destination, target = sourceFs, "SOURCE.TXT"
	case "transformed":
		require.NoError(t, transform.SetOptions(ctx, "all,uppercase"))
		_, err = operations.Move(ctx, destination, nil, target, source)
		target = "SOURCE.TXT"
	}
	require.NoError(t, err)
	require.NotZero(t, x.moves+x.patches)
	require.Zero(t, x.copies)
	require.Zero(t, x.creates)
	require.Zero(t, x.publications)
	o, err := destination.NewObject(ctx, target)
	require.NoError(t, err)
	b2SyncSHA(t, ctx, o, "data")
	_, err = x.freshMove(ctx, "src").NewObject(ctx, "source.txt")
	require.ErrorIs(t, err, fs.ErrorObjectNotFound)
	if route == "move" || route == "move_transfer" {
		require.Equal(t, 1, x.deletes)
	}
	if route == "case_change" {
		require.Equal(t, 2, x.patches)
	}
}
