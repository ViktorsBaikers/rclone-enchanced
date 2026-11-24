package sync

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/operations"
	"github.com/stretchr/testify/require"
)

// This fixture tests the generic consumer seam, not TelDrive or SQL admission.
type b2ConsumerFs struct {
	fs.Fs
	features *fs.Features
	check    func(context.Context, fs.Object, string) (bool, error)
}

func (f *b2ConsumerFs) Features() *fs.Features { return f.features }
func (f *b2ConsumerFs) CheckUploadAdmission(ctx context.Context, source fs.Object, remote string) (bool, error) {
	return f.check(ctx, source, remote)
}

type b2ConsumerObject struct {
	fs.Object
	removes *int
}

func (o *b2ConsumerObject) Remove(ctx context.Context) error {
	(*o.removes)++
	return o.Object.Remove(ctx)
}

func TestB2AdmissionConsumerStops(t *testing.T) {
	for _, consumer := range []string{"pair_checker", "src_only"} {
		t.Run(consumer, func(t *testing.T) {
			for _, mode := range []string{"first_present", "ordered", "entry_equal", "entry_rename", "unmarked_fallback"} {
				t.Run(mode, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					ctx, ci := fs.AddConfig(ctx)
					ctx = accounting.WithStatsGroup(ctx, t.Name())
					ci.LowLevelRetries, ci.MultiThreadStreams = 1, 0
					ci.Checkers, ci.Transfers = 1, 1
					ci.NoUpdateDirModTime = true
					ci.Inplace = true
					ci.FixCase = mode != "unmarked_fallback"
					if mode == "ordered" {
						ci.CheckFirst, ci.OrderBy = true, "name,ascending"
					}
					modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
					makeFs := func(name, remote, body string, stamp time.Time) fs.Fs {
						root := t.TempDir()
						file := filepath.Join(root, remote)
						require.NoError(t, os.WriteFile(file, []byte(body), 0600))
						require.NoError(t, os.Chtimes(file, stamp, stamp))
						f, err := local.NewFs(ctx, name, root, configmap.Simple{})
						require.NoError(t, err)
						return f
					}
					sourceFs := makeFs("b2-source", "source.txt", "data", modTime)
					base := makeFs("b2-consumer", "SOURCE.txt", "old bytes", modTime.Add(-time.Hour))
					comparison := makeFs("b2-consumer", "SOURCE.txt", "data", modTime)
					if consumer == "src_only" {
						comparison = makeFs("b2-consumer", "source.txt", "data", modTime)
					}
					original, err := sourceFs.NewObject(ctx, "source.txt")
					require.NoError(t, err)
					removes := 0
					source := &b2ConsumerObject{Object: original, removes: &removes}
					cause := errors.New("comparison admission lost destination")
					admitted := mode == "entry_equal" || mode == "entry_rename"
					copies, checks := 0, 0
					features := *base.Features()
					destination := &b2ConsumerFs{Fs: base, features: &features}
					destination.features.Move = func(context.Context, fs.Object, string) (fs.Object, error) {
						t.Error("losing consumer reached FixCase or backup move")
						return nil, cause
					}
					destination.check = func(_ context.Context, got fs.Object, remote string) (bool, error) {
						checks++
						require.Same(t, source, got)
						require.Equal(t, "source.txt", remote)
						if admitted {
							return true, cause
						}
						return false, nil
					}
					destination.features.Copy = func(copyCtx context.Context, transport fs.Object, remote string) (fs.Object, error) {
						copies++
						intent, ok := fs.UploadAdmissionIntentFromContext(copyCtx)
						require.True(t, ok)
						require.Same(t, source, intent.Source)
						require.Same(t, destination, intent.Destination)
						require.Equal(t, "source.txt", intent.DestinationRemote)
						require.NotSame(t, source, transport)
						if consumer == "pair_checker" {
							require.Equal(t, "SOURCE.txt", remote, "wire destination keeps the existing override")
						} else {
							require.Equal(t, "source.txt", remote)
						}
						if mode == "unmarked_fallback" {
							return nil, cause
						}
						admitted = true
						return nil, &fs.UploadAdmissionError{Err: cause}
					}
					s, err := newSyncCopyMove(ctx, destination, sourceFs, fs.DeleteModeOff, true, false, false, false)
					require.NoError(t, err)
					defer s.cancel()
					defer s.inCancel()
					ci.CopyDest = []string{"fixture-comparison"}
					s.compareCopyDest = []fs.Fs{comparison}
					var logged error
					s.logger = func(_ context.Context, sigil operations.Sigil, _, _ fs.DirEntry, err error) {
						if sigil == operations.TransferError {
							logged = err
						}
					}
					beforeChecks := accounting.Stats(ctx).GetChecks()
					if consumer == "pair_checker" {
						dst, err := base.NewObject(ctx, "SOURCE.txt")
						require.NoError(t, err)
						if mode == "entry_equal" {
							dst = original
						}
						require.True(t, s.toBeChecked.Put(ctx, fs.ObjectPair{Src: source, Dst: dst}))
						s.toBeChecked.Close()
						var wg sync.WaitGroup
						wg.Add(1)
						s.pairChecker(s.toBeChecked, s.toBeUploaded, -1, &wg)
						wg.Wait()
						var checking []accounting.TransferSnapshot
						for _, snapshot := range accounting.Stats(ctx).Transferred() {
							if snapshot.Checked && snapshot.What == "checking" {
								checking = append(checking, snapshot)
							}
						}
						require.Len(t, checking, 1)
						require.False(t, checking[0].CompletedAt.IsZero())
						require.Equal(t, beforeChecks+1, accounting.Stats(ctx).GetChecks(), "checking Done must run exactly once")
						if mode != "unmarked_fallback" {
							require.ErrorIs(t, checking[0].Error, cause)
						}
					} else {
						s.trackRenames = mode == "entry_rename"
						require.False(t, s.SrcOnly(source))
						require.Equal(t, beforeChecks, accounting.Stats(ctx).GetChecks(), "SrcOnly owns no checking transfer")
						completed := accounting.Stats(ctx).Transferred()
						if mode == "entry_equal" || mode == "entry_rename" {
							require.Empty(t, completed)
						} else {
							require.Len(t, completed, 1, "real comparison Copy must finish its one transfer")
							require.False(t, completed[0].CompletedAt.IsZero())
							require.ErrorIs(t, completed[0].Error, cause)
						}
					}
					stats, err := accounting.Stats(ctx).RemoteStats(false)
					require.NoError(t, err)
					require.NotContains(t, stats, "checking", "no checking transfer may remain active")
					require.NotContains(t, stats, "transferring", "no comparison transfer may remain active")
					require.NotZero(t, checks)
					if mode == "unmarked_fallback" {
						items, _ := s.toBeUploaded.Stats()
						require.Equal(t, 1, items)
						s.toBeUploaded.Close()
						pair, ok := s.toBeUploaded.Get(ctx)
						require.True(t, ok)
						require.Same(t, source, pair.Src)
						require.False(t, pair.Src == pair.Dst, "ordinary fallback is not an ordered deletion token")
					} else {
						require.ErrorIs(t, s.currentError(), cause)
						require.True(t, fs.IsUploadAdmissionError(s.currentError()))
						require.ErrorIs(t, logged, cause)
						items, _ := s.toBeUploaded.Stats()
						require.Zero(t, items, "no fallback upload or ordered source-deletion token")
					}
					if mode == "entry_equal" || mode == "entry_rename" {
						require.Zero(t, copies)
					} else {
						require.Equal(t, 1, copies)
					}
					items, _ := s.toBeRenamed.Stats()
					require.Zero(t, items)
					require.Empty(t, s.trackRenamesCh)
					require.Empty(t, s.deleteFilesCh)
					require.Zero(t, removes)
					body, err := os.ReadFile(filepath.Join(sourceFs.Root(), "source.txt"))
					require.NoError(t, err)
					require.Equal(t, sha256.Sum256([]byte("data")), sha256.Sum256(body))
				})
			}
		})
	}
}
