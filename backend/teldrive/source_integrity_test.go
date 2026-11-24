package teldrive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/operations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestB1LocalMutationPreventsMove(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	for _, test := range []struct {
		name    string
		mutate  bool
		restore bool
		noCheck bool
	}{
		{name: "unchanged"},
		{name: "changed-mtime", mutate: true},
		{name: "same-size-restored-mtime", mutate: true, restore: true},
		{name: "no-check-updated", mutate: true, noCheck: true},
	} {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/override=%t", test.name, wrapped), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				ctx, ci := fs.AddConfig(ctx)
				ci.LowLevelRetries = 1
				ci.MultiThreadStreams = 0
				ci.BufferSize = 0
				ci.Inplace = true
				ctx = accounting.WithStatsGroup(ctx, t.Name())
				root := t.TempDir()
				name := filepath.Join(root, "source")
				data := bytes.Repeat([]byte("a"), tdhash.BlockSize+64)
				initialSHA := sha256.Sum256(data)
				require.NoError(t, os.WriteFile(name, data, 0600))
				f, err := local.NewFs(ctx, "local", root, configmap.Simple{
					"no_check_updated": strconv.FormatBool(test.noCheck),
				})
				require.NoError(t, err)
				src, err := f.NewObject(ctx, "source")
				require.NoError(t, err)
				when := src.ModTime(ctx)
				_, err = src.Hash(ctx, telDriveHash)
				require.NoError(t, err)
				remote := src.Remote()
				if wrapped {
					remote = "renamed"
				}
				oldBytes := []byte("prior nonempty destination")
				oldSHA := sha256.Sum256(oldBytes)
				oldFile := api.FileInfo{Id: "old", Name: remote, Kind: "file", Size: int64(len(oldBytes)),
					ModTime: when.Add(-time.Hour), Generation: 7, Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString(oldBytes)}}
				var mu sync.Mutex
				var session api.UploadSession
				file := oldFile
				published := oldBytes
				shareActive := true
				parts := make(map[int][]byte)
				completeCalls := 0
				mutationDone := false
				o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					defer mu.Unlock()
					w.Header().Set("Content-Type", "application/json")
					switch {
					case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/upload":
						_ = json.NewEncoder(w).Encode(session)
					case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads":
						_ = json.NewEncoder(w).Encode(api.UploadSessionPage{Items: []api.UploadSession{}})
					case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads":
						var request api.UploadCreateRequest
						if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
							t.Error(err)
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						assert.Equal(t, int64(len(data)), request.Size)
						assert.Equal(t, remote, request.Name)
						session = api.UploadSession{ID: "upload", Name: request.Name, ParentId: request.ParentId, ExpectedSize: request.Size, MimeType: request.MimeType, ModTime: request.ModTime, Encryption: request.Encryption, ConflictPolicy: request.ConflictPolicy, State: "open", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour), PartSize: tdhash.BlockSize, RequiresCompletionManifest: true}
						_ = json.NewEncoder(w).Encode(session)
					case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/api/v1/uploads/upload/parts/"):
						partNo, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/api/v1/uploads/upload/parts/"))
						if err != nil {
							t.Error(err)
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						body, err := io.ReadAll(r.Body)
						if err != nil {
							// A detected metadata change interrupts the next request body.
							if !mutationDone || test.restore || test.noCheck || partNo != 2 {
								t.Error(err)
							}
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						parts[partNo] = body
						if partNo == 1 && test.mutate && !mutationDone {
							// The first part is consumed; its ACK gates the next upload read.
							data[0] = 'b'
							fd, err := os.OpenFile(name, os.O_WRONLY, 0600)
							if err == nil {
								_, err = fd.WriteAt(data[:1], 0)
								closeErr := fd.Close()
								if err == nil {
									err = closeErr
								}
							}
							mtime := when.Add(time.Hour)
							if test.restore {
								mtime = when
							}
							if err == nil {
								err = os.Chtimes(name, mtime, mtime)
							}
							if err == nil {
								var later io.ReadCloser
								later, err = src.Open(ctx)
								if err == nil {
									err = later.Close()
								}
							}
							if err != nil {
								t.Errorf("mutation barrier: %v", err)
								w.WriteHeader(http.StatusBadRequest)
								return
							}
							mutationDone = true
						}
						_ = json.NewEncoder(w).Encode(api.UploadPart{UploadId: "upload", PartNo: partNo,
							State: "stored", PlainSize: int64(len(body)), Checksum: tdhash.SumString(body)})
					case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/upload/complete":
						completeCalls++
						var manifest api.UploadCompleteRequest
						if err := json.NewDecoder(r.Body).Decode(&manifest); err != nil || manifest.Hash == nil {
							t.Errorf("completion manifest: %v", err)
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						published = append(append([]byte(nil), parts[1]...), parts[2]...)
						assert.Equal(t, int64(len(published)), manifest.Size)
						assert.Equal(t, tdhash.SumString(published), manifest.Hash.Value)
						file = api.FileInfo{Id: "new", Name: remote, Kind: "file", Size: manifest.Size,
							ModTime: when, Generation: 8, Hash: manifest.Hash}
						shareActive = false
						_ = json.NewEncoder(w).Encode(file)
					default:
						t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
						w.WriteHeader(http.StatusBadRequest)
					}
				})
				o.fs.name = "b1-destination"
				o.fs.features = &fs.Features{}
				o.remote = remote
				o.applyFileInfo(&oldFile)
				_, err = operations.Move(ctx, o.fs, o, remote, src)
				mu.Lock()
				defer mu.Unlock()
				if test.mutate {
					require.Error(t, err)
					assert.True(t, mutationDone)
					assert.Zero(t, completeCalls, "mutation must fail before publication")
					assert.Equal(t, oldSHA, sha256.Sum256(published))
					assert.Equal(t, oldFile, file)
					assert.True(t, shareActive)
					current, readErr := os.ReadFile(name)
					require.NoError(t, readErr, "Move must not delete the changed source")
					assert.Equal(t, sha256.Sum256(data), sha256.Sum256(current))
					assert.NotEqual(t, initialSHA, sha256.Sum256(current))
					if test.restore {
						assert.Len(t, parts, 2, "fresh byte proof, not read-time metadata checks, must reject this case")
					}
				} else {
					require.NoError(t, err)
					assert.Equal(t, 1, completeCalls)
					assert.Equal(t, initialSHA, sha256.Sum256(published))
					assert.False(t, shareActive)
					_, statErr := os.Stat(name)
					assert.ErrorIs(t, statErr, os.ErrNotExist)
				}
			})
		}
	}
}

func TestB2ParentFailurePreventsMove(t *testing.T) {
	cacheDir := config.GetCacheDir()
	require.NoError(t, config.SetCacheDir(t.TempDir()))
	t.Cleanup(func() { require.NoError(t, config.SetCacheDir(cacheDir)) })
	for _, mode := range []string{"parent", "legacy-unknown", "ordinary-conflict", "explicit-root", "completed-result", "transient"} {
		for _, data := range []string{"", "independent source bytes"} {
			t.Run(fmt.Sprintf("%s/size=%d", mode, len(data)), func(t *testing.T) {
				ctx, ci := fs.AddConfig(context.Background())
				ci.LowLevelRetries = 1
				ci.MultiThreadStreams = 0
				ci.BufferSize = 0
				ci.Inplace = true
				ctx = accounting.WithStatsGroup(ctx, t.Name())
				root := t.TempDir()
				name := filepath.Join(root, "source")
				require.NoError(t, os.WriteFile(name, []byte(data), 0600))
				sourceSHA := sha256.Sum256([]byte(data))
				localFS, err := local.NewFs(ctx, "local", root, configmap.Simple{})
				require.NoError(t, err)
				src, err := localFS.NewObject(ctx, "source")
				require.NoError(t, err)
				oldBytes := []byte("independent old target")
				oldSHA := sha256.Sum256(oldBytes)
				old := api.FileInfo{Id: "old", Name: "source", Kind: "file", Size: int64(len(oldBytes)), Generation: 7,
					ModTime: src.ModTime(ctx).Add(-time.Hour), Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString(oldBytes)}}
				file := old
				published := oldBytes
				shareActive := true
				puts, completes := 0, 0
				var mu sync.Mutex
				var session api.UploadSession
				o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					defer mu.Unlock()
					w.Header().Set("Content-Type", "application/json")
					switch {
					case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/upload":
						_ = json.NewEncoder(w).Encode(session)
					case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads":
						_ = json.NewEncoder(w).Encode(api.UploadSessionPage{Items: []api.UploadSession{}})
					case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads":
						var request api.UploadCreateRequest
						require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
						assert.Empty(t, request.ParentId)
						assert.Equal(t, int64(len(data)), request.Size)
						session = api.UploadSession{ID: "upload", Name: request.Name, ParentId: request.ParentId, ExpectedSize: request.Size, MimeType: request.MimeType, ModTime: request.ModTime, Encryption: request.Encryption, ConflictPolicy: request.ConflictPolicy, State: "open", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour), PartSize: tdhash.BlockSize, RequiresCompletionManifest: true}
						_ = json.NewEncoder(w).Encode(session)
					case r.Method == http.MethodPut:
						puts++
						body, readErr := io.ReadAll(r.Body)
						require.NoError(t, readErr)
						assert.Equal(t, data, string(body))
						_, _ = io.WriteString(w, uploadAck(string(body)))
					case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete"):
						completes++
						var manifest api.UploadCompleteRequest
						require.NoError(t, json.NewDecoder(r.Body).Decode(&manifest))
						require.NotNil(t, manifest.Hash)
						assert.Equal(t, int64(len(data)), manifest.Size)
						assert.Equal(t, tdhash.SumString([]byte(data)), manifest.Hash.Value)
						if mode == "parent" || mode == "legacy-unknown" || mode == "ordinary-conflict" {
							code := "upload_parent_unavailable"
							if mode == "ordinary-conflict" {
								code = "conflict"
							}
							w.WriteHeader(http.StatusConflict)
							_, _ = fmt.Fprintf(w, `{"error":{"code":%q,"message":"upload destination is unavailable"}}`, code)
							return
						}
						if mode == "transient" && completes == 1 {
							w.WriteHeader(http.StatusServiceUnavailable)
							_, _ = io.WriteString(w, `{"error":{"code":"unavailable"}}`)
							return
						}
						published = []byte(data)
						file = api.FileInfo{Id: "new", Name: "source", Kind: "file", Size: manifest.Size, Generation: 8, ModTime: src.ModTime(ctx), Hash: manifest.Hash}
						shareActive = false
						if mode == "completed-result" && completes == 1 {
							_, _ = io.WriteString(w, `{"id":`)
							return
						}
						_ = json.NewEncoder(w).Encode(struct {
							api.FileInfo
							Size int64 `json:"size"`
						}{file, file.Size})
					default:
						t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
						w.WriteHeader(http.StatusBadRequest)
					}
				})
				o.fs.name = "b2-destination"
				o.fs.features = &fs.Features{}
				o.remote = "source"
				o.applyFileInfo(&old)
				_, err = operations.Move(ctx, o.fs, o, "source", src)
				mu.Lock()
				defer mu.Unlock()
				if mode == "parent" || mode == "legacy-unknown" || mode == "ordinary-conflict" {
					require.Error(t, err)
					assert.Equal(t, mode != "ordinary-conflict", fserrors.IsFatalError(err))
					current, readErr := os.ReadFile(name)
					require.NoError(t, readErr)
					assert.Len(t, current, len(data))
					assert.Equal(t, sourceSHA, sha256.Sum256(current))
					assert.Equal(t, oldSHA, sha256.Sum256(published))
					assert.Equal(t, old, file)
					assert.True(t, shareActive)
					assert.Equal(t, 1, completes)
				} else {
					require.NoError(t, err)
					assert.Equal(t, sourceSHA, sha256.Sum256(published))
					_, statErr := os.Stat(name)
					assert.ErrorIs(t, statErr, os.ErrNotExist)
					assert.False(t, shareActive)
					if mode == "transient" || mode == "completed-result" {
						assert.Equal(t, 2, completes)
					} else {
						assert.Equal(t, 1, completes)
					}
				}
				if len(data) == 0 {
					assert.Zero(t, puts)
				} else {
					assert.Equal(t, 1, puts)
				}
			})
		}
	}
}

func TestB2CopyAdmissionPacer(t *testing.T) {
	for _, mode := range []string{"terminal_reply_lost", "success_reply_lost", "fresh_fs_objects", "fingerprint_mismatch", "current_result_access", "ordinary_retry_controls"} {
		t.Run(mode, func(t *testing.T) {
			ctx := b2CommandContext(t)
			const sourceData = "independent original source bytes"
			const oldData = "independent old destination bytes"
			sourceSHA, oldSHA := sha256.Sum256([]byte(sourceData)), sha256.Sum256([]byte(oldData))
			var mu sync.Mutex
			var key string
			var payload api.FileCopy
			posts, operationsCount, publications := 0, 0, 0
			terminal, denied := false, false
			oldShare := "original-share"
			result := api.FileInfo{Id: "original-result", Name: "file.txt", Kind: "file", Size: int64(len(sourceData)),
				Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString([]byte(sourceData))}}
			o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/content") {
					switch r.URL.Path {
					case "/api/v1/files/source-v1/content":
						_, _ = io.WriteString(w, sourceData)
					case "/api/v1/files/old/content":
						_, _ = io.WriteString(w, oldData)
					default:
						t.Errorf("unexpected content request %s", r.URL.Path)
						w.WriteHeader(http.StatusBadRequest)
					}
					return
				}
				if r.Method != http.MethodPost || r.URL.Path != "/api/v1/files/source-v1/copy" {
					t.Errorf("unexpected copy request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				posts++
				var current api.FileCopy
				require.NoError(t, json.NewDecoder(r.Body).Decode(&current))
				if key == "" {
					key, payload = r.Header.Get("Idempotency-Key"), current
					require.NotEmpty(t, key)
					operationsCount++
					if mode != "terminal_reply_lost" {
						publications++
					}
				} else {
					assert.Equal(t, key, r.Header.Get("Idempotency-Key"))
					assert.Equal(t, payload, current)
				}
				if mode == "ordinary_retry_controls" && posts == 1 {
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = io.WriteString(w, `{"error":{"code":"unavailable"}}`)
					return
				}
				if mode == "current_result_access" && posts > 1 {
					denied = true
				}
				if denied {
					w.WriteHeader(http.StatusForbidden)
					_, _ = io.WriteString(w, `{"error":{"code":"forbidden"}}`)
					return
				}
				if terminal {
					w.WriteHeader(http.StatusConflict)
					_, _ = io.WriteString(w, `{"error":{"code":"upload_parent_unavailable"}}`)
					return
				}
				if posts == 1 && mode != "fingerprint_mismatch" && mode != "ordinary_retry_controls" {
					terminal = mode == "terminal_reply_lost"
					_, _ = io.WriteString(w, `{"id":`)
					return
				}
				_ = json.NewEncoder(w).Encode(result)
			})
			o.fs.name, o.fs.opt.ApiHost, o.fs.opt.APIKey, o.fs.userId = "b2-copy", t.Name(), "synthetic-copy-key", 42
			source := &Object{fs: o.fs, id: "source-v1", remote: "source.txt", size: int64(len(sourceData)), modTime: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
			old := &Object{fs: o.fs, id: "old", remote: "file.txt", size: int64(len(oldData))}
			readSHA := func(object *Object) [32]byte {
				t.Helper()
				body, err := object.Open(ctx)
				require.NoError(t, err)
				data, err := io.ReadAll(body)
				require.NoError(t, err)
				require.NoError(t, body.Close())
				return sha256.Sum256(data)
			}
			assert.Equal(t, sourceSHA, readSHA(source))
			assert.Equal(t, oldSHA, readSHA(old))
			copied, err := o.fs.Copy(ctx, source, "file.txt")
			if mode == "terminal_reply_lost" || mode == "current_result_access" {
				require.Error(t, err)
				assert.Equal(t, mode == "terminal_reply_lost", fserrors.IsFatalError(err))
			} else {
				require.NoError(t, err)
				assert.Equal(t, result.Id, copied.(fs.IDer).ID())
			}
			mu.Lock()
			before := posts
			mu.Unlock()
			fresh := b2FreshAdmissionObject(o, "", "file.txt")
			if mode == "fingerprint_mismatch" {
				sourceCopy := *source
				sourceCopy.id = "source-v2"
				_, err = fresh.fs.Copy(ctx, &sourceCopy, "file.txt")
				require.Error(t, err)
				assert.Equal(t, before, posts, "changed transport must not send a new request")
			} else {
				copied, err = fresh.fs.Copy(ctx, source, "file.txt")
				if terminal || denied {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Equal(t, result.Id, copied.(fs.IDer).ID())
				}
			}
			assert.Equal(t, 1, operationsCount)
			if mode == "terminal_reply_lost" {
				assert.Zero(t, publications)
			} else {
				assert.Equal(t, 1, publications)
			}
			assert.Equal(t, "original-share", oldShare)
			assert.Equal(t, sourceSHA, readSHA(source))
			assert.Equal(t, oldSHA, readSHA(old))
			if mode == "ordinary_retry_controls" {
				resp := &http.Response{StatusCode: 409, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"conflict"}}`))}
				err := errorHandler(resp)
				assert.False(t, fserrors.IsFatalError(err))
				retry, _ := shouldRetry(ctx, resp, err)
				assert.False(t, retry)
				self := &Object{fs: o.fs, id: "old", remote: "file.txt"}
				_, err = o.fs.Copy(ctx, self, "file.txt")
				require.ErrorIs(t, err, fs.ErrorCantCopy)
				_, err = o.fs.selectUploadAdmission(ctx, "upload", self, "file.txt", "file.txt", [32]byte{})
				require.NoError(t, err, "an unadmitted copy must not veto ordinary manual fallback")
			}
		})
	}
}
