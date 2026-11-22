package teldrive

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestB2LegacyResumeHint(t *testing.T) {
	for _, mode := range []string{"matching_unknown", "matching_truncated", "listing_failure", "adoption_store_failure", "normal_resume", "incompatible_hash_mime", "unrelated_parent", "unrelated_name", "unrelated_size", "unrelated_modtime"} {
		t.Run(mode, func(t *testing.T) {
			ctx := b2CommandContext(t)
			modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			session := api.UploadSession{ID: "30000000-0000-4000-8000-000000000001", Name: "file.txt", ExpectedSize: 0, MimeType: "text/plain; charset=utf-8", ModTime: modTime, ConflictPolicy: "replace", PartSize: tdhash.BlockSize, State: "open", ExpiresAt: modTime.AddDate(10, 0, 0), RequiresCompletionManifest: true}
			switch mode {
			case "incompatible_hash_mime":
				session.ExpectedHash = &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString(nil)}
				session.MimeType = ""
			case "unrelated_parent":
				session.ParentId = "unrelated-parent"
			case "unrelated_name":
				session.Name = "other.txt"
			case "unrelated_size":
				session.ExpectedSize = 1
			case "unrelated_modtime":
				session.ModTime = modTime.Add(time.Second)
			}
			store := &b2AdmissionJournal{state: fs.UploadAdmissionState{Generation: "f9f8e89b-154d-4246-a105-d78873d105e6"}}
			ctx = fs.WithUploadAdmissionStore(ctx, store)
			var scans, gets, creates, partReads atomic.Int32
			o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads":
					n := scans.Add(1)
					if mode == "listing_failure" && n == 1 {
						w.WriteHeader(http.StatusServiceUnavailable)
						_, _ = io.WriteString(w, `{}`)
						return
					}
					if mode == "adoption_store_failure" && n == 1 {
						store.mu.Lock()
						store.err = io.ErrShortWrite
						store.mu.Unlock()
					}
					_ = json.NewEncoder(w).Encode(api.UploadSessionPage{Items: []api.UploadSession{session}})
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/parts"):
					partReads.Add(1)
					_ = json.NewEncoder(w).Encode(api.UploadPartPage{Items: []api.UploadPart{}})
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/"+session.ID:
					gets.Add(1)
					if mode == "normal_resume" {
						_ = json.NewEncoder(w).Encode(session)
						return
					}
					if mode == "matching_truncated" {
						_, _ = io.WriteString(w, `{"id":`)
						return
					}
					w.WriteHeader(http.StatusConflict)
					_, _ = io.WriteString(w, `{"error":{"code":"conflict","message":"upload destination unavailable"}}`)
				case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads":
					creates.Add(1)
					var request api.UploadCreateRequest
					require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
					_ = json.NewEncoder(w).Encode(api.UploadSession{ID: "30000000-0000-4000-8000-000000000003", ParentId: request.ParentId, Name: request.Name, ExpectedSize: request.Size, MimeType: request.MimeType, ModTime: request.ModTime, Encryption: request.Encryption, ConflictPolicy: request.ConflictPolicy, PartSize: tdhash.BlockSize, State: "open", ExpiresAt: time.Now().Add(time.Hour), RequiresCompletionManifest: true})
				default:
					t.Errorf("unexpected resume request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			o.fs.pacer.SetRetries(1)
			source := noHashSource{object.NewStaticObjectInfo("file.txt", modTime, 0, true, nil, nil)}
			info, err := o.prepareUpload(ctx, "file.txt", source)
			if mode == "listing_failure" || mode == "adoption_store_failure" {
				require.Error(t, err)
				assert.Zero(t, gets.Load())
				assert.Zero(t, creates.Load())
				store.mu.Lock()
				assert.Empty(t, store.state.Request, "failed discovery/adoption must not become POST-replay permission")
				assert.Empty(t, store.state.UploadID)
				store.err = nil
				store.mu.Unlock()
				info, err = b2FreshAdmissionObject(o, "", "file.txt").prepareUpload(ctx, "file.txt", source)
			}
			unknown := strings.HasPrefix(mode, "matching_") || mode == "listing_failure" || mode == "adoption_store_failure"
			if unknown {
				require.Error(t, err)
				require.Nil(t, info)
				assert.Zero(t, creates.Load())
				assert.Zero(t, partReads.Load())
				require.Positive(t, gets.Load())
				store.mu.Lock()
				assert.Equal(t, session.ID, store.state.UploadID)
				store.mu.Unlock()
				before := scans.Load()
				_, err = b2FreshAdmissionObject(o, "", "file.txt").prepareUpload(ctx, "file.txt", source)
				require.Error(t, err)
				assert.Equal(t, before, scans.Load())
				assert.Zero(t, creates.Load())
				return
			}
			require.NoError(t, err)
			if mode == "normal_resume" {
				assert.Equal(t, session.ID, info.uploadID)
				assert.Zero(t, creates.Load())
				assert.EqualValues(t, 1, partReads.Load())
			} else {
				assert.Equal(t, "30000000-0000-4000-8000-000000000003", info.uploadID)
				assert.EqualValues(t, 1, creates.Load())
				assert.Zero(t, gets.Load())
				assert.Zero(t, partReads.Load())
			}
		})
	}
}

func TestB2UploadResponseBoundary(t *testing.T) {
	for _, mode := range []string{"missing_page_items", "null_page_items", "malformed_session_row", "unknown_session_state", "missing_part_identity", "unknown_part_state", "missing_result_id", "missing_result_size", "trailing_completion", "stale_completion", "stale_page", "stale_exact", "stale_create", "session_cursor_repeat", "session_cursor_cycle", "part_cursor_repeat", "part_cursor_cycle"} {
		t.Run(mode, func(t *testing.T) {
			ctx := b2CommandContext(t)
			modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			session := api.UploadSession{ID: "30000000-0000-4000-8000-000000000001", Name: "file.txt", ExpectedSize: 0, MimeType: "text/plain; charset=utf-8", ModTime: modTime, ConflictPolicy: "replace", PartSize: tdhash.BlockSize, State: "open", ExpiresAt: modTime.AddDate(10, 0, 0), RequiresCompletionManifest: true}
			completion := api.FileInfo{Id: "30000000-0000-4000-8000-000000000002", Name: "file.txt", Kind: "file", Size: 0, ModTime: modTime, Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString(nil)}}
			var scans, gets, creates, partReads, completes atomic.Int32
			var createKey atomic.Value
			o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				write := func(value any, short bool) {
					if file, ok := value.(api.FileInfo); ok {
						value = struct {
							api.FileInfo
							Size int64 `json:"size"`
						}{FileInfo: file, Size: file.Size}
					}
					body, err := json.Marshal(value)
					require.NoError(t, err)
					if short {
						w.Header().Set("Content-Length", strconv.Itoa(len(body)+1))
					}
					_, _ = w.Write(body)
				}
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads":
					n := scans.Add(1)
					switch mode {
					case "missing_page_items":
						_, _ = io.WriteString(w, `{}`)
						return
					case "null_page_items":
						_, _ = io.WriteString(w, `{"items":null}`)
						return
					case "malformed_session_row":
						_, _ = io.WriteString(w, `{"items":[{}]}`)
						return
					case "unknown_session_state":
						s := session
						s.State = "unknown"
						write(api.UploadSessionPage{Items: []api.UploadSession{s}}, false)
						return
					case "stale_page":
						if n == 1 {
							write(api.UploadSessionPage{Items: []api.UploadSession{}}, true)
						} else {
							_, _ = io.WriteString(w, `{}`)
						}
						return
					case "session_cursor_repeat", "session_cursor_cycle":
						limit := int32(2)
						cursor := "one"
						if mode == "session_cursor_cycle" {
							limit = 3
							if n == 2 {
								cursor = "two"
							}
						}
						if n > limit {
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						write(api.UploadSessionPage{Items: []api.UploadSession{}, NextCursor: cursor}, false)
						return
					}
					items := []api.UploadSession{}
					if strings.Contains(mode, "part_") || mode == "stale_exact" {
						items = append(items, session)
					}
					write(api.UploadSessionPage{Items: items}, false)
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/parts"):
					n := partReads.Add(1)
					if mode == "missing_part_identity" {
						_, _ = io.WriteString(w, `{"items":[{"state":"uploading"}]}`)
						return
					}
					if mode == "unknown_part_state" {
						write(api.UploadPartPage{Items: []api.UploadPart{{UploadId: session.ID, PartNo: 1, State: "unknown", PlainSize: 1}}}, false)
						return
					}
					if mode == "part_cursor_repeat" || mode == "part_cursor_cycle" {
						limit := int32(2)
						cursor := "one"
						if mode == "part_cursor_cycle" {
							limit = 3
							if n == 2 {
								cursor = "two"
							}
						}
						if n > limit {
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						write(api.UploadPartPage{Items: []api.UploadPart{}, NextCursor: cursor}, false)
						return
					}
					write(api.UploadPartPage{Items: []api.UploadPart{}}, false)
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/"+session.ID:
					n := gets.Add(1)
					if mode == "stale_exact" {
						if n == 1 {
							write(session, true)
						} else {
							_, _ = io.WriteString(w, `null`)
						}
						return
					}
					write(session, false)
				case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads":
					n := creates.Add(1)
					key := r.Header.Get("Idempotency-Key")
					if n == 1 {
						createKey.Store(key)
					} else {
						assert.Equal(t, createKey.Load(), key)
					}
					if mode == "stale_create" {
						if n == 1 {
							write(session, true)
						} else {
							_, _ = io.WriteString(w, `{}`)
						}
						return
					}
					write(session, false)
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete"):
					n := completes.Add(1)
					switch mode {
					case "missing_result_id":
						file := completion
						file.Id = ""
						write(file, false)
					case "missing_result_size":
						body, err := json.Marshal(completion)
						require.NoError(t, err)
						var fields map[string]json.RawMessage
						require.NoError(t, json.Unmarshal(body, &fields))
						delete(fields, "size")
						write(fields, false)
					case "trailing_completion":
						body, err := json.Marshal(struct {
							api.FileInfo
							Size int64 `json:"size"`
						}{completion, completion.Size})
						require.NoError(t, err)
						_, _ = w.Write(body)
						_, _ = io.WriteString(w, ` {}`)
					case "stale_completion":
						if n == 1 {
							write(completion, true)
						} else {
							_, _ = io.WriteString(w, `{}`)
						}
					default:
						write(completion, false)
					}
				default:
					t.Errorf("unexpected boundary request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			o.fs.pacer.SetRetries(2)
			source := noHashSource{object.NewStaticObjectInfo("file.txt", modTime, 0, true, nil, nil)}
			completionCase := strings.Contains(mode, "result_") || strings.Contains(mode, "completion")
			if completionCase {
				info, err := o.uploadMultipart(ctx, "file.txt", strings.NewReader(""), source)
				require.NoError(t, err)
				_, err = o.completeUpload(ctx, info)
				require.Error(t, err)
				assert.Empty(t, o.id)
			} else {
				_, err := o.prepareUpload(ctx, "file.txt", source)
				require.Error(t, err)
				assert.Zero(t, completes.Load())
			}
			if mode == "session_cursor_repeat" {
				assert.EqualValues(t, 2, scans.Load())
			}
			if mode == "session_cursor_cycle" {
				assert.EqualValues(t, 3, scans.Load())
			}
			if mode == "part_cursor_repeat" {
				assert.EqualValues(t, 2, partReads.Load())
			}
			if mode == "part_cursor_cycle" {
				assert.EqualValues(t, 3, partReads.Load())
			}
			if strings.Contains(mode, "page") || strings.Contains(mode, "session_row") || mode == "unknown_session_state" || strings.HasPrefix(mode, "session_cursor_") {
				assert.Zero(t, creates.Load())
			}
		})
	}
}

func TestB2PutPreservesCompletionIdentity(t *testing.T) {
	ctx := b2CommandContext(t)
	modTime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	const uploadID = "30000000-0000-4000-8000-000000000001"
	const resultID = "30000000-0000-4000-8000-000000000002"
	var completes, lookups atomic.Int32
	o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads":
			_ = json.NewEncoder(w).Encode(api.UploadSessionPage{Items: []api.UploadSession{}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads":
			_ = json.NewEncoder(w).Encode(api.UploadSession{ID: uploadID, Name: "file.txt", ExpectedSize: 4, MimeType: "application/octet-stream", ModTime: modTime, ConflictPolicy: "replace", PartSize: tdhash.BlockSize, State: "open", ExpiresAt: modTime.AddDate(10, 0, 0), RequiresCompletionManifest: true})
		case r.Method == http.MethodPut:
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			assert.Equal(t, "data", string(body))
			_ = json.NewEncoder(w).Encode(api.UploadPart{UploadId: uploadID, PartNo: 1, State: "stored", PlainSize: 4, Checksum: tdhash.SumString(body)})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete"):
			completes.Add(1)
			_ = json.NewEncoder(w).Encode(api.FileInfo{Id: resultID, Name: "file.txt", Kind: "file", Size: 4, ModTime: modTime, Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString([]byte("data"))}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files":
			lookups.Add(1)
			_ = json.NewEncoder(w).Encode(api.ReadMetadataResponse{Files: []api.FileInfo{{Id: "30000000-0000-4000-8000-000000000003", Name: "file.txt", Kind: "file", Size: 4, ModTime: modTime, Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString([]byte("evil"))}}}})
		default:
			t.Errorf("unexpected identity request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	o.fs.opt.HashEnabled = false
	source := object.NewStaticObjectInfo("file.txt", modTime, 4, true, nil, nil).WithMimeType("application/octet-stream")
	result, err := o.fs.PutUnchecked(ctx, strings.NewReader("data"), source)
	require.Error(t, err, "a same-name replacement is not the proved completion result")
	if result != nil {
		actual, ok := result.(*Object)
		require.True(t, ok)
		assert.Equal(t, resultID, actual.id)
	}
	assert.EqualValues(t, 1, completes.Load())
	assert.EqualValues(t, 1, lookups.Load())
}

func TestUploadResponseRequiredFields(t *testing.T) {
	for _, body := range []string{`null`, `{}`, `{"id":"file","size":null}`, `{"id":"file","size":"0"}`, `{"id":"file","size":0} {}`} {
		t.Run(body, func(t *testing.T) {
			var result uploadFileResponse
			err := decodeUploadResponse(&http.Response{Body: io.NopCloser(strings.NewReader(body))}, &result)
			require.Error(t, err)
		})
	}
	var result uploadFileResponse
	require.NoError(t, decodeUploadResponse(&http.Response{Body: io.NopCloser(strings.NewReader("{\"id\":\"synthetic-file\",\"size\":0} \n"))}, &result))
	assert.Equal(t, "synthetic-file", result.FileInfo.Id)
	assert.Zero(t, result.FileInfo.Size)
}
