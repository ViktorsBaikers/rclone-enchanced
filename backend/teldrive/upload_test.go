package teldrive

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type noHashSource struct{ fs.ObjectInfo }

func (noHashSource) Hash(context.Context, hash.Type) (string, error) {
	return "", errors.New("unexpected source hash request")
}

func newUploadTestObject(t *testing.T, handler http.HandlerFunc) *Object {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	ctx := context.Background()
	f := &Fs{
		opt:   Options{HashEnabled: true, ChunkSize: defaultChunkSize},
		pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
	}
	f.srv = rest.NewClient(server.Client()).SetRoot(server.URL).SetErrorHandler(errorHandler)
	f.dirCache = dircache.New("", rootID, f)
	return &Object{fs: f, remote: "file.txt"}
}

func TestB1NegotiatedGeometry(t *testing.T) {
	for _, session := range []api.UploadSession{
		{ID: "u", PartSize: tdhash.BlockSize},
		{ID: "u", PartSize: 1, RequiresCompletionManifest: true},
		{ID: "u", PartSize: tdhash.BlockSize, ExpectedSize: -2, RequiresCompletionManifest: true},
		{ID: "u", PartSize: tdhash.BlockSize, ExpectedSize: 1<<63 - 1, RequiresCompletionManifest: true},
	} {
		_, err := uploadInfoFromSession(session, "", "", nil)
		require.Error(t, err)
	}
}

func uploadAck(data string) string {
	return fmt.Sprintf(`{"uploadId":"upload","partNo":1,"state":"stored","plainSize":%d,"checksum":%q}`, len(data), tdhash.SumString([]byte(data)))
}

func TestB1UploadAcknowledgement(t *testing.T) {
	for _, response := range []string{"", `{}`,
		strings.Replace(uploadAck("x"), `"upload"`, `"other"`, 1),
		strings.Replace(uploadAck("x"), `"partNo":1`, `"partNo":2`, 1),
		strings.Replace(uploadAck("x"), `"plainSize":1`, `"plainSize":2`, 1),
		strings.Replace(uploadAck("x"), `"stored"`, `"uploading"`, 1),
		uploadAck("y")} {
		o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = io.WriteString(w, response)
		})
		_, err := o.putUploadPart(context.Background(), &uploadInfo{uploadID: "upload"}, 1, strings.NewReader("x"), 1)
		require.Error(t, err)
	}
	o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	})
	_, err := o.putUploadPart(context.Background(), &uploadInfo{uploadID: "upload"}, 1, strings.NewReader("x"), 1)
	require.Error(t, err)
}

func TestPrepareUploadDoesNotReadSourceHash(t *testing.T) {
	created := false
	o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads":
			_, _ = io.WriteString(w, `{"items":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads":
			var request api.UploadCreateRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			assert.Nil(t, request.Hash)
			created = true
			_, _ = io.WriteString(w, `{"id":"upload","name":"file.txt","partSize":16777216,"expectedSize":4,"modTime":"2026-01-01T00:00:00Z","encryption":false,"conflictPolicy":"replace","state":"open","expiresAt":"2036-01-01T00:00:00Z","createdAt":"2026-01-01T00:00:00Z","requiresCompletionManifest":true}`)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	src := noHashSource{object.NewStaticObjectInfo("file.txt", time.Now(), 4, true, nil, nil)}
	info, err := o.prepareUpload(context.Background(), o.remote, src)
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, "upload", info.uploadID)
}

func TestPutUploadPartHashesWhileSendingWithoutChecksumHeader(t *testing.T) {
	const data = "test payload"
	var received string
	o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("X-Part-Checksum"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		received = string(body)
		_, _ = io.WriteString(w, uploadAck(data))
	})
	blocks, err := o.putUploadPart(context.Background(), &uploadInfo{uploadID: "upload"}, 1, strings.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	assert.Equal(t, data, received)
	hasher := tdhash.New().(interface {
		io.Writer
		BlockHashes() []byte
	})
	_, _ = io.WriteString(hasher, data)
	assert.Equal(t, hasher.BlockHashes(), blocks)
}

func TestPutUploadPartChannelID(t *testing.T) {
	var queries []string
	var host string
	o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/uploads/upload/parts/1", r.URL.Path)
		_, _ = io.Copy(io.Discard, r.Body)
		queries = append(queries, r.URL.RawQuery)
		host = r.Host
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, uploadAck("x"))
	})
	put := func() {
		t.Helper()
		_, err := o.putUploadPart(context.Background(), &uploadInfo{uploadID: "upload"}, 1, strings.NewReader("x"), 1)
		require.NoError(t, err)
	}
	put()
	o.fs.opt.ChannelID = 9002
	put()
	o.fs.opt.UploadHost = "http://" + host + "/"
	put()
	assert.Equal(t, []string{"", "channelId=9002", "channelId=9002"}, queries)
}

func TestB1EmptyCompletion(t *testing.T) {
	for _, expected := range []int64{0, -1} {
		o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
			var manifest api.UploadCompleteRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&manifest))
			assert.Equal(t, int64(0), manifest.Size)
			require.NotNil(t, manifest.Hash)
			assert.Equal(t, tdhash.SumString(nil), manifest.Hash.Value)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "file", "size": 0, "hash": manifest.Hash})
		})
		_, err := o.completeUpload(context.Background(), &uploadInfo{uploadID: "upload", expectedSize: expected, chunkSize: tdhash.BlockSize})
		require.NoError(t, err)
	}
}

func TestUnknownSizeResumeChecksum(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, matches := range []bool{false, true} {
			t.Run(fmt.Sprintf("hash=%v/matches=%v", enabled, matches), func(t *testing.T) {
				aborted := false
				o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, http.MethodDelete, r.Method)
					aborted = true
					w.WriteHeader(http.StatusNoContent)
				})
				o.fs.opt.HashEnabled = enabled
				data := "old!"
				if !matches {
					data = "new!"
				}
				writer := &objectChunkWriter{o: o, uploadInfo: &uploadInfo{
					uploadID: "upload", chunkSize: 4, expectedSize: -1,
					storedParts: map[int]storedPart{1: {size: 4, checksum: tdhash.SumString([]byte("old!"))}},
				}}
				n, err := writer.WriteChunk(context.Background(), 0, strings.NewReader(data))
				if matches {
					require.NoError(t, err)
					assert.Equal(t, int64(4), n)
				} else {
					require.ErrorContains(t, err, "does not match source")
				}
				assert.False(t, aborted)
			})
		}
	}
}

type uploadTestTransport func(*http.Request) (*http.Response, error)

func (f uploadTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestUploadPartEarlyResponse(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			const data = "complete payload"
			o := newUploadTestObject(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected HTTP request") })
			o.fs.opt.HashEnabled = enabled
			o.fs.srv = rest.NewClient(&http.Client{Transport: uploadTestTransport(func(r *http.Request) (*http.Response, error) {
				defer r.Body.Close()
				_, err := io.CopyN(io.Discard, r.Body, 1)
				if err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(uploadAck(data))), Request: r}, nil
			})}).SetRoot("http://fake")
			reader := strings.NewReader(data + "next part")
			blocks, err := o.putUploadPart(context.Background(), &uploadInfo{uploadID: "upload"}, 1, reader, int64(len(data)))
			require.NoError(t, err)
			if enabled {
				assert.Equal(t, tdhash.SumString([]byte(data)), hex.EncodeToString(tdhash.SumBlockHashes(blocks)))
			}
			rest, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, "next part", string(rest))
		})
	}
}

func TestUploadHashCoverage(t *testing.T) {
	data := bytes.Repeat([]byte("x"), tdhash.BlockSize+4)
	for _, missing := range []int{-1, 0, 1} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			info := &uploadInfo{expectedSize: int64(len(data)), chunkSize: tdhash.BlockSize, totalChunks: 2}
			for _, chunk := range []int{1, 0} {
				if chunk == missing {
					continue
				}
				start := chunk * tdhash.BlockSize
				part := data[start:min(start+tdhash.BlockSize, len(data))]
				h := tdhash.New()
				_, _ = h.Write(part)
				info.addBlockHashes(chunk, int64(start), int64(len(part)), h.(interface{ BlockHashes() []byte }).BlockHashes())
			}
			manifest, err := info.completionManifest()
			if missing < 0 {
				require.NoError(t, err)
				assert.Equal(t, tdhash.SumString(data), manifest.Hash.Value)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestUploadPartRetryAndShortSource(t *testing.T) {
	for _, short := range []bool{false, true} {
		t.Run(fmt.Sprint(short), func(t *testing.T) {
			attempts := 0
			o := newUploadTestObject(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected HTTP request") })
			o.fs.srv = rest.NewClient(&http.Client{Transport: uploadTestTransport(func(r *http.Request) (*http.Response, error) {
				defer r.Body.Close()
				attempts++
				status := http.StatusOK
				if !short && attempts == 1 {
					_, err := io.Copy(io.Discard, r.Body)
					require.NoError(t, err)
					status = http.StatusServiceUnavailable
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(uploadAck("retry payload"))), Request: r}, nil
			})}).SetRoot("http://fake").SetErrorHandler(errorHandler)
			data := "retry payload"
			size := int64(len(data))
			if short {
				size++
			}
			blocks, err := o.putUploadPart(context.Background(), &uploadInfo{uploadID: "upload"}, 1, strings.NewReader(data), size)
			if short {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
				assert.Empty(t, blocks)
			} else {
				require.NoError(t, err)
				assert.Equal(t, 2, attempts)
				assert.Equal(t, tdhash.SumString([]byte(data)), hex.EncodeToString(tdhash.SumBlockHashes(blocks)))
			}
		})
	}
}

func TestB1ResumeCurrentBytes(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, size := range []int64{-1, 4} {
			for _, matches := range []bool{false, true} {
				o := newUploadTestObject(t, func(http.ResponseWriter, *http.Request) { t.Error("reuse contacted server") })
				o.fs.opt.HashEnabled = enabled
				info := &uploadInfo{uploadID: "upload", chunkSize: tdhash.BlockSize, expectedSize: size, totalChunks: 1,
					storedParts: map[int]storedPart{1: {size: 4, checksum: tdhash.SumString([]byte("old!"))}}}
				w := &objectChunkWriter{o: o, uploadInfo: info}
				data := "new!"
				if matches {
					data = "old!"
				}
				_, err := w.WriteChunk(context.Background(), 0, strings.NewReader(data))
				if matches {
					require.NoError(t, err)
					manifest, err := info.completionManifest()
					require.NoError(t, err)
					assert.Equal(t, tdhash.SumString([]byte(data)), manifest.Hash.Value)
				} else {
					require.Error(t, err)
					_, err = info.completionManifest()
					require.Error(t, err)
				}
			}
		}
	}
}

func TestB1UnknownRetryCoverage(t *testing.T) {
	full := strings.Repeat("x", tdhash.BlockSize)
	for _, tc := range []struct {
		name   string
		stored []string
		chunks map[int]string
		order  []int
		ok     bool
	}{
		{"empty", nil, map[int]string{0: ""}, []int{0}, true},
		{"empty-old-tail", []string{"old"}, map[int]string{0: ""}, []int{0}, false},
		{"whole-part-short", []string{full, "tail"}, map[int]string{0: full}, []int{0}, false},
		{"partial-tail-short", []string{full, "tail"}, map[int]string{0: full, 1: "tai"}, []int{0, 1}, false},
		{"sparse", nil, map[int]string{1: "tail"}, []int{1}, false},
		{"out-of-order", nil, map[int]string{0: full, 1: "tail"}, []int{1, 0}, true},
		{"exact-full", nil, map[int]string{0: full}, []int{0}, true},
		{"short-nonfinal", nil, map[int]string{0: "x", 1: "tail"}, []int{0, 1}, false},
		{"failed-chunk", nil, map[int]string{0: "fail"}, []int{0}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			completed := 0
			var want strings.Builder
			for i := range len(tc.chunks) {
				want.WriteString(tc.chunks[i])
			}
			o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut {
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					if tc.name == "failed-chunk" {
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					var part int
					_, err = fmt.Sscanf(r.URL.Path, "/api/v1/uploads/upload/parts/%d", &part)
					require.NoError(t, err)
					_ = json.NewEncoder(w).Encode(api.UploadPart{UploadId: "upload", PartNo: part, State: "stored", PlainSize: int64(len(body)), Checksum: tdhash.SumString(body)})
					return
				}
				completed++
				var manifest api.UploadCompleteRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&manifest))
				assert.Equal(t, int64(want.Len()), manifest.Size)
				assert.Equal(t, tdhash.SumString([]byte(want.String())), manifest.Hash.Value)
				_ = json.NewEncoder(w).Encode(map[string]any{"id": "file", "size": manifest.Size, "hash": manifest.Hash})
			})
			info := &uploadInfo{uploadID: "upload", expectedSize: -1, chunkSize: tdhash.BlockSize, storedParts: make(map[int]storedPart)}
			for i, data := range tc.stored {
				info.storedParts[i+1] = storedPart{size: int64(len(data)), checksum: tdhash.SumString([]byte(data))}
			}
			writer := &objectChunkWriter{o: o, uploadInfo: info}
			var writeErr error
			for _, chunk := range tc.order {
				if _, writeErr = writer.WriteChunk(context.Background(), chunk, strings.NewReader(tc.chunks[chunk])); writeErr != nil {
					break
				}
			}
			err := writer.Close(context.Background())
			if tc.ok {
				require.NoError(t, writeErr)
				require.NoError(t, err)
				assert.Equal(t, 1, completed)
			} else {
				require.Error(t, err)
				assert.Zero(t, completed)
			}
		})
	}
}

func TestB1SequentialResumeCurrentBytes(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, matches := range []bool{false, true} {
			modTime := time.Now().UTC().Truncate(time.Microsecond)
			o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v1/uploads":
					_ = json.NewEncoder(w).Encode(api.UploadSessionPage{Items: []api.UploadSession{{
						ID: "upload", Name: "file.txt", ExpectedSize: 4, MimeType: "text/plain; charset=utf-8",
						ModTime: modTime, ConflictPolicy: "replace", PartSize: tdhash.BlockSize,
						State: "open", ExpiresAt: time.Now().Add(time.Hour), RequiresCompletionManifest: true,
					}}})
				case "/api/v1/uploads/upload":
					_ = json.NewEncoder(w).Encode(api.UploadSession{
						ID: "upload", Name: "file.txt", ExpectedSize: 4, MimeType: "text/plain; charset=utf-8",
						ModTime: modTime, ConflictPolicy: "replace", PartSize: tdhash.BlockSize,
						State: "open", ExpiresAt: time.Now().Add(time.Hour), RequiresCompletionManifest: true,
					})
				case "/api/v1/uploads/upload/parts":
					_ = json.NewEncoder(w).Encode(api.UploadPartPage{Items: []api.UploadPart{{
						UploadId: "upload", PartNo: 1, State: "stored", PlainSize: 4, Checksum: tdhash.SumString([]byte("old!")),
					}}})
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			o.fs.opt.HashEnabled = enabled
			data := "new!"
			if matches {
				data = "old!"
			}
			src := object.NewStaticObjectInfo("file.txt", modTime, 4, true, nil, nil).WithMimeType("text/plain; charset=utf-8")
			info, err := o.uploadMultipart(context.Background(), "file.txt", strings.NewReader(data), src)
			if matches {
				require.NoError(t, err)
				manifest, err := info.completionManifest()
				require.NoError(t, err)
				assert.Equal(t, tdhash.SumString([]byte(data)), manifest.Hash.Value)
			} else {
				require.Error(t, err)
			}
		}
	}
}

func TestB1HashDisabledProof(t *testing.T) {
	for _, known := range []bool{false, true} {
		for _, size := range []int{0, 4, tdhash.BlockSize} {
			t.Run(fmt.Sprintf("known=%v/size=%d", known, size), func(t *testing.T) {
				data := strings.Repeat("x", size)
				completed := 0
				o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
					switch {
					case r.Method == http.MethodGet:
						_, _ = io.WriteString(w, `{"items":[]}`)
					case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads":
						var request api.UploadCreateRequest
						require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
						_ = json.NewEncoder(w).Encode(api.UploadSession{ID: "upload", Name: request.Name, State: "open", PartSize: tdhash.BlockSize, ExpectedSize: request.Size, RequiresCompletionManifest: true})
					case r.Method == http.MethodPut:
						body, err := io.ReadAll(r.Body)
						require.NoError(t, err)
						_, _ = io.WriteString(w, uploadAck(string(body)))
					case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete"):
						completed++
						var manifest api.UploadCompleteRequest
						require.NoError(t, json.NewDecoder(r.Body).Decode(&manifest))
						require.NotNil(t, manifest.Hash)
						assert.Equal(t, int64(size), manifest.Size)
						assert.Equal(t, tdhash.SumString([]byte(data)), manifest.Hash.Value)
						_ = json.NewEncoder(w).Encode(map[string]any{"id": "file", "size": manifest.Size, "hash": manifest.Hash})
					default:
						t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
						w.WriteHeader(http.StatusBadRequest)
					}
				})
				o.fs.opt.HashEnabled = false
				expected := int64(size)
				if !known {
					expected = -1
				}
				src := object.NewStaticObjectInfo("file.txt", time.Now(), expected, true, nil, nil)
				_, err := o.fs.putUnchecked(context.Background(), strings.NewReader(data), src)
				require.NoError(t, err)
				assert.Equal(t, 1, completed)
			})
		}
	}
}

func TestB1UploadAcknowledgementConcurrentEarly(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, matches := range []bool{false, true} {
			o := newUploadTestObject(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected loopback request") })
			o.fs.opt.HashEnabled = enabled
			readDone := make(chan error, 1)
			ack := uploadAck("current")
			if !matches {
				ack = uploadAck("stale!!")
			}
			o.fs.srv = rest.NewClient(&http.Client{Transport: uploadTestTransport(func(r *http.Request) (*http.Response, error) {
				go func() {
					_, err := io.CopyN(io.Discard, r.Body, 1)
					_ = r.Body.Close()
					readDone <- err
				}()
				return &http.Response{StatusCode: http.StatusCreated, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(ack)), Request: r}, nil
			})}).SetRoot("http://fake")
			reader := strings.NewReader("currentNEXT")
			blocks, err := o.putUploadPart(context.Background(), &uploadInfo{uploadID: "upload"}, 1, reader, 7)
			require.NoError(t, <-readDone)
			if matches {
				require.NoError(t, err)
				assert.Equal(t, tdhash.SumString([]byte("current")), hex.EncodeToString(tdhash.SumBlockHashes(blocks)))
			} else {
				require.Error(t, err)
			}
			tail, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, "NEXT", string(tail))
		}
	}
}

type nonlocalUploadSource struct {
	fs.ObjectInfo
	underlying fs.Object
}

func (source nonlocalUploadSource) UnWrap() fs.Object { return source.underlying }

func TestB1LocalCompletionEntryPoints(t *testing.T) {
	for _, entry := range []string{"complete", "create-file", "chunk-close"} {
		for _, change := range []string{"unchanged", "bytes", "metadata", "missing", "unreopenable"} {
			t.Run(entry+"/"+change, func(t *testing.T) {
				ctx := context.Background()
				root := t.TempDir()
				name := filepath.Join(root, "original")
				require.NoError(t, os.WriteFile(name, []byte("test"), 0600))
				f, err := local.NewFs(ctx, "local", root, configmap.Simple{})
				require.NoError(t, err)
				src, err := f.NewObject(ctx, "original")
				require.NoError(t, err)
				when := src.ModTime(ctx)
				var source fs.ObjectInfo = fs.NewOverrideRemote(src, "file.txt")
				if change == "unreopenable" {
					source = object.NewStaticObjectInfo("file.txt", when, 4, true, nil, f)
				}
				completeCalls := 0
				o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					switch {
					case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads":
						_, _ = io.WriteString(w, `{"items":[]}`)
					case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads":
						_, _ = io.WriteString(w, `{"id":"upload","name":"file.txt","partSize":16777216,"expectedSize":4,"modTime":"2026-01-01T00:00:00Z","encryption":false,"conflictPolicy":"replace","state":"open","expiresAt":"2036-01-01T00:00:00Z","createdAt":"2026-01-01T00:00:00Z","requiresCompletionManifest":true}`)
					case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/upload/complete":
						completeCalls++
						var manifest api.UploadCompleteRequest
						require.NoError(t, json.NewDecoder(r.Body).Decode(&manifest))
						assert.Equal(t, int64(4), manifest.Size)
						assert.Equal(t, tdhash.SumString([]byte("test")), manifest.Hash.Value)
						_ = json.NewEncoder(w).Encode(map[string]any{"id": "file", "size": manifest.Size, "hash": manifest.Hash})
					default:
						t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
						w.WriteHeader(http.StatusBadRequest)
					}
				})
				info, err := o.prepareUpload(ctx, o.remote, source)
				require.NoError(t, err)
				assert.Equal(t, int64(4), info.sourceSize)
				assert.True(t, when.Equal(info.sourceTime))
				h := tdhash.New()
				_, _ = h.Write([]byte("test"))
				info.addBlockHashes(0, 0, 4, h.(interface{ BlockHashes() []byte }).BlockHashes())
				switch change {
				case "bytes":
					// Same metadata and a primed source cache cannot authorize different bytes.
					_, err = src.Hash(ctx, telDriveHash)
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(name, []byte("TEST"), 0600))
					require.NoError(t, os.Chtimes(name, when, when))
				case "metadata":
					require.NoError(t, os.Chtimes(name, when.Add(time.Hour), when.Add(time.Hour)))
				case "missing":
					require.NoError(t, os.Remove(name))
				}
				switch entry {
				case "complete":
					_, err = o.completeUpload(ctx, info)
				case "create-file":
					err = o.createFile(ctx, source, info)
				case "chunk-close":
					err = (&objectChunkWriter{o: o, uploadInfo: info}).Close(ctx)
				}
				if change == "unchanged" {
					require.NoError(t, err)
					assert.Equal(t, 1, completeCalls)
				} else {
					require.ErrorContains(t, err, "local source")
					assert.Zero(t, completeCalls)
				}
			})
		}
	}
}

func TestB1NonlocalUploadSource(t *testing.T) {
	for _, transform := range []bool{false, true} {
		t.Run(fmt.Sprint(transform), func(t *testing.T) {
			ctx := context.Background()
			var source fs.ObjectInfo = noHashSource{object.NewStaticObjectInfo("file.txt", time.Now(), 4, true, nil, nil)}
			if transform {
				root := t.TempDir()
				require.NoError(t, os.WriteFile(filepath.Join(root, "underlying"), []byte("different bytes"), 0600))
				f, err := local.NewFs(ctx, "local", root, configmap.Simple{})
				require.NoError(t, err)
				underlying, err := f.NewObject(ctx, "underlying")
				require.NoError(t, err)
				source = nonlocalUploadSource{ObjectInfo: source, underlying: underlying}
			}
			completed := false
			o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads":
					_, _ = io.WriteString(w, `{"items":[]}`)
				case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads":
					_, _ = io.WriteString(w, `{"id":"upload","name":"file.txt","partSize":16777216,"expectedSize":4,"modTime":"2026-01-01T00:00:00Z","encryption":false,"conflictPolicy":"replace","state":"open","expiresAt":"2036-01-01T00:00:00Z","createdAt":"2026-01-01T00:00:00Z","requiresCompletionManifest":true}`)
				case r.Method == http.MethodPut:
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					assert.Equal(t, "test", string(body))
					_, _ = io.WriteString(w, uploadAck("test"))
				case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/upload/complete":
					var manifest api.UploadCompleteRequest
					require.NoError(t, json.NewDecoder(r.Body).Decode(&manifest))
					assert.Equal(t, tdhash.SumString([]byte("test")), manifest.Hash.Value)
					completed = true
					_ = json.NewEncoder(w).Encode(map[string]any{"id": "file", "size": manifest.Size, "hash": manifest.Hash})
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			info, err := o.uploadMultipart(ctx, o.remote, strings.NewReader("test"), source)
			require.NoError(t, err)
			require.NoError(t, o.createFile(ctx, source, info))
			assert.True(t, completed)
		})
	}
}

func TestB2ParentFailurePropagation(t *testing.T) {
	for _, entry := range []string{"create", "part", "sequential", "update", "chunk-close"} {
		t.Run(entry, func(t *testing.T) {
			ctx := accounting.WithStatsGroup(context.Background(), t.Name())
			calls := 0
			o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet {
					_, _ = io.WriteString(w, `{"items":[]}`)
					return
				}
				if entry != "create" && r.URL.Path == "/api/v1/uploads" {
					_ = json.NewEncoder(w).Encode(api.UploadSession{ID: "upload", Name: "file.txt", State: "open", PartSize: tdhash.BlockSize, ExpectedSize: 0, RequiresCompletionManifest: true})
					return
				}
				calls++
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"error":{"code":"upload_parent_unavailable","message":"upload destination is unavailable"}}`)
			})
			src := object.NewStaticObjectInfo("file.txt", time.Now(), 0, true, nil, nil)
			var err error
			switch entry {
			case "create":
				_, err = o.prepareUpload(ctx, o.remote, src)
			case "part":
				_, err = o.putUploadPart(ctx, &uploadInfo{uploadID: "upload"}, 1, strings.NewReader("test"), 4)
			case "sequential":
				_, err = o.fs.putUnchecked(ctx, strings.NewReader(""), src)
			case "update":
				err = o.Update(ctx, strings.NewReader(""), src)
			case "chunk-close":
				err = (&objectChunkWriter{o: o, uploadInfo: &uploadInfo{uploadID: "upload"}}).Close(ctx)
			}
			require.Error(t, err)
			require.True(t, fserrors.IsFatalError(err))
			var decoded *apiError
			require.ErrorAs(t, err, &decoded)
			assert.Equal(t, "upload_parent_unavailable", decoded.Code)
			assert.Equal(t, 1, calls)
			stats := accounting.Stats(ctx)
			stats.Error(fserrors.RetryError(errors.New("other file")))
			stats.Error(fmt.Errorf("transfer failed: %w", err))
			assert.True(t, stats.HadRetryError())
			assert.True(t, stats.HadFatalError())
		})
	}
	for _, test := range []struct {
		status   int
		body     string
		retry    bool
		sentinel error
	}{
		{409, `{"error":{"code":"conflict"}}`, false, nil},
		{503, `{"error":{"code":"upload_parent_unavailable"}}`, true, nil},
		{401, `{}`, false, fs.ErrorPermissionDenied},
		{403, `{}`, false, fs.ErrorPermissionDenied},
		{404, `{}`, false, fs.ErrorObjectNotFound},
		{412, `{}`, false, fs.ErrorImmutableModified},
		{409, `{"error":`, true, nil},
		{409, `not json`, false, nil},
	} {
		t.Run(fmt.Sprintf("status=%d/body=%s", test.status, test.body), func(t *testing.T) {
			resp := &http.Response{StatusCode: test.status, Body: io.NopCloser(strings.NewReader(test.body))}
			err := errorHandler(resp)
			assert.False(t, fserrors.IsFatalError(err))
			retry, _ := shouldRetry(context.Background(), resp, err)
			assert.Equal(t, test.retry, retry)
			if test.sentinel != nil {
				assert.ErrorIs(t, err, test.sentinel)
			}
		})
	}
}

// A hard max-transfer limit is a fatal error: the chunk writer must return it
// rather than report the bytes as written.
func TestWriteChunkPropagatesAccountingError(t *testing.T) {
	data := "accounting payload"
	for _, test := range []struct {
		name       string
		accountErr error
	}{
		{"fatal", accounting.ErrorMaxTransferLimitReachedFatal},
		{"canceled", context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := newUploadTestObject(t, func(http.ResponseWriter, *http.Request) {
				t.Error("matching stored part contacted the server")
			})
			info := &uploadInfo{
				uploadID:     "upload",
				chunkSize:    tdhash.BlockSize,
				expectedSize: int64(len(data)),
				totalChunks:  1,
				storedParts:  map[int]storedPart{1: {size: int64(len(data)), checksum: tdhash.SumString([]byte(data))}},
			}
			src := object.NewMemoryObject("file.txt", time.Now(), []byte(data))
			r, err := operations.NewReOpen(context.Background(), src, 1)
			require.NoError(t, err)
			t.Cleanup(func() { _ = r.Close() })
			r.SetAccounting(func(int) error { return test.accountErr })

			w := &objectChunkWriter{o: o, uploadInfo: info}
			n, err := w.WriteChunk(context.Background(), 0, r)
			require.Error(t, err)
			assert.ErrorIs(t, err, test.accountErr)
			assert.Zero(t, n)
			assert.True(t, info.hashInvalid)
		})
	}
}
