package teldrive

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const guardedIncarnationFixture = "11111111-1111-4111-8111-111111111111"

var guardedModTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newGuardedTestObject(t *testing.T, handler http.HandlerFunc) *Object {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	ctx := context.Background()
	f := &Fs{
		opt:   Options{HashEnabled: true, ChunkSize: defaultChunkSize, GuardedUpload: true},
		pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
	}
	f.srv = rest.NewClient(server.Client()).SetRoot(server.URL).SetErrorHandler(errorHandler)
	f.dirCache = dircache.New("", rootID, f)
	return &Object{fs: f, remote: "file.txt"}
}

func guardedStaticSource(size int64) fs.ObjectInfo {
	return object.NewStaticObjectInfo("file.txt", guardedModTime, size, true, nil, nil)
}

func writeGuardedJSON(t *testing.T, w http.ResponseWriter, status int, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	require.NoError(t, json.NewEncoder(w).Encode(value))
}

func guardedSessionBody(id, name, parentID string, size int64, mimeType, state string) map[string]any {
	return map[string]any{
		"id": id, "name": name, "parentId": parentID, "expectedSize": size, "mimeType": mimeType,
		"modTime": guardedModTime.Format(time.RFC3339Nano), "encryption": false,
		"conflictPolicy": "fail", "partSize": tdhash.BlockSize,
		"requiresCompletionManifest": true, "state": state,
		"expiresAt": "2036-01-01T00:00:00Z", "createdAt": "2026-01-01T00:00:00Z",
	}
}

func guardedEntryBody(id, name string, size int64, hash string) map[string]any {
	return map[string]any{
		"id": id, "name": name, "kind": "file", "mimeType": "", "size": size,
		"hash":       map[string]any{"algorithm": "blake3", "value": hash},
		"encryption": false, "status": "active", "modTime": guardedModTime.Format(time.RFC3339Nano),
		"generation": 1, "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z",
		"parentSegments": []string{},
	}
}

func guardedErrorBody(code string) map[string]any {
	return map[string]any{"error": map[string]any{"code": code, "message": code}}
}

func TestB3GuardedClientTracer(t *testing.T) {
	const data = "guarded payload"
	var mu sync.Mutex
	var discoveryCalls, createCalls, statusCalls, partCalls, completeCalls int
	var createKey, completeKey, createIncarnation, partIncarnation, completeIncarnation string
	var createBody api.GuardedUploadCreateRequest
	var completeBody api.UploadCompleteRequest

	o := newGuardedTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded":
			discoveryCalls++
			w.Header().Set(recoveryIncarnationHeader, guardedIncarnationFixture)
			writeGuardedJSON(t, w, http.StatusOK, map[string]any{})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files":
			writeGuardedJSON(t, w, http.StatusOK, map[string]any{"items": []any{}, "nextCursor": ""})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/guarded":
			createCalls++
			createKey = r.Header.Get("Idempotency-Key")
			createIncarnation = r.Header.Get(recoveryIncarnationHeader)
			require.NoError(t, json.NewDecoder(r.Body).Decode(&createBody))
			writeGuardedJSON(t, w, http.StatusCreated, guardedSessionBody("upload", createBody.Name, "", createBody.Size, createBody.MimeType, "open"))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded/upload":
			statusCalls++
			w.Header().Set(recoveryIncarnationHeader, guardedIncarnationFixture)
			w.Header().Set(uploadGuardHeader, "1")
			writeGuardedJSON(t, w, http.StatusOK, guardedSessionBody("upload", "file.txt", "", int64(len(data)), "text/plain; charset=utf-8", "open"))
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/uploads/guarded/upload/parts/1":
			partCalls++
			partIncarnation = r.Header.Get(recoveryIncarnationHeader)
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			assert.Equal(t, data, string(body))
			assert.Equal(t, int64(len(data)), r.ContentLength)
			writeGuardedJSON(t, w, http.StatusCreated, map[string]any{
				"uploadId": "upload", "partNo": 1, "state": "stored",
				"plainSize": len(data), "checksum": tdhash.SumString([]byte(data)),
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/guarded/upload/complete":
			completeCalls++
			completeKey = r.Header.Get("Idempotency-Key")
			completeIncarnation = r.Header.Get(recoveryIncarnationHeader)
			require.NoError(t, json.NewDecoder(r.Body).Decode(&completeBody))
			w.Header().Set("Etag", `"1"`)
			w.Header().Set("Location", "/files/file-1")
			writeGuardedJSON(t, w, http.StatusOK, guardedEntryBody("file-1", "file.txt", int64(len(data)), tdhash.SumString([]byte(data))))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	})

	src := guardedStaticSource(int64(len(data)))
	info, err := o.uploadMultipart(context.Background(), o.remote, strings.NewReader(data), src)
	require.NoError(t, err)
	file, err := o.completeUpload(context.Background(), info)
	require.NoError(t, err)
	assert.Equal(t, "file-1", file.Id)
	assert.Equal(t, "file-1", o.id)

	assert.Equal(t, 1, discoveryCalls)
	assert.Equal(t, 1, createCalls)
	assert.Equal(t, 1, statusCalls)
	assert.Equal(t, 1, partCalls)
	assert.Equal(t, 1, completeCalls)
	assert.Equal(t, guardedIncarnationFixture, createIncarnation)
	assert.Equal(t, guardedIncarnationFixture, partIncarnation)
	assert.Equal(t, guardedIncarnationFixture, completeIncarnation)
	assert.NotEmpty(t, createKey)
	assert.NotEmpty(t, completeKey)
	assert.NotEqual(t, createKey, completeKey)
	_, err = uuid.Parse(createKey)
	require.NoError(t, err)
	assert.Nil(t, createBody.Destination.MountRoot)
	assert.Nil(t, createBody.Destination.ParentID)
	assert.Equal(t, api.GuardedSlotExpectation{Kind: "absent"}, createBody.Destination.Expected)
	assert.Equal(t, "file.txt", createBody.Name)
	assert.Equal(t, int64(len(data)), createBody.Size)
	require.NotNil(t, completeBody.Hash)
	assert.Equal(t, int64(len(data)), completeBody.Size)
	assert.Equal(t, tdhash.SumString([]byte(data)), completeBody.Hash.Value)
}

func TestB3GuardedClientSkew(t *testing.T) {
	t.Run("retry reuses frozen create", func(t *testing.T) {
		const data = "retry payload"
		var mu sync.Mutex
		var keys []string
		var bodies []string
		o := newGuardedTestObject(t, func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded":
				w.Header().Set(recoveryIncarnationHeader, guardedIncarnationFixture)
				writeGuardedJSON(t, w, http.StatusOK, map[string]any{})
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files":
				writeGuardedJSON(t, w, http.StatusOK, map[string]any{"items": []any{}, "nextCursor": ""})
			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/guarded":
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				keys = append(keys, r.Header.Get("Idempotency-Key"))
				bodies = append(bodies, string(body))
				if len(keys) == 1 {
					writeGuardedJSON(t, w, http.StatusInternalServerError, guardedErrorBody("internal"))
					return
				}
				writeGuardedJSON(t, w, http.StatusCreated, guardedSessionBody("upload", "file.txt", "", int64(len(data)), "text/plain; charset=utf-8", "open"))
			default:
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusBadRequest)
			}
		})
		admission := &uploadAdmission{kind: "upload", key: uuid.NewString()}
		src := guardedStaticSource(int64(len(data)))
		info, err := o.prepareGuardedUpload(context.Background(), admission, admission.snapshot(), o.remote, src)
		require.NoError(t, err)
		assert.Equal(t, "upload", info.uploadID)
		require.Len(t, keys, 2)
		assert.Equal(t, keys[0], keys[1])
		assert.Equal(t, bodies[0], bodies[1])
	})

	t.Run("recovery mismatch stops the attempt", func(t *testing.T) {
		const data = "stale payload"
		var mu sync.Mutex
		var creates, parts, completes, mkdirs int
		o := newGuardedTestObject(t, func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded":
				w.Header().Set(recoveryIncarnationHeader, guardedIncarnationFixture)
				writeGuardedJSON(t, w, http.StatusOK, map[string]any{})
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files":
				writeGuardedJSON(t, w, http.StatusOK, map[string]any{"items": []any{}, "nextCursor": ""})
			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/guarded":
				creates++
				writeGuardedJSON(t, w, http.StatusPreconditionFailed, guardedErrorBody("upload_recovery_changed"))
			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/folders":
				mkdirs++
				w.WriteHeader(http.StatusBadRequest)
			case strings.HasPrefix(r.URL.Path, "/api/v1/uploads/"):
				if r.Method == http.MethodPut {
					parts++
				} else {
					completes++
				}
				w.WriteHeader(http.StatusBadRequest)
			default:
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusBadRequest)
			}
		})
		src := guardedStaticSource(int64(len(data)))
		_, err := o.uploadMultipart(context.Background(), o.remote, strings.NewReader(data), src)
		require.Error(t, err)
		assert.ErrorIs(t, err, errUploadRecoveryChanged)
		assert.Equal(t, 1, creates)
		assert.Zero(t, parts)
		assert.Zero(t, completes)
		assert.Zero(t, mkdirs)
		state := o.fs.guardedState()
		state.mu.Lock()
		cached := state.value
		state.mu.Unlock()
		assert.Empty(t, cached)
	})

	t.Run("part guard mismatch consumes no body", func(t *testing.T) {
		const data = "part payload"
		var mu sync.Mutex
		var creates, parts int
		o := newGuardedTestObject(t, func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded":
				w.Header().Set(recoveryIncarnationHeader, guardedIncarnationFixture)
				writeGuardedJSON(t, w, http.StatusOK, map[string]any{})
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files":
				writeGuardedJSON(t, w, http.StatusOK, map[string]any{"items": []any{}, "nextCursor": ""})
			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/guarded":
				creates++
				var body api.GuardedUploadCreateRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				writeGuardedJSON(t, w, http.StatusCreated, guardedSessionBody("upload", body.Name, "", body.Size, body.MimeType, "open"))
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded/upload":
				writeGuardedJSON(t, w, http.StatusPreconditionFailed, guardedErrorBody("upload_recovery_changed"))
			case r.Method == http.MethodPut && r.URL.Path == "/api/v1/uploads/guarded/upload/parts/1":
				parts++
				w.WriteHeader(http.StatusCreated)
			default:
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusBadRequest)
			}
		})
		reader := &countingReader{Reader: strings.NewReader(data)}
		src := guardedStaticSource(int64(len(data)))
		_, err := o.uploadMultipart(context.Background(), o.remote, reader, src)
		require.ErrorIs(t, err, errUploadRecoveryChanged)
		assert.Equal(t, 1, creates)
		assert.Zero(t, parts)
		assert.Zero(t, reader.n)
	})
}

type countingReader struct {
	io.Reader
	n int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.n += int64(n)
	return n, err
}

func TestB3GuardedClientReplay(t *testing.T) {
	const data = "replayed payload"
	var mu sync.Mutex
	var createKey string
	var creates, statusCalls, completes int
	var completed bool

	o := newGuardedTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded":
			w.Header().Set(recoveryIncarnationHeader, guardedIncarnationFixture)
			writeGuardedJSON(t, w, http.StatusOK, map[string]any{})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files":
			writeGuardedJSON(t, w, http.StatusOK, map[string]any{"items": []any{}, "nextCursor": ""})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/guarded":
			creates++
			createKey = r.Header.Get("Idempotency-Key")
			writeGuardedJSON(t, w, http.StatusCreated, guardedSessionBody("upload", "file.txt", "", int64(len(data)), "text/plain; charset=utf-8", "open"))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded/upload":
			statusCalls++
			w.Header().Set(recoveryIncarnationHeader, guardedIncarnationFixture)
			w.Header().Set(uploadGuardHeader, "1")
			session := guardedSessionBody("upload", "file.txt", "", int64(len(data)), "text/plain; charset=utf-8", "open")
			if completed {
				session["state"] = "completed"
				session["fileId"] = "file-1"
				session["completedReceipt"] = map[string]any{
					"admissionKey": createKey, "uploadId": "upload", "size": len(data),
					"hash":   map[string]any{"algorithm": "blake3", "value": tdhash.SumString([]byte(data))},
					"result": guardedEntryBody("file-1", "file.txt", int64(len(data)), tdhash.SumString([]byte(data))),
				}
			}
			writeGuardedJSON(t, w, http.StatusOK, session)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/guarded/upload/complete":
			completes++
			w.Header().Set("Etag", `"1"`)
			w.Header().Set("Location", "/files/file-1")
			writeGuardedJSON(t, w, http.StatusOK, guardedEntryBody("file-1", "file.txt", int64(len(data)), tdhash.SumString([]byte(data))))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	})

	admission := &uploadAdmission{kind: "upload", key: uuid.NewString()}
	src := guardedStaticSource(int64(len(data)))
	first, err := o.prepareGuardedUpload(context.Background(), admission, admission.snapshot(), o.remote, src)
	require.NoError(t, err)
	require.Empty(t, first.guardedReceipt)

	mu.Lock()
	completed = true
	mu.Unlock()

	second, err := o.prepareGuardedUpload(context.Background(), admission, admission.snapshot(), o.remote, src)
	require.NoError(t, err)
	require.NotNil(t, second.guardedReceipt)
	require.NoError(t, verifyGuardedReceiptSource(context.Background(), strings.NewReader(data), second))
	second.receiptVerified = true
	file, err := o.completeUpload(context.Background(), second)
	require.NoError(t, err)
	assert.Equal(t, "file-1", file.Id)

	assert.Equal(t, 1, creates)
	assert.Equal(t, 1, statusCalls)
	assert.Zero(t, completes)

	wrong, err := o.prepareGuardedUpload(context.Background(), admission, admission.snapshot(), o.remote, src)
	require.NoError(t, err)
	assert.ErrorIs(t, verifyGuardedReceiptSource(context.Background(), strings.NewReader("wrong payload"), wrong), errAdmissionConflict)
}

func newGuardedFolderObject(t *testing.T, handler http.HandlerFunc) *Object {
	t.Helper()
	o := newGuardedTestObject(t, handler)
	o.remote = "dir/file.txt"
	require.NoError(t, o.fs.dirCache.FindRoot(context.Background(), false))
	o.fs.dirCache.Put("dir", "parent-1")
	return o
}

func TestB3GuardedDestinationIdentity(t *testing.T) {
	t.Run("same UUID moved inside mount is followed by UUID", func(t *testing.T) {
		var mu sync.Mutex
		var nameListings, parentFetches, creates, statusCalls int
		var createBody api.GuardedUploadCreateRequest
		o := newGuardedFolderObject(t, func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded":
				w.Header().Set(recoveryIncarnationHeader, guardedIncarnationFixture)
				writeGuardedJSON(t, w, http.StatusOK, map[string]any{})
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files/parent-1":
				parentFetches++
				writeGuardedJSON(t, w, http.StatusOK, map[string]any{
					"id": "parent-1", "parentId": "moved-1", "name": "renamed-dir", "kind": "folder",
					"encryption": false, "status": "active", "modTime": guardedModTime.Format(time.RFC3339Nano),
					"generation": 1, "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z",
				})
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files/moved-1":
				writeGuardedJSON(t, w, http.StatusOK, map[string]any{
					"id": "moved-1", "name": "moved", "kind": "folder",
					"encryption": false, "status": "active", "modTime": guardedModTime.Format(time.RFC3339Nano),
					"generation": 1, "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z",
				})
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files":
				if r.URL.Query().Get("parentId") == "" {
					nameListings++
				}
				writeGuardedJSON(t, w, http.StatusOK, map[string]any{"items": []any{}, "nextCursor": ""})
			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/guarded":
				creates++
				require.NoError(t, json.NewDecoder(r.Body).Decode(&createBody))
				writeGuardedJSON(t, w, http.StatusCreated, guardedSessionBody("upload", createBody.Name, "parent-1", createBody.Size, createBody.MimeType, "open"))
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded/upload":
				statusCalls++
				w.Header().Set(recoveryIncarnationHeader, guardedIncarnationFixture)
				w.Header().Set(uploadGuardHeader, "1")
				writeGuardedJSON(t, w, http.StatusOK, guardedSessionBody("upload", createBody.Name, "parent-1", createBody.Size, createBody.MimeType, "open"))
			default:
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusBadRequest)
			}
		})
		admission := &uploadAdmission{kind: "upload", key: uuid.NewString()}
		src := object.NewStaticObjectInfo("file.txt", guardedModTime, 4, true, nil, nil)
		_, err := o.prepareGuardedUpload(context.Background(), admission, admission.snapshot(), o.remote, src)
		require.NoError(t, err)
		require.NotNil(t, createBody.Destination.ParentID)
		assert.Equal(t, "parent-1", *createBody.Destination.ParentID)
		assert.Zero(t, nameListings)
		assert.Equal(t, 1, parentFetches)
		assert.Equal(t, 1, creates)

		// A retained attempt probes the selected session; it must never
		// re-resolve or re-create the captured destination.
		_, err = o.prepareGuardedUpload(context.Background(), admission, admission.snapshot(), o.remote, src)
		require.NoError(t, err)
		assert.Equal(t, 1, statusCalls)
		assert.Equal(t, 1, parentFetches)
		assert.Equal(t, 1, creates)
	})

	t.Run("bare path match is refused", func(t *testing.T) {
		var mu sync.Mutex
		var creates, nameListings int
		o := newGuardedFolderObject(t, func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded":
				w.Header().Set(recoveryIncarnationHeader, guardedIncarnationFixture)
				writeGuardedJSON(t, w, http.StatusOK, map[string]any{})
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files/parent-1":
				writeGuardedJSON(t, w, http.StatusNotFound, guardedErrorBody("not_found"))
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files":
				nameListings++
				writeGuardedJSON(t, w, http.StatusOK, map[string]any{
					"items": []any{map[string]any{
						"id": "other-folder", "name": "dir", "kind": "folder", "encryption": false,
						"status": "active", "modTime": guardedModTime.Format(time.RFC3339Nano), "generation": 1,
						"createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z",
					}},
					"nextCursor": "",
				})
			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/guarded":
				creates++
				w.WriteHeader(http.StatusCreated)
			default:
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusBadRequest)
			}
		})
		admission := &uploadAdmission{kind: "upload", key: uuid.NewString()}
		src := object.NewStaticObjectInfo("file.txt", guardedModTime, 4, true, nil, nil)
		_, err := o.prepareGuardedUpload(context.Background(), admission, admission.snapshot(), o.remote, src)
		require.Error(t, err)
		assert.ErrorIs(t, err, errUploadDestinationChanged)
		assert.Zero(t, creates)
		assert.Zero(t, nameListings)
	})
}

func TestB3GuardedClientOrdinaryUploadUnchanged(t *testing.T) {
	var guardedCalls int
	o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/v1/uploads/guarded"):
			guardedCalls++
			w.WriteHeader(http.StatusBadRequest)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads":
			writeGuardedJSON(t, w, http.StatusOK, map[string]any{"items": []any{}, "nextCursor": ""})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads":
			var request api.UploadCreateRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			writeGuardedJSON(t, w, http.StatusCreated, guardedSessionBody("upload", request.Name, "", request.Size, request.MimeType, "open"))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	src := noHashSource{object.NewStaticObjectInfo("file.txt", guardedModTime, 4, true, nil, nil)}
	info, err := o.prepareUpload(context.Background(), o.remote, src)
	require.NoError(t, err)
	assert.Equal(t, "upload", info.uploadID)
	assert.Nil(t, info.guarded)
	assert.Zero(t, guardedCalls)
}

func TestB3GuardedClientUploadHost(t *testing.T) {
	const data = "host payload"
	var mu sync.Mutex
	var uploadStatus, uploadPart int
	var partIncarnation string
	uploadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded/upload":
			uploadStatus++
			assert.Equal(t, guardedIncarnationFixture, r.Header.Get(recoveryIncarnationHeader))
			w.Header().Set(recoveryIncarnationHeader, guardedIncarnationFixture)
			w.Header().Set(uploadGuardHeader, "1")
			writeGuardedJSON(t, w, http.StatusOK, guardedSessionBody("upload", "file.txt", "", int64(len(data)), "text/plain; charset=utf-8", "open"))
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/uploads/guarded/upload/parts/1":
			uploadPart++
			partIncarnation = r.Header.Get(recoveryIncarnationHeader)
			_, err := io.Copy(io.Discard, r.Body)
			require.NoError(t, err)
			writeGuardedJSON(t, w, http.StatusCreated, map[string]any{
				"uploadId": "upload", "partNo": 1, "state": "stored",
				"plainSize": len(data), "checksum": tdhash.SumString([]byte(data)),
			})
		default:
			t.Errorf("unexpected upload-host request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(uploadServer.Close)

	o := newGuardedTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads/guarded":
			w.Header().Set(recoveryIncarnationHeader, guardedIncarnationFixture)
			writeGuardedJSON(t, w, http.StatusOK, map[string]any{})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files":
			writeGuardedJSON(t, w, http.StatusOK, map[string]any{"items": []any{}, "nextCursor": ""})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/guarded":
			var body api.GuardedUploadCreateRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			writeGuardedJSON(t, w, http.StatusCreated, guardedSessionBody("upload", body.Name, "", body.Size, body.MimeType, "open"))
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads/guarded/upload/complete":
			w.Header().Set("Etag", `"1"`)
			w.Header().Set("Location", "/files/file-1")
			writeGuardedJSON(t, w, http.StatusOK, guardedEntryBody("file-1", "file.txt", int64(len(data)), tdhash.SumString([]byte(data))))
		default:
			t.Errorf("unexpected api-host request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	o.fs.opt.UploadHost = uploadServer.URL

	src := guardedStaticSource(int64(len(data)))
	info, err := o.uploadMultipart(context.Background(), o.remote, strings.NewReader(data), src)
	require.NoError(t, err)
	_, err = o.completeUpload(context.Background(), info)
	require.NoError(t, err)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, uploadStatus)
	assert.Equal(t, 1, uploadPart)
	assert.Equal(t, guardedIncarnationFixture, partIncarnation)
}
