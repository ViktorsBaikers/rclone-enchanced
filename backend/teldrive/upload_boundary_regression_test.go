package teldrive

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestB2UploadNullCursor(t *testing.T) {
	for _, partPage := range []bool{false, true} {
		name := "sessions"
		if partPage {
			name = "parts"
		}
		t.Run(name, func(t *testing.T) {
			ctx := b2CommandContext(t)
			when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			session := api.UploadSession{ID: "30000000-0000-4000-8000-000000000001", Name: "file.txt", ExpectedSize: 0, MimeType: "text/plain; charset=utf-8", ModTime: when, ConflictPolicy: "replace", State: "open", ExpiresAt: when.AddDate(10, 0, 0), PartSize: tdhash.BlockSize, RequiresCompletionManifest: true}
			creates := 0
			o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == "GET" && r.URL.Path == "/api/v1/uploads":
					if !partPage {
						_, _ = io.WriteString(w, `{"items":[],"nextCursor":null}`)
					} else {
						_ = json.NewEncoder(w).Encode(api.UploadSessionPage{Items: []api.UploadSession{session}})
					}
				case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/parts"):
					_, _ = io.WriteString(w, `{"items":[],"nextCursor":null}`)
				case r.Method == "GET" && r.URL.Path == "/api/v1/uploads/"+session.ID:
					_ = json.NewEncoder(w).Encode(session)
				case r.Method == "POST":
					creates++
					_ = json.NewEncoder(w).Encode(session)
				default:
					w.WriteHeader(400)
				}
			})
			_, err := o.prepareUpload(ctx, "file.txt", noHashSource{object.NewStaticObjectInfo("file.txt", when, 0, true, nil, nil)})
			require.Error(t, err)
			assert.Zero(t, creates)
		})
	}
}

func TestB2UnknownUpdateCompletionIdentity(t *testing.T) {
	ctx := b2CommandContext(t)
	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	const uploadID = "30000000-0000-4000-8000-000000000001"
	const resultID = "30000000-0000-4000-8000-000000000002"
	lookups, completed := 0, 0
	o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "GET" && r.URL.Path == "/api/v1/uploads":
			_ = json.NewEncoder(w).Encode(api.UploadSessionPage{Items: []api.UploadSession{}})
		case r.Method == "POST" && r.URL.Path == "/api/v1/uploads":
			var request api.UploadCreateRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			assert.Equal(t, "file.txt", request.Name)
			_ = json.NewEncoder(w).Encode(api.UploadSession{ID: uploadID, Name: request.Name, ExpectedSize: -1, MimeType: request.MimeType, ModTime: when, ConflictPolicy: request.ConflictPolicy, State: "open", ExpiresAt: when.AddDate(10, 0, 0), PartSize: tdhash.BlockSize, RequiresCompletionManifest: true})
		case r.Method == "PUT":
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			assert.Equal(t, "data", string(body))
			_ = json.NewEncoder(w).Encode(api.UploadPart{UploadId: uploadID, PartNo: 1, State: "stored", PlainSize: 4, Checksum: tdhash.SumString(body)})
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/complete"):
			completed++
			_ = json.NewEncoder(w).Encode(api.FileInfo{Id: resultID, Kind: "file", Name: "file.txt", Size: 4, ModTime: when, Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString([]byte("data"))}})
		case r.Method == "GET" && r.URL.Path == "/api/v1/files":
			lookups++
			_ = json.NewEncoder(w).Encode(api.ReadMetadataResponse{Files: []api.FileInfo{{Id: "replacement", Kind: "file", Name: "file.txt", Size: 4, ModTime: when, Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString([]byte("evil"))}}}})
		default:
			w.WriteHeader(400)
		}
	})
	o.id = "existing"
	o.fs.opt.HashEnabled = false
	source := object.NewStaticObjectInfo("source.bin", when, -1, true, nil, nil).WithMimeType("application/octet-stream")
	require.NoError(t, o.Update(ctx, strings.NewReader("data"), source))
	assert.Equal(t, resultID, o.id)
	assert.Equal(t, "file.txt", o.remote)
	assert.Equal(t, 1, completed)
	assert.Zero(t, lookups)
}

type b2ResponseReadError struct{ err error }

func (r b2ResponseReadError) Read([]byte) (int, error) { return 0, r.err }
func (b2ResponseReadError) Close() error               { return nil }

func TestB2UploadErrorCauses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		sentinel error
	}{{"unauthorized", 401, fs.ErrorPermissionDenied}, {"forbidden", 403, fs.ErrorPermissionDenied}, {"missing", 404, fs.ErrorObjectNotFound}, {"immutable", 412, fs.ErrorImmutableModified}} {
		t.Run(tc.name, func(t *testing.T) {
			err := errorHandler(&http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(`{"error":`))})
			require.ErrorIs(t, err, tc.sentinel)
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		})
	}
	t.Run("nonretryable_read", func(t *testing.T) {
		cause := errors.New("synthetic response read failure")
		err := errorHandler(&http.Response{StatusCode: 400, Body: b2ResponseReadError{cause}})
		require.ErrorIs(t, err, cause)
	})
}

func TestB2AdmissionCancellationCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(b2CommandContext(t))
	cause := errors.New("synthetic canceled admission")
	cancel(cause)
	o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("canceled admission dispatched HTTP")
		w.WriteHeader(500)
	})
	_, err := o.fs.Put(ctx, strings.NewReader("data"), b2AdmissionSource())
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

type b2TerminalPutFs struct {
	*Fs
	failure error
	calls   int
}

func (f *b2TerminalPutFs) Put(context.Context, io.Reader, fs.ObjectInfo, ...fs.OpenOption) (fs.Object, error) {
	f.calls++
	return nil, f.failure
}

func TestB2RcatTerminalRetryStop(t *testing.T) {
	for _, spooled := range []bool{false, true} {
		name := "buffered"
		if spooled {
			name = "spooled"
		}
		t.Run(name, func(t *testing.T) {
			for _, kind := range []string{"fatal", "no_retry", "fatal_wrapped_retry", "no_retry_wrapped_retry"} {
				t.Run(kind, func(t *testing.T) {
					ctx, ci := fs.AddConfig(context.Background())
					ci.LowLevelRetries = 3
					ci.StreamingUploadCutoff = 64
					if spooled {
						ci.StreamingUploadCutoff = 1
					}
					x := newB2DiscoveryOracle(t, ctx)
					var cause error = io.ErrUnexpectedEOF
					if strings.Contains(kind, "wrapped_retry") {
						cause = fserrors.RetryError(cause)
					}
					if strings.HasPrefix(kind, "fatal") {
						cause = fserrors.FatalError(cause)
					} else {
						cause = fserrors.NoRetryError(cause)
					}
					f := &b2TerminalPutFs{Fs: x.f, failure: cause}
					f.features.PutStream = nil
					_, err := operations.RcatSize(ctx, f, "file.txt", io.NopCloser(strings.NewReader("data")), -1, time.Now(), nil)
					require.ErrorIs(t, err, io.ErrUnexpectedEOF)
					assert.Equal(t, 1, f.calls)
				})
			}
		})
	}
}

type b2BoundaryObject struct {
	fs.Object
	open func() (io.ReadCloser, error)
}

func (o b2BoundaryObject) Open(context.Context, ...fs.OpenOption) (io.ReadCloser, error) {
	return o.open()
}
func (o b2BoundaryObject) Fs() fs.Info { return nil }

type b2BoundaryReadCloser struct {
	io.Reader
	closeErr error
}

func (r b2BoundaryReadCloser) Close() error { return r.closeErr }

func TestB2DigestCancellationCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause, closeCause := errors.New("synthetic digest cancellation"), errors.New("synthetic digest close failure")
	result := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("canceled digest reached HTTP")
		w.WriteHeader(500)
	})
	source := b2BoundaryObject{Object: result, open: func() (io.ReadCloser, error) {
		cancel(cause)
		return b2BoundaryReadCloser{strings.NewReader("data"), closeCause}, nil
	}}
	err := result.fs.verifyAdmissionCopy(ctx, source, result)
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, closeCause)
}

func TestB2SourceTerminalRetryStop(t *testing.T) {
	for _, kind := range []string{"fatal", "no_retry", "no_low_level"} {
		t.Run(kind, func(t *testing.T) {
			base := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) { t.Error("source retry reached HTTP"); w.WriteHeader(500) })
			var cause error = fserrors.RetryError(io.ErrUnexpectedEOF)
			switch kind {
			case "fatal":
				cause = fserrors.FatalError(cause)
			case "no_retry":
				cause = fserrors.NoRetryError(cause)
			default:
				cause = fserrors.NoLowLevelRetryError(cause)
			}
			opens := 0
			source := b2BoundaryObject{Object: base, open: func() (io.ReadCloser, error) { opens++; return io.NopCloser(b2ResponseReadError{cause}), nil }}
			reader, err := operations.NewReOpen(context.Background(), source, 3)
			require.NoError(t, err)
			defer reader.Close()
			_, err = reader.Read(make([]byte, 4))
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			assert.Equal(t, 1, opens)
		})
	}
}
