package vfscache

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rclone/rclone/backend/teldrive"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/stretchr/testify/require"
)

type b2ContendingItemStore struct {
	fs.UploadAdmissionStore
	ready   chan<- struct{}
	release <-chan struct{}
}

func (s b2ContendingItemStore) StoreUploadAdmission(ctx context.Context, state fs.UploadAdmissionState) error {
	select {
	case s.ready <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.UploadAdmissionStore.StoreUploadAdmission(ctx, state)
}

func TestB2VFSJournalContention(t *testing.T) {
	for _, mode := range []string{"adoption_vs_adoption", "adoption_vs_freeze", "stale_generation"} {
		t.Run(mode, func(t *testing.T) {
			var scans, gets, posts, parts atomic.Int32
			ids := []string{uuid.NewString(), uuid.NewString()}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.URL.Path == "/api/v1/me":
					_ = json.NewEncoder(w).Encode(api.UserProfile{UserId: 42})
				case r.Method == "GET" && r.URL.Path == "/api/v1/files":
					_ = json.NewEncoder(w).Encode(api.ReadMetadataResponse{})
				case r.Method == "GET" && r.URL.Path == "/api/v1/uploads":
					index := int(scans.Add(1)) - 1
					items := []api.UploadSession{}
					if mode != "adoption_vs_freeze" || index == 0 {
						items = append(items, api.UploadSession{ID: ids[index%len(ids)], Name: "file.txt", ExpectedSize: 4,
							MimeType: "text/plain; charset=utf-8", ModTime: b2CASModTime, ConflictPolicy: "replace", PartSize: tdhash.BlockSize,
							State: "open", RequiresCompletionManifest: true, CreatedAt: b2CASModTime, ExpiresAt: b2CASModTime.AddDate(10, 0, 0)})
					}
					_ = json.NewEncoder(w).Encode(api.UploadSessionPage{Items: items})
				case strings.Contains(r.URL.Path, "/parts"):
					parts.Add(1)
					w.WriteHeader(http.StatusBadRequest)
				case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/v1/uploads/"):
					gets.Add(1)
					w.WriteHeader(http.StatusServiceUnavailable)
				case r.Method == "POST" && r.URL.Path == "/api/v1/uploads":
					posts.Add(1)
					w.WriteHeader(http.StatusServiceUnavailable)
				default:
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			ctx, c := b2OwnedCache(t, t.TempDir(), server.URL)
			item, _ := c.get("file.txt")
			require.NoError(t, item.Open(nil))
			_, err := item.WriteAt([]byte("data"), 0)
			require.NoError(t, err)
			require.NoError(t, item.Close(nil))
			c.SetModTime(item.name, b2CASModTime)
			source, err := c.fcache.NewObject(ctx, item.name)
			require.NoError(t, err)
			require.NoError(t, source.SetModTime(ctx, b2CASModTime))
			source, err = c.fcache.NewObject(ctx, item.name)
			require.NoError(t, err)
			original, err := item.admissionStore().LoadUploadAdmission(ctx)
			require.NoError(t, err)
			ready, release := make(chan struct{}, 2), make(chan struct{})
			count := 2
			if mode == "stale_generation" {
				count = 1
			}
			results := make(chan error, count)
			for range count {
				remote, err := teldrive.NewFs(ctx, "b2-vfs", "", configmap.Simple{"api_host": server.URL, "api_key": "synthetic-key", "chunk_size": "16Mi"})
				require.NoError(t, err)
				store := b2ContendingItemStore{item.admissionStore(), ready, release}
				go func() {
					_, err := remote.Features().PutUnchecked(fs.WithUploadAdmissionStore(ctx, store), strings.NewReader("data"), source)
					results <- err
				}()
			}
			for range count {
				select {
				case <-ready:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if mode == "stale_generation" {
				require.NoError(t, item.Open(nil))
				_, err := item.WriteAt([]byte("DATA"), 0)
				require.NoError(t, err)
				require.NoError(t, item.Close(nil))
			}
			close(release)
			for range count {
				require.Error(t, <-results)
			}
			saved, err := item.admissionStore().LoadUploadAdmission(ctx)
			require.NoError(t, err)
			require.Zero(t, parts.Load())
			if mode == "stale_generation" {
				require.NotEqual(t, original.Generation, saved.Generation)
				require.Empty(t, saved.RequestKey)
				require.Zero(t, gets.Load()+posts.Load())
			} else {
				require.Equal(t, original.Generation, saved.Generation)
				require.NotEmpty(t, saved.RequestKey)
				require.NotEmpty(t, saved.Request)
				require.EqualValues(t, 1, gets.Load()+posts.Load(), "only the committed selection may dispatch")
				if saved.UploadID == "" {
					require.EqualValues(t, 1, posts.Load())
				} else {
					require.Contains(t, ids, saved.UploadID)
					require.EqualValues(t, 1, gets.Load())
				}
				// A later conflicting writer cannot replace the actual persisted selection.
				other := saved
				other.UploadID = uuid.NewString()
				other.RequestKey = uuid.NewString()
				require.Error(t, item.admissionStore().StoreUploadAdmission(ctx, other))
				retained, err := item.admissionStore().LoadUploadAdmission(ctx)
				require.NoError(t, err)
				require.Equal(t, saved, retained)
			}
			require.True(t, item.IsDirty())
		})
	}
}

var b2CASModTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
