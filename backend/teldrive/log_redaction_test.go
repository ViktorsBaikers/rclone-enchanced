package teldrive

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// b4LogCapture records every rclone log record for assertion.
type b4LogCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *b4LogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (h *b4LogCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *b4LogCapture) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *b4LogCapture) WithGroup(string) slog.Handler { return h }

func (h *b4LogCapture) messages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	messages := make([]string, 0, len(h.records))
	for _, r := range h.records {
		messages = append(messages, r.Message)
	}
	return messages
}

// TestMoveTypeMismatchLogHidesPath proves the two type-mismatch debug lines
// name the rejected Go type and never the remote-relative path.
func TestMoveTypeMismatchLogHidesPath(t *testing.T) {
	o := newUploadTestObject(t, func(http.ResponseWriter, *http.Request) {
		t.Error("type mismatch contacted the server")
	})

	sink := &b4LogCapture{}
	fs.SetLogger(sink)
	cfg := fs.GetConfig(context.Background())
	oldLevel := cfg.LogLevel
	cfg.LogLevel = fs.LogLevelDebug
	t.Cleanup(func() { fs.GetConfig(context.Background()).LogLevel = oldLevel })

	const planted = "planted-name"
	foreign := mockobject.New("dir/" + planted + ".mkv")
	_, err := o.fs.Move(context.Background(), foreign, "dst.mkv")
	require.ErrorIs(t, err, fs.ErrorCantMove)

	srcFs, err := mockfs.NewFs(context.Background(), "mock", "dir/"+planted, nil)
	require.NoError(t, err)
	err = o.fs.DirMove(context.Background(), srcFs, "dir/"+planted, "dst")
	require.ErrorIs(t, err, fs.ErrorCantDirMove)

	messages := sink.messages()
	joined := strings.Join(messages, "\n")
	assert.Contains(t, joined, "Can't move - not same remote type")
	assert.Contains(t, joined, "Can't move directory - not same remote type")
	assert.Contains(t, joined, "mockobject.Object")
	assert.Contains(t, joined, "mockfs.Fs")
	assert.NotContains(t, joined, planted)
}
