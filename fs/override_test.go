package fs_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Check all optional interfaces satisfied
var _ fs.FullObjectInfo = (*fs.OverrideRemote)(nil)

func TestB1ObjectInfoUnwrap(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "source"), []byte("content"), 0600))
	f, err := local.NewFs(ctx, "local", root, configmap.Simple{})
	require.NoError(t, err)
	src, err := f.NewObject(ctx, "source")
	require.NoError(t, err)
	wrapped := fs.NewOverrideRemote(src, "destination")
	assert.Equal(t, "destination", wrapped.Remote())
	assert.Same(t, src, fs.UnWrapObjectInfo(wrapped))
	assert.Equal(t, "source", fs.UnWrapObjectInfo(wrapped).Remote())
	assert.Same(t, f, fs.UnWrapObjectInfo(wrapped).Fs())
	assert.Same(t, src, fs.UnWrapObjectInfo(src))

	info := object.NewStaticObjectInfo("stream", time.Now(), -1, true, nil, nil)
	assert.Nil(t, fs.UnWrapObjectInfo(info))
	assert.Nil(t, fs.UnWrapObjectInfo(fs.NewOverrideRemote(info, "destination")))
	assert.Nil(t, fs.UnWrapObjectInfo(nil))
}
