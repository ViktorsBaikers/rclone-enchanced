package fs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

func TestGetConfig(t *testing.T) {
	ctx := context.Background()

	// Check nil
	//lint:ignore SA1012 false positive when running staticcheck, we want to test passing a nil Context and therefore ignore lint suggestion to use context.TODO
	//nolint:staticcheck // Don't include staticcheck when running golangci-lint to avoid SA1012
	config := GetConfig(nil)
	assert.Equal(t, globalConfig, config)

	// Check empty config
	config = GetConfig(ctx)
	assert.Equal(t, globalConfig, config)

	// Check adding a config
	ctx2, config2 := AddConfig(ctx)
	config2.Transfers++
	assert.NotEqual(t, config2, config)

	// Check can get config back
	config2ctx := GetConfig(ctx2)
	assert.Equal(t, config2, config2ctx)
}

// The rc request marker must survive CopyConfig, which is how rclone
// does detach context but keep config.
func TestRCRequestContext(t *testing.T) {
	ctx := context.Background()
	assert.False(t, IsRCRequest(ctx))

	rcCtx := WithRCRequest(ctx)
	assert.True(t, IsRCRequest(rcCtx))

	// CopyConfig carries the marker even when there is no config to copy
	assert.True(t, IsRCRequest(CopyConfig(context.Background(), rcCtx)))
	// and when there is
	rcCtx, _ = AddConfig(rcCtx)
	assert.True(t, IsRCRequest(CopyConfig(context.Background(), rcCtx)))
	// An unmarked context stays unmarked
	assert.False(t, IsRCRequest(CopyConfig(context.Background(), ctx)))
}

func TestB2CommandRunIdentity(t *testing.T) {
	first, second := NewCommandRunID(), NewCommandRunID()
	assert.NotZero(t, first)
	assert.Greater(t, second, first)
	ctx, ci := AddConfig(context.Background())
	ci.CommandRunID = first
	child, copied := AddConfig(ctx)
	assert.Equal(t, first, copied.CommandRunID)
	assert.Equal(t, first, GetConfig(CopyConfig(context.Background(), child)).CommandRunID)
	store := &b2ConfigAdmissionStore{}
	withStore := WithUploadAdmissionStore(child, store)
	actual, ok := UploadAdmissionStoreFromContext(CopyConfig(context.Background(), withStore))
	assert.True(t, ok)
	assert.Same(t, store, actual)
	copied.CommandRunID = second
	assert.Equal(t, first, ci.CommandRunID)
	body, err := json.Marshal(ci)
	assert.NoError(t, err)
	assert.NotContains(t, strings.ToLower(string(body)), "commandrun")
	var decoded ConfigInfo
	assert.NoError(t, json.Unmarshal([]byte(`{"CommandRunID":123,"command_run_id":456}`), &decoded))
	assert.Zero(t, decoded.CommandRunID)
	assert.NoError(t, configstruct.Set(configmap.Simple{"command_run_id": "789"}, ci))
	assert.Equal(t, first, ci.CommandRunID)
	items, err := configstruct.Items(ci)
	assert.NoError(t, err)
	for _, item := range items {
		assert.NotEqual(t, "CommandRunID", item.Field)
		assert.NotEqual(t, "command_run_id", item.Name)
	}
	for _, option := range ConfigOptionsInfo {
		assert.NotContains(t, option.Name, "command_run")
	}
	assert.Nil(t, pflag.Lookup("command-run-id"))
	t.Run("admission_context", TestB2UploadAdmissionContext)
	t.Run("transient_context", TestB2UploadAdmissionTransientContext)
	t.Run("marked_error", TestB2UploadAdmissionError)
}

type b2ConfigAdmissionStore struct{}

func (*b2ConfigAdmissionStore) LoadUploadAdmission(context.Context) (UploadAdmissionState, error) {
	return UploadAdmissionState{Generation: "6a78cbd7-9ee5-48d8-ae82-e0999a99f7dd"}, nil
}

func (*b2ConfigAdmissionStore) StoreUploadAdmission(context.Context, UploadAdmissionState) error {
	return nil
}

func TestB2UploadAdmissionContext(t *testing.T) {
	store := &b2ConfigAdmissionStore{}
	ctx := WithUploadAdmissionStore(context.Background(), store)
	intent := UploadAdmissionIntent{DestinationRemote: "primary/file.txt"}
	ctx = WithUploadAdmissionIntent(ctx, intent)
	remote := "original-source.txt"
	ctx = WithUploadAdmissionDiscovery(ctx, nil, &remote)
	for _, copied := range []context.Context{ctx, CopyConfig(context.Background(), ctx)} {
		got, ok := UploadAdmissionStoreFromContext(copied)
		assert.True(t, ok)
		assert.Same(t, store, got)
		original, present := UploadAdmissionIntentFromContext(copied)
		assert.True(t, present)
		assert.Equal(t, intent, original)
		_, exact, marked := UploadAdmissionDiscoveryFromContext(copied)
		assert.True(t, marked)
		assert.Equal(t, remote, *exact)
	}
	_, ok := UploadAdmissionStoreFromContext(context.Background())
	assert.False(t, ok)
	body, err := json.Marshal(GetConfig(ctx))
	assert.NoError(t, err)
	assert.NotContains(t, string(body), "UploadAdmission")
}

func TestB2UploadAdmissionTransientContext(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, release := WithUploadAdmissionTransientScope(parent)
	defer release()
	id, lifetime, ok := UploadAdmissionTransientScopeFromContext(ctx)
	assert.True(t, ok)
	assert.NotZero(t, id)
	other, cancelOther := WithUploadAdmissionTransientScope(parent)
	defer cancelOther()
	otherID, _, _ := UploadAdmissionTransientScopeFromContext(other)
	assert.NotEqual(t, id, otherID)
	store := &b2ConfigAdmissionStore{}
	ctx = WithUploadAdmissionStore(ctx, store)
	ctx = WithUploadAdmissionIntent(ctx, UploadAdmissionIntent{DestinationRemote: "requested-b"})
	remote := ""
	ctx = WithUploadAdmissionDiscovery(ctx, nil, &remote)
	copied := CopyConfig(context.Background(), CopyConfig(context.Background(), ctx))
	gotID, gotLifetime, present := UploadAdmissionTransientScopeFromContext(copied)
	assert.True(t, present)
	assert.Equal(t, id, gotID)
	assert.Same(t, lifetime, gotLifetime)
	gotStore, _ := UploadAdmissionStoreFromContext(copied)
	assert.Same(t, store, gotStore)
	intent, _ := UploadAdmissionIntentFromContext(copied)
	assert.Equal(t, "requested-b", intent.DestinationRemote)
	_, exact, marked := UploadAdmissionDiscoveryFromContext(copied)
	assert.True(t, marked)
	assert.NotNil(t, exact)
	assert.Empty(t, *exact)
	cancelParent()
	assert.ErrorIs(t, lifetime.Err(), context.Canceled)
	assert.NoError(t, copied.Err())
	_, _, present = UploadAdmissionTransientScopeFromContext(context.Background())
	assert.False(t, present)
	_, _, marked = UploadAdmissionDiscoveryFromContext(CopyConfig(context.Background(), context.Background()))
	assert.False(t, marked)
}

func TestB2UploadAdmissionError(t *testing.T) {
	cause := errors.New("dispatch failed")
	err := &UploadAdmissionError{Err: cause}
	assert.ErrorIs(t, err, cause)
	assert.True(t, IsUploadAdmissionError(fmt.Errorf("outer: %w", err)))
	assert.False(t, IsUploadAdmissionError(cause))
	assert.Equal(t, cause.Error(), err.Error())
}
