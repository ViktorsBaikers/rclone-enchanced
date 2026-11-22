package teldrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/lib/rest"
)

// requiredUploadField distinguishes omitted and null fields from valid zero values.
type requiredUploadField[T any] struct {
	value   T
	present bool
}

func (f *requiredUploadField[T]) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return errors.New("null upload response field")
	}
	if err := json.Unmarshal(data, &f.value); err != nil {
		return err
	}
	f.present = true
	return nil
}

type uploadSessionResponse struct {
	api.UploadSession
	ID             requiredUploadField[string]    `json:"id"`
	Name           requiredUploadField[string]    `json:"name"`
	ExpectedSize   requiredUploadField[int64]     `json:"expectedSize"`
	ModTime        requiredUploadField[time.Time] `json:"modTime"`
	Encryption     requiredUploadField[bool]      `json:"encryption"`
	ConflictPolicy requiredUploadField[string]    `json:"conflictPolicy"`
	PartSize       requiredUploadField[int64]     `json:"partSize"`
	Manifest       requiredUploadField[bool]      `json:"requiresCompletionManifest"`
	State          requiredUploadField[string]    `json:"state"`
	ExpiresAt      requiredUploadField[time.Time] `json:"expiresAt"`
	CreatedAt      requiredUploadField[time.Time] `json:"createdAt"`
}

func (r *uploadSessionResponse) validate() error {
	if !r.ID.present || !r.Name.present || !r.ExpectedSize.present || !r.ModTime.present || !r.Encryption.present || !r.ConflictPolicy.present || !r.PartSize.present || !r.Manifest.present || !r.State.present || !r.ExpiresAt.present || !r.CreatedAt.present || r.ID.value == "" || r.Name.value == "" || r.ExpectedSize.value < -1 || r.PartSize.value <= 0 {
		return errors.New("invalid upload session response")
	}
	switch r.State.value {
	case "open", "completing", "completed", "aborted", "expired":
	default:
		return errors.New("invalid upload session state")
	}
	r.UploadSession.ID, r.UploadSession.Name, r.UploadSession.ExpectedSize = r.ID.value, r.Name.value, r.ExpectedSize.value
	r.UploadSession.ModTime, r.UploadSession.Encryption, r.UploadSession.ConflictPolicy = r.ModTime.value, r.Encryption.value, r.ConflictPolicy.value
	r.UploadSession.PartSize, r.UploadSession.RequiresCompletionManifest, r.UploadSession.State = r.PartSize.value, r.Manifest.value, r.State.value
	r.UploadSession.ExpiresAt, r.UploadSession.CreatedAt = r.ExpiresAt.value, r.CreatedAt.value
	return nil
}

type uploadPartResponse struct {
	api.UploadPart
	UploadID  requiredUploadField[string] `json:"uploadId"`
	PartNo    requiredUploadField[int]    `json:"partNo"`
	State     requiredUploadField[string] `json:"state"`
	PlainSize requiredUploadField[int64]  `json:"plainSize"`
}

func (r *uploadPartResponse) validate() error {
	if !r.UploadID.present || !r.PartNo.present || !r.State.present || !r.PlainSize.present || r.UploadID.value == "" || r.PartNo.value < 1 || r.PlainSize.value < 0 {
		return errors.New("invalid upload part response")
	}
	switch r.State.value {
	case "reserved", "uploading", "stored", "failed":
	default:
		return errors.New("invalid upload part state")
	}
	r.UploadPart.UploadId, r.UploadPart.PartNo, r.UploadPart.State, r.UploadPart.PlainSize = r.UploadID.value, r.PartNo.value, r.State.value, r.PlainSize.value
	return nil
}

type uploadPageResponse[T any] struct {
	Items      []T                         `json:"items"`
	NextCursor requiredUploadField[string] `json:"nextCursor"`
}

func (r *uploadPageResponse[T]) validate() error {
	if r.Items == nil {
		return errors.New("missing upload page items")
	}
	for i := range r.Items {
		if err := any(&r.Items[i]).(interface{ validate() error }).validate(); err != nil {
			return err
		}
	}
	return nil
}

type uploadFileResponse struct {
	api.FileInfo
	ID   requiredUploadField[string] `json:"id"`
	Size requiredUploadField[int64]  `json:"size"`
}

func (r *uploadFileResponse) validate() error {
	if !r.ID.present || r.ID.value == "" || !r.Size.present || r.Size.value < 0 {
		return errors.New("invalid upload completion response")
	}
	r.FileInfo.Id, r.FileInfo.Size = r.ID.value, r.Size.value
	return nil
}

func decodeUploadResponse(resp *http.Response, result interface{ validate() error }) (err error) {
	if resp == nil || resp.Body == nil {
		return errors.New("missing upload response body")
	}
	defer func() {
		_, drainErr := io.CopyN(io.Discard, resp.Body, 10<<20)
		if errors.Is(drainErr, io.EOF) {
			drainErr = nil
		}
		err = errors.Join(err, drainErr, resp.Body.Close())
	}()
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(result); err != nil {
		return err
	}
	var trailing struct{}
	if err = decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("trailing upload response JSON")
		}
		return fmt.Errorf("upload response trailer: %w", err)
	}
	return result.validate()
}

func callUploadResponse[T any, P interface {
	*T
	validate() error
}](ctx context.Context, client *rest.Client, opts *rest.Opts, request any) (result T, resp *http.Response, err error) {
	resp, err = client.CallJSON(ctx, opts, request, nil)
	if err == nil {
		err = decodeUploadResponse(resp, P(&result))
	}
	return result, resp, err
}
