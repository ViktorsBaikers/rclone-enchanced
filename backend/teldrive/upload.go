// Package teldrive implements TelDrive v2 upload sessions.
package teldrive

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/lib/pool"
	"github.com/rclone/rclone/lib/readers"
	"github.com/rclone/rclone/lib/rest"
	"github.com/zeebo/blake3"
)

type storedPart struct {
	size     int64
	checksum string
}

type uploadInfo struct {
	uploadID        string
	chunkSize       int64
	expectedSize    int64
	totalChunks     int64
	fileName        string
	dir             string
	storedParts     map[int]storedPart
	source          fs.ObjectInfo
	sourceSize      int64
	completed       bool
	completedFileID string
	guarded         *guardedIntent
	guardedReceipt  *api.GuardedCompletedReceipt
	guardedResult   *api.GuardedFileEntry
	receiptVerified bool
	sourceTime      time.Time
	blockMu         sync.Mutex
	blockHashes     map[int][]byte
	chunkSizes      map[int]int64
	hashInvalid     bool
}

type objectChunkWriter struct {
	o          *Object
	uploadInfo *uploadInfo
}

func (w *objectChunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (n int64, err error) {
	info := w.uploadInfo
	defer func() {
		if err != nil {
			info.blockMu.Lock()
			info.hashInvalid = true
			info.blockMu.Unlock()
		}
	}()
	if info.chunkSize <= 0 || chunkNumber < 0 || int64(chunkNumber) >= math.MaxInt32 || int64(chunkNumber) > math.MaxInt64/info.chunkSize {
		return 0, errors.New("invalid upload chunk number")
	}
	size, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if _, err = reader.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	if size == 0 && chunkNumber == 0 && info.expectedSize <= 0 {
		return 0, nil
	}
	if size <= 0 || size > info.chunkSize {
		return 0, errors.New("invalid upload chunk size")
	}
	if info.expectedSize >= 0 {
		expected, ok := expectedUploadPartSize(info.expectedSize, info.chunkSize, chunkNumber+1)
		if !ok || size != expected {
			return 0, errors.New("upload chunk does not match expected size")
		}
	}
	var blocks []byte
	if stored, ok := info.storedParts[chunkNumber+1]; ok {
		if info.expectedSize >= 0 {
			if delay, ok := reader.(interface{ DelayAccounting(int) }); ok {
				delay.DelayAccounting(2)
			}
		}
		h := tdhash.New()
		if _, err = io.CopyN(h, reader, size); err != nil {
			return 0, err
		}
		blocks = h.(interface{ BlockHashes() []byte }).BlockHashes()
		if stored.size != size || !validPartChecksum(stored.checksum) || !strings.EqualFold(stored.checksum, hex.EncodeToString(tdhash.SumBlockHashes(blocks))) {
			return 0, fmt.Errorf("stored upload part %d does not match source", chunkNumber+1)
		}
		if info.expectedSize >= 0 {
			var accountErr error
			switch r := reader.(type) {
			case *operations.ReOpen:
				accountErr = r.Account(int(size))
			case *pool.RW:
				accountErr = r.Account(int(size))
			}
			if accountErr != nil {
				return 0, accountErr
			}
		}
	} else {
		if info.completed {
			return 0, errAdmissionUnresolved
		}
		blocks, err = w.o.putUploadPart(ctx, info, chunkNumber+1, reader, size)
		if err != nil {
			return 0, err
		}
	}
	info.addBlockHashes(chunkNumber, int64(chunkNumber)*info.chunkSize, size, blocks)
	return size, nil
}

func (w *objectChunkWriter) Close(ctx context.Context) error {
	_, err := w.o.completeUpload(ctx, w.uploadInfo)
	return err
}

func (w *objectChunkWriter) Abort(ctx context.Context) error {
	return nil
}

func (o *Object) prepareUpload(ctx context.Context, remote string, src fs.ObjectInfo) (*uploadInfo, error) {
	requestedRemote := remote
	if intent, ok := fs.UploadAdmissionIntentFromContext(ctx); ok {
		requestedRemote = intent.DestinationRemote
	}
	admission, err := o.fs.selectUploadAdmission(ctx, "upload", src, requestedRemote, remote, [32]byte{})
	if err != nil {
		return nil, err
	}
	release, err := admission.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	selected := admission.snapshot()
	if o.fs.opt.GuardedUpload {
		return o.prepareGuardedUpload(ctx, admission, selected, remote, src)
	}
	if selected.request == nil {
		// Inspect an existing parent before adoption; create a missing path only
		// when no compatible session can already identify that destination.
		leaf, directoryID, pathErr := o.fs.dirCache.FindPath(ctx, remote, false)
		existingParent := pathErr == nil
		if pathErr != nil {
			if !errors.Is(pathErr, fs.ErrorDirNotFound) {
				return nil, pathErr
			}
			leaf, directoryID, pathErr = o.fs.dirCache.FindPath(ctx, remote, true)
			if pathErr != nil {
				return nil, pathErr
			}
		}
		request := api.UploadCreateRequest{
			ParentId: requestParentID(directoryID), Name: o.fs.opt.Enc.FromStandardName(leaf),
			Size: selected.source.Size, MimeType: selected.source.MIME, ModTime: selected.source.ModTime,
			Encryption: o.fs.opt.EncryptFiles, ConflictPolicy: "replace", PreferredPartSize: int64(o.fs.opt.ChunkSize),
		}
		if existingParent {
			resumed, err := o.findResumableUpload(ctx, request, leaf, directoryID, admission)
			if err != nil {
				return nil, err
			}
			if resumed != nil {
				resumed.source, resumed.sourceSize, resumed.sourceTime = src, request.Size, request.ModTime
				return resumed, nil
			}
		}
		if err := admission.FreezeRequest(ctx, request); err != nil {
			return nil, err
		}
		selected = admission.snapshot()
	}
	request := selected.request
	if request == nil {
		return nil, errAdmissionUnresolved
	}
	leaf, directoryID := o.fs.opt.Enc.ToStandardName(request.Name), parentIDOrRoot(request.ParentId)
	if selected.uploadID != "" {
		info, err := o.selectedUploadInfo(ctx, selected.uploadID, *request, leaf, directoryID, admission)
		if err != nil {
			return nil, err
		}
		info.source, info.sourceSize, info.sourceTime = src, request.Size, request.ModTime
		return info, nil
	}
	opts := rest.Opts{
		Method: http.MethodPost, Path: "/api/v1/uploads",
		ExtraHeaders: map[string]string{"Idempotency-Key": selected.key},
	}
	var session api.UploadSession
	var resp *http.Response
	err = o.fs.pacer.Call(func() (bool, error) {
		var callErr error
		var response uploadSessionResponse
		response, resp, callErr = callUploadResponse[uploadSessionResponse](ctx, o.fs.srv, &opts, request)
		session = response.UploadSession
		return shouldRetry(ctx, resp, callErr)
	})
	if err != nil {
		return nil, err
	}
	if err := admission.SelectUploadID(ctx, session.ID); err != nil {
		return nil, err
	}
	if session.State != "" && session.State != "open" && session.State != "completed" {
		return nil, errAdmissionUnresolved
	}
	if err := admission.observeSession(ctx, session); err != nil {
		return nil, err
	}
	var info *uploadInfo
	// A same-key create replay can return an already completed original.
	if session.State == "completed" {
		info, err = o.selectedUploadInfo(ctx, session.ID, *request, leaf, directoryID, admission)
	} else {
		info, err = uploadInfoFromSession(session, leaf, directoryID, nil)
	}
	if err == nil {
		info.source, info.sourceSize, info.sourceTime = src, request.Size, request.ModTime
	}
	return info, err
}

func (o *Object) findResumableUpload(ctx context.Context, request api.UploadCreateRequest, leaf, directoryID string, admission *uploadAdmission) (*uploadInfo, error) {
	cursor := ""
	var seen map[string]struct{}
	for {
		parameters := url.Values{"state": []string{"open"}, "limit": []string{"200"}}
		if cursor != "" {
			parameters.Set("cursor", cursor)
		}
		opts := rest.Opts{Method: http.MethodGet, Path: "/api/v1/uploads", Parameters: parameters}
		var page uploadPageResponse[uploadSessionResponse]
		var resp *http.Response
		err := o.fs.pacer.Call(func() (bool, error) {
			var callErr error
			page, resp, callErr = callUploadResponse[uploadPageResponse[uploadSessionResponse]](ctx, o.fs.srv, &opts, nil)
			return shouldRetry(ctx, resp, callErr)
		})
		if err != nil {
			return nil, err
		}
		for _, row := range page.Items {
			session := row.UploadSession
			if !uploadSessionMatches(session, request, time.Now().UTC()) {
				continue
			}
			if err := admission.AdoptUpload(ctx, request, session.ID); err != nil {
				return nil, err
			}
			// Listing is only a hint. Unknown/terminal/missing candidates stay
			// selected even when this exact currently authorized GET fails.
			return o.selectedUploadInfo(ctx, session.ID, request, leaf, directoryID, admission)
		}
		next := page.NextCursor.value
		if next == "" {
			return nil, nil
		}
		if _, duplicate := seen[next]; duplicate {
			return nil, errors.New("cyclic upload session cursor")
		}
		if seen == nil {
			seen = make(map[string]struct{})
		}
		seen[next] = struct{}{}
		cursor = next
	}
}

func (o *Object) getUploadSession(ctx context.Context, uploadID string) (*api.UploadSession, error) {
	opts := rest.Opts{Method: http.MethodGet, Path: "/api/v1/uploads/" + url.PathEscape(uploadID)}
	var session api.UploadSession
	var resp *http.Response
	err := o.fs.pacer.Call(func() (bool, error) {
		var callErr error
		var response uploadSessionResponse
		response, resp, callErr = callUploadResponse[uploadSessionResponse](ctx, o.fs.srv, &opts, nil)
		session = response.UploadSession
		return shouldRetry(ctx, resp, callErr)
	})
	if err != nil {
		return nil, err
	}
	if session.ID != uploadID {
		return nil, errAdmissionUnresolved
	}
	return &session, nil
}

func (o *Object) selectedUploadInfo(ctx context.Context, uploadID string, request api.UploadCreateRequest, leaf, directoryID string, admission *uploadAdmission) (*uploadInfo, error) {
	version := admission.selectionVersion()
	session, err := o.getUploadSession(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if admission.selectionVersion() != version {
		return nil, errAdmissionUnresolved
	}
	if session.State != "open" && session.State != "completed" {
		return nil, errAdmissionUnresolved
	}
	if !uploadSessionRequestMatches(*session, request) ||
		(session.State == "open" && !session.ExpiresAt.After(time.Now().UTC())) ||
		(session.State == "completed" && session.FileId == "") {
		return nil, errAdmissionConflict
	}
	if err := admission.observeSession(ctx, *session); err != nil {
		return nil, err
	}
	version = admission.selectionVersion()
	parts, err := o.listUploadParts(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if admission.selectionVersion() != version {
		return nil, errAdmissionUnresolved
	}
	stored, ok := compatibleStoredParts(*session, parts)
	if !ok {
		return nil, errAdmissionConflict
	}
	info, err := uploadInfoFromSession(*session, leaf, directoryID, stored)
	if err != nil {
		return nil, err
	}
	info.completed = session.State == "completed"
	if info.completed {
		info.completedFileID = session.FileId
	}
	if info.completed && info.expectedSize >= 0 && int64(len(stored)) != info.totalChunks {
		return nil, errAdmissionUnresolved
	}
	return info, nil
}

func (o *Object) listUploadParts(ctx context.Context, uploadID string) ([]api.UploadPart, error) {
	cursor := ""
	var seen map[string]struct{}
	var parts []api.UploadPart
	for {
		parameters := url.Values{"limit": []string{"200"}}
		if cursor != "" {
			parameters.Set("cursor", cursor)
		}
		opts := rest.Opts{
			Method:     http.MethodGet,
			Path:       "/api/v1/uploads/" + uploadID + "/parts",
			Parameters: parameters,
		}
		var page uploadPageResponse[uploadPartResponse]
		var resp *http.Response
		err := o.fs.pacer.Call(func() (bool, error) {
			var callErr error
			page, resp, callErr = callUploadResponse[uploadPageResponse[uploadPartResponse]](ctx, o.fs.srv, &opts, nil)
			return shouldRetry(ctx, resp, callErr)
		})
		if err != nil {
			return nil, err
		}
		for _, row := range page.Items {
			if row.UploadPart.UploadId != uploadID {
				return nil, errors.New("upload part belongs to another session")
			}
			parts = append(parts, row.UploadPart)
		}
		next := page.NextCursor.value
		if next == "" {
			return parts, nil
		}
		if _, duplicate := seen[next]; duplicate {
			return nil, errors.New("cyclic upload part cursor")
		}
		if seen == nil {
			seen = make(map[string]struct{})
		}
		seen[next] = struct{}{}
		cursor = next
	}
}

func uploadSessionMatches(session api.UploadSession, request api.UploadCreateRequest, now time.Time) bool {
	return session.State == "open" &&
		session.ID != "" && session.PartSize > 0 && session.ExpiresAt.After(now) &&
		uploadSessionRequestMatches(session, request)
}

func uploadSessionRequestMatches(session api.UploadSession, request api.UploadCreateRequest) bool {
	// A completed session may have lost its old parent FK. Only its actual
	// currently authorized result and read-only part proof can authorize replay.
	return session.ExpectedHash == nil &&
		(session.State == "completed" || session.ParentId == request.ParentId) && session.Name == request.Name &&
		session.ExpectedSize == request.Size && session.MimeType == request.MimeType &&
		session.Encryption == request.Encryption && session.ConflictPolicy == request.ConflictPolicy &&
		session.ModTime.UTC().Truncate(time.Microsecond).Equal(request.ModTime.UTC().Truncate(time.Microsecond))
}

func compatibleStoredParts(session api.UploadSession, parts []api.UploadPart) (map[int]storedPart, bool) {
	stored := make(map[int]storedPart)
	maxPart := 0
	for _, part := range parts {
		if part.State != "stored" {
			continue
		}
		if part.PartNo < 1 || part.PlainSize <= 0 || !validPartChecksum(part.Checksum) || part.UploadId != session.ID {
			return nil, false
		}
		if _, duplicate := stored[part.PartNo]; duplicate {
			return nil, false
		}
		if session.ExpectedSize >= 0 {
			expected, ok := expectedUploadPartSize(session.ExpectedSize, session.PartSize, part.PartNo)
			if !ok || part.PlainSize != expected {
				return nil, false
			}
		} else if part.PlainSize > session.PartSize {
			return nil, false
		}
		stored[part.PartNo] = storedPart{size: part.PlainSize, checksum: part.Checksum}
		maxPart = max(maxPart, part.PartNo)
	}
	if session.ExpectedSize < 0 {
		if maxPart != len(stored) {
			return nil, false
		}
		for partNo := 1; partNo < maxPart; partNo++ {
			if stored[partNo].size != session.PartSize {
				return nil, false
			}
		}
	}
	return stored, true
}

func expectedUploadPartSize(totalSize, partSize int64, partNo int) (int64, bool) {
	if totalSize < 0 || partSize <= 0 || partNo < 1 || int64(partNo) > math.MaxInt32 || int64(partNo-1) > math.MaxInt64/partSize {
		return 0, false
	}
	offset := int64(partNo-1) * partSize
	if offset >= totalSize {
		return 0, false
	}
	remaining := totalSize - offset
	if remaining < partSize {
		return remaining, true
	}
	return partSize, true
}

func uploadInfoFromSession(session api.UploadSession, leaf, directoryID string, stored map[int]storedPart) (*uploadInfo, error) {
	if !session.RequiresCompletionManifest || session.ID == "" || session.PartSize <= 0 || session.PartSize%tdhash.BlockSize != 0 || session.ExpectedSize < -1 {
		return nil, fmt.Errorf("invalid TelDrive upload session")
	}
	totalChunks := int64(0)
	if session.ExpectedSize > 0 {
		totalChunks = (session.ExpectedSize-1)/session.PartSize + 1
		if totalChunks > math.MaxInt32 {
			return nil, errors.New("upload exceeds part number limit")
		}
	}
	if stored == nil {
		stored = make(map[int]storedPart)
	}
	return &uploadInfo{
		uploadID: session.ID, chunkSize: session.PartSize, expectedSize: session.ExpectedSize, totalChunks: totalChunks,
		fileName: leaf, dir: directoryID, storedParts: stored,
	}, nil
}
func (o *Object) uploadMultipart(ctx context.Context, remote string, in io.Reader, src fs.ObjectInfo) (*uploadInfo, error) {
	info, err := o.prepareUpload(ctx, remote, src)
	if err != nil {
		return nil, err
	}
	if info.guardedReceipt != nil {
		// A lost Complete acknowledgement reconciles by read-only proof of the
		// exact current source; no part or completion request is resent.
		if err := verifyGuardedReceiptSource(ctx, in, info); err != nil {
			return nil, err
		}
		info.receiptVerified = true
		return info, nil
	}
	var consumed int64
	for partNo := int64(1); partNo <= info.totalChunks; partNo++ {
		partSize := info.chunkSize
		if remaining := info.expectedSize - consumed; remaining < partSize {
			partSize = remaining
		}
		if stored, ok := info.storedParts[int(partNo)]; ok {
			h := tdhash.New()
			if _, err := io.CopyN(h, in, partSize); err != nil {
				return nil, fmt.Errorf("read stored upload part %d: %w", partNo, err)
			}
			blocks := h.(interface{ BlockHashes() []byte }).BlockHashes()
			if stored.size != partSize || !validPartChecksum(stored.checksum) || !strings.EqualFold(stored.checksum, hex.EncodeToString(tdhash.SumBlockHashes(blocks))) {
				return nil, fmt.Errorf("stored upload part %d does not match source", partNo)
			}
			info.addBlockHashes(int(partNo-1), consumed, partSize, blocks)
			consumed += partSize
			continue
		}
		if info.completed {
			return nil, errAdmissionUnresolved
		}
		blocks, err := o.putUploadPart(ctx, info, int(partNo), io.LimitReader(in, partSize), partSize)
		if err != nil {
			return nil, err
		}
		info.addBlockHashes(int(partNo-1), consumed, partSize, blocks)
		consumed += partSize
	}
	var tail [1]byte
	if n, err := io.ReadFull(in, tail[:]); n != 0 || err != io.EOF {
		return nil, errors.New("source exceeds declared upload size")
	}
	return info, nil
}

func (o *Object) putUploadPart(ctx context.Context, info *uploadInfo, partNo int, reader io.Reader, size int64) ([]byte, error) {
	if info == nil || info.uploadID == "" || partNo < 1 || size < 0 {
		return nil, fmt.Errorf("invalid TelDrive upload part")
	}
	path := fmt.Sprintf("/api/v1/uploads/%s/parts/%d", info.uploadID, partNo)
	var guardHeaders map[string]string
	if info.guarded != nil {
		path = fmt.Sprintf("/api/v1/uploads/guarded/%s/parts/%d", info.uploadID, partNo)
		guardHeaders = map[string]string{recoveryIncarnationHeader: info.guarded.incarnation}
		// The probe must succeed at the exact origin that receives the body.
		if err := o.probeGuardedPartOrigin(ctx, info.uploadID, info.guarded); err != nil {
			return nil, o.fs.noteGuardedError(err)
		}
	}
	opts := rest.Opts{
		Method: "PUT", Path: path, Body: reader, ContentLength: &size,
		ContentType: "application/octet-stream", ExtraHeaders: guardHeaders,
	}
	if o.fs.opt.ChannelID != 0 {
		opts.Parameters = url.Values{"channelId": {strconv.FormatInt(o.fs.opt.ChannelID, 10)}}
	}
	if o.fs.opt.UploadHost != "" {
		opts.RootURL = strings.TrimRight(o.fs.opt.UploadHost, "/") + path
		opts.Path = ""
	}
	var resp *http.Response
	var previousErr error
	attempt := 0
	var hasher interface {
		io.Writer
		Reset()
		BlockHashes() []byte
	}
	hasher = tdhash.New().(interface {
		io.Writer
		Reset()
		BlockHashes() []byte
	})
	err := o.fs.pacer.Call(func() (bool, error) {
		if attempt > 0 {
			seeker, ok := reader.(io.Seeker)
			if !ok {
				return false, fserrors.RetryError(previousErr)
			}
			if _, seekErr := seeker.Seek(0, io.SeekStart); seekErr != nil {
				return false, seekErr
			}
		}
		var body io.Reader = io.LimitReader(reader, size)
		if hasher != nil {
			hasher.Reset()
			body = io.TeeReader(body, hasher)
		}
		tracked := &uploadPartBody{reader: body}
		opts.Body = tracked
		attempt++
		var callErr error
		resp, callErr = o.fs.srv.Call(ctx, &opts)
		if callErr != nil {
			previousErr = callErr
			return shouldRetry(ctx, resp, callErr)
		}
		// An existing part can be acknowledged without consuming the request body.
		// lib/rest waits for the transport to close tracked before returning.
		if callErr == nil {
			var remaining int64
			remaining, callErr = io.Copy(io.Discard, body)
			if callErr == nil && tracked.n+remaining != size {
				callErr = fmt.Errorf("upload part %d: read %d bytes, expected %d: %w", partNo, tracked.n+remaining, size, io.ErrUnexpectedEOF)
			}
			if callErr == nil {
				var response uploadPartResponse
				if resp == nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated) {
					callErr = errors.New("missing upload part acknowledgement")
				} else {
					callErr = decodeUploadResponse(resp, &response)
					resp.Body = nil // decodeUploadResponse owns the body; retain retry status.
					ack := response.UploadPart
					if callErr == nil && (ack.UploadId != info.uploadID || ack.PartNo != partNo || ack.State != "stored" || ack.PlainSize != size || !validPartChecksum(ack.Checksum) || !strings.EqualFold(ack.Checksum, hex.EncodeToString(tdhash.SumBlockHashes(hasher.BlockHashes())))) {
						callErr = errors.New("upload part acknowledgement does not match current bytes")
					}
				}
			}
		}
		if resp != nil && resp.Body != nil {
			_, drainErr := io.Copy(io.Discard, resp.Body)
			callErr = errors.Join(callErr, drainErr, resp.Body.Close())
		}
		previousErr = callErr
		return shouldRetry(ctx, resp, callErr)
	})
	if err != nil {
		return nil, o.fs.noteGuardedError(err)
	}
	return hasher.BlockHashes(), nil
}

type uploadPartBody struct {
	reader io.Reader
	n      int64
}

func (body *uploadPartBody) Read(p []byte) (int, error) {
	n, err := body.reader.Read(p)
	body.n += int64(n)
	return n, err
}

func (body *uploadPartBody) Close() error {
	return nil
}

func (o *Object) completeUpload(ctx context.Context, info *uploadInfo) (*api.FileInfo, error) {
	if info == nil || info.uploadID == "" {
		return nil, fmt.Errorf("invalid TelDrive upload session")
	}
	if info.guarded != nil {
		return o.completeGuardedUpload(ctx, info)
	}
	manifest, err := info.completionManifest()
	if err != nil {
		return nil, err
	}
	if intent, ok := fs.UploadAdmissionIntentFromContext(ctx); ok {
		if err := verifyAdmissionUploadSource(ctx, intent.Source, manifest); err != nil {
			return nil, err
		}
	} else if err := info.verifyLocalSource(ctx, manifest); err != nil {
		return nil, err
	}
	opts := rest.Opts{
		Method:       "POST",
		Path:         "/api/v1/uploads/" + info.uploadID + "/complete",
		ExtraHeaders: map[string]string{"Idempotency-Key": uuid.NewString()},
	}
	var file api.FileInfo
	var resp *http.Response
	err = o.fs.pacer.Call(func() (bool, error) {
		var callErr error
		var response uploadFileResponse
		response, resp, callErr = callUploadResponse[uploadFileResponse](ctx, o.fs.srv, &opts, manifest)
		file = response.FileInfo
		return shouldRetry(ctx, resp, callErr)
	})
	if err != nil {
		return nil, err
	}
	if file.Id == "" || (info.completed && file.Id != info.completedFileID) {
		return nil, errAdmissionConflict
	}
	if file.Size != manifest.Size || file.Hash == nil || file.Hash.Algorithm != "blake3" || !strings.EqualFold(file.Hash.Value, manifest.Hash.Value) {
		return nil, errors.New("completed upload does not match current stream manifest")
	}
	o.applyFileInfo(&file)
	return &file, nil
}

func (info *uploadInfo) verifyLocalSource(ctx context.Context, manifest *api.UploadCompleteRequest) error {
	if info.source == nil {
		return nil
	}
	sourceFs := info.source.Fs()
	if sourceFs == nil || sourceFs.Features() == nil || !sourceFs.Features().IsLocal {
		return nil
	}
	source := fs.UnWrapObjectInfo(info.source)
	if source == nil {
		return errors.New("local source cannot be reopened for verification")
	}
	owner, ok := source.Fs().(fs.Fs)
	if !ok || owner.Features() == nil || !owner.Features().IsLocal {
		return errors.New("local source filesystem cannot provide fresh verification")
	}
	fresh, err := owner.NewObject(ctx, source.Remote())
	if err != nil {
		return fmt.Errorf("reopen local source for verification: %w", err)
	}
	if fresh.Size() != info.sourceSize || !fresh.ModTime(ctx).Equal(info.sourceTime) || manifest.Size != info.sourceSize {
		return errors.New("local source metadata changed before upload completion")
	}
	in, err := fresh.Open(ctx)
	if err != nil {
		return fmt.Errorf("open local source for verification: %w", err)
	}
	h := tdhash.New()
	size, readErr := io.Copy(h, readers.NewContextReader(ctx, in))
	closeErr := in.Close()
	if readErr != nil {
		return fmt.Errorf("read local source for verification: %w", readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close local source verification read: %w", closeErr)
	}
	after, err := owner.NewObject(ctx, source.Remote())
	if err != nil {
		return fmt.Errorf("restat local source after verification: %w", err)
	}
	if size != info.sourceSize || fresh.Size() != info.sourceSize || !fresh.ModTime(ctx).Equal(info.sourceTime) ||
		after.Size() != info.sourceSize || !after.ModTime(ctx).Equal(info.sourceTime) ||
		!strings.EqualFold(hex.EncodeToString(h.Sum(nil)), manifest.Hash.Value) {
		return errors.New("local source bytes or metadata changed before upload completion")
	}
	// This proof is not an atomic compare-and-unlink against a later concurrent writer.
	return nil
}

func verifyAdmissionUploadSource(ctx context.Context, source fs.Object, manifest *api.UploadCompleteRequest) error {
	if source == nil || manifest == nil || manifest.Hash == nil {
		return errAdmissionUnresolved
	}
	current := source
	if owner, ok := source.Fs().(fs.Fs); ok && owner.Features().IsLocal {
		fresh, err := owner.NewObject(ctx, source.Remote())
		if err != nil {
			return err
		}
		if fresh.Size() != source.Size() || !fresh.ModTime(ctx).Equal(source.ModTime(ctx)) {
			return errAdmissionConflict
		}
		current = fresh
	}
	in, err := current.Open(ctx)
	if err != nil {
		return err
	}
	h := tdhash.New()
	size, readErr := io.Copy(h, readers.NewContextReader(ctx, in))
	closeErr := in.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return err
	}
	if size != manifest.Size || (current.Size() >= 0 && size != current.Size()) ||
		!strings.EqualFold(hex.EncodeToString(h.Sum(nil)), manifest.Hash.Value) {
		return errAdmissionConflict
	}
	if owner, ok := source.Fs().(fs.Fs); ok && owner.Features().IsLocal {
		after, err := owner.NewObject(ctx, source.Remote())
		if err != nil {
			return err
		}
		if after.Size() != current.Size() || !after.ModTime(ctx).Equal(current.ModTime(ctx)) {
			return errAdmissionConflict
		}
	}
	return ctx.Err()
}

func (info *uploadInfo) addBlockHashes(chunk int, offset, size int64, blocks []byte) {
	if len(blocks) == 0 {
		return
	}
	if offset%tdhash.BlockSize != 0 || (info.expectedSize >= 0 && offset+size < info.expectedSize && size%tdhash.BlockSize != 0) || len(blocks)%32 != 0 {
		info.blockMu.Lock()
		info.hashInvalid = true
		info.blockMu.Unlock()
		return
	}
	info.blockMu.Lock()
	if info.blockHashes == nil {
		info.blockHashes = make(map[int][]byte)
	}
	if info.chunkSizes == nil {
		info.chunkSizes = make(map[int]int64)
	}
	info.blockHashes[chunk] = blocks
	info.chunkSizes[chunk] = size
	info.blockMu.Unlock()
}

func validPartChecksum(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func (info *uploadInfo) completionManifest() (*api.UploadCompleteRequest, error) {
	info.blockMu.Lock()
	defer info.blockMu.Unlock()
	if info.hashInvalid {
		return nil, errors.New("invalid upload coverage")
	}
	count := len(info.chunkSizes)
	if int64(count) > math.MaxInt32 || (info.expectedSize >= 0 && int64(count) != info.totalChunks) {
		return nil, errors.New("incomplete upload coverage")
	}
	var size int64
	root := blake3.New()
	for chunk := range count {
		n, ok := info.chunkSizes[chunk]
		digests := info.blockHashes[chunk]
		if !ok || n <= 0 || n > info.chunkSize || (chunk < count-1 && n != info.chunkSize) || int64(len(digests)) != ((n-1)/tdhash.BlockSize+1)*32 || size > math.MaxInt64-n {
			return nil, errors.New("incomplete upload coverage")
		}
		size += n
		_, _ = root.Write(digests)
	}
	for part := range info.storedParts {
		if part < 1 || part > count {
			return nil, errors.New("unvisited stored upload tail")
		}
	}
	if info.expectedSize >= 0 && size != info.expectedSize {
		return nil, errors.New("upload coverage size mismatch")
	}
	return &api.UploadCompleteRequest{Size: size, Hash: &api.FileHash{Algorithm: "blake3", Value: hex.EncodeToString(root.Sum(nil))}}, nil
}

func (o *Object) createFile(ctx context.Context, _ fs.ObjectInfo, info *uploadInfo) error {
	_, err := o.completeUpload(ctx, info)
	return err
}
