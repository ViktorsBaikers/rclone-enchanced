package teldrive

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/readers"
	"github.com/rclone/rclone/lib/rest"
)

const (
	recoveryIncarnationHeader = "X-TelDrive-Recovery-Incarnation"
	uploadGuardHeader         = "X-TelDrive-Upload-Guard"
)

var (
	errUploadRecoveryChanged    = errors.New("TelDrive upload recovery incarnation changed")
	errUploadDestinationChanged = errors.New("TelDrive guarded upload destination changed")
	errGuardedUnavailable       = errors.New("TelDrive server does not support guarded uploads")
)

// guardedProtocolError keeps the ordinary API envelope and the typed refusal
// reachable through errors.Is.
type guardedProtocolError struct {
	apiError *apiError
	sentinel error
}

func (e *guardedProtocolError) Error() string {
	return e.apiError.Error()
}

func (e *guardedProtocolError) Unwrap() []error {
	return []error{e.apiError, e.sentinel}
}

// guardedIncarnation caches the server's current recovery incarnation for
// this backend instance. A 412 clears it so the next attempt rediscovers.
type guardedIncarnation struct {
	mu    sync.Mutex
	value string
}

var guardedIncarnationInit sync.Mutex

func (f *Fs) guardedState() *guardedIncarnation {
	guardedIncarnationInit.Lock()
	defer guardedIncarnationInit.Unlock()
	if f.guarded == nil {
		f.guarded = new(guardedIncarnation)
	}
	return f.guarded
}

func (f *Fs) invalidateGuardedIncarnation() {
	state := f.guardedState()
	state.mu.Lock()
	state.value = ""
	state.mu.Unlock()
}

// noteGuardedError refreshes the cached incarnation after a mismatched attempt.
// The failed attempt itself is never retried under the discovered value.
func (f *Fs) noteGuardedError(err error) error {
	if errors.Is(err, errUploadRecoveryChanged) {
		f.invalidateGuardedIncarnation()
	}
	if errors.Is(err, errUploadRecoveryChanged) || errors.Is(err, errUploadDestinationChanged) ||
		errors.Is(err, errAdmissionConflict) || errors.Is(err, errGuardedUnavailable) {
		err = errors.Join(err, fs.ErrGuardedConflict)
	}
	return err
}

// GuardedWriteEnabled reports whether this remote requires guarded writes.
func (f *Fs) GuardedWriteEnabled() bool {
	return f.opt.GuardedUpload
}

// partOrigin is the exact origin which receives guarded part bodies.
func (f *Fs) partOrigin() string {
	if f.opt.UploadHost != "" {
		return strings.TrimRight(f.opt.UploadHost, "/")
	}
	return strings.TrimRight(f.opt.ApiHost, "/")
}

// GuardedNamespaceEpoch snapshots the namespace a receipt is installed against.
func (f *Fs) GuardedNamespaceEpoch() uint64 {
	return f.dirCache.NamespaceEpoch()
}

// HashGuardedPlaintext derives the exact plaintext digest a receipt publishes.
func (f *Fs) HashGuardedPlaintext(ctx context.Context, content io.Reader, size int64) (fs.ContentHash, error) {
	return f.guardedPlaintextHash(ctx, content, size)
}

// ResolveGuardedAlias returns the standard-encoded path of the frozen
// destination inside the pinned root. It validates the same-UUID parent and
// never creates a directory or follows a name.
func (f *Fs) ResolveGuardedAlias(ctx context.Context, intent fs.GuardedWriteIntent) (string, error) {
	parent, err := f.ResolveGuardedParent(ctx, intent)
	if err != nil {
		return "", err
	}
	if intent.Leaf == "" {
		return "", fs.ErrGuardedConflict
	}
	return path.Join(parent.Path, f.opt.Enc.ToStandardName(intent.Leaf)), nil
}

// CaptureWriteDestination pins the destination and its exact baseline without
// creating a directory or following a name.
func (f *Fs) CaptureWriteDestination(ctx context.Context, remote string) (fs.GuardedWriteIntent, error) {
	var intent fs.GuardedWriteIntent
	if !f.opt.GuardedUpload {
		return intent, fs.ErrGuardedUnsupported
	}
	incarnation, err := f.discoverGuarded(ctx)
	if err != nil {
		return intent, err
	}
	leaf, directoryID, err := f.dirCache.FindPath(ctx, remote, false)
	if err != nil {
		return intent, err
	}
	mountRoot, err := f.guardedMountRoot(ctx)
	if err != nil {
		return intent, err
	}
	intent = fs.GuardedWriteIntent{
		Service:             strings.TrimRight(f.opt.ApiHost, "/"),
		PartOrigin:          f.partOrigin(),
		Owner:               strconv.FormatInt(f.userId, 10),
		RecoveryIncarnation: incarnation,
		MountRoot:           mountRoot,
		Leaf:                f.opt.Enc.FromStandardName(leaf),
	}
	if directoryID != "" && directoryID != rootID {
		parentID := directoryID
		intent.ParentID = &parentID
	}
	destination := api.GuardedUploadDestination{MountRoot: mountRoot, ParentID: intent.ParentID}
	if err := f.verifyGuardedParent(ctx, destination); err != nil {
		return intent, err
	}
	expected, err := f.guardedSlotExpectation(ctx, destination, leaf)
	if err != nil {
		return intent, err
	}
	switch expected.Kind {
	case "absent":
		intent.Baseline = fs.GuardedBaseline{Kind: fs.GuardedBaselineAbsent}
	case "version":
		info, err := f.getFileByID(ctx, expected.FileID)
		if err != nil {
			return intent, err
		}
		if info.Kind != "file" || info.Status != "active" ||
			strconv.FormatInt(info.Generation, 10) != expected.Generation ||
			info.Hash == nil || info.Hash.Algorithm != "blake3" {
			return intent, fs.ErrGuardedProofUnavailable
		}
		intent.Baseline = fs.GuardedBaseline{
			Kind: fs.GuardedBaselineVersion, FileID: info.Id,
			Generation: strconv.FormatInt(info.Generation, 10), Size: info.Size,
			Hash: &fs.ContentHash{Algorithm: info.Hash.Algorithm, Value: strings.ToLower(info.Hash.Value)},
		}
	default:
		return intent, fs.ErrGuardedConflict
	}
	return intent, nil
}

// ResolveGuardedParent revalidates the pinned parent by UUID and returns its
// canonical stored-name path from the pinned root.
func (f *Fs) ResolveGuardedParent(ctx context.Context, intent fs.GuardedWriteIntent) (fs.GuardedParent, error) {
	var parent fs.GuardedParent
	if !f.opt.GuardedUpload {
		return parent, fs.ErrGuardedUnsupported
	}
	destination := api.GuardedUploadDestination{MountRoot: intent.MountRoot, ParentID: intent.ParentID}
	if err := f.verifyGuardedParent(ctx, destination); err != nil {
		return parent, err
	}
	if intent.ParentID == nil {
		parent.Segments = []string{}
		parent.Path = ""
		return parent, nil
	}
	stored := []string{}
	parentID := *intent.ParentID
	seen := map[string]struct{}{}
	for {
		if _, duplicate := seen[parentID]; duplicate {
			return parent, fs.ErrGuardedConflict
		}
		seen[parentID] = struct{}{}
		info, err := f.getFileByID(ctx, parentID)
		if err != nil {
			return parent, err
		}
		stored = append(stored, info.Name)
		if intent.MountRoot != nil && parentID == *intent.MountRoot {
			break
		}
		if info.ParentId == "" {
			if intent.MountRoot != nil {
				return parent, fs.ErrGuardedConflict
			}
			break
		}
		parentID = info.ParentId
	}
	segments := make([]string, len(stored))
	decoded := make([]string, len(stored))
	for i, name := range stored {
		segments[len(stored)-1-i] = name
		decoded[len(stored)-1-i] = f.opt.Enc.ToStandardName(name)
	}
	parent.Segments = segments
	parent.Path = strings.Join(decoded, "/")
	return parent, nil
}

// VerifyGuardedOriginal verifies the complete original plaintext against the
// captured baseline, including cached spans, and returns its tree digest.
func (f *Fs) VerifyGuardedOriginal(ctx context.Context, intent fs.GuardedWriteIntent, content io.Reader, size int64) (fs.ContentHash, error) {
	var hash fs.ContentHash
	if !f.opt.GuardedUpload {
		return hash, fs.ErrGuardedUnsupported
	}
	baseline := intent.Baseline
	if baseline.Kind == fs.GuardedBaselineAbsent {
		// A genuinely new file needs no original proof, but the local digest is
		// still the exact one a later receipt reconciliation must match.
		return f.guardedPlaintextHash(ctx, content, size)
	}
	if baseline.Kind != fs.GuardedBaselineVersion || baseline.FileID == "" || baseline.Generation == "" ||
		baseline.Hash == nil || baseline.Hash.Algorithm != "blake3" || !validPartChecksum(baseline.Hash.Value) ||
		size != baseline.Size {
		return hash, fs.ErrGuardedProofUnavailable
	}
	info, err := f.getFileByID(ctx, baseline.FileID)
	if err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) || errors.Is(err, fs.ErrorPermissionDenied) {
			return hash, errors.Join(err, fs.ErrGuardedConflict)
		}
		return hash, err
	}
	if info.Kind != "file" || info.Status != "active" || info.Id != baseline.FileID ||
		strconv.FormatInt(info.Generation, 10) != baseline.Generation || info.Size != baseline.Size ||
		info.Hash == nil || info.Hash.Algorithm != "blake3" ||
		!strings.EqualFold(info.Hash.Value, baseline.Hash.Value) {
		return hash, fs.ErrGuardedConflict
	}
	hash, err = f.guardedPlaintextHash(ctx, content, size)
	if err != nil {
		return hash, err
	}
	if !strings.EqualFold(hash.Value, baseline.Hash.Value) {
		return hash, fs.ErrGuardedConflict
	}
	return hash, nil
}

func (f *Fs) guardedPlaintextHash(ctx context.Context, content io.Reader, size int64) (fs.ContentHash, error) {
	if size < 0 || content == nil {
		return fs.ContentHash{}, fs.ErrGuardedProofUnavailable
	}
	hasher := tdhash.New()
	n, err := io.Copy(hasher, readers.NewContextReader(ctx, content))
	if err != nil {
		return fs.ContentHash{}, err
	}
	if n != size {
		return fs.ContentHash{}, fs.ErrGuardedProofUnavailable
	}
	return fs.ContentHash{Algorithm: "blake3", Value: strings.ToLower(hex.EncodeToString(hasher.Sum(nil)))}, nil
}

// ReconcileGuardedReceipt proves one exact completed result by read-only
// guarded status only. No source reader, part callback or mutating upload runs.
func (f *Fs) ReconcileGuardedReceipt(ctx context.Context, proof fs.GuardedReceiptProof) (fs.GuardedWriteResult, error) {
	var result fs.GuardedWriteResult
	if !f.opt.GuardedUpload {
		return result, fs.ErrGuardedUnsupported
	}
	if proof.Owner != strconv.FormatInt(f.userId, 10) || proof.AdmissionKey == "" || proof.SessionID == "" ||
		proof.RecoveryIncarnation == "" || proof.Size < 0 || proof.Hash.Algorithm != "blake3" ||
		!validPartChecksum(proof.Hash.Value) || proof.LocalGeneration == "" {
		return result, fs.ErrGuardedConflict
	}
	if proof.NamespaceEpoch != 0 && f.dirCache.NamespaceEpoch() != proof.NamespaceEpoch {
		return result, fs.ErrGuardedConflict
	}
	intent := &guardedIntent{incarnation: proof.RecoveryIncarnation, createKey: proof.AdmissionKey}
	object := &Object{fs: f}
	session, err := object.getGuardedUploadSession(ctx, proof.SessionID, intent, true)
	if err != nil {
		return result, f.noteGuardedError(err)
	}
	if session.State != "completed" || session.CompletedReceipt == nil || session.FileId == "" {
		return result, fs.ErrGuardedConflict
	}
	receipt := session.CompletedReceipt
	if receipt.UploadId != proof.SessionID || receipt.AdmissionKey != proof.AdmissionKey ||
		receipt.Size != proof.Size || receipt.Hash.Algorithm != "blake3" ||
		!strings.EqualFold(receipt.Hash.Value, proof.Hash.Value) {
		return result, fs.ErrGuardedConflict
	}
	entry := receipt.Result
	if entry.Id == "" || entry.Kind != "file" || entry.Status != "active" ||
		entry.Size != proof.Size || entry.Hash == nil || entry.Hash.Algorithm != "blake3" ||
		!strings.EqualFold(entry.Hash.Value, proof.Hash.Value) || session.FileId != entry.Id {
		return result, fs.ErrGuardedConflict
	}
	if proof.MountRoot != nil {
		parentID := parentIDOrRoot(entry.ParentId)
		destination := api.GuardedUploadDestination{MountRoot: proof.MountRoot, ParentID: &parentID}
		if err := f.verifyGuardedParent(ctx, destination); err != nil {
			return result, err
		}
	}
	if proof.NamespaceEpoch != 0 && f.dirCache.NamespaceEpoch() != proof.NamespaceEpoch {
		return result, fs.ErrGuardedConflict
	}
	remote := f.opt.Enc.ToStandardName(entry.Name)
	if entry.ParentSegments != nil {
		decoded := make([]string, len(*entry.ParentSegments))
		for i, segment := range *entry.ParentSegments {
			decoded[i] = f.opt.Enc.ToStandardName(segment)
		}
		if len(decoded) != 0 {
			remote = strings.Join(append(decoded, remote), "/")
		}
	}
	resolved, err := f.newObjectWithInfo(ctx, remote, &entry.FileInfo)
	if err != nil {
		return result, err
	}
	obj, ok := resolved.(*Object)
	if !ok {
		return result, fs.ErrGuardedConflict
	}
	obj.generation = entry.Generation
	result = fs.GuardedWriteResult{
		Object: obj, ResultID: entry.Id, Generation: strconv.FormatInt(entry.Generation, 10),
		Size: entry.Size, Hash: fs.ContentHash{Algorithm: entry.Hash.Algorithm, Value: strings.ToLower(entry.Hash.Value)},
		NamespaceEpoch: proof.NamespaceEpoch,
	}
	if entry.ParentSegments != nil {
		result.ParentSegments = append([]string(nil), (*entry.ParentSegments)...)
		result.LocationKnown = true
	}
	return result, nil
}

// guardedIntent freezes the destination, incarnation and keys of one attempt.
type guardedIntent struct {
	incarnation string
	createKey   string
	completeKey string
	destination api.GuardedUploadDestination
}

type guardedDiscovery struct{}

func (*guardedDiscovery) validate() error { return nil }

func (f *Fs) discoverGuarded(ctx context.Context) (string, error) {
	state := f.guardedState()
	state.mu.Lock()
	cached := state.value
	state.mu.Unlock()
	if cached != "" {
		return cached, nil
	}
	opts := rest.Opts{Method: http.MethodGet, Path: "/api/v1/uploads/guarded"}
	var resp *http.Response
	err := f.pacer.Call(func() (bool, error) {
		var callErr error
		_, resp, callErr = callUploadResponse[guardedDiscovery](ctx, f.srv, &opts, nil)
		return shouldRetry(ctx, resp, callErr)
	})
	if err != nil {
		if resp != nil && (resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed) {
			return "", errors.Join(errGuardedUnavailable, err)
		}
		return "", err
	}
	value := ""
	if resp != nil {
		value = resp.Header.Get(recoveryIncarnationHeader)
	}
	if _, err := uuid.Parse(value); err != nil {
		return "", errors.Join(errGuardedUnavailable, fmt.Errorf("invalid recovery incarnation header: %w", err))
	}
	state.mu.Lock()
	state.value = value
	state.mu.Unlock()
	return value, nil
}

func (o *Object) prepareGuardedUpload(ctx context.Context, admission *uploadAdmission, selected admissionSnapshot, remote string, src fs.ObjectInfo) (*uploadInfo, error) {
	requestedRemote := remote
	if intent, ok := fs.UploadAdmissionIntentFromContext(ctx); ok {
		requestedRemote = intent.DestinationRemote
	}
	if selected.request == nil {
		leaf, directoryID, err := o.fs.dirCache.FindPath(ctx, requestedRemote, false)
		if err != nil {
			return nil, err
		}
		request := api.UploadCreateRequest{
			ParentId: requestParentID(directoryID), Name: o.fs.opt.Enc.FromStandardName(leaf),
			Size: src.Size(), MimeType: fs.MimeType(ctx, src), ModTime: src.ModTime(ctx),
			Encryption: o.fs.opt.EncryptFiles, ConflictPolicy: "replace", PreferredPartSize: int64(o.fs.opt.ChunkSize),
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
	intent, frozen := admission.guardedSnapshot()
	if !frozen {
		var incarnation string
		var destination api.GuardedUploadDestination
		if locked, ok := fs.GuardedWriteIntentFromContext(ctx); ok {
			// The VFS journal already pinned this destination. Never recapture
			// a different parent, root or baseline at send time.
			if err := guardedRequestMatchesIntent(*request, locked); err != nil {
				return nil, o.fs.noteGuardedError(err)
			}
			incarnation = locked.RecoveryIncarnation
			destination = guardedDestinationFromIntent(locked)
			if err := o.fs.verifyGuardedParent(ctx, destination); err != nil {
				return nil, o.fs.noteGuardedError(err)
			}
		} else {
			var err error
			incarnation, err = o.fs.discoverGuarded(ctx)
			if err != nil {
				return nil, err
			}
			destination, err = o.captureGuardedDestination(ctx, *request)
			if err != nil {
				return nil, err
			}
		}
		var err error
		intent, err = admission.freezeGuardedIntent(ctx, incarnation, destination)
		if err != nil {
			return nil, err
		}
	}
	leaf, directoryID := o.fs.opt.Enc.ToStandardName(request.Name), parentIDOrRoot(request.ParentId)
	if selected.uploadID != "" {
		info, err := o.selectedGuardedUploadInfo(ctx, selected.uploadID, intent, *request, leaf, directoryID, admission)
		if err != nil {
			return nil, o.fs.noteGuardedError(err)
		}
		info.source, info.sourceSize, info.sourceTime = src, request.Size, request.ModTime
		return info, nil
	}
	encryption := o.fs.opt.EncryptFiles
	payload := api.GuardedUploadCreateRequest{
		Destination: intent.destination, Name: request.Name, Size: request.Size,
		MimeType: request.MimeType, ModTime: request.ModTime, Encryption: &encryption,
		PreferredPartSize: int64(o.fs.opt.ChunkSize),
	}
	opts := rest.Opts{
		Method: http.MethodPost, Path: "/api/v1/uploads/guarded",
		ExtraHeaders: map[string]string{"Idempotency-Key": intent.createKey, recoveryIncarnationHeader: intent.incarnation},
	}
	var session api.UploadSession
	var resp *http.Response
	err := o.fs.pacer.Call(func() (bool, error) {
		var callErr error
		var response uploadSessionResponse
		response, resp, callErr = callUploadResponse[uploadSessionResponse](ctx, o.fs.srv, &opts, &payload)
		session = response.UploadSession
		return shouldRetry(ctx, resp, callErr)
	})
	if err != nil {
		return nil, o.fs.noteGuardedError(err)
	}
	if err := guardedSessionMatches(session, *request); err != nil {
		return nil, err
	}
	if err := admission.SelectUploadID(ctx, session.ID); err != nil {
		return nil, err
	}
	info, err := o.guardedUploadInfo(ctx, intent, *request, leaf, directoryID, session, admission)
	if err != nil {
		return nil, err
	}
	info.source, info.sourceSize, info.sourceTime = src, request.Size, request.ModTime
	return info, nil
}

// captureGuardedDestination freezes the UUID-based destination before the
// first guarded mutation. It never creates a directory and never follows a name.
func (o *Object) captureGuardedDestination(ctx context.Context, request api.UploadCreateRequest) (api.GuardedUploadDestination, error) {
	var destination api.GuardedUploadDestination
	mountRoot, err := o.fs.guardedMountRoot(ctx)
	if err != nil {
		return destination, err
	}
	destination.MountRoot = mountRoot
	switch {
	case request.ParentId != "":
		parentID := request.ParentId
		destination.ParentID = &parentID
	case mountRoot != nil:
		parentID := *mountRoot
		destination.ParentID = &parentID
	}
	if err := o.fs.verifyGuardedParent(ctx, destination); err != nil {
		return destination, err
	}
	expected, err := o.fs.guardedSlotExpectation(ctx, destination, o.fs.opt.Enc.ToStandardName(request.Name))
	if err != nil {
		return destination, err
	}
	destination.Expected = expected
	return destination, nil
}

func (f *Fs) guardedMountRoot(ctx context.Context) (*string, error) {
	if f.root == "" {
		return nil, nil
	}
	rootID, err := f.dirCache.RootID(ctx, false)
	if err != nil {
		return nil, err
	}
	if _, err := uuid.Parse(rootID); err != nil {
		return nil, errAdmissionUnresolved
	}
	return &rootID, nil
}

func (f *Fs) guardedSlotExpectation(ctx context.Context, destination api.GuardedUploadDestination, leaf string) (api.GuardedSlotExpectation, error) {
	parentID := rootID
	if destination.ParentID != nil {
		parentID = *destination.ParentID
	}
	files, err := f.findObject(ctx, parentID, leaf)
	if err != nil {
		return api.GuardedSlotExpectation{}, err
	}
	if len(files) == 0 {
		return api.GuardedSlotExpectation{Kind: "absent"}, nil
	}
	file := files[0]
	if file.Kind != "file" || file.Generation <= 0 {
		return api.GuardedSlotExpectation{}, errUploadDestinationChanged
	}
	return api.GuardedSlotExpectation{
		Kind: "version", FileID: file.Id, Generation: strconv.FormatInt(file.Generation, 10),
	}, nil
}

// verifyGuardedParent resolves the frozen parent by UUID and proves active
// ancestry inside the pinned mount. A name or path match never authorizes it.
func (f *Fs) verifyGuardedParent(ctx context.Context, destination api.GuardedUploadDestination) error {
	if destination.ParentID == nil {
		if destination.MountRoot != nil {
			return errUploadDestinationChanged
		}
		return nil
	}
	parentID := *destination.ParentID
	seen := map[string]struct{}{parentID: {}}
	for depth := 0; depth < 256; depth++ {
		info, err := f.getFileByID(ctx, parentID)
		if err != nil {
			if errors.Is(err, fs.ErrorObjectNotFound) || errors.Is(err, fs.ErrorPermissionDenied) ||
				errors.Is(err, fs.ErrorImmutableModified) {
				return errUploadDestinationChanged
			}
			return err
		}
		if info.Kind != "folder" || info.Status != "active" {
			return errUploadDestinationChanged
		}
		if destination.MountRoot != nil && parentID == *destination.MountRoot {
			return nil
		}
		if info.ParentId == "" {
			if destination.MountRoot != nil {
				return errUploadDestinationChanged
			}
			return nil
		}
		if _, duplicate := seen[info.ParentId]; duplicate {
			return errUploadDestinationChanged
		}
		seen[info.ParentId] = struct{}{}
		parentID = info.ParentId
	}
	return errUploadDestinationChanged
}

func (f *Fs) getFileByID(ctx context.Context, id string) (*api.FileInfo, error) {
	opts := rest.Opts{Method: http.MethodGet, Path: "/api/v1/files/" + url.PathEscape(id)}
	var file api.FileInfo
	var resp *http.Response
	err := f.pacer.Call(func() (bool, error) {
		var callErr error
		resp, callErr = f.srv.CallJSON(ctx, &opts, nil, &file)
		return shouldRetry(ctx, resp, callErr)
	})
	if err != nil {
		return nil, err
	}
	if file.Id != id {
		return nil, errUploadDestinationChanged
	}
	return &file, nil
}

func (o *Object) selectedGuardedUploadInfo(ctx context.Context, uploadID string, intent *guardedIntent, request api.UploadCreateRequest, leaf, directoryID string, admission *uploadAdmission) (*uploadInfo, error) {
	version := admission.selectionVersion()
	session, err := o.getGuardedUploadSession(ctx, uploadID, intent, false)
	if err != nil {
		return nil, err
	}
	if admission.selectionVersion() != version {
		return nil, errAdmissionUnresolved
	}
	info, err := o.guardedUploadInfo(ctx, intent, request, leaf, directoryID, *session, admission)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// getGuardedUploadSession probes the guarded status route. uploadOrigin makes
// the request against the alternate upload host that will receive part bodies.
func (o *Object) getGuardedUploadSession(ctx context.Context, uploadID string, intent *guardedIntent, uploadOrigin bool) (*api.UploadSession, error) {
	opts := o.guardedStatusOpts(uploadID, intent, uploadOrigin)
	var session api.UploadSession
	var resp *http.Response
	err := o.fs.pacer.Call(func() (bool, error) {
		var callErr error
		var response uploadSessionResponse
		response, resp, callErr = callUploadResponse[uploadSessionResponse](ctx, o.fs.srv, &opts, nil)
		if callErr == nil && (resp == nil || resp.Header.Get(uploadGuardHeader) != "1") {
			callErr = errGuardedUnavailable
		}
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

func (o *Object) guardedStatusOpts(uploadID string, intent *guardedIntent, uploadOrigin bool) rest.Opts {
	path := "/api/v1/uploads/guarded/" + url.PathEscape(uploadID)
	opts := rest.Opts{
		Method: http.MethodGet, Path: path,
		ExtraHeaders: map[string]string{recoveryIncarnationHeader: intent.incarnation},
	}
	if uploadOrigin && o.fs.opt.UploadHost != "" {
		opts.RootURL = strings.TrimRight(o.fs.opt.UploadHost, "/") + path
		opts.Path = ""
	}
	return opts
}

// probeGuardedPartOrigin rechecks the guard at the origin that will receive
// the part body.
func (o *Object) probeGuardedPartOrigin(ctx context.Context, uploadID string, intent *guardedIntent) error {
	session, err := o.getGuardedUploadSession(ctx, uploadID, intent, true)
	if err != nil {
		return err
	}
	if session.State != "open" && session.State != "completed" {
		return errAdmissionUnresolved
	}
	return nil
}

func (o *Object) guardedUploadInfo(ctx context.Context, intent *guardedIntent, request api.UploadCreateRequest, leaf, directoryID string, session api.UploadSession, admission *uploadAdmission) (*uploadInfo, error) {
	if err := guardedSessionMatches(session, request); err != nil {
		return nil, err
	}
	if err := admission.observeSession(ctx, session); err != nil {
		return nil, err
	}
	info, err := uploadInfoFromSession(session, leaf, directoryID, nil)
	if err != nil {
		return nil, err
	}
	info.guarded = intent
	if session.State != "completed" {
		return info, nil
	}
	receipt := session.CompletedReceipt
	if receipt == nil {
		return nil, errAdmissionUnresolved
	}
	if err := validateGuardedReceipt(receipt, intent, session, request); err != nil {
		return nil, err
	}
	result := receipt.Result
	info.completed = true
	info.completedFileID = result.Id
	info.guardedReceipt = receipt
	info.guardedResult = &result
	return info, nil
}

func guardedSessionMatches(session api.UploadSession, request api.UploadCreateRequest) error {
	if session.ID == "" || session.PartSize <= 0 || !session.RequiresCompletionManifest ||
		session.ExpectedHash != nil ||
		(session.State != "open" && session.State != "completed") ||
		session.Name != request.Name || session.ExpectedSize != request.Size ||
		session.MimeType != request.MimeType || session.Encryption != request.Encryption ||
		session.ParentId != request.ParentId ||
		!session.ModTime.UTC().Truncate(time.Microsecond).Equal(request.ModTime.UTC().Truncate(time.Microsecond)) {
		return errAdmissionConflict
	}
	if session.State == "open" && !session.ExpiresAt.After(time.Now().UTC()) {
		return errAdmissionConflict
	}
	if session.State == "completed" && session.FileId == "" && session.CompletedReceipt == nil {
		return errAdmissionUnresolved
	}
	return nil
}

func validateGuardedReceipt(receipt *api.GuardedCompletedReceipt, intent *guardedIntent, session api.UploadSession, request api.UploadCreateRequest) error {
	if receipt.UploadId != session.ID || receipt.AdmissionKey != intent.createKey {
		return errAdmissionConflict
	}
	if receipt.Size < 0 || receipt.Hash.Algorithm != "blake3" || !validPartChecksum(receipt.Hash.Value) {
		return errAdmissionConflict
	}
	result := receipt.Result
	if result.Id == "" || result.Kind != "file" || result.Status != "active" ||
		result.Name != request.Name || result.ParentId != request.ParentId {
		return errAdmissionConflict
	}
	if result.Size != receipt.Size || result.Hash == nil ||
		result.Hash.Algorithm != "blake3" || !strings.EqualFold(result.Hash.Value, receipt.Hash.Value) {
		return errAdmissionConflict
	}
	if session.FileId != "" && session.FileId != result.Id {
		return errAdmissionConflict
	}
	return nil
}

// verifyGuardedReceiptSource proves the current source is the exact completed
// result before the caller treats a replayed attempt as success.
func verifyGuardedReceiptSource(ctx context.Context, in io.Reader, info *uploadInfo) error {
	if info == nil || info.guardedReceipt == nil {
		return errAdmissionUnresolved
	}
	hasher := tdhash.New()
	size, err := io.Copy(hasher, readers.NewContextReader(ctx, in))
	if err != nil {
		return err
	}
	receipt := info.guardedReceipt
	if size != receipt.Size || (info.expectedSize >= 0 && info.expectedSize != size) ||
		!strings.EqualFold(hex.EncodeToString(hasher.Sum(nil)), receipt.Hash.Value) {
		return errAdmissionConflict
	}
	return ctx.Err()
}

func (o *Object) completeGuardedUpload(ctx context.Context, info *uploadInfo) (*api.FileInfo, error) {
	if info.guarded == nil {
		return nil, errAdmissionUnresolved
	}
	if info.guardedReceipt != nil {
		if !info.receiptVerified {
			return nil, errAdmissionUnresolved
		}
		result := info.guardedResult
		if result == nil {
			return nil, errAdmissionUnresolved
		}
		file := result.FileInfo
		o.applyFileInfo(&file)
		o.guardedResult = result
		return &file, nil
	}
	manifest, err := info.completionManifest()
	if err != nil {
		return nil, err
	}
	if manifest.Hash == nil {
		return nil, errAdmissionUnresolved
	}
	epoch := o.fs.dirCache.NamespaceEpoch()
	if intent, ok := fs.UploadAdmissionIntentFromContext(ctx); ok {
		if err := verifyAdmissionUploadSource(ctx, intent.Source, manifest); err != nil {
			return nil, err
		}
	} else if err := info.verifyLocalSource(ctx, manifest); err != nil {
		return nil, err
	}
	opts := rest.Opts{
		Method: http.MethodPost, Path: "/api/v1/uploads/guarded/" + url.PathEscape(info.uploadID) + "/complete",
		ExtraHeaders: map[string]string{"Idempotency-Key": info.guarded.completeKey, recoveryIncarnationHeader: info.guarded.incarnation},
	}
	var resp *http.Response
	var result api.GuardedFileEntry
	err = o.fs.pacer.Call(func() (bool, error) {
		var callErr error
		var response guardedFileResponse
		response, resp, callErr = callUploadResponse[guardedFileResponse](ctx, o.fs.srv, &opts, &api.UploadCompleteRequest{Size: manifest.Size, Hash: manifest.Hash})
		result = response.GuardedFileEntry
		return shouldRetry(ctx, resp, callErr)
	})
	if err != nil {
		return nil, o.fs.noteGuardedError(err)
	}
	if err := o.fs.dirCache.CheckNamespace(epoch); err != nil {
		return nil, errors.Join(err, fs.ErrGuardedConflict)
	}
	if resp == nil || resp.Header.Get("Etag") == "" || resp.Header.Get("Location") == "" {
		return nil, errors.New("guarded completion is missing its exact result identity")
	}
	if result.Kind != "file" || result.Status != "active" || result.Name != o.fs.opt.Enc.FromStandardName(info.fileName) {
		return nil, errAdmissionConflict
	}
	parentID := rootID
	if info.guarded.destination.ParentID != nil {
		parentID = *info.guarded.destination.ParentID
	}
	if parentIDOrRoot(result.ParentId) != parentID {
		return nil, errAdmissionConflict
	}
	if result.Size != manifest.Size || result.Hash == nil || result.Hash.Algorithm != "blake3" ||
		!strings.EqualFold(result.Hash.Value, manifest.Hash.Value) {
		return nil, errors.New("guarded completion does not match the current stream manifest")
	}
	file := result.FileInfo
	o.applyFileInfo(&file)
	o.guardedResult = &result
	o.guardedEpoch = epoch
	return &file, nil
}

type guardedFileResponse struct {
	api.GuardedFileEntry
	ID   requiredUploadField[string] `json:"id"`
	Size requiredUploadField[int64]  `json:"size"`
}

func (r *guardedFileResponse) validate() error {
	if !r.ID.present || r.ID.value == "" || !r.Size.present || r.Size.value < 0 {
		return errors.New("invalid guarded completion response")
	}
	r.GuardedFileEntry.Id, r.GuardedFileEntry.Size = r.ID.value, r.Size.value
	return nil
}

func guardedDestinationEqual(a, b api.GuardedUploadDestination) bool {
	if a.Expected != b.Expected || (a.MountRoot == nil) != (b.MountRoot == nil) || (a.ParentID == nil) != (b.ParentID == nil) {
		return false
	}
	if a.MountRoot != nil && *a.MountRoot != *b.MountRoot {
		return false
	}
	if a.ParentID != nil && *a.ParentID != *b.ParentID {
		return false
	}
	return true
}

// guardedDestinationFromIntent converts the journal's frozen intent without
// resolving a name or path.
func guardedDestinationFromIntent(intent fs.GuardedWriteIntent) api.GuardedUploadDestination {
	destination := api.GuardedUploadDestination{MountRoot: intent.MountRoot, ParentID: intent.ParentID}
	switch intent.Baseline.Kind {
	case fs.GuardedBaselineVersion:
		destination.Expected = api.GuardedSlotExpectation{
			Kind: "version", FileID: intent.Baseline.FileID, Generation: intent.Baseline.Generation,
		}
	default:
		destination.Expected = api.GuardedSlotExpectation{Kind: "absent"}
	}
	return destination
}

// guardedRequestMatchesIntent rejects a request whose encoded leaf or stable
// parent no longer matches the captured destination.
func guardedRequestMatchesIntent(request api.UploadCreateRequest, intent fs.GuardedWriteIntent) error {
	if intent.Leaf == "" || request.Name != intent.Leaf {
		return errUploadDestinationChanged
	}
	parent := requestParentID(request.ParentId)
	switch {
	case intent.ParentID == nil && parent != "":
		return errUploadDestinationChanged
	case intent.ParentID != nil && parent != *intent.ParentID:
		return errUploadDestinationChanged
	}
	return nil
}
