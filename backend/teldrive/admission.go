package teldrive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/lib/readers"
)

var (
	errAdmissionConflict   = errors.New("TelDrive upload admission conflicts with the original request")
	errAdmissionUnresolved = errors.New("TelDrive original admission is unavailable or requires current source proof")
)

// Paths stay private and root-relative spellings are canonicalized before indexing.
// Source metadata and wire policy are fingerprints, not keys that can select a new attempt.
type admissionIdentity struct {
	remote          [32]byte
	source          [32]byte
	sourcePath      string
	requestedTarget string
}

type admissionSourceFingerprint struct {
	Size    int64
	ModTime time.Time
	MIME    string
}

type uploadAdmission struct {
	mu                 sync.Mutex
	busy               chan struct{}
	identity           admissionIdentity
	effectiveTarget    string
	kind               string
	fingerprint        [32]byte
	sourceFingerprint  admissionSourceFingerprint
	logicalFingerprint admissionSourceFingerprint
	key                string
	request            *api.UploadCreateRequest
	copyRequest        *api.FileCopy
	copySourceID       string
	uploadID           string
	partSize           int64
	encryption         bool
	resultID           string
	version            uint64
	store              fs.UploadAdmissionStore
	durable            fs.UploadAdmissionState
	guarded            *guardedIntent
}

type admissionSnapshot struct {
	version      uint64
	key          string
	request      *api.UploadCreateRequest
	copyRequest  *api.FileCopy
	copySourceID string
	uploadID     string
	source       admissionSourceFingerprint
}

type admissionScopeKey struct {
	kind string
	id   uint64
}

type admissionDestination struct {
	remote [32]byte
	source [32]byte
	target string
}

type admissionScope struct {
	entries      map[admissionIdentity]*uploadAdmission
	destinations map[admissionDestination]map[admissionIdentity]*uploadAdmission
	ready        chan struct{}
	closed       bool
	err          error
}

// One registry owns all ephemeral entries. Live jobs are never evicted by age.
var commandAdmissions = struct {
	sync.Mutex
	run    uint64
	scopes map[admissionScopeKey]*admissionScope
}{scopes: make(map[admissionScopeKey]*admissionScope)}

func admissionScopeForContext(ctx context.Context) (*admissionScope, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	var lifetime context.Context
	var key admissionScopeKey
	if fs.IsRCRequest(ctx) && cache.JobGetJobID == nil {
		return nil, errAdmissionUnresolved
	}
	if cache.JobGetJobID != nil {
		if id, ok := cache.JobGetJobID(fs.UploadAdmissionJobContext(ctx)); ok {
			if id <= 0 || cache.JobOnFinish == nil || cache.JobIsRunning == nil || !cache.JobIsRunning(id) {
				return nil, errAdmissionUnresolved
			}
			key = admissionScopeKey{"job", uint64(id)}
		}
	}
	if key.kind == "" {
		if run := commandAdmissionRun(ctx); run != 0 {
			key = admissionScopeKey{"command", run}
		}
	}
	if key.kind == "" {
		if id, owner, ok := fs.UploadAdmissionTransientScopeFromContext(ctx); ok {
			if id == 0 || owner == nil {
				return nil, errAdmissionUnresolved
			}
			if err := context.Cause(owner); err != nil {
				return nil, err
			}
			key, lifetime = admissionScopeKey{"transient", id}, owner
		}
	}
	if key.kind == "" {
		return nil, nil
	}
	commandAdmissions.Lock()
	scope := commandAdmissions.scopes[key]
	if scope == nil {
		if key.kind == "command" {
			if key.id < commandAdmissions.run {
				commandAdmissions.Unlock()
				return nil, errAdmissionUnresolved
			}
			if key.id != commandAdmissions.run {
				delete(commandAdmissions.scopes, admissionScopeKey{"command", commandAdmissions.run})
				commandAdmissions.run = key.id
			}
		}
		scope = &admissionScope{entries: make(map[admissionIdentity]*uploadAdmission),
			destinations: make(map[admissionDestination]map[admissionIdentity]*uploadAdmission), ready: make(chan struct{})}
		commandAdmissions.scopes[key] = scope
		commandAdmissions.Unlock()
		var err error
		if key.kind == "job" {
			_, err = cache.JobOnFinish(int64(key.id), func() {
				commandAdmissions.Lock()
				scope.closed = true
				scope.entries = nil
				scope.destinations = nil
				delete(commandAdmissions.scopes, key)
				commandAdmissions.Unlock()
			})
			if err == nil && !cache.JobIsRunning(int64(key.id)) {
				err = errAdmissionUnresolved
			}
		}
		if key.kind == "transient" {
			context.AfterFunc(lifetime, func() {
				commandAdmissions.Lock()
				scope.closed = true
				scope.entries, scope.destinations = nil, nil
				delete(commandAdmissions.scopes, key)
				commandAdmissions.Unlock()
			})
		}
		commandAdmissions.Lock()
		scope.err = err
		if err != nil {
			scope.closed = true
			delete(commandAdmissions.scopes, key)
		}
		close(scope.ready)
		commandAdmissions.Unlock()
	} else {
		commandAdmissions.Unlock()
	}
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-scope.ready:
	}
	commandAdmissions.Lock()
	defer commandAdmissions.Unlock()
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if scope.closed || scope.err != nil {
		return nil, errAdmissionUnresolved
	}
	// A positive final probe admits this work; later completion cannot undo it.
	if key.kind == "job" && !cache.JobIsRunning(int64(key.id)) {
		return nil, errAdmissionUnresolved
	}
	return scope, nil
}

func loadDurableAdmission(ctx context.Context, store fs.UploadAdmissionStore) (fs.UploadAdmissionState, *api.UploadCreateRequest, error) {
	state, err := store.LoadUploadAdmission(ctx)
	if err != nil {
		return state, nil, err
	}
	if _, err := uuid.Parse(state.Generation); err != nil {
		return state, nil, errAdmissionUnresolved
	}
	if state.RequestKey == "" && len(state.Request) == 0 && state.IntentFingerprint == "" && state.UploadID == "" {
		return state, nil, nil
	}
	if _, err := uuid.Parse(state.RequestKey); err != nil {
		return state, nil, errAdmissionUnresolved
	}
	digest, err := hex.DecodeString(state.IntentFingerprint)
	if err != nil || len(digest) != sha256.Size {
		return state, nil, errAdmissionUnresolved
	}
	if state.UploadID != "" {
		if _, err := uuid.Parse(state.UploadID); err != nil {
			return state, nil, errAdmissionUnresolved
		}
	}
	var request api.UploadCreateRequest
	decoder := json.NewDecoder(bytes.NewReader(state.Request))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || request.Name == "" || request.Size < 0 {
		return state, nil, errAdmissionUnresolved
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return state, nil, errAdmissionUnresolved
	}
	return state, &request, nil
}

func canonicalAdmissionPath(root, remote string) string {
	return strings.TrimPrefix(path.Clean("/"+strings.Trim(root, "/")+"/"+strings.Trim(remote, "/")), "/")
}

func admissionDigest(value any) [32]byte {
	body, err := json.Marshal(value)
	if err != nil {
		panic("invalid internal admission fingerprint")
	}
	return sha256.Sum256(body)
}

// The credential participates only in this private digest, never in errors or journals.
func (f *Fs) admissionRemoteIdentity() [32]byte {
	return admissionDigest(struct {
		Name, Host, Credential string
		UserID                 int64
	}{
		f.name, strings.TrimRight(f.opt.ApiHost, "/"), f.opt.APIKey, f.userId,
	})
}

func admissionSourceIdentity(source fs.Info) [32]byte {
	if source == nil {
		return admissionDigest("source-less")
	}
	if f, ok := source.(*Fs); ok {
		return f.admissionRemoteIdentity()
	}
	return admissionDigest(struct{ Name string }{source.Name()})
}

func admissionSourcePath(source fs.ObjectInfo) string {
	root := ""
	if source.Fs() != nil {
		root = source.Fs().Root()
	}
	return canonicalAdmissionPath(root, source.Remote())
}

func commandAdmissionRun(ctx context.Context) uint64 {
	if fs.IsRCRequest(ctx) {
		return 0
	}
	return fs.GetConfig(ctx).CommandRunID
}

// selectUploadAdmission is the single selector for upload and copy. Kind is a
// transport discriminator: a retained copy vetoes a replacement upload as well.
func (f *Fs) selectUploadAdmission(ctx context.Context, kind string, source fs.ObjectInfo, requestedRemote, effectiveRemote string, transport [32]byte) (*uploadAdmission, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	logical := source
	destinationRoot := f.root
	if intent, ok := fs.UploadAdmissionIntentFromContext(ctx); ok {
		primary, usable := intent.Destination.(*Fs)
		if !usable || primary.admissionRemoteIdentity() != f.admissionRemoteIdentity() || intent.Source == nil {
			return nil, errAdmissionConflict
		}
		logical, requestedRemote, destinationRoot = intent.Source, intent.DestinationRemote, primary.root
	}
	if logical == nil {
		return nil, errAdmissionUnresolved
	}
	identity := admissionIdentity{
		remote: f.admissionRemoteIdentity(), source: admissionSourceIdentity(logical.Fs()),
		sourcePath: admissionSourcePath(logical), requestedTarget: canonicalAdmissionPath(destinationRoot, requestedRemote),
	}
	// Static raw-stream metadata identifies transport only, not a persistent source.
	if _, _, transient := fs.UploadAdmissionTransientScopeFromContext(ctx); transient {
		if _, hasIntent := fs.UploadAdmissionIntentFromContext(ctx); !hasIntent {
			identity.source, identity.sourcePath = admissionSourceIdentity(nil), ""
		}
	}
	effective := canonicalAdmissionPath(f.root, effectiveRemote)
	sourceFingerprint := admissionSourceFingerprint{source.Size(), source.ModTime(ctx).UTC(), fs.MimeType(ctx, source)}
	logicalFingerprint := admissionSourceFingerprint{logical.Size(), logical.ModTime(ctx).UTC(), fs.MimeType(ctx, logical)}
	fingerprint := admissionDigest(struct {
		Size                                 int64
		ModTime, MIME, Effective, UploadHost string
		Transport                            [32]byte
		Logical                              admissionSourceFingerprint
		Encrypt                              bool
		ChunkSize                            fs.SizeSuffix
		ChannelID                            int64
		Encoding                             string
	}{sourceFingerprint.Size, sourceFingerprint.ModTime.Format(time.RFC3339Nano), sourceFingerprint.MIME, effective,
		f.opt.UploadHost, transport, logicalFingerprint, f.opt.EncryptFiles, f.opt.ChunkSize, f.opt.ChannelID, f.opt.Enc.String()})
	newAdmission := func() *uploadAdmission {
		return &uploadAdmission{identity: identity, effectiveTarget: effective, kind: kind, fingerprint: fingerprint,
			sourceFingerprint: sourceFingerprint, logicalFingerprint: logicalFingerprint}
	}
	if store, ok := fs.UploadAdmissionStoreFromContext(ctx); ok {
		if kind != "upload" {
			return nil, errAdmissionConflict
		}
		state, request, err := loadDurableAdmission(ctx, store)
		if err != nil {
			return nil, err
		}
		if request != nil && (request.Name != f.opt.Enc.FromStandardName(path.Base(effective)) ||
			request.Size != sourceFingerprint.Size || !request.ModTime.Equal(sourceFingerprint.ModTime) ||
			request.MimeType != sourceFingerprint.MIME || request.Encryption != f.opt.EncryptFiles ||
			request.ConflictPolicy != "replace" || request.PreferredPartSize != int64(f.opt.ChunkSize)) {
			return nil, errAdmissionConflict
		}
		digest := admissionDigest([]any{identity.remote, identity.source, identity.sourcePath, identity.requestedTarget, fingerprint})
		binding := hex.EncodeToString(digest[:])
		if state.IntentFingerprint != "" && state.IntentFingerprint != binding {
			return nil, errAdmissionConflict
		}
		selected := newAdmission()
		selected.store, selected.durable = store, state
		selected.durable.IntentFingerprint = binding
		selected.request, selected.key, selected.uploadID = request, state.RequestKey, state.UploadID
		if selected.key == "" {
			selected.key = uuid.NewString()
		}
		return selected, nil
	}
	scope, err := admissionScopeForContext(ctx)
	if err != nil {
		return nil, err
	}
	if scope == nil {
		selected := newAdmission()
		selected.key = uuid.NewString()
		return selected, nil
	}
	commandAdmissions.Lock()
	defer commandAdmissions.Unlock()
	if scope.closed {
		return nil, errAdmissionUnresolved
	}
	if retained := scope.entries[identity]; retained != nil {
		retained.mu.Lock()
		defer retained.mu.Unlock()
		// Only an unfrozen preflight may take ordinary manual-copy fallback.
		if retained.kind != kind && retained.request == nil && retained.copyRequest == nil && retained.busy == nil &&
			retained.logicalFingerprint == logicalFingerprint && retained.effectiveTarget == effective {
			retained.kind, retained.fingerprint, retained.effectiveTarget = kind, fingerprint, effective
			retained.sourceFingerprint = sourceFingerprint
		}
		if retained.kind != kind || retained.fingerprint != fingerprint || retained.effectiveTarget != effective {
			return nil, errAdmissionConflict
		}
		return retained, nil
	}
	selected := newAdmission()
	selected.key = uuid.NewString()
	scope.entries[identity] = selected
	// Index pending selections too; a snapshot becomes admitted atomically when
	// FreezeRequest/SelectUploadID publishes its immutable request under entry.mu.
	for _, target := range []string{identity.requestedTarget, effective} {
		key := admissionDestination{identity.remote, identity.source, target}
		if scope.destinations[key] == nil {
			scope.destinations[key] = make(map[admissionIdentity]*uploadAdmission)
		}
		scope.destinations[key][identity] = selected
	}
	return selected, nil
}

// acquire serializes duplicate preparation without holding a mutex over IO.
func (a *uploadAdmission) acquire(ctx context.Context) (func(), error) {
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		a.mu.Lock()
		if a.busy == nil {
			done := make(chan struct{})
			a.busy = done
			a.mu.Unlock()
			return func() { a.mu.Lock(); a.busy = nil; close(done); a.mu.Unlock() }, nil
		}
		done := a.busy
		a.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-done:
		}
	}
}

func cloneAdmissionRequest(request *api.UploadCreateRequest) *api.UploadCreateRequest {
	if request == nil {
		return nil
	}
	copy := *request
	if request.Hash != nil {
		hash := *request.Hash
		copy.Hash = &hash
	}
	return &copy
}

func (a *uploadAdmission) snapshot() admissionSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	snapshot := admissionSnapshot{version: a.version, key: a.key, request: cloneAdmissionRequest(a.request), copySourceID: a.copySourceID, uploadID: a.uploadID, source: a.sourceFingerprint}
	if a.copyRequest != nil {
		copy := *a.copyRequest
		snapshot.copyRequest = &copy
	}
	return snapshot
}

func (a *uploadAdmission) selectionVersion() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.version
}

// freezeGuardedIntent binds the incarnation, keys and captured destination to
// this admission once. Later attempts must present the identical intent.
func (a *uploadAdmission) freezeGuardedIntent(ctx context.Context, incarnation string, destination api.GuardedUploadDestination) (*guardedIntent, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if _, err := uuid.Parse(incarnation); err != nil {
		return nil, errAdmissionConflict
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.kind != "upload" || a.request == nil || a.key == "" {
		return nil, errAdmissionConflict
	}
	if a.guarded != nil {
		if a.guarded.incarnation != incarnation || !guardedDestinationEqual(a.guarded.destination, destination) {
			return nil, errAdmissionConflict
		}
		copy := *a.guarded
		return &copy, nil
	}
	intent := guardedIntent{
		incarnation: incarnation, createKey: a.key, completeKey: uuid.NewString(), destination: destination,
	}
	a.guarded = &intent
	a.version++
	copy := intent
	return &copy, nil
}

func (a *uploadAdmission) guardedSnapshot() (*guardedIntent, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.guarded == nil {
		return nil, false
	}
	copy := *a.guarded
	return &copy, true
}

// FreezeRequest authorizes same-key POST replay only after successful absence discovery.
func (a *uploadAdmission) FreezeRequest(ctx context.Context, request api.UploadCreateRequest) error {
	return a.freezeUploadRequest(ctx, request, "")
}

// AdoptUpload commits request and selected ID together before the first exact GET.
func (a *uploadAdmission) AdoptUpload(ctx context.Context, request api.UploadCreateRequest, uploadID string) error {
	if uploadID == "" {
		return errAdmissionUnresolved
	}
	if a.store != nil {
		if _, err := uuid.Parse(uploadID); err != nil {
			return errAdmissionUnresolved
		}
	}
	return a.freezeUploadRequest(ctx, request, uploadID)
}

func (a *uploadAdmission) freezeUploadRequest(ctx context.Context, request api.UploadCreateRequest, uploadID string) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	request.ModTime = request.ModTime.UTC()
	a.mu.Lock()
	if a.kind != "upload" {
		a.mu.Unlock()
		return errAdmissionConflict
	}
	if a.request != nil {
		sameHash := (a.request.Hash == nil && request.Hash == nil) ||
			(a.request.Hash != nil && request.Hash != nil && *a.request.Hash == *request.Hash)
		original, candidate := *a.request, request
		original.Hash, candidate.Hash = nil, nil
		sameID := uploadID == "" || a.uploadID == uploadID
		a.mu.Unlock()
		if !sameHash || original != candidate || !sameID {
			return errAdmissionConflict
		}
		return nil
	}
	state, version := a.durable, a.version
	state.RequestKey, state.UploadID = a.key, uploadID
	if a.store != nil {
		state.Request, _ = json.Marshal(request)
	}
	a.mu.Unlock()
	if a.store != nil {
		if err := a.store.StoreUploadAdmission(ctx, state); err != nil {
			return err
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.version != version {
		return errAdmissionConflict
	}
	a.request, a.uploadID, a.durable = cloneAdmissionRequest(&request), uploadID, state
	a.version++
	return nil
}

// SelectUploadID is monotone even when the following exact GET fails or truncates.
func (a *uploadAdmission) SelectUploadID(ctx context.Context, uploadID string) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if uploadID == "" {
		return errAdmissionUnresolved
	}
	if a.store != nil {
		if _, err := uuid.Parse(uploadID); err != nil {
			return errAdmissionUnresolved
		}
	}
	a.mu.Lock()
	if a.kind != "upload" || a.request == nil {
		a.mu.Unlock()
		return errAdmissionConflict
	}
	if a.uploadID != "" {
		same := a.uploadID == uploadID
		a.mu.Unlock()
		if !same {
			return errAdmissionConflict
		}
		return nil
	}
	state, version := a.durable, a.version
	state.UploadID = uploadID
	a.mu.Unlock()
	if a.store != nil {
		if err := a.store.StoreUploadAdmission(ctx, state); err != nil {
			return err
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.version != version {
		return errAdmissionConflict
	}
	a.uploadID, a.durable = uploadID, state
	a.version++
	return nil
}

func (a *uploadAdmission) observeSession(ctx context.Context, session api.UploadSession) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if session.ID != a.uploadID || session.PartSize <= 0 || !session.RequiresCompletionManifest {
		return errAdmissionUnresolved
	}
	if a.partSize != 0 && (a.partSize != session.PartSize || a.encryption != session.Encryption) {
		return errAdmissionConflict
	}
	if a.resultID != "" && a.resultID != session.FileId {
		return errAdmissionConflict
	}
	if a.partSize == 0 {
		a.partSize, a.encryption = session.PartSize, session.Encryption
		a.version++
	}
	if session.State == "completed" && a.resultID == "" {
		if session.FileId == "" {
			return errAdmissionUnresolved
		}
		a.resultID = session.FileId
		a.version++
	}
	return nil
}

func (a *uploadAdmission) freezeCopyRequest(ctx context.Context, sourceID string, request api.FileCopy) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.kind != "copy" || sourceID == "" {
		return errAdmissionConflict
	}
	if a.copyRequest != nil {
		if a.copySourceID != sourceID || *a.copyRequest != request {
			return errAdmissionConflict
		}
		return nil
	}
	a.copySourceID = sourceID
	a.copyRequest = &request
	a.version++
	return nil
}

func admissionPathContains(parent, child string) bool {
	return parent == "" || parent == child || strings.HasPrefix(child, parent+"/")
}

// reconcileAdmissions reads original authorized sessions only. Frozen POSTs and
// metadata-only completed results never authorize folder creation or a move.
func (f *Fs) reconcileAdmissions(ctx context.Context, source fs.Info, sourceRemote, destinationRemote string, subtree bool) error {
	return f.reconcileAdmissionTarget(ctx, source, sourceRemote, canonicalAdmissionPath(f.root, destinationRemote), subtree)
}

func (f *Fs) reconcileCreateDir(ctx context.Context, pathID, leaf string) error {
	parent, ok := f.dirCache.GetInv(pathID)
	if !ok {
		return f.reconcileAdmissions(ctx, nil, "", "", true)
	}
	root := f.root
	// Before FindRoot completes the inverse cache is relative to the drive root.
	// Do not call FoundRoot here: dircache holds its own mutex during this callback.
	if cachedRoot, ok := f.dirCache.Get(""); ok && cachedRoot == rootID {
		root = ""
	}
	return f.reconcileAdmissionTarget(ctx, nil, "", canonicalAdmissionPath(root, path.Join(parent, leaf)), true)
}

func (f *Fs) reconcileAdmissionTarget(ctx context.Context, source fs.Info, sourceRemote, target string, subtree bool) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if store, ok := fs.UploadAdmissionStoreFromContext(ctx); ok {
		state, request, err := loadDurableAdmission(ctx, store)
		if err != nil {
			return err
		}
		if request == nil {
			return nil
		}
		if state.UploadID == "" {
			return errAdmissionUnresolved
		}
		if _, err := (&Object{fs: f}).getUploadSession(ctx, state.UploadID); err != nil {
			return err
		}
		return errAdmissionUnresolved
	}
	scope, err := admissionScopeForContext(ctx)
	if err != nil {
		return err
	}
	if scope == nil {
		return nil
	}
	remote := f.admissionRemoteIdentity()
	sourceID := admissionSourceIdentity(source)
	sourcePath := ""
	if source != nil {
		sourcePath = canonicalAdmissionPath(source.Root(), sourceRemote)
	}
	commandAdmissions.Lock()
	if scope.closed {
		commandAdmissions.Unlock()
		return errAdmissionUnresolved
	}
	var affected []*uploadAdmission
	{
		for identity, entry := range scope.entries {
			if identity.remote != remote {
				continue
			}
			targetMatch := identity.requestedTarget == target || entry.effectiveTarget == target
			if subtree {
				targetMatch = admissionPathContains(target, identity.requestedTarget) || admissionPathContains(target, entry.effectiveTarget)
			}
			sourceMatch := source != nil && identity.source == sourceID && identity.sourcePath == sourcePath
			if subtree && source != nil {
				sourceMatch = identity.source == sourceID && admissionPathContains(sourcePath, identity.sourcePath)
			}
			if targetMatch || sourceMatch {
				affected = append(affected, entry)
			}
		}
	}
	commandAdmissions.Unlock()
	for _, entry := range affected {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		snapshot := entry.snapshot()
		// A selector with no frozen request has not admitted any remote work.
		if snapshot.request == nil && snapshot.copyRequest == nil {
			continue
		}
		if snapshot.uploadID == "" {
			return errAdmissionUnresolved
		}
		_, err := (&Object{fs: f}).getUploadSession(ctx, snapshot.uploadID)
		if err != nil {
			return err
		}
		if entry.selectionVersion() != snapshot.version {
			return errAdmissionUnresolved
		}
		// No body/manifest was verified by this directory or movement callback.
		return errAdmissionUnresolved
	}
	return nil
}

// CheckUploadAdmission never allocates or replays a POST to prove absence.
func (f *Fs) CheckUploadAdmission(ctx context.Context, source fs.Object, destinationRemote string) (bool, error) {
	if err := context.Cause(ctx); err != nil {
		return false, err
	}
	if source == nil {
		return false, errAdmissionUnresolved
	}
	if intent, ok := fs.UploadAdmissionIntentFromContext(ctx); ok {
		primary, usable := intent.Destination.(*Fs)
		if !usable || primary.admissionRemoteIdentity() != f.admissionRemoteIdentity() || intent.Source == nil {
			return false, errAdmissionConflict
		}
		f, source, destinationRemote = primary, intent.Source, intent.DestinationRemote
	}
	if store, ok := fs.UploadAdmissionStoreFromContext(ctx); ok {
		state, request, err := loadDurableAdmission(ctx, store)
		if err != nil {
			return false, err
		}
		if request == nil {
			return false, nil
		}
		// Reconstruct through the same selector to validate the durable binding.
		entry, err := f.selectUploadAdmission(ctx, "upload", source, destinationRemote, destinationRemote, [32]byte{})
		if err != nil {
			return true, err
		}
		if state.RequestKey != entry.snapshot().key {
			return true, errAdmissionConflict
		}
		return f.reconcileAdmissionEntry(ctx, entry)
	}
	scope, err := admissionScopeForContext(ctx)
	if err != nil || scope == nil {
		return false, err
	}
	identity := admissionIdentity{f.admissionRemoteIdentity(), admissionSourceIdentity(source.Fs()),
		admissionSourcePath(source), canonicalAdmissionPath(f.root, destinationRemote)}
	commandAdmissions.Lock()
	entry := scope.entries[identity]
	closed := scope.closed
	commandAdmissions.Unlock()
	if closed {
		return false, errAdmissionUnresolved
	}
	if entry == nil {
		return false, nil
	}
	entry.mu.Lock()
	original := entry.logicalFingerprint
	entry.mu.Unlock()
	current := admissionSourceFingerprint{source.Size(), source.ModTime(ctx).UTC(), fs.MimeType(ctx, source)}
	if current != original {
		return true, errAdmissionConflict
	}
	return f.reconcileAdmissionEntry(ctx, entry)
}

func (f *Fs) reconcileAdmissionEntry(ctx context.Context, entry *uploadAdmission) (bool, error) {
	snapshot := entry.snapshot()
	if snapshot.request == nil && snapshot.copyRequest == nil {
		if entry.selectionVersion() != snapshot.version {
			return false, errAdmissionUnresolved
		}
		return false, nil
	}
	if snapshot.uploadID == "" {
		return true, errAdmissionUnresolved
	}
	session, err := (&Object{fs: f}).getUploadSession(ctx, snapshot.uploadID)
	if err != nil {
		return true, err
	}
	if entry.selectionVersion() != snapshot.version {
		return true, errAdmissionUnresolved
	}
	if snapshot.request == nil || !uploadSessionRequestMatches(*session, *snapshot.request) {
		return true, errAdmissionConflict
	}
	// A read-only discovery has not read current source bytes through EOF or
	// authorized the original result after that read, even for completed work.
	return true, errAdmissionUnresolved
}

type admissionSourceReadKey struct{}

func (f *Fs) reconcileDiscovery(ctx context.Context, destinationRemote string, subtree bool) error {
	if reading, _ := ctx.Value(admissionSourceReadKey{}).(bool); reading {
		return nil
	}
	source, exact, marked := fs.UploadAdmissionDiscoveryFromContext(ctx)
	if !marked {
		return nil
	}
	refuse := func(err error) error {
		if err == nil || fs.IsUploadAdmissionError(err) {
			return err
		}
		return &fs.UploadAdmissionError{Err: err}
	}
	if source == nil {
		return refuse(errAdmissionUnresolved)
	}
	if _, durable := fs.UploadAdmissionStoreFromContext(ctx); durable {
		intent, ok := fs.UploadAdmissionIntentFromContext(ctx)
		if !ok || intent.Source == nil {
			return refuse(errAdmissionUnresolved)
		}
		_, err := f.CheckUploadAdmission(ctx, intent.Source, intent.DestinationRemote)
		return refuse(err)
	}
	scope, err := admissionScopeForContext(ctx)
	if err != nil || scope == nil {
		return refuse(err)
	}
	target := canonicalAdmissionPath(f.root, destinationRemote)
	remote, sourceID := f.admissionRemoteIdentity(), admissionSourceIdentity(source)
	sourceRoot := canonicalAdmissionPath(source.Root(), "")
	commandAdmissions.Lock()
	if scope.closed {
		commandAdmissions.Unlock()
		return refuse(errAdmissionUnresolved)
	}
	affected := make(map[admissionIdentity]*uploadAdmission)
	for key, entries := range scope.destinations {
		if key.remote != remote || key.source != sourceID {
			continue
		}
		if key.target != target && !(subtree && admissionPathContains(target, key.target)) {
			continue
		}
		for identity, entry := range entries {
			if exact != nil && identity.sourcePath != canonicalAdmissionPath(source.Root(), *exact) {
				continue
			}
			if !admissionPathContains(sourceRoot, identity.sourcePath) {
				continue
			}
			affected[identity] = entry
		}
	}
	commandAdmissions.Unlock()
	for identity, entry := range affected {
		if exact == nil {
			owner, ok := source.(fs.Fs)
			if !ok {
				return refuse(errAdmissionUnresolved)
			}
			sourceRemote := strings.TrimPrefix(strings.TrimPrefix(identity.sourcePath, sourceRoot), "/")
			current, err := owner.NewObject(context.WithValue(ctx, admissionSourceReadKey{}, true), sourceRemote)
			if errors.Is(err, fs.ErrorObjectNotFound) && !fs.IsUploadAdmissionError(err) {
				continue
			}
			if err != nil {
				return refuse(err)
			}
			if !filter.GetConfig(ctx).IncludeObject(ctx, current) {
				continue
			}
		}
		if _, err := f.reconcileAdmissionEntry(ctx, entry); err != nil {
			return refuse(err)
		}
	}
	return nil
}

func (f *Fs) verifyAdmissionCopy(ctx context.Context, source, result fs.Object) error {
	if source == nil || result == nil {
		return errAdmissionUnresolved
	}
	current := source
	if owner, ok := source.Fs().(fs.Fs); ok {
		fresh, err := owner.NewObject(ctx, source.Remote())
		if err != nil {
			return err
		}
		if fresh.Size() != source.Size() || !fresh.ModTime(ctx).Equal(source.ModTime(ctx)) {
			return errAdmissionConflict
		}
		current = fresh
	}
	digest := func(object fs.Object) ([32]byte, int64, error) {
		in, err := object.Open(ctx)
		if err != nil {
			return [32]byte{}, 0, err
		}
		hash := sha256.New()
		size, readErr := io.Copy(hash, readers.NewContextReader(ctx, in))
		closeErr := in.Close()
		var sum [32]byte
		copy(sum[:], hash.Sum(nil))
		return sum, size, errors.Join(readErr, closeErr, context.Cause(ctx))
	}
	sourceSum, sourceSize, err := digest(current)
	if err != nil {
		return err
	}
	resultSum, resultSize, err := digest(result)
	if err != nil {
		return err
	}
	if sourceSum != resultSum || sourceSize != resultSize ||
		(current.Size() >= 0 && sourceSize != current.Size()) || resultSize != result.Size() {
		return errAdmissionConflict
	}
	// Re-resolve after reading: a historical copy response is not current access.
	authorized, err := f.NewObject(ctx, result.Remote())
	if err != nil {
		return err
	}
	actual, ok := authorized.(*Object)
	original, originalOK := result.(*Object)
	if !ok || !originalOK || actual.id != original.id {
		return errAdmissionUnresolved
	}
	if owner, ok := source.Fs().(fs.Fs); ok {
		after, err := owner.NewObject(ctx, source.Remote())
		if err != nil {
			return err
		}
		if after.Size() != current.Size() || !after.ModTime(ctx).Equal(current.ModTime(ctx)) {
			return errAdmissionConflict
		}
	}
	return context.Cause(ctx)
}
