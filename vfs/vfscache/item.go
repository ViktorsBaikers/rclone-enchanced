package vfscache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/lib/file"
	"github.com/rclone/rclone/lib/ranges"
	"github.com/rclone/rclone/vfs/vfscache/downloaders"
	"github.com/rclone/rclone/vfs/vfscache/writeback"
	"github.com/rclone/rclone/vfs/vfscommon"
)

// NB as Cache and Item are tightly linked it is necessary to have a
// total lock ordering between them. So Cache.mu must always be
// taken before Item.mu to avoid deadlocks.
//
// Cache may call into Item but care is needed if Item calls Cache
//
// A lot of the Cache methods do not require locking, these include
//
// - Cache.toOSPath
// - Cache.toOSPathMeta
// - Cache.createItemDir
// - Cache.objectFingerprint
// - Cache.AddVirtual

// NB Item and downloader are tightly linked so it is necessary to
// have a total lock ordering between them. downloader.mu must always
// be taken before Item.mu. downloader may call into Item but Item may
// **not** call downloader methods with Item.mu held

// NB Item and writeback are tightly linked so it is necessary to
// have a total lock ordering between them. writeback.mu must always
// be taken before Item.mu. writeback may call into Item but Item may
// **not** call writeback methods with Item.mu held

// LL Item reset is invoked by cache cleaner for synchronous recovery
// from ENOSPC errors. The reset operation removes the cache file and
// closes/reopens the downloaders.  Although most parts of reset and
// other item operations are done with the item mutex held, the mutex
// is released during fd.WriteAt and downloaders calls. We use preAccess
// and postAccess calls to serialize reset and other item operations.

// Item is stored in the item map
//
// The Info field is written to the backing store to store status
type Item struct {
	// read only
	c                 *Cache                   // cache this is part of
	mu                sync.Mutex               // protect the variables
	mutation          sync.Mutex               // serialize marker-through-clear local mutation across handles
	cond              sync.Cond                // synchronize with cache cleaner
	name              string                   // name in the VFS
	opens             int                      // number of times file is open
	downloaders       *downloaders.Downloaders // a record of the downloaders in action - may be nil
	o                 fs.Object                // object we are caching - may be nil
	fd                *os.File                 // handle we are using to read and write to the file
	info              Info                     // info about the file to persist to backing store
	writeBackID       writeback.Handle         // id of any writebacks in progress
	pendingAccesses   int                      // number of threads - cache reset not allowed if not zero
	modified          bool                     // set if the file has been modified since the last Open
	beingReset        bool                     // cache cleaner is resetting the cache file, access not allowed
	graceTimer        *time.Timer              // timer for delayed close after grace period
	closing           chan struct{}            // non-nil while a grace-period close is tearing the handle down, closed when done
	admissionChanging bool                     // old writer is being canceled before new intent
	admissionErr      error                    // retain uncertain metadata instead of deleting data
	admissionUnsaved  bool                     // retry only failed journal saves before admission
	admissionFrozen   bool                     // a further write must not reuse admitted content
	writeWasDirty     bool
	storeCancel       context.CancelFunc
	storeDone         chan struct{}
	guardActive       bool // the remote requires guarded writes
	guardJournal      *fs.GuardedWriteJournal
	guardHeld         string // sticky non-runnable reason
	mutationUncertain bool   // a local mutation marker outlived its data sync
}

// Info is persisted to backing store
type Info struct {
	ModTime          time.Time       // last time file was modified
	ATime            time.Time       // last time file was accessed
	Size             int64           // size of the file
	Rs               ranges.Ranges   // which parts of the file are present
	Fingerprint      string          // fingerprint of remote object
	Dirty            bool            // set if the backing file has been modified
	UploadGeneration string          `json:",omitempty"`
	UploadAdmission  json.RawMessage `json:",omitempty"` // validate only through the optional store
	UploadGuard      json.RawMessage `json:",omitempty"` // versioned guarded write journal
	MutationMarker   string          `json:",omitempty"` // generation of in-flight local mutation
	GuardHold        string          `json:",omitempty"` // sticky non-runnable reason
}

// Items are a slice of *Item ordered by ATime
type Items []*Item

// ResetResult reports the actual action taken in the Reset function and reason
type ResetResult int

// Constants used to report actual action taken in the Reset function and reason
const (
	SkippedDirty         ResetResult = iota // Dirty item cannot be reset
	SkippedPendingAccess                    // Reset pending access can lead to deadlock
	SkippedEmpty                            // Reset empty item does not save space
	SkippedGrace                            // Item is in grace period, treat as in-use
	RemovedNotInUse                         // Item not used. Remove instead of reset
	ResetFailed                             // Reset failed with an error
	ResetComplete                           // Reset completed successfully
)

func (rr ResetResult) String() string {
	return [...]string{"Dirty item skipped", "In-access item skipped", "Empty item skipped",
		"Grace period item skipped", "Not-in-use item removed", "Item reset failed", "Item reset completed"}[rr]
}

func (v Items) Len() int      { return len(v) }
func (v Items) Swap(i, j int) { v[i], v[j] = v[j], v[i] }
func (v Items) Less(i, j int) bool {
	if i == j {
		return false
	}
	iItem := v[i]
	jItem := v[j]
	iItem.mu.Lock()
	defer iItem.mu.Unlock()
	jItem.mu.Lock()
	defer jItem.mu.Unlock()

	return iItem.info.ATime.Before(jItem.info.ATime)
}

// clean the item after its cache file has been deleted
func (info *Info) clean() {
	*info = Info{}
	info.ModTime = time.Now()
	info.ATime = info.ModTime
}

// StoreFn is called back with an object after it has been uploaded
type StoreFn func(fs.Object)

// newItem returns an item for the cache
func newItem(c *Cache, name string) (item *Item) {
	now := time.Now()
	item = &Item{
		c:    c,
		name: name,
		info: Info{
			ModTime: now,
			ATime:   now,
		},
	}
	item.cond = sync.Cond{L: &item.mu}
	// check the cache file exists
	osPath := c.toOSPath(name)
	fi, statErr := os.Stat(osPath)

	// Try to load the metadata
	exists, err := item.load()
	if !exists {
		item._removeFile("metadata doesn't exist")
	} else if err != nil {
		// Uncertain metadata may contain the only selected remote admission.
		item.info.Dirty = true
		item.admissionErr = err
		fs.Errorf(name, "vfs cache: retaining unreadable metadata: %v", err)
	}
	if statErr != nil && exists {
		if item.info.Dirty || len(item.info.UploadAdmission) != 0 {
			item.info.Dirty = true
			item.admissionErr = fmt.Errorf("vfs cache: retaining unavailable dirty source: %w", statErr)
		} else if os.IsNotExist(statErr) {
			item._removeMeta("clean cache file doesn't exist")
		} else {
			item.remove(fmt.Sprintf("failed to stat clean cache file: %v", statErr))
		}
	}

	// Get size estimate (which is best we can do until Open() called)
	if statErr == nil {
		item.info.Size = fi.Size()
	}
	if guard, ok := c.fremote.(fs.GuardedWriteFs); ok && guard.GuardedWriteEnabled() {
		item.guardActive = true
		item._loadGuard()
	}
	return item
}

// inUse returns true if the item is open or dirty
func (item *Item) inUse() bool {
	item.mu.Lock()
	defer item.mu.Unlock()
	return item.opens != 0 || item.info.Dirty
}

// getDiskSize returns the size on disk (approximately) of the item
//
// We return the sizes of the chunks we have fetched, however there is
// likely to be some overhead which we are not taking into account.
func (item *Item) getDiskSize() int64 {
	item.mu.Lock()
	defer item.mu.Unlock()
	return item.info.Rs.Size()
}

// load reads an item from the disk or returns nil if not found
func (item *Item) load() (exists bool, err error) {
	item.mu.Lock()
	defer item.mu.Unlock()
	osPathMeta := item.c.toOSPathMeta(item.name) // No locking in Cache
	in, err := file.Open(osPathMeta)
	if err != nil {
		if os.IsNotExist(err) {
			return false, err
		}
		return true, fmt.Errorf("vfs cache item: failed to read metadata: %w", err)
	}
	defer fs.CheckClose(in, &err)
	decoder := json.NewDecoder(in)
	err = decoder.Decode(&item.info)
	if err != nil {
		return true, fmt.Errorf("vfs cache item: corrupt metadata: %w", err)
	}
	return true, nil
}

// save writes an item to the disk
//
// call with the lock held
func (item *Item) _save() (err error) {
	defer func() { item.admissionUnsaved = err != nil }()
	if item.admissionErr != nil {
		return item.admissionErr
	}
	osPathMeta := item.c.toOSPathMeta(item.name) // No locking in Cache
	out, err := os.CreateTemp(filepath.Dir(osPathMeta), ".admission-*")
	if err != nil {
		return fmt.Errorf("vfs cache item: failed to write metadata: %w", err)
	}
	defer func() {
		_ = out.Close()
		_ = os.Remove(out.Name())
	}()
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "\t")
	if err = encoder.Encode(item.info); err != nil {
		return fmt.Errorf("vfs cache item: failed to encode metadata: %w", err)
	}
	if err = out.Sync(); err != nil {
		return fmt.Errorf("vfs cache item: failed to sync metadata: %w", err)
	}
	if err = out.Close(); err != nil {
		return fmt.Errorf("vfs cache item: failed to close metadata: %w", err)
	}
	if err = os.Rename(out.Name(), osPathMeta); err != nil {
		return fmt.Errorf("vfs cache item: failed to replace metadata: %w", err)
	}
	return nil
}

// A store is bound to the captured generation, never to the next user write.
type itemAdmissionStore struct {
	item       *Item
	generation string
}

func (item *Item) admissionStore() fs.UploadAdmissionStore {
	item.mu.Lock()
	defer item.mu.Unlock()
	return &itemAdmissionStore{item: item, generation: item.info.UploadGeneration}
}

func (store *itemAdmissionStore) load(ctx context.Context) (fs.UploadAdmissionState, error) {
	item := store.item
	if err := ctx.Err(); err != nil {
		return fs.UploadAdmissionState{}, err
	}
	if item.admissionErr != nil {
		return fs.UploadAdmissionState{}, item.admissionErr
	}
	if item.admissionChanging || store.generation == "" || item.info.UploadGeneration != store.generation {
		return fs.UploadAdmissionState{}, errors.New("vfs cache: unavailable or stale upload generation")
	}
	var state fs.UploadAdmissionState
	decoder := json.NewDecoder(bytes.NewReader(item.info.UploadAdmission))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return state, fmt.Errorf("vfs cache: invalid upload admission: %w", err)
	}
	if state.Generation != store.generation {
		return state, errors.New("vfs cache: upload admission generation mismatch")
	}
	return state, nil
}

func (store *itemAdmissionStore) LoadUploadAdmission(ctx context.Context) (fs.UploadAdmissionState, error) {
	store.item.mu.Lock()
	defer store.item.mu.Unlock()
	state, err := store.load(ctx)
	if err != nil {
		return state, err
	}
	// A prior dirty-marker save may have failed; no remote send may bypass it.
	if store.item.admissionUnsaved {
		return state, store.item._save()
	}
	return state, nil
}

func (store *itemAdmissionStore) StoreUploadAdmission(ctx context.Context, state fs.UploadAdmissionState) error {
	item := store.item
	item.mu.Lock()
	defer item.mu.Unlock()
	previous, err := store.load(ctx)
	if err != nil {
		return err
	}
	if state.Generation != previous.Generation ||
		(previous.RequestKey != "" && (previous.RequestKey != state.RequestKey ||
			previous.IntentFingerprint != state.IntentFingerprint || string(previous.Request) != string(state.Request))) ||
		(previous.UploadID != "" && previous.UploadID != state.UploadID) {
		return errors.New("vfs cache: conflicting upload admission")
	}
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	old := item.info.UploadAdmission
	item.info.UploadAdmission = body
	if err = item._save(); err != nil {
		item.info.UploadAdmission = old
	} else if state.RequestKey != "" {
		item.admissionFrozen = true
	}
	return err
}

func (item *Item) _newUploadGeneration() {
	item.info.UploadGeneration = uuid.NewString()
	body, _ := json.Marshal(fs.UploadAdmissionState{Generation: item.info.UploadGeneration})
	item.info.UploadAdmission = body
	item.admissionErr = nil
	item.admissionFrozen = false
}

// _loadGuard restores the durable guarded journal and any interrupted local
// mutation. Old, malformed or path-only proof stays held, never runnable.
//
// Called once during construction before the item is published.
func (item *Item) _loadGuard() {
	if item.info.GuardHold != "" {
		item.guardHeld = item.info.GuardHold
	}
	if item.info.MutationMarker != "" {
		item.mutationUncertain = true
		if item.guardHeld == "" {
			item.guardHeld = "vfs cache: uncertain local mutation"
		}
		// The saved ranges describe the bytes before the interrupted write.
		// They are not proof for the bytes now on disk.
		item.info.Rs = nil
	}
	if len(item.info.UploadGuard) != 0 {
		var journal fs.GuardedWriteJournal
		decoder := json.NewDecoder(bytes.NewReader(item.info.UploadGuard))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&journal); err != nil || journal.Version != fs.GuardedWriteJournalVersion {
			if item.guardHeld == "" {
				item.guardHeld = "vfs cache: unreadable guarded journal"
			}
		} else {
			item.guardJournal = &journal
			if journal.Reason != "" && item.guardHeld == "" {
				item.guardHeld = journal.Reason
			}
			if journal.Generation != item.info.UploadGeneration && item.guardHeld == "" {
				item.guardHeld = "vfs cache: guarded journal generation mismatch"
			}
		}
	} else if item.info.Dirty && item.guardHeld == "" {
		item.guardHeld = "vfs cache: missing guarded journal"
	}
	if item.guardHeld != "" {
		item.info.GuardHold = item.guardHeld
		if item.guardJournal != nil {
			item.guardJournal.Reason = item.guardHeld
		}
	}
}

// HeldReason returns the sticky non-runnable reason, if any.
func (item *Item) HeldReason() string {
	item.mu.Lock()
	defer item.mu.Unlock()
	return item.guardHeld
}

// _heldErrorLocked records a definitive hold and returns a queue disposition
// which can never fall into generic retry or success.
//
// Call with item.mu held.
func (item *Item) _heldErrorLocked(reason string) error {
	if reason == "" {
		reason = "vfs cache: guarded write is held"
	}
	item.guardHeld = reason
	item.info.GuardHold = reason
	if item.guardJournal != nil {
		item.guardJournal.Reason = reason
		_ = item._saveGuardLocked()
	}
	if err := item._save(); err != nil {
		fs.Errorf(item.name, "vfs cache: failed to persist held reason: %v", err)
	}
	return &writeback.HeldError{Reason: reason}
}

// _saveGuardLocked serialises the guarded journal into the persisted info.
//
// Call with item.mu held.
func (item *Item) _saveGuardLocked() error {
	if item.guardJournal == nil {
		return nil
	}
	body, err := json.Marshal(item.guardJournal)
	if err != nil {
		return err
	}
	item.info.UploadGuard = body
	return nil
}

// _admissionStateLocked decodes the current B2 admission for the journal.
//
// Call with item.mu held.
func (item *Item) _admissionStateLocked() (fs.UploadAdmissionState, bool) {
	var state fs.UploadAdmissionState
	if len(item.info.UploadAdmission) == 0 {
		return state, false
	}
	if err := json.Unmarshal(item.info.UploadAdmission, &state); err != nil || state.Generation != item.info.UploadGeneration {
		return state, false
	}
	return state, true
}

// _beginMutation durably pins the destination and marker before any local byte
// changes. It verifies the complete original plaintext for a partial edit of an
// authoritative file. Call with item.mu held; item.mu may be released during
// backend calls, which the mutation gate protects across all handles.
func (item *Item) _beginMutation(ctx context.Context, preserveOriginal bool) (began bool, err error) {
	if !item.guardActive {
		return item._beginWrite()
	}
	if item.mutationUncertain || item.guardHeld != "" {
		return false, &writeback.HeldError{Reason: item.guardHeld}
	}
	guard := item.c.fremote.(fs.GuardedWriteFs)
	generation := item.info.UploadGeneration
	var intent fs.GuardedWriteIntent
	if item.guardJournal != nil && item.guardJournal.Generation == generation {
		intent = item.guardJournal.Intent
	} else {
		item.mu.Unlock()
		captured, captureErr := guard.CaptureWriteDestination(ctx, item.name)
		item.mu.Lock()
		if captureErr != nil {
			return false, captureErr
		}
		intent = captured
	}
	if preserveOriginal && !item.info.Dirty {
		switch intent.Baseline.Kind {
		case fs.GuardedBaselineAbsent:
		case fs.GuardedBaselineVersion:
			if intent.Baseline.Hash == nil || intent.Baseline.Hash.Algorithm != "blake3" {
				return false, fs.ErrGuardedProofUnavailable
			}
			size, sizeErr := item._getSize()
			if sizeErr != nil {
				return false, sizeErr
			}
			if size != intent.Baseline.Size {
				return false, fs.ErrGuardedConflict
			}
			if err := item._ensure(0, size); err != nil {
				return false, err
			}
			item.mu.Unlock()
			_, verifyErr := guard.VerifyGuardedOriginal(ctx, intent, io.NewSectionReader(item.fd, 0, size), size)
			item.mu.Lock()
			if verifyErr != nil {
				return false, verifyErr
			}
		default:
			return false, fs.ErrGuardedProofUnavailable
		}
	}
	began, err = item._beginWrite()
	if err != nil {
		return false, err
	}
	journal := &fs.GuardedWriteJournal{
		Version: fs.GuardedWriteJournalVersion, Intent: intent, Generation: item.info.UploadGeneration,
	}
	if state, ok := item._admissionStateLocked(); ok {
		journal.AdmissionKey, journal.SessionID = state.RequestKey, state.UploadID
	}
	item.guardJournal = journal
	marker := item.info.UploadGeneration
	if marker == "" {
		marker = mutationMarkerPending
	}
	item.info.MutationMarker = marker
	oldGuard := item.info.UploadGuard
	saveErr := item._saveGuardLocked()
	if saveErr == nil {
		saveErr = item._save()
	}
	if saveErr != nil {
		item.info.UploadGuard, item.info.MutationMarker = oldGuard, ""
		item.guardJournal = nil
		if began {
			item.info.Dirty = item.writeWasDirty
			item.admissionChanging = false
			item.cond.Broadcast()
		}
		return false, fmt.Errorf("vfs cache: failed to persist local mutation marker: %w", saveErr)
	}
	return began, nil
}

// mutationMarkerPending marks a first mutation whose upload generation is
// created only when the changed bytes are durably synced.
const mutationMarkerPending = "pending"

// GuardedReceiptProof returns the durable, read-only proof for one unresolved
// guarded generation. It is only valid when the local bytes and digest survived.
func (item *Item) GuardedReceiptProof() (fs.GuardedReceiptProof, bool) {
	item.mu.Lock()
	defer item.mu.Unlock()
	if !item.guardActive || item.guardJournal == nil || item.mutationUncertain ||
		item.info.MutationMarker != "" || item.guardJournal.Receipt != nil ||
		item.guardJournal.PlainHash == nil || item.guardJournal.PlainSize < 0 {
		return fs.GuardedReceiptProof{}, false
	}
	journal := *item.guardJournal
	if journal.AdmissionKey == "" || journal.SessionID == "" {
		// The journal is the durable attempt. The admission state is only a
		// fallback for a save that never completed.
		if state, ok := item._admissionStateLocked(); ok {
			if journal.AdmissionKey == "" {
				journal.AdmissionKey = state.RequestKey
			}
			if journal.SessionID == "" {
				journal.SessionID = state.UploadID
			}
		}
	}
	if journal.AdmissionKey == "" || journal.SessionID == "" {
		return fs.GuardedReceiptProof{}, false
	}
	return fs.GuardedReceiptProof{
		Service: journal.Intent.Service, PartOrigin: journal.Intent.PartOrigin, Owner: journal.Intent.Owner,
		MountRoot: journal.Intent.MountRoot, RecoveryIncarnation: journal.Intent.RecoveryIncarnation,
		AdmissionKey: journal.AdmissionKey, SessionID: journal.SessionID,
		LocalGeneration: journal.Generation, Size: journal.PlainSize, Hash: *journal.PlainHash,
	}, true
}

// guardedLocalProof hashes the complete local plaintext and proves the frozen
// baseline at the backend. The cache item must be unlocked by the caller.
func (item *Item) guardedLocalProof(ctx context.Context, guard fs.GuardedWriteFs, cacheObj fs.Object, intent fs.GuardedWriteIntent, size int64) (fs.ContentHash, error) {
	in, err := cacheObj.Open(ctx)
	if err != nil {
		return fs.ContentHash{}, fmt.Errorf("vfs cache: failed to open guarded source: %w", err)
	}
	hash, verifyErr := guard.VerifyGuardedOriginal(ctx, intent, in, size)
	return hash, errors.Join(verifyErr, in.Close())
}

// guardedNamespaceEpoch snapshots the receipt-installation fence, or zero when
// the backend publishes no namespace epoch.
func guardedNamespaceEpoch(f fs.Fs) uint64 {
	if ns, ok := f.(fs.GuardedNamespaceFs); ok {
		return ns.GuardedNamespaceEpoch()
	}
	return 0
}

// _holdGuard records a definitive non-runnable reason and persists it. Call
// with item.mu held.
func (item *Item) _holdGuard(reason string) error {
	item.guardHeld = reason
	item.info.GuardHold = reason
	if item.guardJournal != nil {
		item.guardJournal.Reason = reason
		_ = item._saveGuardLocked()
	}
	if err := item._save(); err != nil {
		fs.Errorf(item.name, "vfs cache: failed to persist guarded hold: %v", err)
	}
	return &writeback.HeldError{Reason: reason}
}

// _proveGuardedBytes re-hashes the complete local plaintext against the digest
// captured before the send. Bytes that cannot be proven never reconcile.
//
// Call with item.mu held.
func (item *Item) _proveGuardedBytes(ctx context.Context, journal *fs.GuardedWriteJournal) error {
	if journal.PlainHash == nil || journal.PlainSize < 0 {
		return fs.ErrGuardedProofUnavailable
	}
	hasher, ok := item.c.fremote.(fs.GuardedPlaintextHasher)
	if !ok {
		return fs.ErrGuardedUnsupported
	}
	in, err := file.Open(item.c.toOSPath(item.name)) // No locking in Cache
	if err != nil {
		return err
	}
	hash, hashErr := hasher.HashGuardedPlaintext(ctx, in, journal.PlainSize)
	closeErr := in.Close()
	if hashErr != nil || closeErr != nil {
		return errors.Join(hashErr, closeErr)
	}
	if hash.Algorithm != journal.PlainHash.Algorithm ||
		!strings.EqualFold(hash.Value, journal.PlainHash.Value) {
		return fs.ErrGuardedConflict
	}
	return nil
}

// errGuardNamespaceMoved reports that a namespace invalidation crossed an
// attempt before its publication fence, so the attempt proved nothing durable.
var errGuardNamespaceMoved = errors.New("vfs cache: guarded namespace moved during reconciliation")

// reconcileGuard resolves one reloaded guarded generation without a new attempt.
// It adopts a durable receipt, or the exact read-only result, and re-homes the
// local alias onto the authoritative destination. It returns true only when the
// generation became clean; every other outcome stays held and readable.
//
// A restart can replay the invalidation for the very move its receipt already
// records. Nothing turns clean before an attempt's fence, so a fresh attempt
// under the new epoch is safe; a namespace that never settles still holds.
func (item *Item) reconcileGuard(ctx context.Context) (bool, error) {
	const attempts = 5
	for attempt := 0; ; attempt++ {
		adopted, err := item.reconcileGuardAttempt(ctx)
		if !errors.Is(err, errGuardNamespaceMoved) {
			return adopted, err
		}
		if attempt+1 >= attempts {
			item.mu.Lock()
			defer item.mu.Unlock()
			return false, item._holdGuard("vfs cache: namespace changed during reconciliation")
		}
	}
}

// reconcileGuardAttempt performs one read-only receipt proof and alias
// placement fenced by the namespace epoch it started in.
func (item *Item) reconcileGuardAttempt(ctx context.Context) (bool, error) {
	item.mu.Lock()
	if !item.guardActive || !item.info.Dirty {
		item.mu.Unlock()
		return false, nil
	}
	journal := item.guardJournal
	generation := item.info.UploadGeneration
	uncertain := item.mutationUncertain
	item.mu.Unlock()

	if uncertain || journal == nil || journal.Generation != generation {
		item.mu.Lock()
		defer item.mu.Unlock()
		if item.guardHeld != "" {
			return false, &writeback.HeldError{Reason: item.guardHeld}
		}
		return false, item._holdGuard("vfs cache: uncertain guarded generation")
	}

	item.mu.Lock()
	proofErr := item._proveGuardedBytes(ctx, journal)
	if proofErr != nil {
		if errors.Is(proofErr, fs.ErrGuardedConflict) || errors.Is(proofErr, fs.ErrGuardedProofUnavailable) ||
			errors.Is(proofErr, fs.ErrGuardedUnsupported) {
			heldErr := item._holdGuard("vfs cache: local guarded bytes are not proven")
			item.mu.Unlock()
			return false, heldErr
		}
		item.mu.Unlock()
		return false, proofErr
	}
	epoch := guardedNamespaceEpoch(item.c.fremote)
	item.mu.Unlock()

	resolver, ok := item.c.fremote.(fs.GuardedAliasResolver)
	if !ok {
		item.mu.Lock()
		defer item.mu.Unlock()
		return false, item._holdGuard("vfs cache: guarded alias cannot be resolved")
	}
	alias, err := resolver.ResolveGuardedAlias(ctx, journal.Intent)
	if err != nil {
		item.mu.Lock()
		defer item.mu.Unlock()
		return false, item._holdGuard("vfs cache: guarded destination is not current")
	}

	var receipt fs.GuardedCommit
	var object fs.Object
	if journal.Receipt != nil {
		receipt = *journal.Receipt
		if receipt.Generation != generation || receipt.Size != journal.PlainSize ||
			journal.PlainHash == nil || receipt.Hash.Algorithm != journal.PlainHash.Algorithm ||
			!strings.EqualFold(receipt.Hash.Value, journal.PlainHash.Value) {
			item.mu.Lock()
			defer item.mu.Unlock()
			return false, item._holdGuard("vfs cache: durable receipt does not match the local proof")
		}
		object, err = item.c.fremote.NewObject(ctx, alias)
		if err != nil {
			item.mu.Lock()
			defer item.mu.Unlock()
			return false, item._holdGuard("vfs cache: guarded result is not readable")
		}
	} else {
		reconciler, ok := item.c.fremote.(fs.GuardedReceiptFs)
		if !ok {
			item.mu.Lock()
			defer item.mu.Unlock()
			return false, item._holdGuard("vfs cache: guarded receipt cannot be reconciled")
		}
		proof, ok := item.GuardedReceiptProof()
		if !ok {
			item.mu.Lock()
			defer item.mu.Unlock()
			if item.guardHeld != "" {
				return false, &writeback.HeldError{Reason: item.guardHeld}
			}
			return false, item._holdGuard("vfs cache: guarded attempt is not proven")
		}
		proof.NamespaceEpoch = epoch
		result, reconcileErr := reconciler.ReconcileGuardedReceipt(ctx, proof)
		if reconcileErr != nil {
			item.mu.Lock()
			defer item.mu.Unlock()
			return false, item._holdGuard("vfs cache: guarded result could not be reconciled")
		}
		if result.ResultID == "" || result.Size != journal.PlainSize ||
			result.Hash.Algorithm != journal.PlainHash.Algorithm ||
			!strings.EqualFold(result.Hash.Value, journal.PlainHash.Value) ||
			(result.NamespaceEpoch != 0 && result.NamespaceEpoch != epoch) {
			item.mu.Lock()
			defer item.mu.Unlock()
			return false, item._holdGuard("vfs cache: reconciled result does not match the local proof")
		}
		object = result.Object
		receipt = fs.GuardedCommit{
			AdmissionKey: journal.AdmissionKey, SessionID: journal.SessionID, Generation: generation,
			ResultID: result.ResultID, ResultGeneration: result.Generation,
			ParentSegments: append([]string(nil), result.ParentSegments...), LocationKnown: result.LocationKnown,
			NamespaceEpoch: epoch, Size: result.Size, Hash: result.Hash,
		}
	}

	// Persist the exact receipt before the alias is reserved and before the
	// generation may turn clean.
	item.mu.Lock()
	if item.info.UploadGeneration != generation {
		item.mu.Unlock()
		return false, &writeback.HeldError{Reason: "vfs cache: guarded generation changed during reconciliation"}
	}
	journal.Receipt = &receipt
	journal.Reason = ""
	if saveErr := item._saveGuardLocked(); saveErr != nil {
		item.mu.Unlock()
		return false, &writeback.HeldError{Reason: "vfs cache: reconciled receipt save failed"}
	}
	if saveErr := item._save(); saveErr != nil {
		item.mu.Unlock()
		return false, &writeback.HeldError{Reason: "vfs cache: reconciled receipt save failed"}
	}
	item.mu.Unlock()

	if err := item.c.AdoptAlias(item, alias); err != nil {
		item.mu.Lock()
		defer item.mu.Unlock()
		return false, item._holdGuard("vfs cache: guarded alias is occupied")
	}

	item.mu.Lock()
	defer item.mu.Unlock()
	if guardedNamespaceEpoch(item.c.fremote) != epoch {
		return false, errGuardNamespaceMoved
	}
	item.o = object
	// No fingerprint yet: the adopted local bytes stay authoritative until a
	// real listing binds this item to its object. A nil-object open must not
	// treat the proven result as a deleted remote.
	item.guardHeld = ""
	item.info.GuardHold = ""
	item.info.MutationMarker = ""
	item.info.Dirty = false
	if err := item._save(); err != nil {
		item.info.Dirty = true
		return false, item._holdGuard("vfs cache: reconciled clean metadata save failed")
	}
	return true, nil
}

// _syncGuardAttemptLocked copies the B2 admission identity into the durable
// journal so a restart can prove this exact attempt read-only. Call with
// item.mu held.
func (item *Item) _syncGuardAttemptLocked() {
	if !item.guardActive || item.guardJournal == nil {
		return
	}
	state, ok := item._admissionStateLocked()
	if !ok {
		return
	}
	if item.guardJournal.AdmissionKey == state.RequestKey && item.guardJournal.SessionID == state.UploadID {
		return
	}
	item.guardJournal.AdmissionKey, item.guardJournal.SessionID = state.RequestKey, state.UploadID
	if err := item._saveGuardLocked(); err != nil {
		fs.Errorf(item.name, "vfs cache: failed to persist guarded attempt: %v", err)
		return
	}
	if err := item._save(); err != nil {
		fs.Errorf(item.name, "vfs cache: failed to persist guarded attempt: %v", err)
	}
}

// guardedHoldReason maps a typed guard refusal to one stable durable reason.
func guardedHoldReason(err error) string {
	switch {
	case errors.Is(err, fs.ErrGuardedProofUnavailable):
		return "vfs cache: original content proof is unavailable"
	case errors.Is(err, fs.ErrGuardedHold):
		return "vfs cache: guarded write is held"
	default:
		return "vfs cache: guarded destination conflict"
	}
}

// _recordGuardedReceipt persists the exact completed result and path before any
// clean transition. Call with item.mu held.
func (item *Item) _recordGuardedReceipt(o fs.Object, generation string) error {
	published, ok := o.(fs.GuardedPublishedObject)
	if !ok {
		return item._heldErrorLocked("vfs cache: guarded completion has no exact result")
	}
	result, ok := published.GuardedWriteResult()
	if !ok || result.ResultID == "" {
		return item._heldErrorLocked("vfs cache: guarded completion has no exact result")
	}
	journal := item.guardJournal
	if journal == nil || journal.Generation != generation || journal.PlainHash == nil {
		return item._heldErrorLocked("vfs cache: guarded journal does not match the completed generation")
	}
	if result.Size != journal.PlainSize || result.Hash.Algorithm != journal.PlainHash.Algorithm ||
		!strings.EqualFold(result.Hash.Value, journal.PlainHash.Value) {
		return item._heldErrorLocked("vfs cache: guarded result does not match the local proof")
	}
	journal.Receipt = &fs.GuardedCommit{
		AdmissionKey: journal.AdmissionKey, SessionID: journal.SessionID, Generation: generation,
		ResultID: result.ResultID, ResultGeneration: result.Generation,
		ParentSegments: append([]string(nil), result.ParentSegments...), LocationKnown: result.LocationKnown,
		NamespaceEpoch: result.NamespaceEpoch, Size: result.Size, Hash: result.Hash,
	}
	journal.Reason = ""
	return item._saveGuardLocked()
}

// truncate the item to the given size, creating it if necessary
//
// this does not mark the object as dirty
//
// call with the lock held
func (item *Item) _truncate(size int64) (err error) {
	if size < 0 {
		// FIXME ignore unknown length files
		return nil
	}

	// Use open handle if available
	fd := item.fd
	if fd == nil {
		// If the metadata says we have some blocks cached then the
		// file should exist, so open without O_CREATE
		oFlags := os.O_WRONLY
		if item.info.Rs.Size() == 0 {
			oFlags |= os.O_CREATE
		}
		osPath := item.c.toOSPath(item.name) // No locking in Cache
		fd, err = file.OpenFile(osPath, oFlags, 0600)
		if err != nil && os.IsNotExist(err) {
			// If the metadata has info but the file doesn't
			// not exist then it has been externally removed
			fs.Errorf(item.name, "vfs cache: detected external removal of cache file")
			item.info.Rs = nil      // show we have no blocks cached
			item.info.Dirty = false // file can't be dirty if it doesn't exist
			item._removeMeta("cache file externally deleted")
			fd, err = file.OpenFile(osPath, os.O_CREATE|os.O_WRONLY, 0600)
		}
		if err != nil {
			return fmt.Errorf("vfs cache: truncate: failed to open cache file: %w", err)
		}

		defer fs.CheckClose(fd, &err)

		err = file.SetSparse(fd)
		if err != nil {
			fs.Errorf(item.name, "vfs cache: truncate: failed to set as a sparse file: %v", err)
		}
	}

	// Check to see what the current size is, and don't truncate
	// if it is already the correct size.
	//
	// Apparently Windows Defender likes to check executables each
	// time they are modified, and truncating a file to its
	// existing size is enough to trigger the Windows Defender
	// scan. This was causing a big slowdown for operations which
	// opened and closed the file a lot, such as looking at
	// properties on an executable.
	fi, err := fd.Stat()
	if err == nil && fi.Size() == size {
		fs.Debugf(item.name, "vfs cache: truncate to size=%d (not needed as size correct)", size)
	} else {
		fs.Debugf(item.name, "vfs cache: truncate to size=%d", size)

		err = fd.Truncate(size)
		if err != nil {
			return fmt.Errorf("vfs cache: truncate: %w", err)
		}
	}

	item.info.Size = size

	return nil
}

// Truncate the item to the current size, creating if necessary
//
// This does not mark the object as dirty.
//
// call with the lock held
func (item *Item) _truncateToCurrentSize() (err error) {
	size, err := item._getSize()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("truncate to current size: %w", err)
	}
	if size < 0 {
		// FIXME ignore unknown length files
		return nil
	}
	err = item._truncate(size)
	if err != nil {
		return err
	}
	return nil
}

// Truncate the item to the given size, creating it if necessary
//
// If the new size is shorter than the existing size then the object
// will be shortened and marked as dirty.
//
// If the new size is longer than the old size then the object will be
// extended and the extended data will be filled with zeros. The
// object will be marked as dirty in this case also.
func (item *Item) Truncate(size int64) (err error) {
	item.preAccess()
	defer item.postAccess()
	item.mutation.Lock()
	defer item.mutation.Unlock()
	item.mu.Lock()
	defer item.mu.Unlock()

	if item.fd == nil {
		return errors.New("vfs cache item truncate: internal error: didn't Open file")
	}
	if size < 0 {
		return nil
	}

	// Read old size
	oldSize, err := item._getSize()
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("truncate failed to read size: %w", err)
		}
		oldSize = 0
	}

	begin := false
	if size != oldSize || item.o == nil {
		// A nonzero truncate preserves part of the original content, so the
		// complete original plaintext must be verified before it changes.
		begin, err = item._beginMutation(item.c.ctx, size != 0)
		if err != nil {
			return err
		}
	} else {
		return nil
	}
	err = item._truncate(size)
	if err != nil {
		return errors.Join(err, item._finishWrite(begin, false))
	}

	changed := true
	if size > oldSize {
		// Truncate extends the file in which case all new bytes are
		// read as zeros. In this case we must show we have written to
		// the new parts of the file.
		item._written(oldSize, size-oldSize)
	} else if size < oldSize {
		// Truncate shrinks the file so clip the downloaded ranges
		item.info.Rs = item.info.Rs.Intersection(ranges.Range{Pos: 0, Size: size})
	} else {
		changed = item.o == nil
	}
	return item._finishWrite(begin, changed)
}

// _stat gets the current stat of the backing file
//
// Call with mutex held
func (item *Item) _stat() (fi os.FileInfo, err error) {
	if item.fd != nil {
		return item.fd.Stat()
	}
	osPath := item.c.toOSPath(item.name) // No locking in Cache
	return os.Stat(osPath)
}

// _getSize gets the current size of the item and updates item.info.Size
//
// Call with mutex held
func (item *Item) _getSize() (size int64, err error) {
	fi, err := item._stat()
	if err != nil {
		if os.IsNotExist(err) && item.o != nil {
			size = item.o.Size()
			err = nil
		}
	} else {
		size = fi.Size()
	}
	if err == nil {
		item.info.Size = size
	}
	return size, err
}

// GetName gets the vfs name of the item
func (item *Item) GetName() (name string) {
	item.mu.Lock()
	defer item.mu.Unlock()
	return item.name
}

// GetSize gets the current size of the item
func (item *Item) GetSize() (size int64, err error) {
	item.mu.Lock()
	defer item.mu.Unlock()
	return item._getSize()
}

// _exists returns whether the backing file for the item exists or not
//
// call with mutex held
func (item *Item) _exists() bool {
	osPath := item.c.toOSPath(item.name) // No locking in Cache
	_, err := os.Stat(osPath)
	return err == nil
}

// Exists returns whether the backing file for the item exists or not
func (item *Item) Exists() bool {
	item.mu.Lock()
	defer item.mu.Unlock()
	return item._exists()
}

// _beginWrite cancels the old writer before content can change.
// Call with item.mu held; the transition keeps later stores fail-closed.
func (item *Item) _beginWrite() (bool, error) {
	for item.admissionChanging {
		item.cond.Wait()
	}
	if item.modified && item.storeCancel == nil && !item.admissionFrozen {
		if item.admissionUnsaved {
			return false, item._save()
		}
		return false, nil
	}
	item.admissionChanging = true
	done := item.storeDone
	if item.storeCancel != nil {
		item.storeCancel()
	}
	item.mu.Unlock()
	item.c.writeback.Remove(item.writeBackID)
	if done != nil {
		<-done
	}
	item.mu.Lock()
	item.writeWasDirty = item.info.Dirty
	item.info.Dirty = true
	// Retain the old admission until bytes actually change. A crash here must
	// not replay an old clean flag over newly written source bytes.
	if !item.writeWasDirty || item.admissionUnsaved {
		if err := item._save(); err != nil {
			item.info.Dirty = item.writeWasDirty
			item.admissionChanging = false
			item.cond.Broadcast()
			return false, err
		}
	}
	return true, nil
}

func (item *Item) _finishWrite(begin, changed bool) (err error) {
	marker := item.info.MutationMarker
	if changed {
		if begin {
			item._newUploadGeneration()
		}
		item.modified, item.info.Dirty = true, true
		item.info.ModTime = time.Now()
		item.info.ATime = item.info.ModTime
		if marker != "" && (marker == item.info.UploadGeneration || marker == mutationMarkerPending || begin) {
			item.info.MutationMarker = ""
			if item.guardJournal != nil {
				item.guardJournal.Generation = item.info.UploadGeneration
				_ = item._saveGuardLocked()
			}
		}
		err = item._save()
		if err != nil && marker != "" {
			// The bytes changed but the ranges and generation were not
			// durably synced, so no range on disk is proof any more.
			item.info.MutationMarker = marker
			item.mutationUncertain = true
			item.guardHeld = "vfs cache: local mutation metadata save failed"
			item.info.GuardHold = item.guardHeld
			if item.guardJournal != nil {
				item.guardJournal.Reason = item.guardHeld
				_ = item._saveGuardLocked()
			}
		}
	} else {
		if marker != "" {
			// No byte changed, so the marker can be cleared alone.
			item.info.MutationMarker = ""
			_ = item._saveGuardLocked()
			if saveErr := item._save(); saveErr != nil {
				item.info.MutationMarker = marker
				item.mutationUncertain = true
				item.guardHeld = "vfs cache: local mutation marker clear failed"
				item.info.GuardHold = item.guardHeld
			} else {
				err = saveErr
			}
		} else if begin && !item.writeWasDirty {
			item.info.Dirty = false
			err = item._save()
		}
	}
	if begin {
		item.admissionChanging = false
		item.cond.Broadcast()
	}
	return err
}

// _dirty marks the item as changed and needing writeback
//
// call with lock held
func (item *Item) _dirty() {
	item.info.ModTime = time.Now()
	item.info.ATime = item.info.ModTime
	save := !item.info.Dirty
	begin, err := item._beginWrite()
	if err != nil {
		fs.Errorf(item.name, "vfs cache: failed to persist dirty marker: %v", err)
	} else if begin {
		if err := item._finishWrite(begin, true); err != nil {
			fs.Errorf(item.name, "vfs cache: failed to save upload generation: %v", err)
		}
		save = false // the generation save was already attempted
	}
	item.info.Dirty = true
	if save {
		if err := item._save(); err != nil {
			fs.Errorf(item.name, "vfs cache: failed to save item info: %v", err)
		}
	}
}

// Dirty marks the item as changed and needing writeback
func (item *Item) Dirty() {
	item.preAccess()
	defer item.postAccess()
	item.mu.Lock()
	item._dirty()
	item.mu.Unlock()
}

// IsDirty returns true if the item data is dirty
func (item *Item) IsDirty() bool {
	item.mu.Lock()
	defer item.mu.Unlock()
	return item.info.Dirty
}

// Create the cache file and store the metadata on disk
// Called with item.mu locked
func (item *Item) _createFile(osPath string) (err error) {
	if item.fd != nil {
		return errors.New("vfs cache item: internal error: didn't Close file")
	}
	item.modified = false
	// t0 := time.Now()
	// Use O_CREATE so the cache file is recreated if it has been removed
	// underneath us (e.g. _checkObject dropped a stale entry, or an external
	// deletion), rather than failing the open with a hard IO error.
	fd, err := file.OpenFile(osPath, os.O_RDWR|os.O_CREATE, 0600)
	// fs.Debugf(item.name, "OpenFile took %v", time.Since(t0))
	if err != nil {
		return fmt.Errorf("vfs cache item: open failed: %w", err)
	}
	err = file.SetSparse(fd)
	if err != nil {
		fs.Errorf(item.name, "vfs cache: failed to set as a sparse file: %v", err)
	}
	item.fd = fd

	err = item._save()
	if err != nil {
		closeErr := item.fd.Close()
		if closeErr != nil {
			fs.Errorf(item.name, "vfs cache: item.fd.Close: closeErr: %v", err)
		}
		item.fd = nil
		return fmt.Errorf("vfs cache item: _save failed: %w", err)
	}
	return err
}

// Open the local file from the object passed in.  Wraps open()
// to provide recovery from out of space error.
func (item *Item) Open(o fs.Object) (err error) {
	for range fs.GetConfig(item.c.ctx).LowLevelRetries {
		item.preAccess()
		err = item.open(o)
		item.postAccess()
		if err == nil {
			break
		}
		fs.Errorf(item.name, "vfs cache: failed to open item: %v", err)
		if !fserrors.IsErrNoSpace(err) && err.Error() != "no space left on device" {
			fs.Errorf(item.name, "Non-out-of-space error encountered during open")
			break
		}
		item.c.KickCleaner()
	}
	return err
}

// Open the local file from the object passed in (which may be nil)
// which implies we are about to create the file
func (item *Item) open(o fs.Object) (err error) {
	// defer log.Trace(o, "item=%p", item)("err=%v", &err)
	item.mu.Lock()
	defer item.mu.Unlock()

	// Wait for any in-progress grace-period close to finish so we start
	// from a fully closed item rather than racing the fd teardown.
	for item.closing != nil {
		closing := item.closing
		item.mu.Unlock()
		<-closing
		item.mu.Lock()
	}

	item.info.ATime = time.Now()

	osPath, err := item.c.createItemDir(item.name) // No locking in Cache
	if err != nil {
		return fmt.Errorf("vfs cache item: createItemDir failed: %w", err)
	}

	err = item._checkObject(o)
	if err != nil {
		return fmt.Errorf("vfs cache item: check object failed: %w", err)
	}

	item.opens++
	if item.opens != 1 {
		return nil
	}

	// Check if recovering from grace period (fd and downloaders still alive)
	if item.graceTimer != nil {
		item.graceTimer.Stop()
		item.graceTimer = nil
		// If the cache file still exists, reuse fd and downloaders.
		// _checkObject (called above) may have removed the cache
		// file if the remote fingerprint changed (e.g. modtime
		// changed during a rename/move). In that case we must
		// close the stale fd and fall through to _createFile.
		if item._exists() {
			return nil
		}
		fs.Debugf(item.name, "vfs cache: cache file vanished during grace period, recreating")
		// The backing file is gone, so any ranges are invalid.
		item.info.Rs = nil
		// It also can't be dirty if the data no longer exists.
		item.info.Dirty = false
		if item.fd != nil {
			_ = item.fd.Close()
			item.fd = nil
		}
		if item.downloaders != nil {
			dls := item.downloaders
			item.downloaders = nil
			item.mu.Unlock()
			_ = dls.Close(nil)
			item.mu.Lock()
		}
		// _checkObject sized the old fd, so size the new file here
		// or GetSize reads 0 and ReadAt returns unfetched zeros.
		err = item._truncateToCurrentSize()
		if err != nil {
			item.opens--
			return fmt.Errorf("vfs cache item: recreate cache file failed: %w", err)
		}
	}

	err = item._createFile(osPath)
	if err != nil {
		if !item.info.Dirty && len(item.info.UploadAdmission) == 0 {
			item._remove("item.open failed on _createFile, remove clean cache data/metadata files")
		}
		item.fd = nil
		item.opens--
		return fmt.Errorf("vfs cache item: create cache file failed: %w", err)
	}
	// Unlock the Item.mu so we can call some methods which take Cache.mu
	item.mu.Unlock()

	// Ensure this item is in the cache. It is possible a cache
	// expiry has run and removed the item if it had no opens so
	// we put it back here. If there was an item with opens
	// already then return an error. This shouldn't happen because
	// there should only be one vfs.File with a pointer to this
	// item in at a time.
	oldItem := item.c.put(item.name, item) // LOCKING in Cache method
	if oldItem != nil {
		oldItem.mu.Lock()
		if oldItem.opens != 0 {
			// Put the item back and return an error
			item.c.put(item.name, oldItem) // LOCKING in Cache method
			err = fmt.Errorf("internal error: item %q already open in the cache", item.name)
		}
		oldItem.mu.Unlock()
	}

	// Relock the Item.mu for the return
	item.mu.Lock()

	// Create the downloaders
	if item.o != nil {
		item.downloaders = downloaders.New(item.c.ctx, item, item.c.opt, item.name, item.o)
	}

	return err
}

// Calls f with mu unlocked, re-locking mu if a panic is raised
//
// mu must be locked when calling this function
func unlockMutexForCall(mu *sync.Mutex, f func()) {
	mu.Unlock()
	defer mu.Lock()
	f()
}

// Store stores the local cache file to the remote object, returning
// the new remote object. objOld is the old object if known.
//
// Call with lock held
func (item *Item) _store(ctx context.Context, storeFn StoreFn) (err error) {
	// defer log.Trace(item.name, "item=%p", item)("err=%v", &err)

	generation := item.info.UploadGeneration
	var epoch uint64
	current := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if item.admissionChanging || item.info.UploadGeneration != generation {
			return errors.New("vfs cache: upload generation changed during writeback")
		}
		return nil
	}
	if err := current(); err != nil {
		return err
	}
	if item.storeCancel != nil {
		return errors.New("vfs cache: writeback already active")
	}
	if item.guardActive {
		if item.mutationUncertain || item.guardHeld != "" {
			return item._heldErrorLocked(item.guardHeld)
		}
		if item.guardJournal == nil || item.guardJournal.Generation != generation {
			return item._heldErrorLocked("vfs cache: guarded journal is unavailable")
		}
		if item.info.MutationMarker != "" {
			return item._heldErrorLocked("vfs cache: local mutation is unresolved")
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	item.storeCancel, item.storeDone = cancel, done
	defer func() {
		cancel()
		item.storeCancel, item.storeDone = nil, nil
		close(done)
	}()
	ctx = fs.WithUploadAdmissionStore(ctx, &itemAdmissionStore{item: item, generation: generation})
	// Transfer the temp file to the remote
	cacheObj, err := item.c.fcache.NewObject(ctx, item.name)
	if err != nil {
		return fmt.Errorf("vfs cache: failed to find cache file: %w", err)
	}

	if cacheObj == nil {
		return errors.New("vfs cache: dirty source is unavailable")
	}
	if item.guardActive {
		// Prove the exact local plaintext and persist it before any send.
		journal := *item.guardJournal
		size := item.info.Size
		guard := item.c.fremote.(fs.GuardedWriteFs)
		// The installation fence is captured before the completing request.
		epoch = guardedNamespaceEpoch(item.c.fremote)
		item.mu.Unlock()
		hash, proofErr := item.guardedLocalProof(ctx, guard, cacheObj, journal.Intent, size)
		item.mu.Lock()
		if proofErr != nil {
			if errors.Is(proofErr, fs.ErrGuardedConflict) || errors.Is(proofErr, fs.ErrGuardedHold) ||
				errors.Is(proofErr, fs.ErrGuardedProofUnavailable) {
				return item._heldErrorLocked(guardedHoldReason(proofErr))
			}
			return proofErr
		}
		if err := current(); err != nil {
			return err
		}
		if state, ok := item._admissionStateLocked(); ok {
			if state.RequestKey != "" {
				journal.AdmissionKey = state.RequestKey
			}
			if state.UploadID != "" {
				journal.SessionID = state.UploadID
			}
		}
		journal.PlainSize = size
		journal.PlainHash = &hash
		item.guardJournal = &journal
		_ = item._saveGuardLocked()
		if saveErr := item._save(); saveErr != nil {
			return item._heldErrorLocked("vfs cache: guarded proof save failed")
		}
		ctx = fs.WithGuardedWriteIntent(ctx, journal.Intent)
	}
	o, name := item.o, item.name
	unlockMutexForCall(&item.mu, func() {
		o, err = operations.Copy(ctx, item.c.fremote, o, name, cacheObj)
	})
	if err := current(); err != nil {
		return err
	}
	if err != nil {
		if errors.Is(err, fs.ErrorCantUploadEmptyFiles) {
			fs.Errorf(name, "Writeback failed: %v", err)
			return nil
		}
		// The attempt identity must be durable before any outcome can hold it.
		item._syncGuardAttemptLocked()
		if item.guardActive && (errors.Is(err, fs.ErrGuardedConflict) || errors.Is(err, fs.ErrGuardedHold) ||
			errors.Is(err, fs.ErrGuardedProofUnavailable)) {
			return item._heldErrorLocked(guardedHoldReason(err))
		}
		return fmt.Errorf("vfs cache: failed to transfer file from cache to remote: %w", err)
	}
	item.o = o
	item._updateFingerprint()
	if item.guardActive {
		if err := item._recordGuardedReceipt(o, generation); err != nil {
			return err
		}
		// A receipt may only become a clean alias in the namespace it was
		// completing in, and only at the authoritative destination.
		if guardedNamespaceEpoch(item.c.fremote) != epoch {
			return item._heldErrorLocked("vfs cache: namespace changed before receipt installation")
		}
		intent := item.guardJournal.Intent
		resolver, ok := item.c.fremote.(fs.GuardedAliasResolver)
		if !ok {
			return item._heldErrorLocked("vfs cache: guarded alias cannot be resolved")
		}
		item.mu.Unlock()
		alias, aliasErr := resolver.ResolveGuardedAlias(ctx, intent)
		if aliasErr != nil {
			alias = ""
		} else {
			aliasErr = item.c.AdoptAlias(item, alias)
		}
		item.mu.Lock()
		if aliasErr != nil {
			return item._heldErrorLocked("vfs cache: guarded result could not be placed")
		}
		if guardedNamespaceEpoch(item.c.fremote) != epoch {
			return item._heldErrorLocked("vfs cache: namespace changed before receipt installation")
		}
	}

	// Persist success before StoreFn, but keep the item ineligible for removal
	// until the VFS callback returns.
	item.info.Dirty = false
	err = item._save()
	item.info.Dirty = true
	if err != nil {
		if item.guardActive {
			return item._heldErrorLocked("vfs cache: completed receipt save failed")
		}
		return fmt.Errorf("vfs cache: failed to write clean metadata: %w", err)
	}
	if err := current(); err != nil {
		return err
	}
	// Write the object back to the VFS layer before we mark it as
	// clean, otherwise it will become eligible for removal which
	// can cause a deadlock
	if storeFn != nil && item.o != nil {
		fs.Debugf(item.name, "vfs cache: writeback object to VFS layer")
		// Write the object back to the VFS layer last with mutex unlocked
		o := item.o
		item.mu.Unlock()
		storeFn(o)
		item.mu.Lock()
	}
	if err := current(); err != nil {
		return err
	}

	// Show item is clean and is eligible for cache removal
	item.info.Dirty = false
	// Clean metadata was committed before exposing the result.

	return nil
}

// Store stores the local cache file to the remote object, returning
// the new remote object. objOld is the old object if known.
func (item *Item) store(ctx context.Context, storeFn StoreFn) (err error) {
	item.mu.Lock()
	defer item.mu.Unlock()
	return item._store(ctx, storeFn)
}

// Close the cache file
func (item *Item) Close(storeFn StoreFn) (err error) {
	// defer log.Trace(item.o, "Item.Close")("err=%v", &err)
	item.preAccess()
	defer item.postAccess()
	syncWriteBack := item.c.opt.WriteBack <= 0
	item.mu.Lock()
	defer item.mu.Unlock()

	item.info.ATime = time.Now()
	item.opens--

	if item.opens < 0 {
		return os.ErrClosed
	} else if item.opens > 0 {
		return nil
	}

	// opens == 0: check for grace period (only for non-dirty files,
	// dirty files need immediate close so writeback can proceed)
	gracePeriod := time.Duration(item.c.opt.HandleCaching)
	if gracePeriod > 0 && !item.info.Dirty {
		item.graceTimer = time.AfterFunc(gracePeriod, item.closeAfterGrace)
		return nil
	}

	return item._actualClose(storeFn, syncWriteBack)
}

// closeAfterGrace is called by the grace timer to perform the actual close.
//
// Grace period only applies to non-dirty files, so storeFn (only
// needed for writeback) and syncWriteBack are not relevant.
func (item *Item) closeAfterGrace() {
	defer vfscommon.RecoverPanic(item.name, nil)
	item.mu.Lock()
	defer item.mu.Unlock()

	// Someone re-opened, another timer took over, or a reset
	// already cancelled the grace timer and cleaned up.
	if item.opens > 0 || item.graceTimer == nil {
		return
	}
	item.graceTimer = nil

	// _actualClose drops item.mu while it tears down the downloaders,
	// during which the fd is still open. Publish that a close is in
	// progress so a concurrent open waits rather than tripping over the
	// half-closed handle.
	item.closing = make(chan struct{})
	// Release the waiters however this ends: if a panic in the backend is
	// recovered above, leaving item.closing open would block every later
	// open of this file forever.
	defer func() {
		close(item.closing)
		item.closing = nil
	}()
	err := item._actualClose(nil, false)
	if err != nil {
		fs.Errorf(item.name, "vfs cache: close after grace period failed: %v", err)
	}
}

// _actualClose performs the actual close operations on the item.
//
// Call with item.mu held. May temporarily unlock item.mu.
func (item *Item) _actualClose(storeFn StoreFn, syncWriteBack bool) (err error) {
	var dls *downloaders.Downloaders

	// Update the size on close
	_, _ = item._getSize()

	// If the file is dirty ensure any segments not transferred
	// are brought in first.
	//
	// FIXME It would be nice to do this asynchronously however it
	// would require keeping the downloaders alive after the item
	// has been closed
	if item.info.Dirty && item.o != nil && !item.mutationUncertain && item.guardHeld == "" {
		err = item._ensure(0, item.info.Size)
		if err != nil {
			return fmt.Errorf("vfs cache: failed to download missing parts of cache file: %w", err)
		}
	}

	// Accumulate and log errors
	checkErr := func(e error) {
		if e != nil {
			fs.Errorf(item.o, "vfs cache: item close failed: %v", e)
			if err == nil {
				err = e
			}
		}
	}

	// Close the downloaders
	if dls = item.downloaders; dls != nil {
		item.downloaders = nil
		// FIXME need to unlock to kill downloader - should we
		// re-arrange locking so this isn't necessary?  maybe
		// downloader should use the item mutex for locking? or put a
		// finer lock on Rs?
		//
		// downloader.Write calls ensure which needs the lock
		// close downloader with mutex unlocked
		item.mu.Unlock()
		checkErr(dls.Close(nil))
		item.mu.Lock()
	}

	// close the file handle
	if item.fd == nil {
		checkErr(errors.New("vfs cache item: internal error: didn't Open file"))
	} else {
		checkErr(item.fd.Close())
		item.fd = nil
	}

	// save the metadata once more since it may be dirty
	// after the downloader
	checkErr(item._save())

	// if the item hasn't been changed but has been completed then
	// set the modtime from the object otherwise set it from the info
	if item._exists() {
		if !item.info.Dirty && item.o != nil {
			item._setModTime(item.o.ModTime(item.c.ctx))
		} else {
			item._setModTime(item.info.ModTime)
		}
	}

	// upload the file to backing store if changed
	if item.info.Dirty {
		fs.Infof(item.name, "vfs cache: queuing for upload in %v", item.c.opt.WriteBack)
		if syncWriteBack {
			// do synchronous writeback
			checkErr(item._store(item.c.ctx, storeFn))
		} else {
			// asynchronous writeback
			item.c.writeback.SetID(&item.writeBackID)
			id := item.writeBackID
			item.mu.Unlock()
			item.c.writeback.Add(id, item.name, item.info.Size, item.modified, func(ctx context.Context) error {
				return item.store(ctx, storeFn)
			})
			item.mu.Lock()
		}
	}

	// mark as not modified now we have uploaded or queued for upload
	item.modified = false

	return err
}

// reload is called with valid items recovered from a cache reload.
//
// If they are dirty then it makes sure they get uploaded.
//
// it is called before the cache has started so opens will be 0 and
// metaDirty will be false.
func (item *Item) reload(ctx context.Context) error {
	item.mu.Lock()
	dirty := item.info.Dirty
	guarded := dirty && item.guardActive
	item.mu.Unlock()
	if !dirty {
		return nil
	}
	if guarded {
		// Restart reconciliation owns eligibility for a guarded generation.
		// Nothing may be queued or sent before the journal is resolved.
		return nil
	}
	store := item.admissionStore()
	ctx = fs.WithUploadAdmissionStore(ctx, store)
	cacheObj, err := item.c.fcache.NewObject(ctx, item.name)
	if err != nil {
		return fmt.Errorf("reload: dirty source is unavailable: %w", err)
	}
	if cacheObj == nil {
		return errors.New("reload: dirty source is unavailable")
	}
	ctx = fs.WithUploadAdmissionIntent(ctx, fs.UploadAdmissionIntent{
		Source: cacheObj, Destination: item.c.fremote, DestinationRemote: item.name,
	})
	fullRecovery := false
	if _, aware := item.c.fremote.(fs.UploadAdmissionChecker); aware {
		state, err := store.LoadUploadAdmission(ctx)
		if err != nil {
			return fmt.Errorf("reload: upload journal is unavailable: %w", err)
		}
		item.mu.Lock()
		fullRecovery = state.RequestKey != "" && len(state.Request) != 0 && state.IntentFingerprint != "" &&
			cacheObj.Size() == item.info.Size && item._present()
		item.mu.Unlock()
	}
	var obj fs.Object
	var lookupErr error
	if !fullRecovery {
		sourceRemote := cacheObj.Remote()
		lookupCtx := fs.WithUploadAdmissionDiscovery(ctx, cacheObj.Fs(), &sourceRemote)
		obj, lookupErr = item.c.fremote.NewObject(lookupCtx, item.name)
		if lookupErr != nil && (fs.IsUploadAdmissionError(lookupErr) || !errors.Is(lookupErr, fs.ErrorObjectNotFound)) {
			return fmt.Errorf("reload: destination proof is unavailable: %w", lookupErr)
		}
	}
	// Full local recovery queues a transfer; only its session/body proof may acknowledge success.
	// open the file with the object (or nil)
	err = item.Open(obj)
	if err != nil {
		return err
	}
	// A newly created dirty file has no remote bytes to refill. Its local
	// contents (including sparse zeroes) are authoritative even if a crash
	// prevented the written ranges from being saved to the metadata.
	if errors.Is(lookupErr, fs.ErrorObjectNotFound) {
		item.mu.Lock()
		if item.info.Fingerprint == "" {
			size, sizeErr := item._getSize()
			if sizeErr == nil {
				item.info.Rs.Insert(ranges.Range{Pos: 0, Size: size})
			}
		}
		item.mu.Unlock()
	}
	// close the file to execute the writeback if needed
	err = item.Close(nil)
	if err != nil {
		return err
	}
	// put the file into the directory listings
	size, err := item._getSize()
	if err != nil {
		return fmt.Errorf("reload: failed to read size: %w", err)
	}
	err = item.c.AddVirtual(item.name, size, false)
	if err != nil {
		return fmt.Errorf("reload: failed to add virtual dir entry: %w", err)
	}
	return nil
}

// check the fingerprint of an object and update the item or delete
// the cached file accordingly
//
// If we have local modifications then they take precedence
// over a change in the remote
//
// It ensures the file is the correct size for the object.
//
// call with lock held
func (item *Item) _checkObject(o fs.Object) error {
	if o == nil {
		if item.info.Fingerprint != "" {
			// no remote object && local object
			// remove local object unless dirty
			if !item.info.Dirty {
				item._remove("stale (remote deleted)")
			} else {
				fs.Debugf(item.name, "vfs cache: remote object has gone but local object modified - keeping it")
			}
			//} else {
			// no remote object && no local object
			// OK
		}
	} else {
		remoteFingerprint := fs.Fingerprint(item.c.ctx, o, item.c.opt.FastFingerprint)
		fs.Debugf(item.name, "vfs cache: checking remote fingerprint %q against cached fingerprint %q", remoteFingerprint, item.info.Fingerprint)
		if item.info.Fingerprint != "" {
			// remote object && local object
			if remoteFingerprint != item.info.Fingerprint {
				if !item.info.Dirty {
					fs.Debugf(item.name, "vfs cache: removing cached entry as stale (remote fingerprint %q != cached fingerprint %q)", remoteFingerprint, item.info.Fingerprint)
					item._remove("stale (remote is different)")
					item.info.Fingerprint = remoteFingerprint
				} else {
					fs.Debugf(item.name, "vfs cache: remote object has changed but local object modified - keeping it (remote fingerprint %q != cached fingerprint %q)", remoteFingerprint, item.info.Fingerprint)
				}
			}
		} else {
			// remote object && no local object
			// Set fingerprint
			item.info.Fingerprint = remoteFingerprint
		}
		if item.guardActive && item.info.Dirty {
			// A dirty guarded generation owns its backing bytes and identity.
			// A resync or a shorter external replacement never truncates or
			// rebinds it.
			return nil
		}
		item.info.Size = o.Size()
	}
	item.o = o

	err := item._truncateToCurrentSize()
	if err != nil {
		return fmt.Errorf("vfs cache item: open truncate failed: %w", err)
	}

	return nil
}

// WrittenBack checks to see if the item has been written back or not
func (item *Item) WrittenBack() bool {
	item.mu.Lock()
	defer item.mu.Unlock()
	return item.info.Fingerprint != ""
}

// remove the cached file
//
// call with lock held
func (item *Item) _removeFile(reason string) {
	osPath := item.c.toOSPath(item.name) // No locking in Cache
	err := os.Remove(osPath)
	if err != nil {
		if !os.IsNotExist(err) {
			fs.Errorf(item.name, "vfs cache: failed to remove cache file as %s: %v", reason, err)
		}
	} else {
		fs.Infof(item.name, "vfs cache: removed cache file as %s", reason)
	}
}

// remove the metadata
//
// call with lock held
func (item *Item) _removeMeta(reason string) {
	osPathMeta := item.c.toOSPathMeta(item.name) // No locking in Cache
	err := os.Remove(osPathMeta)
	if err != nil {
		if !os.IsNotExist(err) {
			fs.Errorf(item.name, "vfs cache: failed to remove metadata from cache as %s: %v", reason, err)
		}
	} else {
		fs.Debugf(item.name, "vfs cache: removed metadata from cache as %s", reason)
	}
}

// remove the cached file and empty the metadata
//
// This returns true if the file was in the transfer queue so may not
// have completely uploaded yet.
//
// call with lock held
func (item *Item) _remove(reason string) (wasWriting bool) {
	// Cancel writeback, if any
	item.mu.Unlock()
	wasWriting = item.c.writeback.Remove(item.writeBackID)
	item.mu.Lock()
	item.info.clean()
	item._removeFile(reason)
	item._removeMeta(reason)
	return wasWriting
}

// remove the cached file and empty the metadata
//
// This returns true if the file was in the transfer queue so may not
// have completely uploaded yet.
func (item *Item) remove(reason string) (wasWriting bool) {
	item.mu.Lock()
	defer item.mu.Unlock()
	return item._remove(reason)
}

// RemoveNotInUse is called to remove cache file that has not been accessed recently
// It may also be called for removing empty cache files too when the quota is already reached.
func (item *Item) RemoveNotInUse(maxAge time.Duration, emptyOnly bool) (removed bool, spaceFreed int64) {
	item.mu.Lock()
	defer item.mu.Unlock()

	spaceFreed = 0
	removed = false

	if item.opens != 0 || item.info.Dirty || item.graceTimer != nil {
		return
	}

	removeIt := false
	if maxAge == 0 {
		removeIt = true // quota-driven removal
	}
	if maxAge != 0 {
		cutoff := time.Now().Add(-maxAge)
		// If not locked and access time too long ago - delete the file
		accessTime := item.info.ATime
		if accessTime.Sub(cutoff) <= 0 {
			removeIt = true
		}
	}
	if removeIt {
		spaceUsed := item.info.Rs.Size()
		if !emptyOnly || spaceUsed == 0 {
			spaceFreed = spaceUsed
			removed = true
			if item._remove("Removing old cache file not in use") {
				fs.Errorf(item.name, "item removed when it was writing/uploaded")
			}
		}
	}
	return
}

// Reset is called by the cache purge functions only to reset (empty the contents) cache files that
// are not dirty.  It is used when cache space runs out and we see some ENOSPC error.
func (item *Item) Reset() (rr ResetResult, spaceFreed int64, err error) {
	item.mu.Lock()
	defer item.mu.Unlock()

	// The item is not being used now.  Just remove it instead of resetting it.
	// Items in their grace period are treated as in-use; the cache
	// cleaner will pick them up on the next pass.
	if item.opens == 0 && !item.info.Dirty && item.graceTimer == nil {
		spaceFreed = item.info.Rs.Size()
		if item._remove("Removing old cache file not in use") {
			fs.Errorf(item.name, "item removed when it was writing/uploaded")
		}
		return RemovedNotInUse, spaceFreed, nil
	}

	// do not reset dirty file
	if item.info.Dirty {
		return SkippedDirty, 0, nil
	}

	// Items in their grace period are treated as in-use; the cache
	// cleaner will pick them up on the next pass.
	if item.graceTimer != nil {
		return SkippedGrace, 0, nil
	}

	/* A wait on pendingAccessCnt to become 0 can lead to deadlock when an item.Open bumps
	   up the pendingAccesses count, calls item.open, which calls cache.put. The cache.put
	   operation needs the cache mutex, which is held here.  We skip this file now. The
	   caller (the cache cleaner thread) may retry resetting this item if the cache size does
	   not reduce below quota. */
	if item.pendingAccesses > 0 {
		return SkippedPendingAccess, 0, nil
	}

	/* Do not need to reset an empty cache file unless it was being reset and the reset failed.
	   Some thread(s) may be waiting on the reset's successful completion in that case. */
	if item.info.Rs.Size() == 0 && !item.beingReset {
		return SkippedEmpty, 0, nil
	}

	item.beingReset = true

	/* Error handling from this point on (setting item.fd and item.beingReset):
	   Since Reset is called by the cache cleaner thread, there is no direct way to return
	   the error to the io threads.  Set item.fd to nil upon internal errors, so that the
	   io threads will return internal errors seeing a nil fd. In the case when the error
	   is ENOSPC, keep the item in isBeingReset state and that will keep the item.ReadAt
	   waiting at its beginning. The cache purge loop will try to redo the reset after cache
	   space is made available again. This recovery design should allow most io threads to
	   eventually go through, unless large files are written/overwritten concurrently and
	   the total size of these files exceed the cache storage limit. */

	// Close the downloaders
	// Accumulate and log errors
	checkErr := func(e error) {
		if e != nil {
			fs.Errorf(item.o, "vfs cache: item reset failed: %v", e)
			if err == nil {
				err = e
			}
		}
	}

	if downloaders := item.downloaders; downloaders != nil {
		item.downloaders = nil
		// FIXME need to unlock to kill downloader - should we
		// re-arrange locking so this isn't necessary?  maybe
		// downloader should use the item mutex for locking? or put a
		// finer lock on Rs?
		//
		// downloader.Write calls ensure which needs the lock
		// close downloader with mutex unlocked
		item.mu.Unlock()
		checkErr(downloaders.Close(nil))
		item.mu.Lock()
	}

	// close the file handle
	// fd can be nil if we tried Reset and failed before because of ENOSPC during reset
	if item.fd != nil {
		checkErr(item.fd.Close())
		if err != nil {
			// Could not close the cache file
			item.beingReset = false
			item.cond.Broadcast()
			return ResetFailed, 0, err
		}
		item.fd = nil
	}

	spaceFreed = item.info.Rs.Size()

	// This should not be possible.  We get here only if cache data is not dirty.
	if item._remove("cache out of space, item is clean") {
		fs.Errorf(item.o, "vfs cache item removed when it was writing/uploaded")
	}

	// can we have an item with no dirty data (so that we can get here) and nil item.o at the same time?
	fso := item.o
	checkErr(item._checkObject(fso))
	if err != nil {
		item.beingReset = false
		item.cond.Broadcast()
		return ResetFailed, spaceFreed, err
	}

	osPath := item.c.toOSPath(item.name)
	checkErr(item._createFile(osPath))
	if err != nil {
		item._remove("cache reset failed on _createFile, removed cache data file")
		item.fd = nil // This allows a new Reset redo to have a clean state to deal with
		if !fserrors.IsErrNoSpace(err) {
			item.beingReset = false
			item.cond.Broadcast()
		}
		return ResetFailed, spaceFreed, err
	}

	// Create the downloaders
	if item.o != nil {
		item.downloaders = downloaders.New(item.c.ctx, item, item.c.opt, item.name, item.o)
	}

	/* The item will stay in the beingReset state if we get an error that prevents us from
	reaching this point.  The cache purge loop will redo the failed Reset. */
	item.beingReset = false
	item.cond.Broadcast()

	return ResetComplete, spaceFreed, err
}

// ProtectCache either waits for an ongoing cache reset to finish or increases pendingReads
// to protect against cache reset on this item while the thread potentially uses the cache file
// Cache cleaner waits until pendingReads is zero before resetting cache.
func (item *Item) preAccess() {
	item.mu.Lock()
	defer item.mu.Unlock()

	if item.beingReset {
		for {
			item.cond.Wait()
			if !item.beingReset {
				break
			}
		}
	}
	item.pendingAccesses++
}

// postAccess reduces the pendingReads count enabling cache reset upon ENOSPC
func (item *Item) postAccess() {
	item.mu.Lock()
	defer item.mu.Unlock()

	item.pendingAccesses--
	item.cond.Broadcast()
}

// _present returns true if the whole file has been downloaded
//
// call with the lock held
func (item *Item) _present() bool {
	return item.info.Rs.Present(ranges.Range{Pos: 0, Size: item.info.Size})
}

// present returns true if the whole file has been downloaded
func (item *Item) present() bool {
	item.mu.Lock()
	defer item.mu.Unlock()
	return item._present()
}

// HasRange returns true if the current ranges entirely include range
func (item *Item) HasRange(r ranges.Range) bool {
	item.mu.Lock()
	defer item.mu.Unlock()
	return item.info.Rs.Present(r)
}

// FindMissing adjusts r returning a new ranges.Range which only
// contains the range which needs to be downloaded. This could be
// empty - check with IsEmpty. It also adjust this to make sure it is
// not larger than the file.
func (item *Item) FindMissing(r ranges.Range) (outr ranges.Range) {
	item.mu.Lock()
	defer item.mu.Unlock()
	outr = item.info.Rs.FindMissing(r)
	// Clip returned block to size of file
	outr.Clip(item.info.Size)
	return outr
}

// ensure the range from offset, size is present in the backing file
//
// call with the item lock held
func (item *Item) _ensure(offset, size int64) (err error) {
	// defer log.Trace(item.name, "offset=%d, size=%d", offset, size)("err=%v", &err)
	if offset+size > item.info.Size {
		size = item.info.Size - offset
	}
	// Check to see if we are about to request data beyond of the size of
	// the remote object. This can happen if the the cached range metadata
	// is out of sync with the cache file after an unclean shutdown.
	if item.o != nil {
		if remoteSize := item.o.Size(); remoteSize >= 0 && offset+size > remoteSize {
			beyond := ranges.Range{Pos: remoteSize, Size: offset + size - remoteSize}
			if !item.info.Rs.Present(beyond) {
				fs.Errorf(item.name, "vfs cache: cached file (%d) is unexpectedly larger than the remote object (%d). The cached file is likely corrupted after an unclean shutdown; recovering the %d bytes available from the remote", offset+size, remoteSize, remoteSize)
			}
			size = remoteSize - offset
		}
	}
	if size <= 0 {
		return nil
	}
	r := ranges.Range{Pos: offset, Size: size}
	present := item.info.Rs.Present(r)
	if !present && item.guardActive && (item.guardHeld != "" || item.mutationUncertain) {
		// A held item must never fill a missing range from an external
		// replacement, and an interrupted mutation left no proven range.
		return &writeback.HeldError{Reason: "unproven range on a held item"}
	}
	/* This statement simulates a cache space error for test purpose */
	/* if present != true && item.info.Rs.Size() > 32*1024*1024 {
		return errors.New("no space left on device")
	} */
	fs.Debugf(nil, "vfs cache: looking for range=%+v in %+v - present %v", r, item.info.Rs, present)
	item.mu.Unlock()
	defer item.mu.Lock()
	if present {
		// This is a file we are writing so no downloaders needed
		if item.downloaders == nil {
			return nil
		}
		// Otherwise start the downloader for the future if required
		return item.downloaders.EnsureDownloader(r)
	}
	if item.downloaders == nil {
		// Downloaders can be nil here if the file has been
		// renamed, so need to make some more downloaders
		// OK to call downloaders constructor with item.mu held

		// item.o can also be nil under some circumstances
		// See: https://github.com/rclone/rclone/issues/6190
		// See: https://github.com/rclone/rclone/issues/6235
		if item.o == nil {
			o, err := item.c.fremote.NewObject(item.c.ctx, item.name)
			if err != nil {
				return err
			}
			item.o = o
		}
		item.downloaders = downloaders.New(item.c.ctx, item, item.c.opt, item.name, item.o)
	}
	return item.downloaders.Download(r)
}

// _written marks the (offset, size) as present in the backing file
//
// This is called by the downloader downloading file segments and the
// vfs layer writing to the file.
//
// This doesn't mark the item as Dirty - that the responsibility
// of the caller as we don't know here whether we are adding reads or
// writes to the cache file.
//
// call with lock held
func (item *Item) _written(offset, size int64) {
	// defer log.Trace(item.name, "offset=%d, size=%d", offset, size)("")
	item.info.Rs.Insert(ranges.Range{Pos: offset, Size: size})
}

// update the fingerprint of the object if any
//
// call with lock held
func (item *Item) _updateFingerprint() {
	if item.o == nil {
		return
	}
	oldFingerprint := item.info.Fingerprint
	item.info.Fingerprint = fs.Fingerprint(item.c.ctx, item.o, item.c.opt.FastFingerprint)
	if oldFingerprint != item.info.Fingerprint {
		fs.Debugf(item.o, "vfs cache: fingerprint now %q", item.info.Fingerprint)
	}
}

// setModTime of the cache file
//
// call with lock held
func (item *Item) _setModTime(modTime time.Time) {
	fs.Debugf(item.name, "vfs cache: setting modification time to %v", modTime)
	osPath := item.c.toOSPath(item.name) // No locking in Cache
	err := os.Chtimes(osPath, modTime, modTime)
	if err != nil {
		fs.Errorf(item.name, "vfs cache: failed to set modification time of cached file: %v", err)
	}
}

// setModTime of the cache file and in the Item
func (item *Item) setModTime(modTime time.Time) {
	// defer log.Trace(item.name, "modTime=%v", modTime)("")
	item.mu.Lock()
	item._updateFingerprint()
	item._setModTime(modTime)
	item.info.ModTime = modTime
	err := item._save()
	if err != nil {
		fs.Errorf(item.name, "vfs cache: setModTime: failed to save item info: %v", err)
	}
	item.mu.Unlock()
}

// GetModTime of the cache file
func (item *Item) GetModTime() (modTime time.Time, err error) {
	// defer log.Trace(item.name, "modTime=%v", modTime)("")
	item.mu.Lock()
	defer item.mu.Unlock()
	fi, err := item._stat()
	if err == nil {
		modTime = fi.ModTime()
	}
	return modTime, nil
}

// ReadAt bytes from the file at off
func (item *Item) ReadAt(b []byte, off int64) (n int, err error) {
	n = 0
	var expBackOff int
	for retries := range fs.GetConfig(item.c.ctx).LowLevelRetries {
		item.preAccess()
		n, err = item.readAt(b, off)
		item.postAccess()
		if err == nil || err == io.EOF {
			break
		}
		fs.Errorf(item.name, "vfs cache: failed to _ensure cache %v", err)
		if !fserrors.IsErrNoSpace(err) && err.Error() != "no space left on device" {
			fs.Debugf(item.name, "vfs cache: failed to _ensure cache %v is not out of space", err)
			break
		}
		item.c.KickCleaner()
		expBackOff = 2 << uint(retries)
		time.Sleep(time.Duration(expBackOff) * time.Millisecond) // Exponential back-off the retries
	}

	if fserrors.IsErrNoSpace(err) {
		fs.Errorf(item.name, "vfs cache: failed to _ensure cache after retries %v", err)
	}

	return n, err
}

// ReadAt bytes from the file at off
func (item *Item) readAt(b []byte, off int64) (n int, err error) {
	item.mu.Lock()
	if item.fd == nil {
		item.mu.Unlock()
		return 0, errors.New("vfs cache item ReadAt: internal error: didn't Open file")
	}
	if off < 0 {
		item.mu.Unlock()
		return 0, io.EOF
	}
	defer item.mu.Unlock()

	err = item._ensure(off, int64(len(b)))
	if err != nil {
		return 0, err
	}

	// Check to see if object has shrunk - if so don't read too much.
	if item.o != nil && !item.info.Dirty && item.o.Size() != item.info.Size {
		fs.Debugf(item.o, "Size has changed from %d to %d", item.info.Size, item.o.Size())
		err = item._truncate(item.o.Size())
		if err != nil {
			return 0, err
		}
	}

	item.info.ATime = time.Now()
	// Do the reading with Item.mu unlocked and cache protected by preAccess
	n, err = item.fd.ReadAt(b, off)
	return n, err
}

// WriteAt bytes to the file at off
func (item *Item) WriteAt(b []byte, off int64) (n int, err error) {
	item.preAccess()
	defer item.postAccess()
	item.mutation.Lock()
	defer item.mutation.Unlock()
	item.mu.Lock()
	if item.fd == nil {
		item.mu.Unlock()
		return 0, errors.New("vfs cache item WriteAt: internal error: didn't Open file")
	}
	begin := false
	if len(b) != 0 {
		begin, err = item._beginMutation(item.c.ctx, true)
		if err != nil {
			item.mu.Unlock()
			return 0, err
		}
	}
	item.mu.Unlock()
	// Do the writing with Item.mu unlocked
	n, err = item.fd.WriteAt(b, off)
	if err == nil && n != len(b) {
		err = fmt.Errorf("short write: tried to write %d but only %d written", len(b), n)
	}
	item.mu.Lock()
	if n > 0 {
		item._written(off, int64(n))
		end := off + int64(n)
		// Writing off the end of the file so need to make some
		// zeroes.  we do this by showing that we have written to the
		// new parts of the file.
		if off > item.info.Size {
			item._written(item.info.Size, off-item.info.Size)
		}
		// Update size
		if end > item.info.Size {
			item.info.Size = end
		}
	}
	if begin || n > 0 || item.info.MutationMarker != "" {
		err = errors.Join(err, item._finishWrite(begin, n > 0))
	}
	item.mu.Unlock()
	return n, err
}

// WriteAtNoOverwrite writes b to the file, but will not overwrite
// already present ranges.
//
// This is used by the downloader to write bytes to the file.
//
// Bytes past the end of the item are not written; they are counted
// as processed and skipped.
//
// It returns n the total bytes processed and skipped the number of
// bytes which were processed but not actually written to the file.
func (item *Item) WriteAtNoOverwrite(b []byte, off int64) (n int, skipped int, err error) {
	item.mu.Lock()

	// Don't write past the end of the item. If the file was shrunk
	// with Truncate while the remote object is still longer, the
	// downloader would otherwise grow the cache file back to the
	// size of the remote object.
	beyond := 0
	if end := off + int64(len(b)); end > item.info.Size {
		keep := max(item.info.Size-off, 0)
		beyond = len(b) - int(keep)
		b = b[:keep]
	}

	var (
		// Range we wish to write
		r = ranges.Range{Pos: off, Size: int64(len(b))}
		// Ranges that we need to write
		foundRanges = item.info.Rs.FindAll(r)
		// Length of each write
		nn int
	)

	// Write the range out ignoring already written chunks
	// fs.Debugf(item.name, "Ranges = %v", item.info.Rs)
	for i := range foundRanges {
		foundRange := &foundRanges[i]
		// fs.Debugf(item.name, "foundRange[%d] = %v", i, foundRange)
		if foundRange.R.Pos != off {
			err = errors.New("internal error: offset of range is wrong")
			break
		}
		size := int(foundRange.R.Size)
		if foundRange.Present {
			// if present want to skip this range
			// fs.Debugf(item.name, "skip chunk offset=%d size=%d", off, size)
			nn = size
			skipped += size
		} else {
			// if range not present then we want to write it
			// fs.Debugf(item.name, "write chunk offset=%d size=%d", off, size)
			nn, err = item.fd.WriteAt(b[:size], off)
			if err == nil && nn != size {
				err = fmt.Errorf("downloader: short write: tried to write %d but only %d written", size, nn)
			}
			item._written(off, int64(nn))
		}
		off += int64(nn)
		b = b[nn:]
		n += nn
		if err != nil {
			break
		}
	}
	item.mu.Unlock()
	if err == nil {
		n += beyond
		skipped += beyond
	}
	return n, skipped, err
}

// Sync commits the current contents of the file to stable storage. Typically,
// this means flushing the file system's in-memory copy of recently written
// data to disk.
func (item *Item) Sync() (err error) {
	item.preAccess()
	defer item.postAccess()
	item.mu.Lock()
	defer item.mu.Unlock()
	if item.fd == nil {
		return errors.New("vfs cache item sync: internal error: didn't Open file")
	}
	// sync the file and the metadata to disk
	err = item.fd.Sync()
	if err != nil {
		return fmt.Errorf("vfs cache item sync: failed to sync file: %w", err)
	}
	err = item._save()
	if err != nil {
		return fmt.Errorf("vfs cache item sync: failed to sync metadata: %w", err)
	}
	return nil
}

// rename the item
func (item *Item) rename(name string, newName string, newObj fs.Object) (err error) {
	item.preAccess()
	defer item.postAccess()
	item.mu.Lock()
	for item.admissionChanging {
		item.cond.Wait()
	}
	if name == newName {
		item.o = newObj
		item._updateFingerprint()
		item.mu.Unlock()
		return nil
	}
	item.admissionChanging = true
	downloaders := item.downloaders
	item.downloaders = nil
	id, done := item.writeBackID, item.storeDone
	if item.storeCancel != nil {
		item.storeCancel()
	}
	item.mu.Unlock()
	if downloaders != nil {
		_ = downloaders.Close(nil)
	}
	item.c.writeback.Rename(id, newName)
	if done != nil {
		<-done
	}
	item.mu.Lock()
	oldInfo, oldObject := item.info, item.o
	oldAdmissionErr, oldFrozen := item.admissionErr, item.admissionFrozen
	dataMoved, metaMoved := false, false
	err = rename(item.c.toOSPath(name), item.c.toOSPath(newName))
	if err == nil {
		dataMoved = true
		err = rename(item.c.toOSPathMeta(name), item.c.toOSPathMeta(newName))
		metaMoved = err == nil
	}
	if err == nil {
		item.name, item.o = newName, newObj
		if item.info.Dirty {
			item._newUploadGeneration()
		}
		item._updateFingerprint()
		err = item._save()
	}
	if err != nil {
		item.name, item.o, item.info = name, oldObject, oldInfo
		item.admissionErr, item.admissionFrozen = oldAdmissionErr, oldFrozen
		if metaMoved {
			err = errors.Join(err, rename(item.c.toOSPathMeta(newName), item.c.toOSPathMeta(name)))
		}
		if dataMoved {
			err = errors.Join(err, rename(item.c.toOSPath(newName), item.c.toOSPath(name)))
		}
	}
	item.admissionChanging = false
	item.cond.Broadcast()
	item.mu.Unlock()
	if err != nil {
		item.c.writeback.Rename(id, name)
	}
	return err
}
