package vfscache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/ranges"
)

// Offline quarantine refuses to resolve held cache state unless every
// precondition below is proven before anything is written or moved.
var (
	// ErrQuarantineUnsupported means this platform cannot prove ownership or
	// quiescence, so no quarantine is attempted.
	ErrQuarantineUnsupported = errors.New("vfs cache: offline quarantine is unsupported on this platform")
	// ErrQuarantineInvalidPath means the cache root or target is not a usable
	// absolute path.
	ErrQuarantineInvalidPath = errors.New("vfs cache: quarantine path is invalid")
	// ErrQuarantineRootNotPrivate means the root is not a 0700 directory owned
	// by the calling OS user and containing exactly vfs and vfsMeta.
	ErrQuarantineRootNotPrivate = errors.New("vfs cache: quarantine root is not a dedicated private cache root")
	// ErrQuarantineNotQuiesced means a live lock or an unrelated pending item
	// shows that the client was not quiesced.
	ErrQuarantineNotQuiesced = errors.New("vfs cache: cache root is not quiesced")
	// ErrQuarantineNothingHeld means no held generation was found.
	ErrQuarantineNothingHeld = errors.New("vfs cache: no held generation is present")
	// ErrQuarantineIncomplete means the held bytes or their ranges are not
	// fully proven.
	ErrQuarantineIncomplete = errors.New("vfs cache: held bytes are not fully proven")
	// ErrQuarantinePublicationUnknown means the durable journal does not prove
	// the publication outcome the caller declared.
	ErrQuarantinePublicationUnknown = errors.New("vfs cache: publication outcome is not proven")
	// ErrQuarantineTargetExists means the quarantine target already exists.
	ErrQuarantineTargetExists = errors.New("vfs cache: quarantine target already exists")
	// ErrQuarantineCrossDevice means the target is on another filesystem.
	ErrQuarantineCrossDevice = errors.New("vfs cache: quarantine target is on another filesystem")
	// ErrQuarantineAlreadyQuarantined means the root is already gone while the
	// target holds the quarantined state.
	ErrQuarantineAlreadyQuarantined = errors.New("vfs cache: cache root is already quarantined")
	// ErrQuarantineManifestMismatch means copied bytes do not match the
	// manifest written beside them.
	ErrQuarantineManifestMismatch = errors.New("vfs cache: copied bytes do not match the manifest")
)

// QuarantineRequest is one explicit operator action. The caller must have
// stopped the guarded client and must declare the publication outcome the
// durable journals prove.
type QuarantineRequest struct {
	CacheRoot   string
	Target      string
	Publication fs.GuardedPublicationOutcome
}

// QuarantineManifest is the witness written beside the copied held bytes.
type QuarantineManifest struct {
	Version     int                          `json:"version"`
	CacheRoot   string                       `json:"cacheRoot"`
	Target      string                       `json:"target"`
	Publication fs.GuardedPublicationOutcome `json:"publication"`
	Items       []QuarantineItem             `json:"items"`
}

// QuarantineItem is one held generation and its proof.
type QuarantineItem struct {
	Path         string                       `json:"path"`
	CopyPath     string                       `json:"copyPath"`
	Generation   string                       `json:"generation"`
	Reason       string                       `json:"reason"`
	AdmissionKey string                       `json:"admissionKey,omitempty"`
	SessionID    string                       `json:"sessionId,omitempty"`
	Size         int64                        `json:"size"`
	SHA256       string                       `json:"sha256"`
	MetadataSHA  string                       `json:"metadataSha256"`
	ProvenRanges []ByteRange                  `json:"provenRanges"`
	Publication  fs.GuardedPublicationOutcome `json:"publication"`
	Receipt      *fs.GuardedCommit            `json:"receipt,omitempty"`
}

// ByteRange is a proven byte range in the manifest.
type ByteRange struct {
	Pos  int64 `json:"pos"`
	Size int64 `json:"size"`
}

// QuarantineResult reports where the verified copy and the separated state
// live. StatePath is the moved dedicated cache root.
type QuarantineResult struct {
	ManifestPath string
	StatePath    string
	Manifest     QuarantineManifest
}

// MissingRangesError preserves the evidence of unproven bytes on refusal.
type MissingRangesError struct {
	Missing []ByteRange
}

func (e *MissingRangesError) Error() string {
	return fmt.Sprintf("%s: missing %v", ErrQuarantineIncomplete, e.Missing)
}

// Unwrap makes errors.Is(err, ErrQuarantineIncomplete) true.
func (e *MissingRangesError) Unwrap() error { return ErrQuarantineIncomplete }

// quarantineOwnerOf is a test seam for the OS owner check.
var quarantineOwnerOf = fileOwner

// QuarantineHeldCache is the only offline resolution entry point. It proves
// every precondition read-only, copies and verifies the held bytes plus a
// witness manifest, then atomically renames the whole dedicated cache root
// (data and metadata) into Target/state on the same filesystem. It never
// deletes anything.
func QuarantineHeldCache(req QuarantineRequest) (QuarantineResult, error) {
	var result QuarantineResult
	root, target, err := quarantinePaths(req.CacheRoot, req.Target)
	if err != nil {
		return result, err
	}
	rootInfo, err := privateCacheRoot(root)
	if err != nil {
		if os.IsNotExist(err) {
			if _, statErr := os.Lstat(filepath.Join(target, "state")); statErr == nil {
				return result, fmt.Errorf("%w: %s", ErrQuarantineAlreadyQuarantined, target)
			}
		}
		return result, err
	}
	if err := checkQuarantineTarget(root, target, rootInfo); err != nil {
		return result, err
	}

	release, err := lockQuarantineRoot(root)
	if err != nil {
		return result, err
	}
	defer release()

	// Recheck after taking the lock so a concurrent change cannot slip in.
	if _, err := privateCacheRoot(root); err != nil {
		return result, err
	}
	items, err := proveHeldCache(root, req.Publication)
	if err != nil {
		return result, err
	}
	if len(items) == 0 {
		return result, fmt.Errorf("%w: %s", ErrQuarantineNothingHeld, root)
	}

	manifest := QuarantineManifest{
		Version:     1,
		CacheRoot:   root,
		Target:      target,
		Publication: req.Publication,
		Items:       items,
	}
	if err := writeVerifiedCopy(root, target, &manifest); err != nil {
		return result, err
	}

	statePath := filepath.Join(target, "state")
	if err := os.Rename(root, statePath); err != nil {
		return result, err
	}
	if err := syncDir(target); err != nil {
		return result, err
	}
	result = QuarantineResult{
		ManifestPath: filepath.Join(target, "manifest.json"),
		StatePath:    statePath,
		Manifest:     manifest,
	}
	return result, nil
}

// quarantinePaths validates both paths and confirms same-filesystem placement.
func quarantinePaths(root, target string) (string, string, error) {
	for _, p := range []string{root, target} {
		if p == "" || !filepath.IsAbs(p) {
			return "", "", fmt.Errorf("%w: %q must be an absolute path", ErrQuarantineInvalidPath, p)
		}
	}
	root = filepath.Clean(root)
	target = filepath.Clean(target)
	if within(target, root) || within(root, target) {
		return "", "", fmt.Errorf("%w: cache root and target must not contain each other", ErrQuarantineInvalidPath)
	}
	return root, target, nil
}

func within(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// privateCacheRoot requires a 0700 directory owned by the calling user whose
// only entries are vfs and vfsMeta, both also private.
func privateCacheRoot(root string) (os.FileInfo, error) {
	info, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: %s is not a directory", ErrQuarantineRootNotPrivate, root)
	}
	if info.Mode().Perm() != 0700 {
		return nil, fmt.Errorf("%w: %s mode is %04o", ErrQuarantineRootNotPrivate, root, info.Mode().Perm())
	}
	owner, ok := quarantineOwnerOf(info)
	if !ok || owner != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("%w: %s is not owned by the calling user", ErrQuarantineRootNotPrivate, root)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, 2)
	for _, entry := range entries {
		seen[entry.Name()] = true
	}
	if len(seen) != 2 || !seen["vfs"] || !seen["vfsMeta"] {
		return nil, fmt.Errorf("%w: %s must contain exactly vfs and vfsMeta", ErrQuarantineRootNotPrivate, root)
	}
	for _, name := range []string{"vfs", "vfsMeta"} {
		dir, err := os.Lstat(filepath.Join(root, name))
		if err != nil {
			return nil, err
		}
		if !dir.IsDir() || dir.Mode()&os.ModeSymlink != 0 || dir.Mode().Perm() != 0700 {
			return nil, fmt.Errorf("%w: %s is not a private directory", ErrQuarantineRootNotPrivate, filepath.Join(root, name))
		}
		if owner, ok := quarantineOwnerOf(dir); !ok || owner != uint32(os.Geteuid()) {
			return nil, fmt.Errorf("%w: %s is not owned by the calling user", ErrQuarantineRootNotPrivate, filepath.Join(root, name))
		}
	}
	return info, nil
}

// checkQuarantineTarget proves the target does not exist and shares the cache
// root's filesystem. It creates nothing.
func checkQuarantineTarget(root, target string, rootInfo os.FileInfo) error {
	if _, err := os.Lstat(target); err == nil {
		return fmt.Errorf("%w: %s", ErrQuarantineTargetExists, target)
	} else if !os.IsNotExist(err) {
		return err
	}
	parent := filepath.Dir(target)
	parentInfo, err := os.Stat(parent)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrQuarantineInvalidPath, err)
	}
	if !parentInfo.IsDir() {
		return fmt.Errorf("%w: %s is not a directory", ErrQuarantineInvalidPath, parent)
	}
	rootDev, rootOK := deviceOf(rootInfo)
	parentDev, parentOK := deviceOf(parentInfo)
	if !rootOK || !parentOK {
		return ErrQuarantineUnsupported
	}
	if rootDev != parentDev {
		return fmt.Errorf("%w: root device %d, target device %d", ErrQuarantineCrossDevice, rootDev, parentDev)
	}
	return nil
}

// proveHeldCache enumerates every held generation and re-verifies its journal,
// ranges and bytes. It is read-only.
func proveHeldCache(root string, declared fs.GuardedPublicationOutcome) ([]QuarantineItem, error) {
	dataRoot := filepath.Join(root, "vfs")
	metaRoot := filepath.Join(root, "vfsMeta")
	var items []QuarantineItem
	err := filepath.WalkDir(metaRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		name := strings.TrimPrefix(path, metaRoot+string(filepath.Separator))
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return fmt.Errorf("%w: %s is not a regular metadata file", ErrQuarantineIncomplete, path)
		}
		if strings.HasPrefix(entry.Name(), ".") {
			return fmt.Errorf("%w: interrupted metadata write %s", ErrQuarantineNotQuiesced, path)
		}
		item, held, err := proveHeldItem(dataRoot, path, name, declared)
		if err != nil {
			return err
		}
		if held {
			items = append(items, item)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Path < items[j].Path })
	return items, nil
}

func proveHeldItem(dataRoot, metaPath, name string, declared fs.GuardedPublicationOutcome) (QuarantineItem, bool, error) {
	var item QuarantineItem
	body, err := os.ReadFile(metaPath)
	if err != nil {
		return item, false, err
	}
	var info Info
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&info); err != nil {
		return item, false, fmt.Errorf("%w: %s: %v", ErrQuarantineIncomplete, metaPath, err)
	}

	var journal *fs.GuardedWriteJournal
	if len(info.UploadGuard) != 0 {
		var decoded fs.GuardedWriteJournal
		decoder := json.NewDecoder(bytes.NewReader(info.UploadGuard))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&decoded); err != nil || decoded.Version != fs.GuardedWriteJournalVersion {
			return item, false, fmt.Errorf("%w: %s: unreadable guarded journal", ErrQuarantinePublicationUnknown, metaPath)
		}
		journal = &decoded
	}

	held := info.GuardHold != "" || journal != nil && journal.Reason != ""
	if !held {
		if info.Dirty {
			return item, false, fmt.Errorf("%w: unrelated pending upload %s", ErrQuarantineNotQuiesced, metaPath)
		}
		return item, false, nil
	}
	if info.MutationMarker != "" {
		return item, false, fmt.Errorf("%w: %s has an interrupted local mutation", ErrQuarantineIncomplete, metaPath)
	}
	if !info.Dirty {
		return item, false, fmt.Errorf("%w: %s is held but not dirty", ErrQuarantineIncomplete, metaPath)
	}
	if journal == nil || journal.Generation != info.UploadGeneration {
		return item, false, fmt.Errorf("%w: %s has no journal for its generation", ErrQuarantinePublicationUnknown, metaPath)
	}
	if journal.PlainSize != info.Size {
		return item, false, fmt.Errorf("%w: %s journal size %d, local size %d", ErrQuarantineIncomplete, metaPath, journal.PlainSize, info.Size)
	}
	if state := admissionOutcome(info.UploadAdmission); state != "" && journal.AdmissionKey == "" && journal.SessionID == "" {
		return item, false, fmt.Errorf("%w: %s has an admitted request without a receipt", ErrQuarantinePublicationUnknown, metaPath)
	}
	if err := journal.RequirePublication(declared); err != nil {
		return item, false, errors.Join(ErrQuarantinePublicationUnknown, err)
	}

	missing, err := missingRanges(info.Rs, info.Size)
	if err != nil {
		return item, false, err
	}
	if len(missing) > 0 {
		return item, false, &MissingRangesError{Missing: missing}
	}

	dataPath := filepath.Join(dataRoot, name)
	dataInfo, err := os.Lstat(dataPath)
	if err != nil {
		return item, false, fmt.Errorf("%w: %s: %v", ErrQuarantineIncomplete, dataPath, err)
	}
	if !dataInfo.Mode().IsRegular() || dataInfo.Mode()&os.ModeSymlink != 0 || dataInfo.Size() != info.Size {
		return item, false, fmt.Errorf("%w: %s size or type mismatch", ErrQuarantineIncomplete, dataPath)
	}
	digest, err := hashFile(dataPath, info.Size)
	if err != nil {
		return item, false, err
	}

	receipt := journal.Receipt
	metaDigest := sha256.Sum256(body)
	reason := info.GuardHold
	if reason == "" {
		reason = journal.Reason
	}
	item = QuarantineItem{
		Path:         filepath.ToSlash(name),
		CopyPath:     "held/" + filepath.ToSlash(name),
		Generation:   journal.Generation,
		Reason:       reason,
		AdmissionKey: journal.AdmissionKey,
		SessionID:    journal.SessionID,
		Size:         info.Size,
		SHA256:       digest,
		MetadataSHA:  hex.EncodeToString(metaDigest[:]),
		ProvenRanges: byteRanges(info.Rs),
		Publication:  journal.PublicationOutcome(),
		Receipt:      receipt,
	}
	return item, true, nil
}

// admissionOutcome reports whether the admission journal admits any send.
func admissionOutcome(body json.RawMessage) string {
	if len(body) == 0 {
		return ""
	}
	var state fs.UploadAdmissionState
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return "unreadable"
	}
	if state.RequestKey != "" || state.UploadID != "" {
		return "admitted"
	}
	return ""
}

// missingRanges returns every byte not covered by proven ranges.
func missingRanges(rs ranges.Ranges, size int64) ([]ByteRange, error) {
	if size < 0 {
		return nil, fmt.Errorf("%w: negative size", ErrQuarantineIncomplete)
	}
	sorted := append(ranges.Ranges(nil), rs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Pos < sorted[j].Pos })
	var missing []ByteRange
	pos := int64(0)
	for _, r := range sorted {
		if r.Size <= 0 {
			continue
		}
		start := r.Pos
		if start < 0 {
			start = 0
		}
		if start >= size {
			continue
		}
		end := r.Pos + r.Size
		if end > size {
			end = size
		}
		if start > pos {
			missing = append(missing, ByteRange{Pos: pos, Size: start - pos})
		}
		if end > pos {
			pos = end
		}
	}
	if pos < size {
		missing = append(missing, ByteRange{Pos: pos, Size: size - pos})
	}
	return missing, nil
}

func byteRanges(rs ranges.Ranges) []ByteRange {
	out := make([]ByteRange, 0, len(rs))
	for _, r := range rs {
		if r.Size > 0 {
			out = append(out, ByteRange{Pos: r.Pos, Size: r.Size})
		}
	}
	return out
}

func hashFile(path string, size int64) (string, error) {
	in, err := os.Open(path)
	if err != nil {
		return "", err
	}
	hasher := sha256.New()
	n, hashErr := io.Copy(hasher, in)
	closeErr := in.Close()
	if hashErr != nil {
		return "", hashErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	if n != size {
		return "", fmt.Errorf("%w: %s read %d of %d bytes", ErrQuarantineIncomplete, path, n, size)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// writeVerifiedCopy creates the target, copies every held file, writes the
// manifest, fsyncs it and verifies each copy against the manifest.
func writeVerifiedCopy(root, target string, manifest *QuarantineManifest) error {
	if err := os.Mkdir(target, 0700); err != nil {
		return err
	}
	heldDir := filepath.Join(target, "held")
	if err := os.Mkdir(heldDir, 0700); err != nil {
		return err
	}
	for i := range manifest.Items {
		item := &manifest.Items[i]
		src := filepath.Join(root, "vfs", filepath.FromSlash(item.Path))
		dst := filepath.Join(target, filepath.FromSlash(item.CopyPath))
		if err := os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
			return err
		}
		if err := copyFile(src, dst, item.Size); err != nil {
			return err
		}
	}
	manifestBody, err := json.MarshalIndent(manifest, "", "\t")
	if err != nil {
		return err
	}
	manifestBody = append(manifestBody, '\n')
	manifestPath := filepath.Join(target, "manifest.json")
	out, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := out.Write(manifestBody)
	syncErr := out.Sync()
	closeErr := out.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	if err := verifyCopies(target, manifest); err != nil {
		return err
	}
	return syncTree(target)
}

// syncTree fsyncs every directory holding copied bytes or the manifest.
func syncTree(root string) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		return syncDir(path)
	})
}

func copyFile(src, dst string, size int64) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		_ = in.Close()
		return err
	}
	n, copyErr := io.Copy(out, in)
	syncErr := out.Sync()
	closeErr := out.Close()
	inCloseErr := in.Close()
	if err := errors.Join(copyErr, syncErr, closeErr, inCloseErr); err != nil {
		return err
	}
	if n != size {
		return fmt.Errorf("%w: %s copied %d of %d bytes", ErrQuarantineIncomplete, src, n, size)
	}
	return nil
}

func verifyCopies(target string, manifest *QuarantineManifest) error {
	for _, item := range manifest.Items {
		path := filepath.Join(target, filepath.FromSlash(item.CopyPath))
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() != item.Size {
			return fmt.Errorf("%w: %s size or type", ErrQuarantineManifestMismatch, path)
		}
		digest, err := hashFile(path, item.Size)
		if err != nil {
			return err
		}
		if !strings.EqualFold(digest, item.SHA256) {
			return fmt.Errorf("%w: %s", ErrQuarantineManifestMismatch, path)
		}
	}
	return nil
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	return errors.Join(syncErr, closeErr)
}
