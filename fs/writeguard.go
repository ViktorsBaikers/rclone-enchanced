package fs

import (
	"context"
	"errors"
	"io"
	"strings"
)

// Guarded write failures which must never be retried as an ordinary transfer.
var (
	// ErrGuardedUnsupported means the backend has no guarded write protocol.
	ErrGuardedUnsupported = errors.New("fs: guarded writes are not enabled")
	// ErrGuardedHold means local bytes must stay held and non-runnable.
	ErrGuardedHold = errors.New("fs: guarded write is held")
	// ErrGuardedConflict means the authoritative destination no longer matches
	// the captured intent, or the completed result is not the exact one.
	ErrGuardedConflict = errors.New("fs: guarded write conflicts with the destination")
	// ErrGuardedProofUnavailable means the required original-content proof is
	// missing or malformed.
	ErrGuardedProofUnavailable = errors.New("fs: guarded original content proof is unavailable")
)

// ContentHash is an authoritative plaintext digest. Only the tree digest the
// server itself publishes is acceptable; an ETag is never a substitute.
type ContentHash struct {
	Algorithm string `json:"algorithm"`
	Value     string `json:"value"`
}

// GuardedBaselineKind distinguishes a genuine absence from an exact version.
type GuardedBaselineKind string

const (
	// GuardedBaselineAbsent is a proven empty destination slot.
	GuardedBaselineAbsent GuardedBaselineKind = "absent"
	// GuardedBaselineVersion is one exact authoritative file version.
	GuardedBaselineVersion GuardedBaselineKind = "version"
)

// GuardedBaseline is the authoritative write-open state captured before the
// first dirty byte. UUID, generation and digest are committed together.
type GuardedBaseline struct {
	Kind       GuardedBaselineKind `json:"kind"`
	FileID     string              `json:"fileId,omitempty"`
	Generation string              `json:"generation,omitempty"`
	Size       int64               `json:"size,omitempty"`
	Hash       *ContentHash        `json:"hash,omitempty"`
}

// GuardedWriteIntent freezes the destination of one guarded write: the service
// and part origin, owner, pinned root, stable parent, encoded leaf and exact
// baseline. It never authorizes a name or path lookup.
type GuardedWriteIntent struct {
	Service             string          `json:"service"`
	PartOrigin          string          `json:"partOrigin"`
	Owner               string          `json:"owner"`
	RecoveryIncarnation string          `json:"recoveryIncarnation"`
	MountRoot           *string         `json:"mountRoot"`
	ParentID            *string         `json:"parentId"`
	Leaf                string          `json:"leaf"`
	Baseline            GuardedBaseline `json:"baseline"`
}

// GuardedCommit is the exact completed result and its authoritative path.
type GuardedCommit struct {
	AdmissionKey     string      `json:"admissionKey"`
	SessionID        string      `json:"sessionId"`
	Generation       string      `json:"generation"`
	ResultID         string      `json:"resultId"`
	ResultGeneration string      `json:"resultGeneration"`
	ParentSegments   []string    `json:"parentSegments"`
	LocationKnown    bool        `json:"locationKnown"`
	NamespaceEpoch   uint64      `json:"namespaceEpoch,omitempty"`
	Size             int64       `json:"size"`
	Hash             ContentHash `json:"hash"`
}

// GuardedWriteJournal is the versioned durable journal for one dirty local
// generation. A non-empty Reason keeps the generation non-runnable.
type GuardedWriteJournal struct {
	Version      int                `json:"version"`
	Intent       GuardedWriteIntent `json:"intent"`
	Generation   string             `json:"generation"`
	AdmissionKey string             `json:"admissionKey,omitempty"`
	SessionID    string             `json:"sessionId,omitempty"`
	PlainSize    int64              `json:"plainSize,omitempty"`
	PlainHash    *ContentHash       `json:"plainHash,omitempty"`
	Reason       string             `json:"reason,omitempty"`
	Receipt      *GuardedCommit     `json:"receipt,omitempty"`
}

// GuardedWriteJournalVersion is the only journal layout this build trusts.
const GuardedWriteJournalVersion = 1

// GuardedPublicationOutcome is the publication state a durable journal proves.
// An operator resolving held state must declare exactly this outcome.
type GuardedPublicationOutcome string

const (
	// GuardedPublicationUnknown means the journal cannot prove an outcome.
	GuardedPublicationUnknown GuardedPublicationOutcome = ""
	// GuardedPublicationAbsent means no guarded upload could have been sent.
	GuardedPublicationAbsent GuardedPublicationOutcome = "absent"
	// GuardedPublicationCommitted means an exact publication receipt exists.
	GuardedPublicationCommitted GuardedPublicationOutcome = "committed"
)

var (
	// ErrGuardedPublicationUnknown means held bytes cannot be resolved because
	// the durable journal does not prove what the remote holds.
	ErrGuardedPublicationUnknown = errors.New("fs: guarded publication outcome is unknown")
	// ErrGuardedPublicationMismatch means the declared outcome is not the one
	// the durable journal proves.
	ErrGuardedPublicationMismatch = errors.New("fs: guarded publication outcome does not match the journal")
)

// PublicationOutcome derives the only outcome the durable journal proves. A
// receipt must match the journal generation, size and plaintext digest. Without
// a receipt, only a journal that never admitted a send proves absence.
func (j *GuardedWriteJournal) PublicationOutcome() GuardedPublicationOutcome {
	if j == nil || j.Version != GuardedWriteJournalVersion || j.Generation == "" {
		return GuardedPublicationUnknown
	}
	if j.Receipt != nil {
		r := j.Receipt
		if r.Generation != j.Generation || r.ResultID == "" || r.Size != j.PlainSize || j.PlainHash == nil {
			return GuardedPublicationUnknown
		}
		if r.Hash.Algorithm != j.PlainHash.Algorithm ||
			!strings.EqualFold(r.Hash.Value, j.PlainHash.Value) {
			return GuardedPublicationUnknown
		}
		return GuardedPublicationCommitted
	}
	if j.AdmissionKey != "" || j.SessionID != "" {
		return GuardedPublicationUnknown
	}
	return GuardedPublicationAbsent
}

// RequirePublication verifies that the operator declared the outcome the
// journal proves. It never upgrades an unknown outcome.
func (j *GuardedWriteJournal) RequirePublication(outcome GuardedPublicationOutcome) error {
	proven := j.PublicationOutcome()
	if proven == GuardedPublicationUnknown {
		return ErrGuardedPublicationUnknown
	}
	if outcome != proven {
		return ErrGuardedPublicationMismatch
	}
	return nil
}

// GuardedParent is a parent resolved by UUID inside the pinned root.
type GuardedParent struct {
	Path     string   `json:"path"`
	Segments []string `json:"segments"`
}

// GuardedWriteResult is the fenced publication returned by a guarded write.
type GuardedWriteResult struct {
	Object         Object
	ParentSegments []string
	LocationKnown  bool
	ResultID       string
	Generation     string
	NamespaceEpoch uint64
	Size           int64
	Hash           ContentHash
}

// GuardedWriteFs is implemented by a backend with the guarded upload protocol.
// VFS captures the destination and verifies the original plaintext before it
// ever dirties a guarded cached file.
type GuardedWriteFs interface {
	// GuardedWriteEnabled reports whether this backend requires guarded writes.
	GuardedWriteEnabled() bool
	// CaptureWriteDestination pins the destination and its exact baseline.
	CaptureWriteDestination(ctx context.Context, remote string) (GuardedWriteIntent, error)
	// ResolveGuardedParent revalidates the pinned parent by UUID.
	ResolveGuardedParent(ctx context.Context, intent GuardedWriteIntent) (GuardedParent, error)
	// VerifyGuardedOriginal verifies the complete original plaintext against
	// the captured baseline and returns its tree digest.
	VerifyGuardedOriginal(ctx context.Context, intent GuardedWriteIntent, content io.Reader, size int64) (ContentHash, error)
}

// GuardedPublishedObject exposes the exact publication carried by the returned
// object so the cache can persist the result and path before it turns clean.
type GuardedPublishedObject interface {
	GuardedWriteResult() (GuardedWriteResult, bool)
}

// GuardedReceiptProof is the durable input to read-only reconciliation. It
// carries provenance and the local generation; it is never mutation authority.
type GuardedReceiptProof struct {
	Service             string      `json:"service"`
	PartOrigin          string      `json:"partOrigin"`
	Owner               string      `json:"owner"`
	MountRoot           *string     `json:"mountRoot"`
	RecoveryIncarnation string      `json:"recoveryIncarnation"`
	AdmissionKey        string      `json:"admissionKey"`
	SessionID           string      `json:"sessionId"`
	LocalGeneration     string      `json:"localGeneration"`
	NamespaceEpoch      uint64      `json:"namespaceEpoch,omitempty"`
	Size                int64       `json:"size"`
	Hash                ContentHash `json:"hash"`
}

// GuardedReceiptFs reconciles one provably completed guarded upload by reading
// the exact-origin guarded status. It receives no source reader, performs no
// part callback and never mutates the remote.
type GuardedReceiptFs interface {
	ReconcileGuardedReceipt(ctx context.Context, proof GuardedReceiptProof) (GuardedWriteResult, error)
}

// GuardedNamespaceFs publishes the namespace epoch a receipt is installed
// against. A receipt captured under an older epoch never becomes clean.
type GuardedNamespaceFs interface {
	GuardedNamespaceEpoch() uint64
}

// GuardedPlaintextHasher derives the same plaintext digest the server publishes
// for completed content. It reads local bytes without any mutation authority.
type GuardedPlaintextHasher interface {
	HashGuardedPlaintext(ctx context.Context, content io.Reader, size int64) (ContentHash, error)
}

// GuardedAliasResolver resolves the journal's frozen destination to its
// authoritative path inside the pinned root. It never creates a directory.
type GuardedAliasResolver interface {
	ResolveGuardedAlias(ctx context.Context, intent GuardedWriteIntent) (string, error)
}

type guardedWriteIntentKey struct{}

// WithGuardedWriteIntent carries the journal's frozen intent into the upload so
// the backend cannot recapture a different destination at send time.
func WithGuardedWriteIntent(ctx context.Context, intent GuardedWriteIntent) context.Context {
	return context.WithValue(ctx, guardedWriteIntentKey{}, intent)
}

// GuardedWriteIntentFromContext returns the frozen destination intent.
func GuardedWriteIntentFromContext(ctx context.Context) (GuardedWriteIntent, bool) {
	intent, ok := ctx.Value(guardedWriteIntentKey{}).(GuardedWriteIntent)
	return intent, ok
}
