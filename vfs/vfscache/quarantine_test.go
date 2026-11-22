package vfscache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/ranges"
)

const quarantineTestGeneration = "22222222-2222-4222-8222-222222222222"

type quarantineFixture struct {
	root string
}

func newQuarantineFixture(t *testing.T) *quarantineFixture {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"vfs", "vfsMeta"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	return &quarantineFixture{root: root}
}

func (f *quarantineFixture) addHeld(t *testing.T, name string, data []byte, journal fs.GuardedWriteJournal, rs ranges.Ranges) {
	t.Helper()
	dataPath := filepath.Join(f.root, "vfs", filepath.FromSlash(name))
	metaPath := filepath.Join(f.root, "vfsMeta", filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(dataPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(metaPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	journalBody, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	info := Info{
		Size:             int64(len(data)),
		Rs:               rs,
		Dirty:            true,
		UploadGeneration: quarantineTestGeneration,
		UploadGuard:      journalBody,
		GuardHold:        "vfs cache: guarded write is held",
	}
	body, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metaPath, body, 0600); err != nil {
		t.Fatal(err)
	}
}

func heldJournal(data []byte) fs.GuardedWriteJournal {
	return fs.GuardedWriteJournal{
		Version:    fs.GuardedWriteJournalVersion,
		Generation: quarantineTestGeneration,
		PlainSize:  int64(len(data)),
		Reason:     "vfs cache: guarded write is held",
	}
}

func fullRanges(data []byte) ranges.Ranges {
	if len(data) == 0 {
		return nil
	}
	return ranges.Ranges{{Pos: 0, Size: int64(len(data))}}
}

func TestB3QuarantineSuccess(t *testing.T) {
	f := newQuarantineFixture(t)
	data := []byte("held bytes 0123456789")
	f.addHeld(t, "dir/file.bin", data, heldJournal(data), fullRanges(data))

	var remoteCalls atomic.Int64
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "no remote calls allowed", http.StatusTeapot)
	}))
	defer remote.Close()

	target := filepath.Join(t.TempDir(), "quarantine")
	result, err := QuarantineHeldCache(QuarantineRequest{
		CacheRoot:   f.root,
		Target:      target,
		Publication: fs.GuardedPublicationAbsent,
	})
	if err != nil {
		t.Fatalf("quarantine failed: %v", err)
	}
	want := sha256.Sum256(data)
	if result.Manifest.Items[0].SHA256 != hex.EncodeToString(want[:]) {
		t.Fatalf("manifest digest %q", result.Manifest.Items[0].SHA256)
	}
	if result.Manifest.Items[0].Publication != fs.GuardedPublicationAbsent {
		t.Fatalf("publication %q", result.Manifest.Items[0].Publication)
	}

	manifestBody, err := os.ReadFile(filepath.Join(target, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest QuarantineManifest
	if err := json.Unmarshal(manifestBody, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Items) != 1 || manifest.Items[0].SHA256 != result.Manifest.Items[0].SHA256 {
		t.Fatalf("manifest mismatch: %+v", manifest)
	}

	copied, err := os.ReadFile(filepath.Join(target, "held", "dir", "file.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(copied) != string(data) {
		t.Fatalf("copied bytes %q", copied)
	}
	if digest := sha256.Sum256(copied); hex.EncodeToString(digest[:]) != manifest.Items[0].SHA256 {
		t.Fatal("manifest digest does not match copied bytes")
	}

	if _, err := os.Lstat(f.root); !os.IsNotExist(err) {
		t.Fatalf("cache root still present: %v", err)
	}
	stateData, err := os.ReadFile(filepath.Join(target, "state", "vfs", "dir", "file.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(stateData) != string(data) {
		t.Fatalf("state bytes %q", stateData)
	}
	if _, err := os.Lstat(filepath.Join(target, "state", "vfsMeta", "dir", "file.bin")); err != nil {
		t.Fatalf("metadata did not move with data: %v", err)
	}
	metadata, err := os.ReadFile(filepath.Join(target, "state", "vfsMeta", "dir", "file.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if digest := sha256.Sum256(metadata); hex.EncodeToString(digest[:]) != manifest.Items[0].MetadataSHA {
		t.Fatal("metadata witness digest does not match")
	}
	if remoteCalls.Load() != 0 {
		t.Fatalf("remote calls: %d", remoteCalls.Load())
	}
}

func TestB3QuarantineRefusesWrongMode(t *testing.T) {
	f := newQuarantineFixture(t)
	data := []byte("bytes")
	f.addHeld(t, "file.bin", data, heldJournal(data), fullRanges(data))
	if err := os.Chmod(f.root, 0755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "quarantine")
	_, err := QuarantineHeldCache(QuarantineRequest{CacheRoot: f.root, Target: target, Publication: fs.GuardedPublicationAbsent})
	if !errors.Is(err, ErrQuarantineRootNotPrivate) {
		t.Fatalf("err = %v", err)
	}
	if _, statErr := os.Lstat(target); !os.IsNotExist(statErr) {
		t.Fatalf("target changed: %v", statErr)
	}
}

func TestB3QuarantineRefusesWrongOwner(t *testing.T) {
	f := newQuarantineFixture(t)
	data := []byte("bytes")
	f.addHeld(t, "file.bin", data, heldJournal(data), fullRanges(data))
	previous := quarantineOwnerOf
	quarantineOwnerOf = func(os.FileInfo) (uint32, bool) { return uint32(os.Geteuid()) + 1, true }
	defer func() { quarantineOwnerOf = previous }()
	target := filepath.Join(t.TempDir(), "quarantine")
	_, err := QuarantineHeldCache(QuarantineRequest{CacheRoot: f.root, Target: target, Publication: fs.GuardedPublicationAbsent})
	if !errors.Is(err, ErrQuarantineRootNotPrivate) {
		t.Fatalf("err = %v", err)
	}
	if _, statErr := os.Lstat(target); !os.IsNotExist(statErr) {
		t.Fatalf("target changed: %v", statErr)
	}
}

func TestB3QuarantineRefusesLiveLock(t *testing.T) {
	f := newQuarantineFixture(t)
	data := []byte("bytes")
	f.addHeld(t, "file.bin", data, heldJournal(data), fullRanges(data))
	release, err := lockQuarantineRoot(f.root)
	if err != nil {
		t.Skipf("root locking unavailable: %v", err)
	}
	defer release()
	target := filepath.Join(t.TempDir(), "quarantine")
	_, err = QuarantineHeldCache(QuarantineRequest{CacheRoot: f.root, Target: target, Publication: fs.GuardedPublicationAbsent})
	if !errors.Is(err, ErrQuarantineNotQuiesced) {
		t.Fatalf("err = %v", err)
	}
	if _, statErr := os.Lstat(target); !os.IsNotExist(statErr) {
		t.Fatalf("target changed: %v", statErr)
	}
}

func TestB3QuarantineRefusesUnprovenRange(t *testing.T) {
	f := newQuarantineFixture(t)
	data := []byte("0123456789")
	journal := heldJournal(data)
	f.addHeld(t, "file.bin", data, journal, ranges.Ranges{{Pos: 0, Size: 3}})
	target := filepath.Join(t.TempDir(), "quarantine")
	_, err := QuarantineHeldCache(QuarantineRequest{CacheRoot: f.root, Target: target, Publication: fs.GuardedPublicationAbsent})
	if !errors.Is(err, ErrQuarantineIncomplete) {
		t.Fatalf("err = %v", err)
	}
	var missing *MissingRangesError
	if !errors.As(err, &missing) || len(missing.Missing) == 0 {
		t.Fatalf("missing-range evidence lost: %v", err)
	}
	if missing.Missing[0].Pos != 3 || missing.Missing[0].Size != 7 {
		t.Fatalf("missing ranges %+v", missing.Missing)
	}
	if _, statErr := os.Lstat(target); !os.IsNotExist(statErr) {
		t.Fatalf("target changed: %v", statErr)
	}
}

func TestB3QuarantineRefusesUnknownPublication(t *testing.T) {
	f := newQuarantineFixture(t)
	data := []byte("bytes")
	journal := heldJournal(data)
	journal.AdmissionKey = "admission"
	journal.SessionID = "session"
	f.addHeld(t, "file.bin", data, journal, fullRanges(data))
	target := filepath.Join(t.TempDir(), "quarantine")
	_, err := QuarantineHeldCache(QuarantineRequest{CacheRoot: f.root, Target: target, Publication: fs.GuardedPublicationCommitted})
	if !errors.Is(err, ErrQuarantinePublicationUnknown) {
		t.Fatalf("err = %v", err)
	}
	if !errors.Is(err, fs.ErrGuardedPublicationUnknown) {
		t.Fatalf("typed journal error lost: %v", err)
	}
	if _, statErr := os.Lstat(target); !os.IsNotExist(statErr) {
		t.Fatalf("target changed: %v", statErr)
	}
}

func TestB3QuarantineRefusesUnrelatedPending(t *testing.T) {
	f := newQuarantineFixture(t)
	data := []byte("bytes")
	f.addHeld(t, "held.bin", data, heldJournal(data), fullRanges(data))
	pending := Info{Size: int64(len(data)), Rs: fullRanges(data), Dirty: true, UploadGeneration: "pending"}
	body, err := json.Marshal(pending)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.root, "vfsMeta", "pending.bin"), body, 0600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "quarantine")
	_, err = QuarantineHeldCache(QuarantineRequest{CacheRoot: f.root, Target: target, Publication: fs.GuardedPublicationAbsent})
	if !errors.Is(err, ErrQuarantineNotQuiesced) {
		t.Fatalf("err = %v", err)
	}
}

func TestB3QuarantineRefusesCrossDevice(t *testing.T) {
	f := newQuarantineFixture(t)
	data := []byte("bytes")
	f.addHeld(t, "file.bin", data, heldJournal(data), fullRanges(data))
	rootInfo, err := os.Stat(f.root)
	if err != nil {
		t.Fatal(err)
	}
	rootDev, ok := deviceOf(rootInfo)
	if !ok {
		t.Skip("device identity unavailable")
	}
	otherParent, err := os.MkdirTemp("/dev/shm", "b3-quarantine-*")
	if err != nil {
		t.Skipf("no second filesystem: %v", err)
	}
	defer os.RemoveAll(otherParent)
	otherInfo, err := os.Stat(otherParent)
	if err != nil {
		t.Fatal(err)
	}
	otherDev, ok := deviceOf(otherInfo)
	if !ok || otherDev == rootDev {
		t.Skip("no different filesystem available")
	}
	target := filepath.Join(otherParent, "quarantine")
	_, err = QuarantineHeldCache(QuarantineRequest{CacheRoot: f.root, Target: target, Publication: fs.GuardedPublicationAbsent})
	if !errors.Is(err, ErrQuarantineCrossDevice) {
		t.Fatalf("err = %v", err)
	}
	if _, statErr := os.Lstat(target); !os.IsNotExist(statErr) {
		t.Fatalf("target changed: %v", statErr)
	}
}

func TestB3QuarantineRefusesSecondRun(t *testing.T) {
	f := newQuarantineFixture(t)
	data := []byte("bytes")
	f.addHeld(t, "file.bin", data, heldJournal(data), fullRanges(data))
	target := filepath.Join(t.TempDir(), "quarantine")
	request := QuarantineRequest{CacheRoot: f.root, Target: target, Publication: fs.GuardedPublicationAbsent}
	if _, err := QuarantineHeldCache(request); err != nil {
		t.Fatalf("first run: %v", err)
	}
	_, err := QuarantineHeldCache(request)
	if !errors.Is(err, ErrQuarantineAlreadyQuarantined) {
		t.Fatalf("second run err = %v", err)
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("target entries changed: %v", entries)
	}
}
