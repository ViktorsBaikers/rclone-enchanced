package vfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/teldrive"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/vfs/vfscache/writeback"
	"github.com/rclone/rclone/vfs/vfscommon"
)

// b3ChildRequest and b3ChildReply are the private line protocol between the
// paired server test and this supervised child. The child owns the VFS, its
// cache and every ordinary file handle; the parent only observes.
type b3ChildRequest struct {
	Op   string
	Path string
	Size int
}

type b3ChildReply struct {
	Error     string
	Exists    bool
	IsDir     bool
	Size      int64
	ID        string
	SHA256    string
	Dirty     bool
	Held      string
	WriteBack float64
	Names     []string
}

func b3PairPayload(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}

// TestB3PairedMountChild is the child half of the paired proof. It only runs
// when the supervised parent supplies an endpoint, a private control socket and
// an owned cache directory; an ordinary `go test ./vfs/...` run skips it.
func TestB3PairedMountChild(t *testing.T) {
	if os.Getenv("B3_PAIR_CHILD") != "1" {
		t.Skip("paired VFS child; driven by the supervised server test")
	}
	endpoint := os.Getenv("B3_PAIR_ENDPOINT")
	control := os.Getenv("B3_PAIR_CONTROL")
	directory := os.Getenv("B3_PAIR_DIR")
	if endpoint == "" || control == "" || directory == "" {
		t.Fatal("paired child requires endpoint, control socket and owned directory")
	}
	conn, err := net.Dial("unix", control)
	if err != nil {
		t.Fatalf("paired child control connect: %v", err)
	}
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	cacheRoot := filepath.Join(directory, "cache")
	if err := os.MkdirAll(cacheRoot, 0700); err != nil {
		t.Fatalf("paired child cache root: %v", err)
	}
	if err := config.SetCacheDir(cacheRoot); err != nil {
		t.Fatalf("paired child cache dir: %v", err)
	}
	// The child never reads inherited rclone configuration or credentials.
	ctx, ci := fs.AddConfig(context.Background())
	ci.BufferSize, ci.LowLevelRetries, ci.Transfers, ci.Checkers = 0, 2, 1, 1
	ci.LogLevel = fs.LogLevelDebug

	newMount := func() *VFS {
		remote, err := teldrive.NewFs(ctx, "b3pair", "", configmap.Simple{
			"api_host": endpoint, "api_key": "test-key", "guarded_upload": "true",
			"encoding": "Slash,BackSlash,LeftSpace,RightSpace,InvalidUtf8",
		})
		if err != nil {
			t.Fatalf("paired child backend: %v", err)
		}
		opt := vfscommon.Opt
		opt.CacheMode = vfscommon.CacheModeFull
		opt.WriteBack = fs.Duration(24 * time.Hour)
		opt.DirCacheTime = fs.Duration(time.Hour)
		opt.PollInterval = fs.Duration(time.Hour)
		opt.HandleCaching = 0
		return New(ctx, remote, &opt)
	}
	v := newMount()
	shutdown := func() {
		if v == nil {
			return
		}
		remote := v.Fs()
		v.Shutdown()
		if do := remote.Features().Shutdown; do != nil {
			_ = do(ctx)
		}
		v = nil
	}
	defer shutdown()

	for {
		var request b3ChildRequest
		if err := decoder.Decode(&request); err != nil {
			if !errors.Is(err, io.EOF) {
				t.Logf("paired child control decode: %v", err)
			}
			return
		}
		reply := b3ChildHandle(v, request)
		if request.Op == "restart" && reply.Error == "" {
			shutdown()
			v = newMount()
			reply = b3ChildState(v, request.Path)
		}
		if err := encoder.Encode(&reply); err != nil {
			t.Logf("paired child control encode: %v", err)
			return
		}
		if request.Op == "exit" {
			return
		}
	}
}

func b3ChildHandle(v *VFS, request b3ChildRequest) b3ChildReply {
	switch request.Op {
	case "stat":
		return b3ChildState(v, request.Path)
	case "state":
		return b3ChildStatus(v, request.Path)
	case "write":
		payload := b3PairPayload(request.Size)
		deadline := time.Now().Add(5 * time.Second)
		var h Handle
		for {
			var err error
			h, err = v.Create(request.Path)
			if err == nil {
				break
			}
			if !strings.Contains(err.Error(), "namespace changed during lookup") || time.Now().After(deadline) {
				return b3ChildReply{Error: err.Error()}
			}
			time.Sleep(50 * time.Millisecond)
		}
		if _, err := h.Write(payload); err != nil {
			_ = h.Close()
			return b3ChildReply{Error: err.Error()}
		}
		if err := h.Close(); err != nil {
			return b3ChildReply{Error: err.Error()}
		}
		return b3ChildReply{Exists: true, Size: int64(len(payload)), SHA256: b3ChildDigest(payload),
			Dirty: v.cache.DirtyItem(request.Path) != nil, WriteBack: time.Duration(v.Opt.WriteBack).Seconds()}
	case "flush":
		if err := b3ChildWriteback(v, request.Path); err != nil {
			return b3ChildReply{Error: err.Error()}
		}
		return b3ChildState(v, request.Path)
	case "read":
		in, err := v.Open(request.Path)
		if err != nil {
			return b3ChildReply{Error: err.Error()}
		}
		data, readErr := io.ReadAll(in)
		closeErr := in.Close()
		if readErr != nil || closeErr != nil {
			return b3ChildReply{Error: errors.Join(readErr, closeErr).Error()}
		}
		return b3ChildReply{Exists: true, Size: int64(len(data)), SHA256: b3ChildDigest(data),
			Dirty: v.cache.DirtyItem(request.Path) != nil, Held: v.cache.HeldItem(request.Path),
			WriteBack: time.Duration(v.Opt.WriteBack).Seconds()}
	case "list":
		entries, err := v.ReadDir(request.Path)
		if err != nil {
			return b3ChildReply{Error: err.Error()}
		}
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		sort.Strings(names)
		return b3ChildReply{Exists: true, IsDir: true, Names: names}
	case "absent":
		_, err := v.Stat(request.Path)
		if errors.Is(err, ENOENT) || os.IsNotExist(err) {
			return b3ChildReply{}
		}
		if err != nil {
			return b3ChildReply{Error: err.Error()}
		}
		return b3ChildReply{Exists: true}
	case "restart":
		return b3ChildReply{WriteBack: time.Duration(v.Opt.WriteBack).Seconds()}
	case "exit":
		return b3ChildReply{}
	default:
		return b3ChildReply{Error: fmt.Sprintf("unknown op %q", request.Op)}
	}
}

func b3ChildStatus(v *VFS, path string) b3ChildReply {
	return b3ChildReply{
		Dirty:     v.cache.DirtyItem(path) != nil,
		Held:      v.cache.HeldItem(path),
		WriteBack: time.Duration(v.Opt.WriteBack).Seconds(),
	}
}

func b3ChildState(v *VFS, path string) b3ChildReply {
	reply := b3ChildReply{
		Dirty:     v.cache.DirtyItem(path) != nil,
		Held:      v.cache.HeldItem(path),
		WriteBack: time.Duration(v.Opt.WriteBack).Seconds(),
	}
	node, err := v.Stat(path)
	if errors.Is(err, ENOENT) || os.IsNotExist(err) {
		return reply
	}
	if err != nil {
		reply.Error = err.Error()
		return reply
	}
	reply.Exists = true
	reply.IsDir = node.IsDir()
	reply.Size = node.Size()
	if id, ok := node.DirEntry().(interface{ ID() string }); ok {
		reply.ID = id.ID()
	}
	return reply
}

func b3ChildWriteback(v *VFS, name string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if v.cache == nil {
			return errors.New("paired child has no VFS cache")
		}
		infos, _ := v.cache.Queue()["queue"].([]writeback.QueueInfo)
		for _, info := range infos {
			if info.Name == name {
				return v.cache.QueueSetExpiry(info.ID, time.Now(), 0)
			}
		}
		if v.cache.DirtyItem(name) == nil {
			return fmt.Errorf("paired child has no dirty item %q", name)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("paired child has no queued writeback for %q", name)
}

func b3ChildDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// TestB3PairedSSEPreamble drives the real backend client against the exact
// ordered-stream preamble the generated server emits. The standard `retry:`
// field is inert: the first event must be delivered and the reconnect must
// resume from that event, never from an empty cursor.
func TestB3PairedSSEPreamble(t *testing.T) {
	const epoch = "11111111-1111-4111-8111-111111111111"
	const owner = "22222222-2222-4222-8222-222222222222"
	readyCursor := epoch + "." + owner + ".5"
	eventCursor := epoch + "." + owner + ".6"
	event := `{"version":1,"occurredAt":"2026-01-01T00:00:00Z","resourceType":"file",` +
		`"resourceId":"33333333-3333-4333-8333-333333333333",` +
		`"payload":{"kind":"file","after":{"parentId":null,"name":"first"}}}`
	var mu sync.Mutex
	var cursors []string
	second := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/me":
			_, _ = io.WriteString(w, `{"userId":1}`)
		case "/api/v1/events":
			mu.Lock()
			cursors = append(cursors, r.Header.Get("Last-Event-ID"))
			connection := len(cursors)
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("X-TelDrive-Events", "ordered")
			w.Header().Set("X-TelDrive-Heartbeat-Ms", "1000")
			w.WriteHeader(http.StatusOK)
			flush := func() { _ = http.NewResponseController(w).Flush() }
			_, _ = io.WriteString(w, "retry: 3000\n\n")
			if connection == 1 {
				_, _ = io.WriteString(w, "event: sync.ready\ndata: {\"mode\":\"baseline\",\"cursor\":\""+readyCursor+"\"}\n\n")
				flush()
				_, _ = io.WriteString(w, "id: "+eventCursor+"\nevent: file.created\ndata: "+event+"\n\n")
			} else {
				_, _ = io.WriteString(w, "event: sync.ready\ndata: {\"mode\":\"replay\",\"cursor\":\""+eventCursor+"\"}\n\n")
				select {
				case second <- r.Header.Get("Last-Event-ID"):
				default:
				}
			}
			flush()
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, ci := fs.AddConfig(ctx)
	ci.LogLevel = fs.LogLevelDebug
	remote, err := teldrive.NewFs(ctx, "sse-preamble", "", configmap.Simple{
		"api_host": server.URL, "api_key": "test-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	notify := remote.Features().ScopedChangeNotify
	if notify == nil {
		t.Fatal("ordered notifications are unavailable")
	}
	called := make(chan struct{}, 1)
	poll := make(chan time.Duration, 1)
	notify(ctx, func(fs.ChangeScope) error {
		select {
		case called <- struct{}{}:
		default:
		}
		return nil
	}, poll)
	poll <- time.Second
	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("first replay event was not delivered through the retry preamble")
	}
	select {
	case cursor := <-second:
		if cursor != eventCursor {
			t.Fatalf("reconnect cursor = %q, want %q", cursor, eventCursor)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client did not reconnect after the replay stream")
	}
}
