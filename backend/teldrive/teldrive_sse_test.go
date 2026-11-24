package teldrive

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	b3CursorPrefix = "11111111-1111-4111-8111-111111111111.22222222-2222-4222-8222-222222222222."
	b3Resource     = "33333333-3333-4333-8333-333333333333"
	b3Left         = "44444444-4444-4444-8444-444444444444"
	b3Right        = "55555555-5555-4555-8555-555555555555"
	b3Child        = "66666666-6666-4666-8666-666666666666"
	b3Mount        = "77777777-7777-4777-8777-777777777777"
	b3Key          = "fixture-private-sse-key"
)

func b3Cursor(position int) string { return fmt.Sprintf("%s%d", b3CursorPrefix, position) }

func b3Ready(mode string, position int) string {
	return fmt.Sprintf("event: sync.ready\ndata: {\"mode\":%q,\"cursor\":%q}\n\n", mode, b3Cursor(position))
}

func b3Location(parentID, name string) string {
	parent := "null"
	if parentID != "" {
		parent = fmt.Sprintf("%q", parentID)
	}
	return fmt.Sprintf(`{"parentId":%s,"name":%q}`, parent, name)
}

func b3Data(position int, event, resource, payload string) string {
	return fmt.Sprintf("id: %s\nevent: %s\ndata: {\"version\":1,\"occurredAt\":\"2026-01-01T00:00:00Z\",\"resourceType\":\"file\",\"resourceId\":%q,\"payload\":{%s}}\n\n", b3Cursor(position), event, resource, payload)
}

func b3Event(position int, name string) string {
	resource := fmt.Sprintf("33333333-3333-4333-8333-%012d", position)
	return b3Data(position, "file.created", resource, `"kind":"folder","after":`+b3Location("", name))
}

func b3Move(position int, resource, kind, before, after string) string {
	payload := fmt.Sprintf(`"kind":%q`, kind)
	if before != "" {
		payload += `,"before":` + before
	}
	if after != "" {
		payload += `,"after":` + after
	}
	return b3Data(position, "file.updated", resource, payload)
}

type b3StreamRequest struct {
	ctx    context.Context
	mode   string
	cursor string
	writes chan b3StreamWrite
}

type b3StreamWrite struct {
	frame string
	done  chan error
}

func b3StreamFixture(t *testing.T, root string, catalog http.HandlerFunc) (*Fs, <-chan b3StreamRequest) {
	t.Helper()
	requests := make(chan b3StreamRequest, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(apiKeyHeaderName) != b3Key {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/api/v1/me":
			_, _ = fmt.Fprint(w, `{"userId":1}`)
			return
		case "/api/v1/events":
		default:
			if catalog != nil && r.URL.Path == "/api/v1/files" {
				catalog(w, r)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}
		request := b3StreamRequest{r.Context(), r.URL.Query().Get("mode"), r.Header.Get("Last-Event-ID"), make(chan b3StreamWrite)}
		select {
		case requests <- request:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-TelDrive-Events", "ordered")
		w.Header().Set("X-TelDrive-Heartbeat-Ms", "15000")
		for {
			select {
			case operation, ok := <-request.writes:
				if !ok {
					return
				}
				_, err := fmt.Fprint(w, operation.frame)
				if err == nil {
					err = http.NewResponseController(w).Flush()
				}
				operation.done <- err
			case <-r.Context().Done():
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	backend, err := NewFs(t.Context(), "b3-loopback", root, configmap.Simple{"api_host": server.URL, "api_key": b3Key})
	require.NoError(t, err)
	return backend.(*Fs), requests
}

func b3Receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	return b3ReceiveWithin(t, ch, 4*time.Second)
}

func b3ReceiveWithin[T any](t *testing.T, ch <-chan T, limit time.Duration) T {
	t.Helper()
	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatal("timed out waiting for fixture observation")
		var zero T
		return zero
	}
}

func (r b3StreamRequest) send(t *testing.T, frame string) {
	t.Helper()
	done := make(chan error, 1)
	select {
	case r.writes <- b3StreamWrite{frame, done}:
	case <-r.ctx.Done():
		t.Fatal("stream closed before fixture write")
	case <-time.After(4 * time.Second):
		t.Fatal("stream did not take fixture write")
	}
	require.NoError(t, b3Receive(t, done))
}

func TestB3SSEExpiryTracer(t *testing.T) {
	for _, leaf := range []string{"old_header_removed", "handoff_waits_for_callback", "post_h_frame_and_next_cursor", "partial_frame_no_ack"} {
		t.Run(leaf, func(t *testing.T) {
			f, requests := b3StreamFixture(t, "", nil)
			poll := make(chan time.Duration, 1)
			applied := make(chan string, 8)
			baselineEntered := make(chan struct{}, 1)
			releaseBaseline := make(chan struct{})
			var blockBaseline atomic.Bool
			f.Features().ScopedChangeNotify(t.Context(), func(scope fs.ChangeScope) error {
				if scope.All && blockBaseline.Load() {
					baselineEntered <- struct{}{}
					select {
					case <-releaseBaseline:
					case <-t.Context().Done():
						return t.Context().Err()
					}
				}
				switch {
				case scope.All:
					applied <- ""
				case len(scope.Subtrees) == 1:
					applied <- scope.Subtrees[0]
				default:
					return fmt.Errorf("unexpected fixture change scope: %v", scope)
				}
				return nil
			}, poll)
			poll <- time.Second

			initial := b3Receive(t, requests)
			require.Empty(t, initial.cursor)
			initial.send(t, b3Ready("baseline", 10)+b3Event(11, "seed"))
			require.Empty(t, b3Receive(t, applied), "initial ready requires an applied baseline")
			require.Equal(t, "seed", b3Receive(t, applied))
			close(initial.writes)

			resume := b3Receive(t, requests)
			require.Equal(t, b3Cursor(11), resume.cursor)
			resume.send(t, "event: sync.required\ndata: {\"reason\":\"cursor_expired\"}\n\n")
			close(resume.writes)
			fresh := b3Receive(t, requests)
			require.Empty(t, fresh.cursor, "expired resume header survived sync.required")
			require.Equal(t, "ordered", initial.mode)
			require.Equal(t, "ordered", resume.mode)
			require.Equal(t, "ordered", fresh.mode)
			if leaf == "old_header_removed" {
				return
			}

			blockBaseline.Store(true)
			fresh.send(t, b3Ready("baseline", 20))
			b3Receive(t, baselineEntered)
			fresh.send(t, b3Event(21, "post-h"))
			select {
			case premature := <-applied:
				t.Fatalf("notification completed before baseline release: %q", premature)
			default:
			}
			close(releaseBaseline)
			require.Empty(t, b3Receive(t, applied))
			require.Equal(t, "post-h", b3Receive(t, applied))
			if leaf == "partial_frame_no_ack" {
				partial := b3Event(22, "partial")
				fresh.send(t, partial[:len(partial)-1])
			}
			close(fresh.writes)
			next := b3Receive(t, requests)
			require.Equal(t, b3Cursor(21), next.cursor, "reconnect must resume after the last fully applied frame")
			select {
			case extra := <-applied:
				t.Fatalf("unexpected notification before next response: %q", extra)
			default:
			}
		})
	}
}

func b3StartScopes(t *testing.T, f *Fs, requests <-chan b3StreamRequest) (chan time.Duration, b3StreamRequest, <-chan fs.ChangeScope) {
	t.Helper()
	poll := make(chan time.Duration, 1)
	applied := make(chan fs.ChangeScope, 8)
	f.Features().ScopedChangeNotify(t.Context(), func(scope fs.ChangeScope) error {
		applied <- scope
		return nil
	}, poll)
	poll <- time.Second
	stream := b3Receive(t, requests)
	stream.send(t, b3Ready("baseline", 10))
	require.Equal(t, fs.ChangeScope{All: true}, b3Receive(t, applied))
	return poll, stream, applied
}

func b3AliasAbsent(t *testing.T, f *Fs, remote, id string) {
	t.Helper()
	_, forward := f.dirCache.Get(remote)
	_, inverse := f.dirCache.GetInv(id)
	assert.False(t, forward, "stale forward alias for %q", remote)
	assert.False(t, inverse, "stale inverse alias for %s", id)
}

func b3Folder(w http.ResponseWriter, id, parent, name string) {
	_ = json.NewEncoder(w).Encode(api.ReadMetadataResponse{Files: []api.FileInfo{{Id: id, ParentId: parent, Name: name, Kind: "folder"}}})
}

func TestB3SSEScopes(t *testing.T) {
	before := b3Location(b3Left, "old")
	after := b3Location(b3Right, "new")
	for _, tc := range []struct {
		name, kind, before, after string
		left, right               bool
		want                      fs.ChangeScope
	}{
		{"known_both", "folder", before, after, true, true, fs.ChangeScope{Parents: []string{"left", "right"}, Subtrees: []string{"left/old", "right/new"}}},
		{"old_only", "file", before, after, true, false, fs.ChangeScope{All: true, Parents: []string{"left"}}},
		{"new_only", "folder", before, after, false, true, fs.ChangeScope{All: true, Parents: []string{"right"}, Subtrees: []string{"right/new"}}},
		{"neither", "file", before, after, false, false, fs.ChangeScope{All: true}},
		{"root_alias", "file", b3Location("", "old"), b3Location("", "new"), false, false, fs.ChangeScope{Parents: []string{""}}},
		{"unknown_location", "folder", `{"name":"old"}`, after, true, true, fs.ChangeScope{All: true, Parents: []string{"right"}, Subtrees: []string{"right/new"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var origin atomic.Int32
			f, requests := b3StreamFixture(t, "", func(w http.ResponseWriter, _ *http.Request) {
				origin.Add(1)
				w.WriteHeader(http.StatusBadRequest)
			})
			_, stream, applied := b3StartScopes(t, f, requests)
			if tc.left {
				f.dirCache.Put("left", b3Left)
			}
			if tc.right {
				f.dirCache.Put("right", b3Right)
			}
			if tc.name == "known_both" {
				f.dirCache.Put("left/old", b3Resource)
				f.dirCache.Put("left/old/child", b3Child)
			}
			stream.send(t, b3Move(11, b3Resource, tc.kind, tc.before, tc.after))
			require.Equal(t, tc.want, b3Receive(t, applied))
			if tc.name == "known_both" {
				b3AliasAbsent(t, f, "left/old", b3Resource)
				b3AliasAbsent(t, f, "left/old/child", b3Child)
				id, found := f.dirCache.Get("right")
				require.True(t, found)
				require.Equal(t, b3Right, id)
			}
			if tc.name == "unknown_location" {
				f.dirCache.Put("right", b3Right)
				f.dirCache.Put("right/new", b3Child)
				stream.send(t, b3Move(12, b3Resource, "folder", after, b3Location(b3Right, "next")))
				require.True(t, b3Receive(t, applied).All, "folder slot held by another UUID must not prove a precise scope")
				b3AliasAbsent(t, f, "right/new", b3Child)
			}
			require.Zero(t, origin.Load(), "scope callback performed origin I/O")
		})
	}
	t.Run("encoded_names", func(t *testing.T) {
		f, requests := b3StreamFixture(t, "", nil)
		f.opt.Enc = encoder.EncodeBackSlash
		_, stream, applied := b3StartScopes(t, f, requests)
		f.dirCache.Put("left", b3Left)
		stream.send(t, b3Move(11, b3Resource, "folder", b3Location(b3Left, "old＼name"), b3Location(b3Left, "new＼name")))
		require.Equal(t, fs.ChangeScope{Parents: []string{"left"}, Subtrees: []string{`left/old\name`, `left/new\name`}}, b3Receive(t, applied))
	})
	t.Run("chained_moves", func(t *testing.T) {
		f, requests := b3StreamFixture(t, "", nil)
		_, stream, applied := b3StartScopes(t, f, requests)
		f.dirCache.Put("left", b3Left)
		f.dirCache.Put("right", b3Right)
		f.dirCache.Put("left/old", b3Resource)
		stream.send(t, b3Move(11, b3Resource, "folder", before, after))
		require.Equal(t, []string{"left/old", "right/new"}, b3Receive(t, applied).Subtrees)
		f.dirCache.Put("right/new", b3Resource)
		f.dirCache.Put("right/new/child", b3Child)
		stream.send(t, b3Move(12, b3Resource, "folder", after, b3Location(b3Left, "last")))
		require.Equal(t, []string{"right/new", "left/last"}, b3Receive(t, applied).Subtrees)
		b3AliasAbsent(t, f, "right/new", b3Resource)
		b3AliasAbsent(t, f, "right/new/child", b3Child)
		b3AliasAbsent(t, f, "left/old", b3Resource)
	})
	for _, leaf := range []string{"proven_outside", "mounted_root_move_both_parents_outside"} {
		t.Run(leaf, func(t *testing.T) {
			var replaced atomic.Bool
			var origin atomic.Int32
			f, requests := b3StreamFixture(t, "top/mount", func(w http.ResponseWriter, r *http.Request) {
				origin.Add(1)
				switch r.URL.Query().Get("parentId") {
				case "":
					b3Folder(w, b3Left, "", "top")
				case b3Left:
					id := b3Mount
					if replaced.Load() {
						id = b3Right
					}
					b3Folder(w, id, b3Left, "mount")
				default:
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			_, stream, applied := b3StartScopes(t, f, requests)
			baselineCalls := origin.Load()
			if leaf == "proven_outside" {
				stream.send(t, b3Move(11, b3Resource, "file", b3Location("", "outside"), b3Location("", "elsewhere")))
				require.Equal(t, fs.ChangeScope{}, b3Receive(t, applied))
				stream.send(t, b3Move(12, b3Resource, "file", b3Location(b3Mount, "inside"), b3Location("", "outside")))
				require.Equal(t, fs.ChangeScope{Parents: []string{""}}, b3Receive(t, applied))
			} else {
				f.dirCache.Put("nested", b3Child)
				replaced.Store(true)
				stream.send(t, b3Move(11, b3Mount, "folder", b3Location(b3Left, "mount"), b3Location("", "moved")))
				assert.True(t, b3Receive(t, applied).All)
				b3AliasAbsent(t, f, "nested", b3Child)
				f.DirCacheFlush()
				id, err := f.dirCache.FindDir(t.Context(), "", false)
				require.NoError(t, err)
				require.Equal(t, b3Mount, id, "reused root name must not replace the pinned UUID")
			}
			require.Equal(t, baselineCalls, origin.Load(), "event processing must not walk the catalog")
		})
	}
	for _, op := range []string{"listp", "listr", "finddir"} {
		t.Run("stale_"+op, func(t *testing.T) { b3BlockedAlias(t, op, false) })
		t.Run("blocked_"+op+"_barrier_and_pause", func(t *testing.T) { b3BlockedAlias(t, op, true) })
	}
}

func b3BlockedAlias(t *testing.T, operation string, pause bool) {
	t.Helper()
	entered := make(chan string, 1)
	release := make(chan struct{})
	var origin atomic.Int32
	f, requests := b3StreamFixture(t, "", func(w http.ResponseWriter, r *http.Request) {
		origin.Add(1)
		if r.URL.Query().Get("parentId") != b3Left {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		entered <- r.URL.Query().Get("scope")
		select {
		case <-release:
			b3Folder(w, b3Resource, b3Left, "stale")
		case <-r.Context().Done():
		}
	})
	poll, stream, applied := b3StartScopes(t, f, requests)
	f.dirCache.Put("left", b3Left)
	f.dirCache.Put("right", b3Right)
	result := make(chan error, 1)
	var entries atomic.Int32
	callback := func(list fs.DirEntries) error {
		entries.Add(int32(len(list)))
		return nil
	}
	go func() {
		switch operation {
		case "listp":
			result <- f.ListP(t.Context(), "left", callback)
		case "listr":
			result <- f.ListR(t.Context(), "left", callback)
		case "finddir":
			_, err := f.dirCache.FindDir(t.Context(), "left/stale", false)
			result <- err
		}
	}()
	if operation == "listr" {
		require.Equal(t, "recursive", b3Receive(t, entered))
	} else {
		require.Empty(t, b3Receive(t, entered))
	}
	stream.send(t, b3Move(11, b3Resource, "folder", b3Location(b3Left, "stale"), b3Location(b3Right, "fresh")))
	assert.Equal(t, fs.ChangeScope{Parents: []string{"left", "right"}, Subtrees: []string{"left/stale", "right/fresh"}}, b3Receive(t, applied))
	if pause {
		poll <- 0
		b3Receive(t, stream.ctx.Done())
	}
	require.EqualValues(t, 1, origin.Load(), "barrier and poll pause must not start a second origin call")
	close(release)
	require.Error(t, b3Receive(t, result), "pre-barrier origin result must not be returned as current")
	require.Zero(t, entries.Load())
	b3AliasAbsent(t, f, "left/stale", b3Resource)
}

type b3WireReply struct {
	response *http.Response
	err      error
}

type b3WireAttempt struct {
	request *http.Request
	at      time.Time
	reply   chan b3WireReply
}

type b3WireBody struct {
	io.ReadCloser
	closed chan struct{}
	stop   func() bool
	once   sync.Once
}

func (b *b3WireBody) Close() error {
	b.once.Do(func() {
		if b.stop != nil {
			b.stop()
		}
		close(b.closed)
	})
	return b.ReadCloser.Close()
}

func (a b3WireAttempt) respond(status int, headers http.Header, body *b3WireBody) <-chan struct{} {
	a.reply <- b3WireReply{response: &http.Response{StatusCode: status, Header: headers, Body: body, Request: a.request}}
	return body.closed
}

func (a b3WireAttempt) fixed(status int, headers http.Header, text string) <-chan struct{} {
	return a.respond(status, headers, &b3WireBody{ReadCloser: io.NopCloser(strings.NewReader(text)), closed: make(chan struct{})})
}

func b3StrictHeaders() http.Header {
	return http.Header{
		"Content-Type":            {"text/event-stream"},
		"X-Teldrive-Events":       {"ordered"},
		"X-Teldrive-Heartbeat-Ms": {"30000"},
	}
}

func (a b3WireAttempt) stream() (*io.PipeWriter, <-chan struct{}) {
	r, w := io.Pipe()
	stop := context.AfterFunc(a.request.Context(), func() { _ = r.CloseWithError(a.request.Context().Err()) })
	closed := a.respond(http.StatusOK, b3StrictHeaders(), &b3WireBody{ReadCloser: r, closed: make(chan struct{}), stop: stop})
	return w, closed
}

type b3TimerClock struct{ active atomic.Int32 }

type b3TrackedTimer struct {
	clock *b3TimerClock
	timer *time.Timer
	live  atomic.Bool
}

func (t *b3TrackedTimer) deactivate() {
	if t.live.Swap(false) {
		t.clock.active.Add(-1)
	}
}

func (t *b3TrackedTimer) Stop() bool {
	stopped := t.timer.Stop()
	if stopped {
		t.deactivate()
	}
	return stopped
}

func (t *b3TrackedTimer) Reset(delay time.Duration) bool {
	if !t.live.Swap(true) {
		t.clock.active.Add(1)
	}
	return t.timer.Reset(delay)
}

func (c *b3TimerClock) AfterFunc(delay time.Duration, callback func()) sseTimer {
	timer := &b3TrackedTimer{clock: c}
	timer.live.Store(true)
	c.active.Add(1)
	timer.timer = time.AfterFunc(delay, func() {
		timer.deactivate()
		callback()
	})
	return timer
}

func (c *b3TimerClock) Wait(ctx context.Context, delay time.Duration) bool {
	c.active.Add(1)
	defer c.active.Add(-1)
	return waitSSE(ctx, delay)
}

func b3WireBackend(t *testing.T) (*Fs, <-chan b3WireAttempt, *b3TimerClock) {
	t.Helper()
	attempts := make(chan b3WireAttempt, 4)
	transport := uploadTestTransport(func(r *http.Request) (*http.Response, error) {
		attempt := b3WireAttempt{r, time.Now(), make(chan b3WireReply, 1)}
		select {
		case attempts <- attempt:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		select {
		case reply := <-attempt.reply:
			return reply.response, reply.err
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	})
	clock := &b3TimerClock{}
	f := &Fs{root: "fixture-private-sse-path", sseClock: clock}
	f.dirCache = dircache.NewPinned("", rootID, f)
	require.NoError(t, f.dirCache.FindRoot(t.Context(), false))
	f.sseSrv = rest.NewClient(&http.Client{Transport: transport}).
		SetRoot("http://127.0.0.1/fixture-private-sse-url").SetHeader(apiKeyHeaderName, b3Key)
	return f, attempts, clock
}

func b3WireNext(t *testing.T, attempts <-chan b3WireAttempt, cursor string) b3WireAttempt {
	t.Helper()
	attempt := b3ReceiveWithin(t, attempts, 40*time.Second)
	require.Equal(t, "ordered", attempt.request.URL.Query().Get("mode"))
	require.False(t, attempt.request.URL.Query().Has("after"))
	require.Equal(t, b3Key, attempt.request.Header.Get(apiKeyHeaderName))
	require.Equal(t, cursor, attempt.request.Header.Get("Last-Event-ID"))
	return attempt
}

func b3WireWorker(t *testing.T, notify func(orderedChange) error) (<-chan b3WireAttempt, <-chan fs.ChangeScope, <-chan bool, context.CancelFunc, *b3TimerClock) {
	t.Helper()
	f, attempts, clock := b3WireBackend(t)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	applied, done := make(chan fs.ChangeScope, 16), make(chan bool, 1)
	go func() {
		done <- f.runOrderedSSE(ctx, func(change orderedChange) error {
			applied <- change.scope
			if notify != nil {
				return notify(change)
			}
			return nil
		})
	}()
	return attempts, applied, done, cancel, clock
}

func b3NoObservation[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("unexpected fixture observation")
	default:
	}
}

// b3LogBuffer is a goroutine-safe sink: the worker logs from its own
// goroutine while the test reads assertions.
type b3LogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *b3LogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *b3LogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *b3LogBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

var (
	b3LogSink  = &b3LogBuffer{}
	b3LogOnce  sync.Once
	b3LogGuard sync.Mutex
)

// b3CaptureLogs installs the test logger once and serialises capture windows.
// Resetting the global logger per test raced with worker goroutines still
// logging, so the sink is reset instead of the logger.
func b3CaptureLogs(t *testing.T) *b3LogBuffer {
	t.Helper()
	b3LogOnce.Do(func() {
		fs.SetLogger(slog.NewTextHandler(b3LogSink, &slog.HandlerOptions{Level: slog.LevelDebug}))
	})
	b3LogGuard.Lock()
	b3LogSink.Reset()
	t.Cleanup(func() { b3LogGuard.Unlock() })
	return b3LogSink
}

func b3WriteFrame(t *testing.T, w io.Writer, frame string) {
	t.Helper()
	_, err := io.WriteString(w, frame)
	require.NoError(t, err)
}

func TestB3SSEOutage(t *testing.T) {
	for _, leaf := range []string{"exceeds_old_pacer_budget", "retry_after_jitter", "eof_tls_5xx", "heartbeat_without_ready", "one_safe_warning_and_recovery", "unsupported_404_unconfirmed"} {
		t.Run(leaf, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logs := b3CaptureLogs(t)
				var release chan struct{}
				if leaf == "one_safe_warning_and_recovery" {
					release = make(chan struct{})
				}
				attempts, applied, done, cancel, clock := b3WireWorker(t, func(change orderedChange) error {
					if release != nil && change.scope.All {
						select {
						case <-release:
						case <-t.Context().Done():
							return t.Context().Err()
						}
					}
					return nil
				})
				initial := b3WireNext(t, attempts, "")
				switch leaf {
				case "exceeds_old_pacer_budget":
					for range max(fs.GetConfig(t.Context()).LowLevelRetries, 1) + 2 {
						initial.reply <- b3WireReply{err: io.ErrUnexpectedEOF}
						initial = b3WireNext(t, attempts, "")
					}
					initial.fixed(http.StatusOK, b3StrictHeaders(), b3Ready("baseline", 10)+b3Event(11, "eventual"))
					require.True(t, b3Receive(t, applied).All)
					require.Equal(t, []string{"eventual"}, b3Receive(t, applied).Subtrees)
				case "retry_after_jitter":
					initial.fixed(http.StatusTooManyRequests, http.Header{"Retry-After": {"9"}}, "")
					next := b3WireNext(t, attempts, "")
					require.Equal(t, 9*time.Second, next.at.Sub(initial.at))
					next.fixed(http.StatusServiceUnavailable, http.Header{"Retry-After": {next.at.Add(40 * time.Second).UTC().Format(http.TimeFormat)}}, "")
					initial = b3WireNext(t, attempts, "")
					require.Equal(t, 30*time.Second, initial.at.Sub(next.at))
					jittered := false
					for _, base := range []time.Duration{4, 8, 16, 30} {
						initial.reply <- b3WireReply{err: io.EOF}
						next := b3WireNext(t, attempts, "")
						delta := next.at.Sub(initial.at)
						base *= time.Second
						require.GreaterOrEqual(t, delta, 4*base/5)
						require.LessOrEqual(t, delta, min(6*base/5, 30*time.Second))
						jittered = jittered || delta != base
						initial = next
					}
					require.True(t, jittered, "retries used only unjittered base delays")
				case "eof_tls_5xx":
					initial.reply <- b3WireReply{err: io.EOF}
					b3WireNext(t, attempts, "").reply <- b3WireReply{err: tls.RecordHeaderError{Msg: "fixture TLS fault"}}
					closed := b3WireNext(t, attempts, "").fixed(http.StatusBadGateway, nil, "fixture-private-sse-body")
					b3Receive(t, closed)
					b3WireNext(t, attempts, "").fixed(http.StatusOK, b3StrictHeaders(), "event: stream.error\ndata: {\"code\":\"temporary\"}\n\n")
					unanswered := b3WireNext(t, attempts, "")
					time.Sleep(10*time.Second - time.Millisecond)
					synctest.Wait()
					b3NoObservation(t, unanswered.request.Context().Done())
					time.Sleep(time.Millisecond)
					b3Receive(t, unanswered.request.Context().Done())
					next := b3WireNext(t, attempts, "")
					require.GreaterOrEqual(t, next.at.Sub(unanswered.at), 10*time.Second)
					next.fixed(http.StatusOK, b3StrictHeaders(), b3Ready("baseline", 10)+b3Event(11, "recovered"))
					require.True(t, b3Receive(t, applied).All)
					require.Equal(t, []string{"recovered"}, b3Receive(t, applied).Subtrees)
				case "heartbeat_without_ready":
					w, closed := initial.stream()
					b3WriteFrame(t, w, ": pulse\n\n")
					time.Sleep(59 * time.Second)
					b3WriteFrame(t, w, ": pulse\n\n")
					synctest.Wait()
					b3NoObservation(t, closed)
					b3NoObservation(t, applied)
					require.NotContains(t, logs.String(), "unavailable for 60s")
					time.Sleep(time.Second)
					synctest.Wait()
					b3Receive(t, closed)
					require.Equal(t, 1, strings.Count(logs.String(), "unavailable for 60s"))
					next := b3WireNext(t, attempts, "")
					w, closed = next.stream()
					b3WriteFrame(t, w, b3Ready("baseline", 10))
					require.True(t, b3Receive(t, applied).All)
					time.Sleep(90*time.Second - time.Millisecond)
					synctest.Wait()
					b3NoObservation(t, closed)
					time.Sleep(time.Millisecond)
					b3Receive(t, closed)
				case "one_safe_warning_and_recovery":
					initial.fixed(http.StatusServiceUnavailable, http.Header{"Retry-After": {"28"}}, "fixture-private-sse-body")
					next := b3WireNext(t, attempts, "")
					require.Equal(t, 28*time.Second, next.at.Sub(initial.at))
					w, _ := next.stream()
					b3WriteFrame(t, w, ": pulse\n\n")
					time.Sleep(40 * time.Second)
					synctest.Wait()
					require.Equal(t, 1, strings.Count(logs.String(), "unavailable for 60s"))
					require.NotContains(t, logs.String(), "SSE recovered")
					b3WriteFrame(t, w, b3Ready("baseline", 10))
					require.True(t, b3Receive(t, applied).All)
					synctest.Wait()
					require.NotContains(t, logs.String(), "SSE recovered")
					close(release)
					synctest.Wait()
					require.Equal(t, 1, strings.Count(logs.String(), "SSE recovered"))
					b3WriteFrame(t, w, b3Event(11, "fixture-private-sse-name"))
					require.Equal(t, []string{"fixture-private-sse-name"}, b3Receive(t, applied).Subtrees)
					b3WriteFrame(t, w, "event: stream.error\ndata: {\"code\":\"fixture-private-sse-body\"}\n\n")
					b3WireNext(t, attempts, b3Cursor(11))
					synctest.Wait()
					log := logs.String()
					require.Contains(t, log, "level=DEBUG")
					require.Equal(t, 1, strings.Count(log, "unavailable for 60s"))
					require.Equal(t, 1, strings.Count(log, "SSE recovered"))
					for _, private := range []string{b3Key, b3CursorPrefix, "fixture-private-sse-path", "fixture-private-sse-url", "fixture-private-sse-name", "fixture-private-sse-body"} {
						require.NotContains(t, log, private)
					}
				case "unsupported_404_unconfirmed":
					initial.fixed(http.StatusNotFound, nil, "fixture-private-sse-body")
					next := b3WireNext(t, attempts, "")
					require.Equal(t, 30*time.Second, next.at.Sub(initial.at))
					legacy := b3StrictHeaders()
					legacy.Del("X-TelDrive-Events")
					b3Receive(t, next.fixed(http.StatusOK, legacy, b3Ready("baseline", 10)+b3Event(11, "legacy")))
					b3NoObservation(t, applied)
					initial = b3WireNext(t, attempts, "")
					require.Equal(t, 30*time.Second, initial.at.Sub(next.at))
					invalidHeartbeat := b3StrictHeaders()
					invalidHeartbeat.Set("X-TelDrive-Heartbeat-Ms", "3600001")
					b3Receive(t, initial.fixed(http.StatusOK, invalidHeartbeat, b3Ready("baseline", 11)))
					b3NoObservation(t, applied)
					next = b3WireNext(t, attempts, "")
					require.Equal(t, 30*time.Second, next.at.Sub(initial.at))
					next.fixed(http.StatusOK, b3StrictHeaders(), b3Ready("baseline", 12))
					require.True(t, b3Receive(t, applied).All)
				}
				cancel()
				require.False(t, b3Receive(t, done))
				require.Zero(t, clock.active.Load(), "no attempt or retry timers may survive cancellation")
			})
		})
	}
}

func TestB3SSETypedExit(t *testing.T) {
	for _, leaf := range []string{"status_401", "status_403", "misleading_digits", "cancel_read", "cancel_backoff", "poll_pause_reenable", "normal_lease_rotation_resumes_without_false_outage"} {
		t.Run(leaf, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logs := b3CaptureLogs(t)
				if leaf == "poll_pause_reenable" {
					f, attempts, clock := b3WireBackend(t)
					poll := make(chan time.Duration)
					applied := make(chan fs.ChangeScope, 4)
					f.ScopedChangeNotify(t.Context(), func(scope fs.ChangeScope) error { applied <- scope; return nil }, poll)
					poll <- time.Second
					first := b3WireNext(t, attempts, "")
					w, closed := first.stream()
					b3WriteFrame(t, w, b3Ready("baseline", 10)+b3Event(11, "seed"))
					require.True(t, b3Receive(t, applied).All)
					require.Equal(t, []string{"seed"}, b3Receive(t, applied).Subtrees)
					poll <- 2 * time.Second
					synctest.Wait()
					b3NoObservation(t, attempts)
					require.EqualValues(t, 1, clock.active.Load(), "ready stream keeps only its silence watchdog")
					poll <- 0
					b3Receive(t, first.request.Context().Done())
					b3Receive(t, closed)
					synctest.Wait()
					require.Zero(t, clock.active.Load(), "pause must stop all timers before re-enable")
					time.Sleep(2 * time.Hour)
					synctest.Wait()
					b3NoObservation(t, attempts)
					poll <- time.Second
					next := b3WireNext(t, attempts, "")
					w, closed = next.stream()
					b3WriteFrame(t, w, b3Ready("baseline", 20))
					require.True(t, b3Receive(t, applied).All)
					close(poll)
					b3Receive(t, next.request.Context().Done())
					b3Receive(t, closed)
					synctest.Wait()
					require.Zero(t, clock.active.Load(), "closing poll channel must stop all timers")
					require.NotContains(t, logs.String(), "unavailable for 60s")
					return
				}
				attempts, applied, done, cancel, clock := b3WireWorker(t, nil)
				first := b3WireNext(t, attempts, "")
				switch leaf {
				case "status_401", "status_403":
					status := http.StatusUnauthorized
					if leaf == "status_403" {
						status = http.StatusForbidden
					}
					b3Receive(t, first.fixed(status, nil, "untyped private text"))
					require.True(t, b3Receive(t, done), "real HTTP auth status is terminal")
					require.Zero(t, clock.active.Load())
					time.Sleep(2 * time.Minute)
					b3NoObservation(t, attempts)
					b3NoObservation(t, applied)
					require.Equal(t, 1, strings.Count(logs.String(), "authorization denied"))
					require.NotContains(t, logs.String(), "untyped private text")
				case "misleading_digits":
					first.reply <- b3WireReply{err: &tls.RecordHeaderError{Msg: "fixture error 401 403 404 token"}}
					next := b3WireNext(t, attempts, "")
					next.fixed(http.StatusOK, b3StrictHeaders(), b3Ready("baseline", 10)+b3Event(11, "delivered"))
					require.True(t, b3Receive(t, applied).All)
					require.Equal(t, []string{"delivered"}, b3Receive(t, applied).Subtrees)
					require.NotContains(t, logs.String(), "401 403 404")
				case "cancel_read":
					w, closed := first.stream()
					b3WriteFrame(t, w, ": waiting\n\n")
					synctest.Wait()
					b3NoObservation(t, closed)
					require.EqualValues(t, 3, clock.active.Load(), "startup, silence and outage timers must be live")
					cancel()
					b3Receive(t, first.request.Context().Done())
					b3Receive(t, closed)
					require.False(t, b3Receive(t, done))
					require.Zero(t, clock.active.Load())
					time.Sleep(2 * time.Hour)
					b3NoObservation(t, attempts)
					require.Empty(t, logs.String())
				case "cancel_backoff":
					b3Receive(t, first.fixed(http.StatusTooManyRequests, http.Header{"Retry-After": {"30"}}, ""))
					synctest.Wait()
					require.EqualValues(t, 2, clock.active.Load(), "outage and backoff timers must be live")
					cancel()
					require.False(t, b3Receive(t, done))
					require.Zero(t, clock.active.Load())
					time.Sleep(2 * time.Hour)
					b3NoObservation(t, attempts)
					require.NotContains(t, logs.String(), "unavailable for 60s")
				case "normal_lease_rotation_resumes_without_false_outage":
					for turn := range 4 {
						w, closed := first.stream()
						mode := "replay"
						if turn == 0 {
							mode = "baseline"
						}
						b3WriteFrame(t, w, b3Ready(mode, 10+turn)+b3Event(11+turn, "healthy"))
						if turn == 0 {
							require.True(t, b3Receive(t, applied).All)
						}
						require.Equal(t, []string{"healthy"}, b3Receive(t, applied).Subtrees)
						time.Sleep(25 * time.Second)
						renewed := time.Now()
						b3WriteFrame(t, w, "event: sync.renew\ndata: {}\n\n")
						b3Receive(t, closed)
						first = b3WireNext(t, attempts, b3Cursor(11+turn))
						require.Equal(t, renewed, first.at, "lease rotation does not consume backoff")
					}
					require.NotContains(t, logs.String(), "unavailable for 60s")
					require.NotContains(t, logs.String(), "SSE recovered")
				}
			})
		})
	}
}

func b3Checkpoint(position int) string {
	return fmt.Sprintf("event: sync.checkpoint\ndata: {\"cursor\":%q}\n\n", b3Cursor(position))
}

func TestB3SSEReplayBarrier(t *testing.T) {
	for _, leaf := range []string{"full_replay_no_blanket_reset", "malformed_frame", "duplicate_ready", "regressing_cursor", "interrupted_replay", "callback_failure", "invalid_cursor_restarts", "oversized_reset_clears_cursor"} {
		t.Run(leaf, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var reject atomic.Bool
				attempts, applied, _, _, _ := b3WireWorker(t, func(change orderedChange) error {
					if reject.Load() && change.scope.All {
						return fmt.Errorf("fixture-private-sse-callback")
					}
					return nil
				})
				first := b3WireNext(t, attempts, "")
				first.fixed(http.StatusOK, b3StrictHeaders(), b3Ready("baseline", 10)+b3Event(11, "seed"))
				require.True(t, b3Receive(t, applied).All)
				require.Equal(t, []string{"seed"}, b3Receive(t, applied).Subtrees)
				resume := b3WireNext(t, attempts, b3Cursor(11))
				switch leaf {
				case "full_replay_no_blanket_reset":
					resume.fixed(http.StatusOK, b3StrictHeaders(), b3Event(12, "replayed")+b3Ready("replay", 14)+b3Event(15, "live")+b3Checkpoint(16))
					require.Equal(t, fs.ChangeScope{Parents: []string{""}, Subtrees: []string{"replayed"}}, b3Receive(t, applied))
					require.Equal(t, fs.ChangeScope{Parents: []string{""}, Subtrees: []string{"live"}}, b3Receive(t, applied))
					b3WireNext(t, attempts, b3Cursor(16))
				case "malformed_frame":
					suffix := b3Ready("replay", 14) + b3Event(15, "must-not-apply")
					for _, malformed := range []string{
						"id: " + b3Cursor(12) + "\nevent: file.created\ndata: {bad-json}\n\n",
						strings.Replace(b3Event(12, "bad-cursor"), b3Cursor(12), b3CursorPrefix+"+12", 1),
						"id: " + b3Cursor(12) + "\n" + b3Ready("replay", 12),
						"event: file.created\ndata: " + strings.Repeat("x", 64*1024) + "\n\n",
					} {
						resume.fixed(http.StatusOK, b3StrictHeaders(), malformed+suffix)
						resume = b3WireNext(t, attempts, b3Cursor(11))
						b3NoObservation(t, applied)
					}
				case "duplicate_ready":
					resume.fixed(http.StatusOK, b3StrictHeaders(), b3Ready("replay", 12)+b3Ready("replay", 13)+b3Event(14, "must-not-apply"))
					b3WireNext(t, attempts, b3Cursor(12))
				case "regressing_cursor":
					resume.fixed(http.StatusOK, b3StrictHeaders(), b3Event(12, "applied")+b3Ready("replay", 11)+b3Event(13, "must-not-apply"))
					require.Equal(t, []string{"applied"}, b3Receive(t, applied).Subtrees)
					resume = b3WireNext(t, attempts, b3Cursor(12))
					resume.fixed(http.StatusOK, b3StrictHeaders(), b3Ready("replay", 13)+b3Checkpoint(13)+b3Event(14, "must-not-apply"))
					resume = b3WireNext(t, attempts, b3Cursor(13))
					resume.fixed(http.StatusOK, b3StrictHeaders(), b3Ready("replay", 13)+b3Event(12, "must-not-apply"))
					b3WireNext(t, attempts, b3Cursor(13))
				case "interrupted_replay":
					partial := b3Event(13, "partial")
					resume.fixed(http.StatusOK, b3StrictHeaders(), b3Event(12, "applied")+partial[:len(partial)-1])
					require.Equal(t, []string{"applied"}, b3Receive(t, applied).Subtrees)
					resume = b3WireNext(t, attempts, b3Cursor(12))
					resume.fixed(http.StatusOK, b3StrictHeaders(), b3Event(13, "resumed")+b3Ready("replay", 14))
					require.Equal(t, []string{"resumed"}, b3Receive(t, applied).Subtrees)
					b3WireNext(t, attempts, b3Cursor(14))
				case "callback_failure":
					resume.fixed(http.StatusOK, b3StrictHeaders(), "event: sync.required\ndata: {\"reason\":\"cursor_expired\"}\n\n")
					fresh := b3WireNext(t, attempts, "")
					reject.Store(true)
					closed := fresh.fixed(http.StatusOK, b3StrictHeaders(), b3Ready("baseline", 20)+b3Event(21, "must-not-apply"))
					require.True(t, b3Receive(t, applied).All)
					b3Receive(t, closed)
					retry := b3WireNext(t, attempts, "")
					reject.Store(false)
					retry.fixed(http.StatusOK, b3StrictHeaders(), b3Ready("baseline", 22)+b3Event(23, "applied"))
					require.True(t, b3Receive(t, applied).All)
					require.Equal(t, []string{"applied"}, b3Receive(t, applied).Subtrees)
					b3WireNext(t, attempts, b3Cursor(23))
				case "invalid_cursor_restarts":
					resume.fixed(http.StatusUnprocessableEntity, nil, `{"error":{"code":"unrelated","message":"invalid_event_cursor"}}`)
					resume = b3WireNext(t, attempts, b3Cursor(11))
					resume.fixed(http.StatusUnprocessableEntity, nil, `{"error":{"code":"invalid_event_cursor","message":"private"}}`)
					fresh := b3WireNext(t, attempts, "")
					fresh.fixed(http.StatusOK, b3StrictHeaders(), b3Ready("baseline", 20))
					require.True(t, b3Receive(t, applied).All)
					b3WireNext(t, attempts, b3Cursor(20))
				case "oversized_reset_clears_cursor":
					for turn := range 2 {
						h := 20 + turn*10
						prefix := ""
						if turn == 1 {
							prefix = b3Ready("replay", 21)
						}
						resume.fixed(http.StatusOK, b3StrictHeaders(), prefix+"event: sync.required\ndata: {\"reason\":\"replay_limit\"}\n\n")
						fresh := b3WireNext(t, attempts, "")
						fresh.fixed(http.StatusOK, b3StrictHeaders(), b3Ready("baseline", h)+b3Event(h+1, "after-reset"))
						require.True(t, b3Receive(t, applied).All)
						require.Equal(t, []string{"after-reset"}, b3Receive(t, applied).Subtrees)
						resume = b3WireNext(t, attempts, b3Cursor(h+1))
					}
				}
				synctest.Wait()
				b3NoObservation(t, applied)
			})
		})
	}
}
