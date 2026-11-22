package teldrive

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand/v2"
	"mime"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/rest"
)

type orderedChange struct {
	scope fs.ChangeScope
	paths []string
	kind  fs.EntryType
}

// ChangeNotify calls notify for changed paths.
func (f *Fs) ChangeNotify(ctx context.Context, notify func(string, fs.EntryType), poll <-chan time.Duration) {
	go f.changeNotifyLoop(ctx, func(change orderedChange) error {
		if change.scope.All {
			notify("", fs.EntryDirectory)
		}
		for _, remote := range change.paths {
			notify(remote, change.kind)
		}
		return nil
	}, poll)
}

// ScopedChangeNotify advances the cursor only after notify acknowledges the scope.
func (f *Fs) ScopedChangeNotify(ctx context.Context, notify func(fs.ChangeScope) error, poll <-chan time.Duration) {
	go f.changeNotifyLoop(ctx, func(change orderedChange) error { return notify(change.scope) }, poll)
}

func (f *Fs) changeNotifyLoop(ctx context.Context, notify func(orderedChange) error, poll <-chan time.Duration) {
	var cancel context.CancelFunc
	var done <-chan bool
	var enabled, denied bool
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case interval, ok := <-poll:
			if !ok {
				return
			}
			enabled = interval > 0
			if !enabled && cancel != nil {
				cancel()
			}
		case denied = <-done:
			cancel()
			cancel, done = nil, nil
		}
		if ctx.Err() == nil && done == nil && enabled && !denied {
			workerCtx, stop := context.WithCancel(ctx)
			finished := make(chan bool, 1)
			cancel, done = stop, finished
			go func() { finished <- f.runOrderedSSE(workerCtx, notify) }()
		}
	}
}

type sseOutcome string

const (
	sseTransport   sseOutcome = "transport"
	sseProtocol    sseOutcome = "protocol"
	sseUnsupported sseOutcome = "unsupported"
	sseAuth        sseOutcome = "auth"
	sseReset       sseOutcome = "reset"
	sseRenew       sseOutcome = "renew"
	sseCallback    sseOutcome = "callback"
)

const (
	sseHeaderDeadline = 10 * time.Second
	sseReadyDeadline  = 60 * time.Second
	sseRetryCap       = 30 * time.Second
)

type sseTimer interface {
	Stop() bool
	Reset(time.Duration) bool
}

type sseClock interface {
	AfterFunc(time.Duration, func()) sseTimer
	Wait(context.Context, time.Duration) bool
}

func (f *Fs) afterSSE(delay time.Duration, callback func()) sseTimer {
	if f.sseClock != nil {
		return f.sseClock.AfterFunc(delay, callback)
	}
	return time.AfterFunc(delay, callback)
}

func waitSSE(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

type sseOutage struct {
	mu         sync.Mutex
	timer      sseTimer
	generation uint64
	warned     bool
}

func (o *sseOutage) begin(ctx context.Context, f *Fs) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.timer != nil || ctx.Err() != nil {
		return
	}
	o.generation++
	generation := o.generation
	o.timer = f.afterSSE(sseReadyDeadline, func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		if o.timer != nil && o.generation == generation && ctx.Err() == nil {
			o.warned = true
			fs.LogPrintfCtx(ctx, fs.LogLevelWarning, nil, "TelDrive SSE unavailable for 60s")
		}
	})
}

func (o *sseOutage) end(ctx context.Context, proven bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.timer != nil {
		o.timer.Stop()
		o.timer = nil
	}
	if proven && o.warned && ctx.Err() == nil {
		fs.LogPrintfCtx(ctx, fs.LogLevelNotice, nil, "TelDrive SSE recovered")
	}
	o.warned = false
}

type sseAttemptResult struct {
	outcome    sseOutcome
	status     int
	retryAfter time.Duration
}

func sseRetryDelay(failures int, attempt sseAttemptResult) time.Duration {
	if attempt.outcome == sseUnsupported {
		return sseRetryCap
	}
	base := min(time.Second<<min(failures, 5), sseRetryCap)
	spread := base / 5
	local := base - spread + time.Duration(rand.Int64N(int64(2*spread)+1))
	return min(max(local, attempt.retryAfter), sseRetryCap)
}

func sseRetryAfter(value string) time.Duration {
	if seconds, ok := decimal(value); ok {
		return time.Duration(min(seconds, int64(sseRetryCap/time.Second))) * time.Second
	}
	if date, err := http.ParseTime(value); err == nil {
		return min(max(time.Until(date), 0), sseRetryCap)
	}
	return 0
}

// runOrderedSSE returns true only for terminal authorization failure.
func (f *Fs) runOrderedSSE(ctx context.Context, notify func(orderedChange) error) bool {
	var cursor sseCursor
	var outage sseOutage
	failures := 0
	proof := func() {
		outage.end(ctx, true)
		failures = 0
	}
	defer outage.end(ctx, false)
	wait := waitSSE
	if f.sseClock != nil {
		wait = f.sseClock.Wait
	}
	for ctx.Err() == nil {
		outage.begin(ctx, f)
		hadCursor := cursor.token != ""
		attempt := f.readOrderedSSE(ctx, notify, &cursor, proof)
		if ctx.Err() != nil {
			return false
		}
		switch attempt.outcome {
		case sseAuth:
			fs.LogPrintfCtx(ctx, fs.LogLevelWarning, nil, "TelDrive SSE authorization denied (HTTP %d)", attempt.status)
			return true
		case sseReset:
			cursor = sseCursor{}
			if hadCursor {
				fs.LogPrintfCtx(ctx, fs.LogLevelDebug, nil, "TelDrive SSE retry: reset")
				continue
			}
		case sseRenew:
			continue
		}
		outage.begin(ctx, f)
		delay := sseRetryDelay(failures, attempt)
		failures = min(failures+1, 5)
		fs.LogPrintfCtx(ctx, fs.LogLevelDebug, nil, "TelDrive SSE retry: %s (HTTP %d), delay %s", attempt.outcome, attempt.status, delay)
		if !wait(ctx, delay) {
			return false
		}
	}
	return false
}

type sseCursor struct {
	token        string
	epoch, owner uuid.UUID
	position     int64
}

func strictUUID(value string) (uuid.UUID, bool) {
	if len(value) != 36 {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(value)
	return id, err == nil && id != uuid.Nil
}

func decimal(value string) (int64, bool) {
	if len(value) == 0 {
		return 0, false
	}
	for i := range len(value) {
		if value[i] < '0' || value[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(value, 10, 64)
	return n, err == nil
}

func parseSSECursor(token string) (sseCursor, bool) {
	epoch, rest, _ := strings.Cut(token, ".")
	owner, position, _ := strings.Cut(rest, ".")
	e, eOK := strictUUID(epoch)
	o, oOK := strictUUID(owner)
	n, nOK := decimal(position)
	return sseCursor{token, e, o, n}, eOK && oOK && nOK
}

func (c sseCursor) accepts(next sseCursor, equal bool) bool {
	return c.token == "" || c.epoch == next.epoch && c.owner == next.owner &&
		(next.position > c.position || equal && next.position == c.position)
}

func (f *Fs) readOrderedSSE(ctx context.Context, notify func(orderedChange) error, cursor *sseCursor, proof func()) sseAttemptResult {
	ctx, cancel := context.WithCancel(ctx)
	headerTimer := f.afterSSE(sseHeaderDeadline, cancel)
	resp, err := f.sseSrv.Call(ctx, &rest.Opts{
		Method:       http.MethodGet,
		Path:         "/api/v1/events",
		Parameters:   url.Values{"mode": {"ordered"}},
		ExtraHeaders: map[string]string{"Accept": "text/event-stream", "Cache-Control": "no-cache", "Last-Event-ID": cursor.token},
		IgnoreStatus: true,
		NoRedirect:   true,
	})
	defer func() {
		cancel()
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()
	result := sseAttemptResult{outcome: sseTransport}
	if !headerTimer.Stop() || ctx.Err() != nil || err != nil || resp == nil || resp.Body == nil {
		return result
	}
	result.status = resp.StatusCode
	result.retryAfter = sseRetryAfter(resp.Header.Get("Retry-After"))
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		result.outcome = sseAuth
		return result
	case http.StatusTooManyRequests:
		return result
	case http.StatusUnprocessableEntity:
		result.outcome = sseUnsupported
		timer := f.afterSSE(sseHeaderDeadline, cancel)
		const maxErrorBody = 4096
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody+1))
		if !timer.Stop() || ctx.Err() != nil || err != nil {
			result.outcome = sseTransport
			return result
		}
		var envelope api.ErrorEnvelope
		if cursor.token != "" && len(body) <= maxErrorBody &&
			json.Unmarshal(body, &envelope) == nil && envelope.Error.Code == "invalid_event_cursor" {
			result.outcome = sseReset
		}
		return result
	case http.StatusOK:
	default:
		if resp.StatusCode < 500 || resp.StatusCode > 599 {
			result.outcome = sseUnsupported
		}
		return result
	}
	media, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	heartbeat, ok := decimal(resp.Header.Get("X-TelDrive-Heartbeat-Ms"))
	if err != nil || media != "text/event-stream" || resp.Header.Get("X-TelDrive-Events") != "ordered" || !ok || heartbeat < 1 || heartbeat > 3600000 {
		result.outcome = sseUnsupported
		return result
	}
	idle := max(sseReadyDeadline, 3*time.Duration(heartbeat)*time.Millisecond)
	silence := f.afterSSE(idle, cancel)
	defer silence.Stop()
	startup := f.afterSSE(sseReadyDeadline, cancel)
	defer startup.Stop()
	body := &sseWatchdogReader{reader: resp.Body, timer: silence, idle: idle}
	result.outcome = f.consumeOrderedSSE(ctx, body, notify, cursor, startup, proof)
	return result
}

type sseWatchdogReader struct {
	reader io.Reader
	timer  sseTimer
	idle   time.Duration
}

func (w *sseWatchdogReader) Read(b []byte) (int, error) {
	n, err := w.reader.Read(b)
	if n > 0 {
		if !w.timer.Stop() {
			return 0, context.DeadlineExceeded
		}
		w.timer.Reset(w.idle)
	}
	return n, err
}

const sseFrameLimit = 64 * 1024

type sseFrames struct {
	reader *bufio.Reader
	data   []byte
}

// next only dispatches a blank-line terminated, bounded frame. A nil data slice
// is a heartbeat, not progress. The data is valid until the next call.
func (r *sseFrames) next() (id, name string, data []byte, outcome sseOutcome) {
	r.data = r.data[:0]
	var hasID, hasName, hasData bool
	size := 0
	for {
		line, err := r.reader.ReadSlice('\n')
		size += len(line)
		if size > sseFrameLimit || err == bufio.ErrBufferFull {
			return "", "", nil, sseProtocol
		}
		if err != nil {
			return "", "", nil, sseTransport
		}
		line = bytes.TrimSuffix(line[:len(line)-1], []byte{'\r'})
		if len(line) == 0 {
			if !hasID && !hasName && !hasData {
				return "", "", nil, ""
			}
			if !hasName || name == "" || !hasData || len(r.data) == 0 || hasID && id == "" {
				return "", "", nil, sseProtocol
			}
			return id, name, r.data, ""
		}
		if line[0] == ':' {
			continue
		}
		field, value, ok := bytes.Cut(line, []byte{':'})
		if !ok {
			return "", "", nil, sseProtocol
		}
		value = bytes.TrimPrefix(value, []byte{' '})
		switch string(field) {
		case "id":
			if hasID {
				return "", "", nil, sseProtocol
			}
			id, hasID = string(value), true
		case "event":
			if hasName {
				return "", "", nil, sseProtocol
			}
			name, hasName = string(value), true
		case "data":
			if hasData {
				r.data = append(r.data, '\n')
			}
			r.data = append(r.data, value...)
			hasData = true
		case "retry":
			// The server may legally advertise a reconnect delay. This client
			// keeps its own jitter schedule, so the hint stays inert; a bare
			// `retry:` is equally valid and must not become a protocol error.
		default:
			return "", "", nil, sseProtocol
		}
	}
}

func (f *Fs) consumeOrderedSSE(ctx context.Context, body io.Reader, notify func(orderedChange) error, cursor *sseCursor, startup sseTimer, proof func()) sseOutcome {
	frames := sseFrames{reader: bufio.NewReaderSize(body, sseFrameLimit)}
	resuming := cursor.token != ""
	ready := false
	apply := func(change orderedChange) sseOutcome {
		if ctx.Err() != nil {
			return sseTransport
		}
		if err := notify(change); err != nil {
			return sseCallback
		}
		if ctx.Err() != nil {
			return sseTransport
		}
		return ""
	}
	for ctx.Err() == nil {
		id, name, data, outcome := frames.next()
		if outcome != "" {
			return outcome
		}
		if ctx.Err() != nil {
			return sseTransport
		}
		if data == nil {
			continue
		}
		if !utf8.Valid(data) {
			return sseProtocol
		}
		switch name {
		case "sync.ready", "sync.checkpoint":
			var marker struct {
				Mode   string `json:"mode"`
				Cursor string `json:"cursor"`
			}
			if id != "" || json.Unmarshal(data, &marker) != nil {
				return sseProtocol
			}
			next, ok := parseSSECursor(marker.Cursor)
			if !ok || !cursor.accepts(next, name == "sync.ready") {
				return sseProtocol
			}
			if name == "sync.ready" {
				if ready || resuming && marker.Mode != "replay" || !resuming && marker.Mode != "baseline" {
					return sseProtocol
				}
				if !resuming {
					if outcome := apply(orderedChange{scope: f.dirCache.InvalidateNamespace(nil)}); outcome != "" {
						return outcome
					}
				}
				if !startup.Stop() || ctx.Err() != nil {
					return sseTransport
				}
				ready = true
				*cursor = next
				proof()
				continue
			} else if !ready || marker.Mode != "" {
				return sseProtocol
			}
			*cursor = next
		case "sync.required":
			var marker struct {
				Reason string `json:"reason"`
			}
			if id != "" || json.Unmarshal(data, &marker) != nil {
				return sseProtocol
			}
			switch marker.Reason {
			case "cursor_expired", "epoch_changed", "replay_limit":
				return sseReset
			default:
				return sseProtocol
			}
		case "sync.renew":
			var empty map[string]json.RawMessage
			if id != "" || !ready || json.Unmarshal(data, &empty) != nil || empty == nil || len(empty) != 0 {
				return sseProtocol
			}
			return sseRenew
		case "stream.error":
			var marker struct {
				Code string `json:"code"`
			}
			if id != "" || json.Unmarshal(data, &marker) != nil || marker.Code == "" {
				return sseProtocol
			}
			return sseTransport
		case "file.created", "file.updated", "file.trashed", "file.deletion_pending", "file.restored", "file.purged":
			next, ok := parseSSECursor(id)
			if !ok || next.position <= 0 || !cursor.accepts(next, false) || !ready && !resuming {
				return sseProtocol
			}
			var event api.EventEnvelope
			if json.Unmarshal(data, &event) != nil || event.Version != 1 || event.ResourceType != "file" || event.OccurredAt.IsZero() {
				return sseProtocol
			}
			if _, ok := strictUUID(event.ResourceID); !ok {
				return sseProtocol
			}
			if outcome := apply(f.orderedEventChange(name, event)); outcome != "" {
				return outcome
			}
			*cursor = next
		default:
			return sseProtocol
		}
	}
	return sseTransport
}

type orderedLocation struct {
	parent, name string
	valid        bool
}

func (f *Fs) orderedLocation(raw json.RawMessage) orderedLocation {
	var loc struct {
		Parent json.RawMessage `json:"parentId"`
		Name   string          `json:"name"`
	}
	if json.Unmarshal(raw, &loc) != nil || loc.Name == "" {
		return orderedLocation{}
	}
	parent := rootID
	if !bytes.Equal(loc.Parent, []byte("null")) {
		if json.Unmarshal(loc.Parent, &parent) != nil {
			return orderedLocation{}
		}
		if _, ok := strictUUID(parent); !ok {
			return orderedLocation{}
		}
		parent = strings.ToLower(parent)
	}
	name := f.opt.Enc.ToStandardName(loc.Name)
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return orderedLocation{}
	}
	return orderedLocation{parent, name, true}
}

func (f *Fs) orderedEventChange(eventName string, event api.EventEnvelope) orderedChange {
	change := orderedChange{kind: fs.EntryObject}
	if event.Payload.Kind == "folder" {
		change.kind = fs.EntryDirectory
	}
	resource := strings.ToLower(event.ResourceID)
	sides := [2]json.RawMessage{event.Payload.Before, event.Payload.After}
	locations := [2]orderedLocation{f.orderedLocation(sides[0]), f.orderedLocation(sides[1])}
	change.scope = f.dirCache.InvalidateNamespace(func(view dircache.NamespaceView) fs.ChangeScope {
		mountRoot := view.RootID()
		if mountRoot == "" || view.InRootLineage(resource) {
			return fs.ChangeScope{All: true}
		}
		scope := fs.ChangeScope{All: event.Payload.Kind != "file" && event.Payload.Kind != "folder"}
		for i, loc := range locations {
			if sides[i] == nil && (i == 0 && eventName == "file.created" || i == 1 && eventName == "file.purged") {
				continue
			}
			if !loc.valid {
				scope.All = true
				continue
			}
			// The account root itself cannot be a descendant of a mounted folder.
			if loc.parent == rootID && mountRoot != rootID {
				continue
			}
			parent, ok := view.Path(loc.parent)
			if !ok {
				scope.All = true
				continue
			}
			if !slices.Contains(scope.Parents, parent) {
				scope.Parents = append(scope.Parents, parent)
			}
			remote := path.Join(parent, loc.name)
			if id, ok := view.ID(remote); ok && id != resource {
				scope.All = true
			}
			if !slices.Contains(change.paths, remote) {
				change.paths = append(change.paths, remote)
			}
		}
		if cached, ok := view.Path(resource); ok && !slices.Contains(change.paths, cached) {
			scope.All = true
		}
		if change.kind == fs.EntryDirectory {
			scope.Subtrees = change.paths
		}
		return scope
	})
	return change
}
