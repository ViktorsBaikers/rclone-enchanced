package chunkedreader

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/rclone/rclone/lib/multipart"
	"github.com/stretchr/testify/require"
)

type failingRangeObject struct {
	fs.Object
	payload    []byte
	openErr    error
	readErr    error
	delayedEnd bool
	stall      bool
}

func (o *failingRangeObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	if o.openErr != nil {
		return nil, o.openErr
	}
	start, count := int64(0), o.Size()
	for _, option := range options {
		if r, ok := option.(*fs.RangeOption); ok {
			start, count = r.Decode(o.Size())
		}
	}
	end := min(start+count, int64(len(o.payload)))
	start = min(start, end)
	return &rangeFailureBody{Reader: bytes.NewReader(o.payload[start:end]), ctx: ctx, err: o.readErr, delayed: o.delayedEnd, stall: o.stall}, nil
}

type rangeFailureBody struct {
	*bytes.Reader
	ctx            context.Context
	err            error
	delayed, stall bool
}

func (b *rangeFailureBody) Read(p []byte) (int, error) {
	if b.stall {
		<-b.ctx.Done()
		return 0, b.ctx.Err()
	}
	n, e := b.Reader.Read(p)
	if e == io.EOF {
		if b.delayed {
			<-b.ctx.Done()
			return n, b.ctx.Err()
		}
		if b.err != nil {
			return n, b.err
		}
	}
	return n, e
}
func (b *rangeFailureBody) Close() error { return nil }

func TestParallelFailureTerminates(t *testing.T) {
	boom := errors.New("source failed")
	for _, tc := range []struct {
		name                        string
		openErr, readErr, errorWant error
		payload                     int
		seek                        bool
	}{
		{name: "empty range", errorWant: io.ErrUnexpectedEOF},
		{name: "short range", payload: 123, errorWant: io.ErrUnexpectedEOF},
		{name: "read error", payload: 123, readErr: boom, errorWant: boom},
		{name: "open error", openErr: boom, errorWant: boom},
		{name: "seek beyond short range", payload: 123, seek: true, errorWant: io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			o := &failingRangeObject{Object: mockobject.New("broken").WithContent(make([]byte, multipart.BufferSize*2), mockobject.SeekModeNone), payload: make([]byte, tc.payload), openErr: tc.openErr, readErr: tc.readErr}
			cr := New(ctx, o, multipart.BufferSize, 0, 4)
			done := make(chan error, 1)
			go func() {
				if tc.seek {
					_, e := cr.Open()
					if e == nil {
						_, e = cr.Seek(1024, io.SeekStart)
					}
					done <- e
				} else {
					_, e := io.ReadAll(cr)
					done <- e
				}
			}()
			select {
			case e := <-done:
				require.ErrorIs(t, e, tc.errorWant)
			case <-time.After(3 * time.Second):
				cancel()
				t.Fatal("reader failed to terminate")
			}
			_ = cr.Close()
		})
	}
}

func TestParallelCompletedRangeDelayedEOF(t *testing.T) {
	content := makeContent(t, 3*multipart.BufferSize)
	o := &failingRangeObject{Object: mockobject.New("healthy").WithContent(content, mockobject.SeekModeNone), payload: content, delayedEnd: true}
	cr := New(context.Background(), o, multipart.BufferSize, 0, 4)
	defer func() { _ = cr.Close() }()
	got, err := io.ReadAll(cr)
	require.NoError(t, err)
	require.Equal(t, content, got)
}

func TestParallelCloseUnblocksRead(t *testing.T) {
	for _, seek := range []bool{false, true} {
		name := "read"
		if seek {
			name = "seek"
		}
		t.Run(name, func(t *testing.T) {
			o := &failingRangeObject{Object: mockobject.New("stalled").WithContent(make([]byte, 2*multipart.BufferSize), mockobject.SeekModeNone), stall: true}
			cr := New(context.Background(), o, multipart.BufferSize, 0, 4)
			_, err := cr.Open()
			require.NoError(t, err)
			readDone := make(chan struct{})
			go func() {
				defer close(readDone)
				if seek {
					_, _ = cr.Seek(1024, io.SeekStart)
				} else {
					_, _ = cr.Read(make([]byte, 1))
				}
			}()
			// Verify the pending operation owns the lock before racing Close against it.
			deadline := time.Now().Add(time.Second)
			for cr.(*parallel).mu.TryLock() {
				cr.(*parallel).mu.Unlock()
				if time.Now().After(deadline) {
					t.Fatal("operation did not acquire reader lock")
				}
				time.Sleep(time.Millisecond)
			}
			closeDone := make(chan struct{})
			go func() { defer close(closeDone); _ = cr.Close() }()
			for _, ch := range []chan struct{}{readDone, closeDone} {
				select {
				case <-ch:
				case <-time.After(3 * time.Second):
					t.Fatal("Close did not release pending operation")
				}
			}
		})
	}
}
