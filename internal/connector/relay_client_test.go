// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/manuelemosr/localzie-connector/internal/tunnel"
)

func TestRelayBackoffResetsAfterStableSession(t *testing.T) {
	b := newRelayBackoff()
	_ = b.retryDelay(0)
	_ = b.retryDelay(0)
	if b.next != 2*time.Second {
		t.Fatalf("backoff before stable session = %v", b.next)
	}
	if !b.resetAfter(relayStableSessionFor) {
		t.Fatal("stable session did not reset backoff")
	}
	if got := b.retryDelay(0); got != relayInitialDelay/2 {
		t.Fatalf("first retry after reset = %v, want %v", got, relayInitialDelay/2)
	}
}

func TestRelayBackoffGrowsForEarlyFailuresAndCaps(t *testing.T) {
	b := newRelayBackoff()
	if b.resetAfter(relayStableSessionFor - time.Nanosecond) {
		t.Fatal("early session unexpectedly reset backoff")
	}
	for wantBase := relayInitialDelay; wantBase < relayMaximumDelay; wantBase *= 2 {
		if got := b.retryDelay(128); got < wantBase/2 || got > wantBase+wantBase/2 {
			t.Fatalf("jittered retry %v outside bounds for %v", got, wantBase)
		}
	}
	for range 4 {
		_ = b.retryDelay(255)
	}
	if b.next != relayMaximumDelay {
		t.Fatalf("backoff cap = %v, want %v", b.next, relayMaximumDelay)
	}
	if got := b.retryDelay(0); got != relayMaximumDelay/2 {
		t.Fatalf("capped lower jitter bound = %v", got)
	}
	if got := b.retryDelay(255); got != relayMaximumDelay+relayMaximumDelay/2 {
		t.Fatalf("capped upper jitter bound = %v", got)
	}
}

type blockingWriter struct {
	started chan struct{}
	once    sync.Once
}

func (w *blockingWriter) Write(ctx context.Context, _ websocket.MessageType, _ []byte) error {
	w.once.Do(func() { close(w.started) })
	<-ctx.Done()
	return ctx.Err()
}

type recordingWriter struct {
	called chan struct{}
}

type countWriter struct{ calls int }

func (w *countWriter) Write(context.Context, websocket.MessageType, []byte) error {
	w.calls++
	return nil
}

func (w *recordingWriter) Write(context.Context, websocket.MessageType, []byte) error {
	close(w.called)
	return nil
}

func testSession(parent context.Context, writer relayFrameWriter) *relaySession {
	s := newRelaySession(parent, nil, "")
	s.writer = writer
	s.writeTimeout = time.Hour // tests exercise cancellation rather than wall-clock timeout.
	return s
}

func TestRelaySessionWriteIsCancellable(t *testing.T) {
	writer := &blockingWriter{started: make(chan struct{})}
	s := testSession(context.Background(), writer)
	result := make(chan error, 1)
	go func() { result <- s.send(tunnel.Frame{Type: tunnel.Ping, StreamID: 1}) }()
	<-writer.started
	s.cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("send error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled write did not return")
	}
}

func TestRelaySessionWriteDeadlinePreventsWrite(t *testing.T) {
	writer := &countWriter{}
	s := testSession(context.Background(), writer)
	s.writeTimeout = 0
	err := s.send(tunnel.Frame{Type: tunnel.Ping, StreamID: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("send error = %v, want deadline exceeded", err)
	}
	if writer.calls != 0 {
		t.Fatalf("writer was called %d times after the deadline", writer.calls)
	}
}

func TestRelaySessionStaleEpochCannotBlockReplacement(t *testing.T) {
	oldWriter := &blockingWriter{started: make(chan struct{})}
	old := testSession(context.Background(), oldWriter)
	oldResult := make(chan error, 1)
	go func() { oldResult <- old.send(tunnel.Frame{Type: tunnel.Ping, StreamID: 1}) }()
	<-oldWriter.started

	newWriter := &recordingWriter{called: make(chan struct{})}
	newSession := testSession(context.Background(), newWriter)
	newResult := make(chan error, 1)
	go func() { newResult <- newSession.send(tunnel.Frame{Type: tunnel.Ping, StreamID: 1}) }()
	select {
	case err := <-newResult:
		if err != nil {
			t.Fatalf("replacement write failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement session was blocked by stale epoch")
	}
	<-newWriter.called

	old.cancel()
	if err := <-oldResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("stale write error = %v, want cancellation", err)
	}
}

func TestRelaySessionCloseTearsDownStreamsAndForwarders(t *testing.T) {
	conn, peer := net.Pipe()
	defer peer.Close()
	s := testSession(context.Background(), &recordingWriter{called: make(chan struct{})})
	s.mu.Lock()
	s.streams[1] = conn
	s.mu.Unlock()
	s.wg.Add(1)
	go s.forward(1, conn)

	done := make(chan struct{})
	go func() { s.close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("session close did not stop its forwarding goroutine")
	}
	if conn := s.stream(1); conn != nil {
		t.Fatal("session close retained a stream")
	}
}

func TestRelaySessionInternalWriteDeadlineClosesOnlyStalledStream(t *testing.T) {
	conn, peer := net.Pipe()
	defer peer.Close()
	s := testSession(context.Background(), &recordingWriter{called: make(chan struct{})})
	s.internalWriteTimeout = 10 * time.Millisecond
	s.mu.Lock()
	s.streams[1] = conn
	s.mu.Unlock()

	result := make(chan error, 1)
	go func() { result <- s.writeInternal(1, conn, []byte("blocked")) }()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("blocked internal write unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked internal write did not honor its deadline")
	}
	if conn := s.stream(1); conn != nil {
		t.Fatal("stalled stream remained registered")
	}

	done := make(chan struct{})
	go func() { s.close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stalled internal write froze session teardown")
	}
}
