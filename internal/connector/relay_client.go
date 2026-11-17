// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package connector

// The relay client only forwards opaque TLS records to this process's injected
// loopback listener. It never has a general destination address.
import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/manuelemosr/localzie-connector/internal/tunnel"
)

const (
	relayInitialDelay     = 500 * time.Millisecond
	relayMaximumDelay     = 15 * time.Second
	relayStableSessionFor = 30 * time.Second
	relayWriteTimeout     = 10 * time.Second
)

type RelayClientConfig struct {
	RelayBaseURL, ConnectorID, Credential, InternalAddress string
	Logger                                                 *slog.Logger
}

type RelayClient struct{ config RelayClientConfig }

// relayBackoff resets after a connection has stayed up for 30 seconds. This
// avoids making a healthy connector wait at the capped delay after a later,
// isolated disconnect, while short handshake/session failures still back off.
type relayBackoff struct{ next time.Duration }

func newRelayBackoff() relayBackoff { return relayBackoff{next: relayInitialDelay} }
func (b *relayBackoff) resetAfter(uptime time.Duration) bool {
	if uptime < relayStableSessionFor {
		return false
	}
	b.next = relayInitialDelay
	return true
}
func (b *relayBackoff) retryDelay(randomByte byte) time.Duration {
	base := b.next
	if b.next < relayMaximumDelay {
		b.next *= 2
		if b.next > relayMaximumDelay {
			b.next = relayMaximumDelay
		}
	}
	// 50%..150%, inclusive. The supplied byte makes the policy deterministic
	// under test; production supplies cryptographic randomness.
	return base/2 + time.Duration(int64(base)*int64(randomByte)/255)
}

func NewRelayClient(c RelayClientConfig) (*RelayClient, error) {
	if c.ConnectorID == "" || c.Credential == "" || c.InternalAddress == "" {
		return nil, errors.New("incomplete relay configuration")
	}
	u, err := url.Parse(c.RelayBaseURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "wss") {
		return nil, errors.New("relay URL must use HTTPS or WSS")
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &RelayClient{config: c}, nil
}

func (c *RelayClient) Run(ctx context.Context) error {
	backoff, attempt := newRelayBackoff(), 0
	for {
		uptime, connected, err := c.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if connected {
			c.config.Logger.Info("relay disconnected", "session_uptime_ms", uptime.Milliseconds())
			if backoff.resetAfter(uptime) {
				attempt = 0
			}
		}
		attempt++
		wait := backoff.retryDelay(c.randomByte())
		// Do not log err: dial errors may contain a URL and its query string.
		_ = err
		c.config.Logger.Info("relay retry scheduled", "attempt", attempt, "delay_ms", wait.Milliseconds())
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
func (c *RelayClient) randomByte() byte {
	var b [1]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 128
	}
	return b[0]
}

func (c *RelayClient) runOnce(ctx context.Context) (time.Duration, bool, error) {
	u, err := url.Parse(c.config.RelayBaseURL)
	if err != nil {
		return 0, false, err
	}
	if u.Scheme == "https" {
		u.Scheme = "wss"
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/v1/tunnel"
	q := u.Query()
	q.Set("role", "connector")
	q.Set("connector_id", c.config.ConnectorID)
	q.Set("peer_id", "connector")
	u.RawQuery = q.Encode()
	ws, _, err := websocket.Dial(ctx, u.String(), &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer " + c.config.Credential}}})
	if err != nil {
		return 0, false, err
	}
	connectedAt := time.Now()
	c.config.Logger.Info("relay connected")
	session := newRelaySession(ctx, ws, c.config.InternalAddress)
	err = session.run()
	return time.Since(connectedAt), true, err
}

type relayFrameWriter interface {
	Write(context.Context, websocket.MessageType, []byte) error
}

// relaySession owns every resource for exactly one WebSocket epoch. No stream,
// goroutine, or write mutex survives into a replacement session.
type relaySession struct {
	ctx                  context.Context
	cancel               context.CancelFunc
	ws                   *websocket.Conn
	writer               relayFrameWriter
	internalAddress      string
	writeTimeout         time.Duration
	internalWriteTimeout time.Duration
	mu                   sync.Mutex
	streams              map[uint32]net.Conn
	writeMu              sync.Mutex
	wg                   sync.WaitGroup
}

func newRelaySession(parent context.Context, ws *websocket.Conn, internalAddress string) *relaySession {
	ctx, cancel := context.WithCancel(parent)
	return &relaySession{ctx: ctx, cancel: cancel, ws: ws, writer: ws, internalAddress: internalAddress, writeTimeout: relayWriteTimeout, internalWriteTimeout: relayWriteTimeout, streams: map[uint32]net.Conn{}}
}
func (s *relaySession) run() error {
	defer s.close()
	s.ws.SetReadLimit(tunnel.MaxMessageSize)
	for {
		kind, b, err := s.ws.Read(s.ctx)
		if err != nil {
			return err
		}
		if kind != websocket.MessageBinary {
			return errors.New("relay sent text")
		}
		f, err := tunnel.Decode(b)
		if err != nil {
			return err
		}
		switch f.Type {
		case tunnel.Open:
			if f.StreamID%2 == 0 {
				return errors.New("client stream ID must be odd")
			}
			if err = s.open(f.StreamID); err != nil {
				_ = s.send(tunnel.Frame{Type: tunnel.Reset, StreamID: f.StreamID, Payload: []byte{3}})
			}
		case tunnel.Data:
			conn := s.stream(f.StreamID)
			if conn == nil {
				return errors.New("data for unknown stream")
			}
			if err = s.writeInternal(f.StreamID, conn, f.Payload); err != nil {
				// A stalled local TLS/HTTP peer must not hold the relay reader. The
				// affected stream is closed before RESET; the WebSocket epoch stays
				// usable for the remaining streams.
				if err = s.send(tunnel.Frame{Type: tunnel.Reset, StreamID: f.StreamID, Payload: []byte{3}}); err != nil {
					return err
				}
			}
		case tunnel.Close, tunnel.Reset:
			s.closeStream(f.StreamID)
		case tunnel.CloseWrite:
			conn := s.stream(f.StreamID)
			if conn == nil {
				return errors.New("close-write for unknown stream")
			}
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.CloseWrite()
			}
		case tunnel.Ping:
			if err = s.send(tunnel.Frame{Type: tunnel.Pong, StreamID: f.StreamID, Payload: f.Payload}); err != nil {
				return err
			}
		case tunnel.Pong:
		default:
			return errors.New("invalid connector frame direction")
		}
	}
}
func (s *relaySession) open(id uint32) error {
	s.mu.Lock()
	if len(s.streams) >= 8 || s.streams[id] != nil {
		s.mu.Unlock()
		return errors.New("stream capacity")
	}
	s.mu.Unlock()
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(s.ctx, "tcp", s.internalAddress)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		_ = conn.Close()
		return s.ctx.Err()
	}
	s.streams[id] = conn
	// Account for the forwarder before releasing the stream map. close can then
	// wait for it even if the WebSocket disconnects between Opened and go.
	s.wg.Add(1)
	s.mu.Unlock()
	if err = s.send(tunnel.Frame{Type: tunnel.Opened, StreamID: id}); err != nil {
		s.closeStream(id)
		s.wg.Done()
		return err
	}
	go s.forward(id, conn)
	return nil
}
func (s *relaySession) forward(id uint32, conn net.Conn) {
	defer s.wg.Done()
	buf := make([]byte, 32*1024)
	for {
		n, err := conn.Read(buf)
		if n > 0 && s.send(tunnel.Frame{Type: tunnel.Data, StreamID: id, Payload: append([]byte(nil), buf[:n]...)}) != nil {
			return
		}
		if err == nil {
			continue
		}
		if !s.isCurrentStream(id, conn) {
			return
		}
		if errors.Is(err, io.EOF) {
			_ = s.send(tunnel.Frame{Type: tunnel.CloseWrite, StreamID: id})
		} else {
			_ = s.send(tunnel.Frame{Type: tunnel.Close, StreamID: id})
			s.closeStream(id)
		}
		return
	}
}
func (s *relaySession) send(f tunnel.Frame) error {
	b, err := f.Encode()
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(s.ctx, s.writeTimeout)
	defer cancel()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := writeCtx.Err(); err != nil {
		return err
	}
	return s.writer.Write(writeCtx, websocket.MessageBinary, b)
}
func (s *relaySession) writeInternal(id uint32, conn net.Conn, payload []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(s.internalWriteTimeout)); err != nil {
		s.closeStreamIfCurrent(id, conn)
		return err
	}
	defer conn.SetWriteDeadline(time.Time{})
	if _, err := conn.Write(payload); err != nil {
		s.closeStreamIfCurrent(id, conn)
		return err
	}
	return nil
}
func (s *relaySession) stream(id uint32) net.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams[id]
}
func (s *relaySession) isCurrentStream(id uint32, conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams[id] == conn
}
func (s *relaySession) closeStream(id uint32) {
	s.mu.Lock()
	conn := s.streams[id]
	delete(s.streams, id)
	s.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}
func (s *relaySession) closeStreamIfCurrent(id uint32, want net.Conn) {
	s.mu.Lock()
	conn := s.streams[id]
	if conn == want {
		delete(s.streams, id)
	}
	s.mu.Unlock()
	if conn == want {
		_ = conn.Close()
	}
}
func (s *relaySession) close() {
	s.cancel()
	if s.ws != nil {
		s.ws.CloseNow()
	}
	s.mu.Lock()
	streams := s.streams
	s.streams = map[uint32]net.Conn{}
	s.mu.Unlock()
	for _, conn := range streams {
		_ = conn.Close()
	}
	s.wg.Wait()
}
