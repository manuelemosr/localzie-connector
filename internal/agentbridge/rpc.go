// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package agentbridge

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

type rpcMessage struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *rpcError       `json:"error,omitempty"`
}
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return e.Message }

type rpc struct {
	command *exec.Cmd
	input   io.WriteCloser
	output  io.ReadCloser
	writeMu sync.Mutex
	mu      sync.Mutex
	next    int
	pending map[int]chan rpcMessage
	done    chan struct{}
}

func startRPC(ctx context.Context, directory string, event func(rpcMessage)) (*rpc, error) {
	command := exec.CommandContext(ctx, "codex", "app-server", "--stdio", "-c", "analytics.enabled=false")
	command.Dir = directory
	command.Stderr = io.Discard
	input, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	output, err := command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	r := &rpc{command: command, input: input, output: output, pending: map[int]chan rpcMessage{}, done: make(chan struct{})}
	if err = command.Start(); err != nil {
		return nil, err
	}
	go func() {
		defer close(r.done)
		scanner := bufio.NewScanner(output)
		scanner.Buffer(make([]byte, 64<<10), 32<<20)
		for scanner.Scan() {
			var m rpcMessage
			if json.Unmarshal(scanner.Bytes(), &m) != nil {
				continue
			}
			if m.Method != "" {
				event(m)
				continue
			}
			var id int
			if json.Unmarshal(m.ID, &id) != nil {
				continue
			}
			r.mu.Lock()
			ch := r.pending[id]
			delete(r.pending, id)
			r.mu.Unlock()
			if ch != nil {
				ch <- m
			}
		}
		// An oversized/malformed producer must not remain alive with a blocked pipe.
		if scanner.Err() != nil {
			_ = command.Process.Kill()
		}
		_ = command.Wait()
	}()
	return r, nil
}
func (r *rpc) write(value any) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	select {
	case <-r.done:
		return errors.New("Codex app-server stopped")
	default:
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if pipe, ok := r.input.(*os.File); ok {
		_ = pipe.SetWriteDeadline(time.Now().Add(15 * time.Second))
	}
	_, err = r.input.Write(append(data, '\n'))
	return err
}
func (r *rpc) call(ctx context.Context, method string, params any, result any) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	r.mu.Lock()
	r.next++
	id := r.next
	ch := make(chan rpcMessage, 1)
	r.pending[id] = ch
	r.mu.Unlock()
	defer func() { r.mu.Lock(); delete(r.pending, id); r.mu.Unlock() }()
	if err := r.write(map[string]any{"id": id, "method": method, "params": params}); err != nil {
		return err
	}
	select {
	case m := <-ch:
		if m.Error != nil {
			return m.Error
		}
		if result != nil {
			return json.Unmarshal(m.Result, result)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return errors.New("Codex app-server stopped; restart the connector")
	}
}
func (r *rpc) respond(id json.RawMessage, result any) error {
	return r.write(map[string]any{"id": id, "result": result})
}
func (r *rpc) close() {
	_ = r.input.Close()
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		_ = r.command.Process.Kill()
		_ = r.output.Close()
		<-r.done
	}
}
