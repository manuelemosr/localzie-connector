// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/protocol"
	"github.com/manuelemosr/localzie-connector/internal/runtime"
	"github.com/manuelemosr/localzie-connector/internal/store"
)

type controlledRuntime struct {
	release chan struct{}
	once    sync.Once
}

func (f *controlledRuntime) Name() string { return "controlled" }
func (f *controlledRuntime) Models(context.Context) ([]protocol.Model, error) {
	return []protocol.Model{{ID: "test-model", Name: "Test model", Runtime: f.Name(), Ready: true, Locality: "local"}}, nil
}
func (f *controlledRuntime) Stream(ctx context.Context, _ runtime.StreamRequest, onDelta func(string) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.release:
	}
	if err := onDelta("survived disconnect"); err != nil {
		return err
	}
	return nil
}
func (f *controlledRuntime) finish() { f.once.Do(func() { close(f.release) }) }

func TestAcceptedJobOutlivesRequestAndDuplicateDoesNotRestartIt(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	adapter := &controlledRuntime{release: make(chan struct{})}
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := NewServer(root, "test", db, adapter, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	pairingToken, _, _ := db.CreatePairing(context.Background(), time.Minute)
	pairResponse := requestJSON[protocol.PairResponse](t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/pair", "", map[string]string{"pairing_token": pairingToken, "device_name": "Test iPhone"})
	conversation := requestJSON[protocol.Conversation](t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/conversations", pairResponse.DeviceToken, map[string]string{"model_id": "test-model"})
	messageRequest := map[string]string{"idempotency_key": "stable-send", "content": "Keep working"}
	accepted := requestJSON[protocol.SendMessageResponse](t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/conversations/"+conversation.ID+"/messages", pairResponse.DeviceToken, messageRequest)
	if accepted.Duplicate {
		t.Fatal("first request was marked duplicate")
	}
	duplicate := requestJSON[protocol.SendMessageResponse](t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/conversations/"+conversation.ID+"/messages", pairResponse.DeviceToken, messageRequest)
	if !duplicate.Duplicate || duplicate.Job.ID != accepted.Job.ID {
		t.Fatalf("duplicate response = %+v", duplicate)
	}

	adapter.finish()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, _ := db.Job(context.Background(), accepted.Job.ID)
		if job.Status == protocol.JobCompleted {
			detail, _ := db.Conversation(context.Background(), conversation.ID)
			if got := detail.Messages[len(detail.Messages)-1].Content; got != "survived disconnect" {
				t.Fatalf("assistant content = %q", got)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job did not complete after HTTP request ended")
}

func TestSendMessageRejectsInvalidAllowReasoningType(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := NewServer(root, "test", db, &controlledRuntime{release: make(chan struct{})}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	pairingToken, _, _ := db.CreatePairing(context.Background(), time.Minute)
	pair := requestJSON[protocol.PairResponse](t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/pair", "", map[string]string{"pairing_token": pairingToken, "device_name": "Test iPhone"})
	conversation := requestJSON[protocol.Conversation](t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/conversations", pair.DeviceToken, map[string]string{"model_id": "test-model"})
	req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/conversations/"+conversation.ID+"/messages", bytes.NewBufferString(`{"idempotency_key":"reasoning-type","content":"Hello","allow_reasoning":"false"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+pair.DeviceToken)
	req.Header.Set("Content-Type", "application/json")
	response, err := httpServer.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", response.StatusCode, http.StatusBadRequest)
	}
}

func TestRemoteHandlerForbidsAutomaticLocalPairing(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := NewServer(root, "test", db, &controlledRuntime{release: make(chan struct{})}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/pair/local", bytes.NewBufferString(`{"device_name":"phone"}`))
	r.RemoteAddr = "127.0.0.1:1234"
	w := httptest.NewRecorder()
	s.RemoteHandler().ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("got %d", w.Code)
	}
}

func TestAutomaticPairingRequiresPrivateNetwork(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	adapter := &controlledRuntime{release: make(chan struct{})}
	server, err := NewServer(context.Background(), "test", db, adapter, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	local := requestJSON[protocol.PairResponse](t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/pair/local", "", map[string]string{"device_name": "Nearby iPhone"})
	if _, ok := db.AuthenticateDevice(context.Background(), local.DeviceToken); !ok {
		t.Fatal("automatically paired local device did not authenticate")
	}

	body := bytes.NewBufferString(`{"device_name":"Remote iPhone"}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/pair/local", body)
	request.RemoteAddr = "203.0.113.10:43210"
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("public-network local pairing status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
}

func TestRevokedDeviceSSEStopsBeforeFutureMessageEvents(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := NewServer(root, "test", db, &controlledRuntime{release: make(chan struct{})}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(s.Handler())
	defer httpServer.Close()
	pair := func(name string) protocol.PairResponse {
		pairing, _, e := db.CreatePairing(context.Background(), time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		return requestJSON[protocol.PairResponse](t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/pair", "", map[string]string{"pairing_token": pairing, "device_name": name})
	}
	watcher, actor := pair("watcher"), pair("actor")
	conversation := requestJSON[protocol.Conversation](t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/conversations", actor.DeviceToken, map[string]string{"model_id": "test-model"})
	request, err := http.NewRequest(http.MethodGet, httpServer.URL+"/v1/events?after=0", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+watcher.DeviceToken)
	response, err := httpServer.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	// Consume the already-persisted conversation event, proving this is a real
	// established HTTP/SSE stream rather than an authorization-only test.
	for {
		line, e := reader.ReadString('\n')
		if e != nil {
			t.Fatalf("initial SSE read: %v", e)
		}
		if line == "\n" {
			break
		}
	}
	revoke, _ := http.NewRequest(http.MethodDelete, httpServer.URL+"/v1/devices/"+watcher.DeviceID, nil)
	revoke.Header.Set("Authorization", "Bearer "+actor.DeviceToken)
	result, err := httpServer.Client().Do(revoke)
	if err != nil || result.StatusCode != http.StatusNoContent {
		t.Fatalf("revoke: %v %v", err, result)
	}
	result.Body.Close()
	_ = requestJSON[protocol.SendMessageResponse](t, httpServer.Client(), http.MethodPost, httpServer.URL+"/v1/conversations/"+conversation.ID+"/messages", actor.DeviceToken, map[string]string{"idempotency_key": "after-revoke", "content": "must not reach watcher"})
	finished := make(chan []byte, 1)
	go func() { b, _ := io.ReadAll(reader); finished <- b }()
	select {
	case b := <-finished:
		if len(b) != 0 {
			t.Fatalf("revoked stream received future bytes: %q", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("revoked SSE stream did not terminate promptly")
	}
}

func TestAuthRequiresStrictBearerScheme(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id, token, err := db.CreateDevice(context.Background(), "phone")
	if err != nil || id == "" {
		t.Fatal(err)
	}
	s, err := NewServer(context.Background(), "test", db, &controlledRuntime{release: make(chan struct{})}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	r.Header.Set("Authorization", "Basic "+token)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("non-Bearer authorization status = %d", w.Code)
	}
}

func TestEventsImmediatelyFlushesConnectedComment(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, token, err := db.CreateDevice(context.Background(), "phone")
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewServer(context.Background(), "test", db, &controlledRuntime{release: make(chan struct{})}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(s.Handler())
	defer httpServer.Close()

	requestContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, httpServer.URL+"/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := httpServer.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if got := response.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("content type = %q", got)
	}
	reader := bufio.NewReader(response.Body)
	for _, want := range []string{": connected\n", "\n"} {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("immediate SSE acknowledgement: %v", err)
		}
		if line != want {
			t.Fatalf("SSE line = %q, want %q", line, want)
		}
	}
}

func requestJSON[T any](t *testing.T, client *http.Client, method, url, token string, body any) T {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(method, url, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("%s %s: HTTP %d: %s", method, url, resp.StatusCode, data)
	}
	var result T
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}
