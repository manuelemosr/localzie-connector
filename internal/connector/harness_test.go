// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/harness"
	"github.com/manuelemosr/localzie-connector/internal/protocol"
	"github.com/manuelemosr/localzie-connector/internal/runtime"
	"github.com/manuelemosr/localzie-connector/internal/store"
)

// fakeOpenCode is a scripted OpenCode server: health, configured providers,
// session creation, prompt submission, abort, and one controllable SSE event
// stream shaped like the real /event endpoint. Turn scripts queued before a
// prompt are emitted only when that prompt arrives, mirroring the real
// server's post-submission event order.
type fakeOpenCode struct {
	mu                sync.Mutex
	prompts           []string
	modelIDs          []string
	abortCalls        int
	failAbort         bool
	failPrompt        bool
	sessions          []string
	deleteCalls       []string
	failSessionDelete bool
	turnScript        chan []string
	// events carries pre-encoded SSE data payloads; done ends the stream and
	// unblocks pending emitters at cleanup without a send-on-closed panic.
	events     chan string
	done       chan struct{}
	subscribed chan struct{}
	once       sync.Once

	observed         map[string]*fakeObservedSession
	observedStatuses map[string]string
}

// fakeObservedSession is one harness session another client is driving: the
// global listing entry plus its authoritative transcript JSON.
type fakeObservedSession struct {
	id         string
	title      string
	directory  string
	provider   string
	model      string
	updated    int64
	transcript string
}

func newFakeOpenCode() *fakeOpenCode {
	return &fakeOpenCode{events: make(chan string, 32), done: make(chan struct{}), subscribed: make(chan struct{}), turnScript: make(chan []string, 4), observed: map[string]*fakeObservedSession{}, observedStatuses: map[string]string{}}
}

// seedObserved registers a root harness session another client drives.
func (f *fakeOpenCode) seedObserved(id, title, directory, provider, model string, updated int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observed[id] = &fakeObservedSession{id: id, title: title, directory: directory, provider: provider, model: model, updated: updated}
}

func (f *fakeOpenCode) setObservedStatus(id, status string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observedStatuses[id] = status
}

func (f *fakeOpenCode) setObservedTranscript(id, transcript string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if session := f.observed[id]; session != nil {
		session.transcript = transcript
	}
}

func (f *fakeOpenCode) observedSessionsJSON() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	entries := make([]string, 0, len(f.observed))
	for _, session := range f.observed {
		model := "null"
		if session.model != "" {
			model = fmt.Sprintf(`{"id":%q,"providerID":%q}`, session.model, session.provider)
		}
		entries = append(entries, fmt.Sprintf(`{"id":%q,"title":%q,"directory":%q,"model":%s,"time":{"created":%d,"updated":%d}}`, session.id, session.title, session.directory, model, session.updated, session.updated))
	}
	return "[" + strings.Join(entries, ",") + "]"
}

func (f *fakeOpenCode) observedStatusJSON() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	entries := make([]string, 0, len(f.observedStatuses))
	for id, status := range f.observedStatuses {
		entries = append(entries, fmt.Sprintf(`%q:{"type":%q}`, id, status))
	}
	return "{" + strings.Join(entries, ",") + "}"
}

func (f *fakeOpenCode) emit(format string, arguments ...any) {
	f.emitRaw(fmt.Sprintf(format, arguments...))
}

func (f *fakeOpenCode) emitRaw(event string) {
	select {
	case f.events <- event:
	case <-f.done:
	}
}

// queueTurn registers the events the next prompt submission emits.
func (f *fakeOpenCode) queueTurn(events ...string) {
	f.turnScript <- events
}

func (f *fakeOpenCode) promptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.prompts)
}

func (f *fakeOpenCode) lastModelID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.modelIDs) == 0 {
		return ""
	}
	return f.modelIDs[len(f.modelIDs)-1]
}

func (f *fakeOpenCode) abortCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.abortCalls
}

func (f *fakeOpenCode) setFailAbort(refused bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAbort = refused
}

func (f *fakeOpenCode) setFailPrompt(fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failPrompt = fail
}

func (f *fakeOpenCode) setFailSessionDelete(refused bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failSessionDelete = refused
}

func (f *fakeOpenCode) deleteSessionIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, len(f.deleteCalls))
	copy(ids, f.deleteCalls)
	return ids
}

func (f *fakeOpenCode) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /global/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"healthy":true,"version":"1.0.0"}`)
	})
	mux.HandleFunc("GET /config/providers", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"providers":[{"id":"ollama","name":"Ollama","source":"config","options":{"baseURL":"http://localhost:11434"},"models":{"qwen3:8b":{"id":"qwen3:8b","providerID":"ollama","name":"Qwen3 8B","status":"active","capabilities":{"reasoning":true,"toolcall":true}}}},{"id":"anthropic","name":"Anthropic","source":"config","options":{},"models":{"claude-sonnet-4":{"id":"claude-sonnet-4","providerID":"anthropic","name":"Claude Sonnet 4","status":"active","capabilities":{"reasoning":false,"toolcall":true}},"claude-inactive":{"id":"claude-inactive","providerID":"anthropic","name":"Inactive","status":"error","capabilities":{}}}}]}`)
	})
	mux.HandleFunc("POST /session", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.sessions = append(f.sessions, fmt.Sprintf("ses_%d", len(f.sessions)+1))
		id := f.sessions[len(f.sessions)-1]
		f.mu.Unlock()
		fmt.Fprintf(w, `{"id":%q,"title":"Localzie"}`, id)
	})
	mux.HandleFunc("POST /session/{id}/prompt_async", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Parts []map[string]string `json:"parts"`
			Model struct {
				ProviderID string `json:"providerID"`
				ModelID    string `json:"modelID"`
			} `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.prompts = append(f.prompts, body.Parts[0]["text"])
		f.modelIDs = append(f.modelIDs, body.Model.ProviderID+"/"+body.Model.ModelID)
		failPrompt := f.failPrompt
		f.mu.Unlock()
		if failPrompt {
			http.Error(w, "prompt response lost", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		select {
		case script := <-f.turnScript:
			go func() {
				for _, event := range script {
					f.emitRaw(event)
				}
			}()
		default:
		}
	})
	mux.HandleFunc("POST /session/{id}/abort", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.abortCalls++
		refused := f.failAbort
		f.mu.Unlock()
		if refused {
			http.Error(w, "abort refused", http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, `true`)
	})
	mux.HandleFunc("DELETE /session/{id}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.deleteCalls = append(f.deleteCalls, r.PathValue("id"))
		refused := f.failSessionDelete
		f.mu.Unlock()
		if refused {
			http.Error(w, "delete refused", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /experimental/session", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, f.observedSessionsJSON())
	})
	mux.HandleFunc("GET /session/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, f.observedStatusJSON())
	})
	mux.HandleFunc("GET /session/{id}/message", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		session := f.observed[r.PathValue("id")]
		transcript := "[]"
		if session != nil && session.transcript != "" {
			transcript = session.transcript
		}
		f.mu.Unlock()
		if session == nil {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, transcript)
	})
	mux.HandleFunc("GET /event", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		fmt.Fprint(w, ": connected\n\n")
		flusher.Flush()
		f.once.Do(func() { close(f.subscribed) })
		for {
			select {
			case <-r.Context().Done():
				return
			case <-f.done:
				return
			case payload := <-f.events:
				if payload == "" {
					// Empty payload is the turn-script sentinel that ends
					// the SSE response, ending the harness event stream.
					return
				}
				fmt.Fprintf(w, "data: %s\n\n", payload)
				flusher.Flush()
			}
		}
	})
	return mux
}

func harnessTestServer(t *testing.T) (*httptest.Server, *fakeOpenCode, string) {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	fake := newFakeOpenCode()
	opencode := httptest.NewServer(fake.handler())
	t.Cleanup(opencode.Close)
	client, err := harness.NewOpenCode(opencode.URL, "/project", "", "")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(context.Background(), "test", db, &controlledRuntime{release: make(chan struct{})}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server.AttachOpenCode(client, true)
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	_, token, err := db.CreateDevice(context.Background(), "phone")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { close(fake.done) })
	return httpServer, fake, token
}

func harnessRequest(t *testing.T, baseURL, method, path, token string, body string) (*http.Response, string) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, baseURL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	return response, string(raw)
}

func waitFor(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

// waitForEvent polls the durable events feed until an event matches the
// predicate or the waitFor deadline elapses. Each poll is a fresh SSE request
// that re-reads every event after cursor 0, so the predicate can accumulate
// state across polls.
func waitForEvent(t *testing.T, baseURL, token string, predicate func(eventType string, payload json.RawMessage) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		requestContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		req, err := http.NewRequestWithContext(requestContext, http.MethodGet, baseURL+"/v1/events?after=0", nil)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		reader := bufio.NewReader(response.Body)
		matched := false
	readLoop:
		for {
			line, readErr := reader.ReadString('\n')
			if readErr != nil {
				break
			}
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			var envelope struct {
				Type    string          `json:"type"`
				Payload json.RawMessage `json:"payload"`
			}
			if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &envelope) != nil {
				continue
			}
			if predicate(envelope.Type, envelope.Payload) {
				matched = true
				break readLoop
			}
		}
		response.Body.Close()
		cancel()
		if matched {
			return
		}
	}
	t.Fatalf("timed out waiting for an event matching the predicate")
}

func TestHarnessModelSurface(t *testing.T) {
	httpServer, _, token := harnessTestServer(t)
	response, body := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/models", token, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.StatusCode, body)
	}
	var payload struct {
		Models []protocol.Model `json:"models"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatal(err)
	}
	byID := map[string]protocol.Model{}
	for _, model := range payload.Models {
		byID[model.ID] = model
	}
	if _, ok := byID["test-model"]; !ok {
		t.Fatalf("runtime model missing: %+v", payload.Models)
	}
	opencodeModel, ok := byID["opencode/ollama/qwen3:8b"]
	if !ok {
		t.Fatalf("opencode model missing: %+v", payload.Models)
	}
	if opencodeModel.Runtime != "opencode" || opencodeModel.Locality != "local" || opencodeModel.ReasoningMode != protocol.ReasoningOptional {
		t.Fatalf("opencode model fields: %+v", opencodeModel)
	}
	cloud, ok := byID["opencode/anthropic/claude-sonnet-4"]
	if !ok || cloud.Locality != "cloud" {
		t.Fatalf("cloud opencode model missing or misclassified: %+v", cloud)
	}
}

// erroringRuntime models a machine with no model runtime at all: the harness
// backend must keep the connector usable on its own.
type erroringRuntime struct{}

func (erroringRuntime) Name() string { return "unavailable" }
func (erroringRuntime) Models(context.Context) ([]protocol.Model, error) {
	return nil, errors.New("no supported local runtime found")
}
func (erroringRuntime) Stream(context.Context, runtime.StreamRequest, func(string) error) error {
	return errors.New("no supported local runtime found")
}

func TestHarnessModelsSurviveMissingRuntime(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	fake := newFakeOpenCode()
	opencode := httptest.NewServer(fake.handler())
	t.Cleanup(opencode.Close)
	client, err := harness.NewOpenCode(opencode.URL, "/project", "", "")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(context.Background(), "test", db, erroringRuntime{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server.AttachOpenCode(client, true)
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	_, token, err := db.CreateDevice(context.Background(), "phone")
	if err != nil {
		t.Fatal(err)
	}
	response, body := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/models", token, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.StatusCode, body)
	}
	var payload struct {
		Models []protocol.Model `json:"models"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Models) == 0 || payload.Models[0].Runtime != "opencode" {
		t.Fatalf("harness-only model list expected, got %+v", payload.Models)
	}
}

func TestHarnessConversationStreamsAndCompletes(t *testing.T) {
	httpServer, fake, token := harnessTestServer(t)

	response, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", response.StatusCode, body)
	}
	var conversation protocol.Conversation
	if err := json.Unmarshal([]byte(body), &conversation); err != nil {
		t.Fatal(err)
	}
	if conversation.Backend != protocol.BackendHarness {
		t.Fatalf("backend=%q", conversation.Backend)
	}

	// A stale idle from before this turn (or another session) must not
	// complete the job before the turn has actually started.
	fake.emit(`{"type":"session.idle","properties":{"sessionID":"ses_1"}}`)

	fake.queueTurn(
		`{"type":"message.updated","properties":{"sessionID":"ses_1","info":{"id":"msg_1","sessionID":"ses_1","role":"user"}}}`,
		`{"type":"message.updated","properties":{"sessionID":"ses_1","info":{"id":"msg_2","sessionID":"ses_1","role":"assistant"}}}`,
		`{"type":"message.part.updated","properties":{"sessionID":"ses_1","part":{"id":"prt_1","sessionID":"ses_1","messageID":"msg_2","type":"reasoning","text":"considering"},"time":1}}`,
		`{"type":"message.part.updated","properties":{"sessionID":"ses_1","part":{"id":"prt_2","sessionID":"ses_1","messageID":"msg_2","type":"text","text":"Hello"},"time":1}}`,
		`{"type":"message.part.delta","properties":{"sessionID":"ses_1","messageID":"msg_2","partID":"prt_2","field":"text","delta":" world"}}`,
		`{"type":"session.idle","properties":{"sessionID":"ses_1"}}`,
	)

	response, body = harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations/"+conversation.ID+"/messages", token, `{"idempotency_key":"harness-1","content":"Say hello"}`)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("send status=%d body=%s", response.StatusCode, body)
	}
	waitFor(t, "completion", func() bool {
		_, detail := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations/"+conversation.ID, token, "")
		var parsed protocol.ConversationDetail
		if err := json.Unmarshal([]byte(detail), &parsed); err != nil {
			return false
		}
		if len(parsed.Messages) < 2 {
			return false
		}
		assistant := parsed.Messages[1]
		return assistant.Status == protocol.MessageComplete && assistant.Content == "<think>considering</think>Hello world"
	})
	if fake.promptCount() != 1 {
		t.Fatalf("prompts=%d", fake.promptCount())
	}
	if model := fake.lastModelID(); model != "ollama/qwen3:8b" {
		t.Fatalf("prompted model=%q", model)
	}

	// A second turn on the same conversation reuses the same harness session.
	fake.queueTurn(
		`{"type":"message.updated","properties":{"sessionID":"ses_1","info":{"id":"msg_3","sessionID":"ses_1","role":"user"}}}`,
		`{"type":"message.updated","properties":{"sessionID":"ses_1","info":{"id":"msg_4","sessionID":"ses_1","role":"assistant"}}}`,
		`{"type":"message.part.updated","properties":{"sessionID":"ses_1","part":{"id":"prt_3","sessionID":"ses_1","messageID":"msg_4","type":"text","text":"Again"},"time":2}}`,
		`{"type":"session.idle","properties":{"sessionID":"ses_1"}}`,
	)
	response, body = harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations/"+conversation.ID+"/messages", token, `{"idempotency_key":"harness-2","content":"Say it again"}`)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("second send status=%d body=%s", response.StatusCode, body)
	}
	waitFor(t, "second completion", func() bool {
		_, detail := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations/"+conversation.ID, token, "")
		var parsed protocol.ConversationDetail
		if err := json.Unmarshal([]byte(detail), &parsed); err != nil {
			return false
		}
		return len(parsed.Messages) >= 3 && parsed.Messages[2].Status == protocol.MessageComplete
	})
}

func TestHarnessStopAbortsOpenCodeTurn(t *testing.T) {
	httpServer, fake, token := harnessTestServer(t)
	_, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
	var conversation protocol.Conversation
	if err := json.Unmarshal([]byte(body), &conversation); err != nil {
		t.Fatal(err)
	}
	// Partial output with no idle: the turn is still running when stop lands.
	fake.queueTurn(
		`{"type":"message.updated","properties":{"sessionID":"ses_1","info":{"id":"msg_1","sessionID":"ses_1","role":"assistant"}}}`,
		`{"type":"message.part.updated","properties":{"sessionID":"ses_1","part":{"id":"prt_1","sessionID":"ses_1","messageID":"msg_1","type":"text","text":"partial"},"time":1}}`,
	)
	response, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations/"+conversation.ID+"/messages", token, `{"idempotency_key":"harness-stop","content":"Long task"}`)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("send status=%d body=%s", response.StatusCode, body)
	}
	waitFor(t, "prompt submission", func() bool { return fake.promptCount() == 1 })
	var job struct {
		Job protocol.Job `json:"job"`
	}
	if err := json.Unmarshal([]byte(body), &job); err != nil {
		t.Fatal(err)
	}
	response, _ = harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/jobs/"+job.Job.ID+"/stop", token, "")
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("stop status=%d", response.StatusCode)
	}
	waitFor(t, "abort", func() bool { return fake.abortCount() == 1 })
	waitFor(t, "canceled status", func() bool {
		_, detail := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations/"+conversation.ID, token, "")
		var parsed protocol.ConversationDetail
		if err := json.Unmarshal([]byte(detail), &parsed); err != nil {
			return false
		}
		return len(parsed.Messages) >= 2 && parsed.Messages[1].Status == protocol.MessageInterrupted
	})
}

func TestHarnessSessionErrorFailsTheJob(t *testing.T) {
	httpServer, fake, token := harnessTestServer(t)
	_, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
	var conversation protocol.Conversation
	if err := json.Unmarshal([]byte(body), &conversation); err != nil {
		t.Fatal(err)
	}
	fake.queueTurn(
		`{"type":"message.updated","properties":{"sessionID":"ses_1","info":{"id":"msg_1","sessionID":"ses_1","role":"assistant"}}}`,
		`{"type":"session.error","properties":{"sessionID":"ses_1","error":{"name":"APIError","data":{"message":"rate limited"}}}}`,
	)
	response, _ := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations/"+conversation.ID+"/messages", token, `{"idempotency_key":"harness-err","content":"Explode"}`)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("send status=%d", response.StatusCode)
	}
	waitFor(t, "failed job", func() bool {
		_, detail := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations/"+conversation.ID, token, "")
		var parsed protocol.ConversationDetail
		if err := json.Unmarshal([]byte(detail), &parsed); err != nil {
			return false
		}
		return len(parsed.Messages) >= 2 && parsed.Messages[1].Status == protocol.MessageInterrupted
	})
}

func TestHarnessesEndpointReportsAttachedServer(t *testing.T) {
	// The server API can run without an opencode executable on this PATH.
	t.Setenv("PATH", t.TempDir())
	httpServer, _, token := harnessTestServer(t)
	_, body := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/harnesses", token, "")
	var payload struct {
		Harnesses []harness.Installation `json:"harnesses"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatal(err)
	}
	for _, installation := range payload.Harnesses {
		if installation.ID != "opencode" {
			continue
		}
		if installation.Server == nil || !installation.Server.Attached || !installation.Server.Healthy || installation.Server.ModelCount != 2 {
			t.Fatalf("opencode server status: %+v", installation.Server)
		}
		if installation.Installed {
			t.Fatal("API attachment must not claim the local CLI is installed")
		}
		if installation.IntegrationStatus != "detected_unverified" {
			t.Fatalf("integration status must stay unverified until fixtures pass: %q", installation.IntegrationStatus)
		}
		return
	}
	t.Fatal("opencode installation missing")
}

func TestHarnessGatedWithoutOverride(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	fake := newFakeOpenCode()
	opencode := httptest.NewServer(fake.handler())
	t.Cleanup(opencode.Close)
	client, err := harness.NewOpenCode(opencode.URL, "/project", "", "")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(context.Background(), "test", db, &controlledRuntime{release: make(chan struct{})}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server.AttachOpenCode(client, false)
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	_, token, err := db.CreateDevice(context.Background(), "phone")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { close(fake.done) })

	// The model surface stays runtime-only until the override is granted.
	response, body := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/models", token, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("models status=%d body=%s", response.StatusCode, body)
	}
	var payload struct {
		Models []protocol.Model `json:"models"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Models) != 1 || payload.Models[0].ID != "test-model" {
		t.Fatalf("gated model surface = %+v, want runtime models only", payload.Models)
	}
	for _, model := range payload.Models {
		if IsOpenCodeModelID(model.ID) {
			t.Fatalf("gated surface leaked an opencode model: %+v", model)
		}
	}

	// Harness conversations are rejected with the unverified code.
	response, body = harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("create status=%d body=%s", response.StatusCode, body)
	}
	var errPayload protocol.ErrorResponse
	if err := json.Unmarshal([]byte(body), &errPayload); err != nil {
		t.Fatal(err)
	}
	if errPayload.Code != "harness_unverified" {
		t.Fatalf("error code=%q body=%s", errPayload.Code, body)
	}

	// The attachment itself stays visible as diagnostics.
	_, body = harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/harnesses", token, "")
	var harnessesPayload struct {
		Harnesses []harness.Installation `json:"harnesses"`
	}
	if err := json.Unmarshal([]byte(body), &harnessesPayload); err != nil {
		t.Fatal(err)
	}
	for _, installation := range harnessesPayload.Harnesses {
		if installation.ID != "opencode" {
			continue
		}
		if installation.Server == nil || !installation.Server.Attached || !installation.Server.Healthy {
			t.Fatalf("opencode server status: %+v", installation.Server)
		}
		return
	}
	t.Fatal("opencode installation missing")

	// The health feature must not promise harness sessions while gated.
	response, body = harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/health", token, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("health status=%d body=%s", response.StatusCode, body)
	}
	var healthPayload protocol.Health
	if err := json.Unmarshal([]byte(body), &healthPayload); err != nil {
		t.Fatal(err)
	}
	for _, feature := range healthPayload.Features {
		if feature == "harness_sessions" {
			t.Fatalf("gated health advertises harness_sessions: %+v", healthPayload.Features)
		}
	}
}

func TestRuntimeConversationUnchangedWithoutAttachment(t *testing.T) {
	// No AttachOpenCode: an opencode model ID must be rejected, and runtime
	// conversations keep working through the untouched path.
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	server, err := NewServer(context.Background(), "test", db, &controlledRuntime{release: make(chan struct{})}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	_, token, err := db.CreateDevice(context.Background(), "phone")
	if err != nil {
		t.Fatal(err)
	}
	response, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("status=%d body=%s", response.StatusCode, body)
	}
	response, body = harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"test-model"}`)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d body=%s", response.StatusCode, body)
	}
}

func TestHarnessReasoningGrowthStreams(t *testing.T) {
	httpServer, fake, token := harnessTestServer(t)
	_, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
	var conversation protocol.Conversation
	if err := json.Unmarshal([]byte(body), &conversation); err != nil {
		t.Fatal(err)
	}
	// A growing reasoning part is never appenditive in rendered form because
	// the closing think tag shifts on every growth. The lazy closer must keep
	// the growth streaming as message.delta instead of dropping it after the
	// first snapshot.
	fake.queueTurn(
		`{"type":"message.updated","properties":{"sessionID":"ses_1","info":{"id":"msg_1","sessionID":"ses_1","role":"assistant"}}}`,
		`{"type":"message.part.updated","properties":{"sessionID":"ses_1","part":{"id":"prt_1","sessionID":"ses_1","messageID":"msg_1","type":"reasoning","text":"con"},"time":1}}`,
		`{"type":"message.part.delta","properties":{"sessionID":"ses_1","messageID":"msg_1","partID":"prt_1","field":"text","delta":"sid"}}`,
		`{"type":"message.part.delta","properties":{"sessionID":"ses_1","messageID":"msg_1","partID":"prt_1","field":"text","delta":"ering"}}`,
		`{"type":"session.idle","properties":{"sessionID":"ses_1"}}`,
	)
	response, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations/"+conversation.ID+"/messages", token, `{"idempotency_key":"harness-reasoning","content":"Think out loud"}`)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("send status=%d body=%s", response.StatusCode, body)
	}
	waitFor(t, "completion", func() bool {
		_, detail := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations/"+conversation.ID, token, "")
		var parsed protocol.ConversationDetail
		if err := json.Unmarshal([]byte(detail), &parsed); err != nil {
			return false
		}
		if len(parsed.Messages) < 2 {
			return false
		}
		assistant := parsed.Messages[1]
		return assistant.Status == protocol.MessageComplete && assistant.Content == "<think>considering</think>"
	})
	// The growth must reach the client as streamed deltas, not vanish in a
	// silent resync.
	seen := map[string]bool{}
	waitForEvent(t, httpServer.URL, token, func(eventType string, payload json.RawMessage) bool {
		if eventType != "message.delta" {
			return false
		}
		var deltaPayload struct {
			Delta string `json:"delta"`
		}
		if err := json.Unmarshal(payload, &deltaPayload); err != nil {
			return false
		}
		seen[deltaPayload.Delta] = true
		return seen["sid"] && seen["ering"]
	})
}

func TestHarnessReasoningStartsWrappedBeforeFirstDelta(t *testing.T) {
	httpServer, fake, token := harnessTestServer(t)
	_, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
	var conversation protocol.Conversation
	if err := json.Unmarshal([]byte(body), &conversation); err != nil {
		t.Fatal(err)
	}
	// OpenCode v1.18 starts both text and reasoning parts with an empty snapshot,
	// then streams their content through part deltas. Reasoning must acquire its
	// opening tag on the first delta rather than appearing as answer text until
	// the final idle event closes it.
	fake.queueTurn(
		`{"type":"message.updated","properties":{"sessionID":"ses_1","info":{"id":"msg_1","sessionID":"ses_1","role":"assistant"}}}`,
		`{"type":"message.part.updated","properties":{"sessionID":"ses_1","part":{"id":"prt_1","sessionID":"ses_1","messageID":"msg_1","type":"reasoning","text":""},"time":1}}`,
		`{"type":"message.part.delta","properties":{"sessionID":"ses_1","messageID":"msg_1","partID":"prt_1","field":"text","delta":"private reasoning"}}`,
	)
	response, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations/"+conversation.ID+"/messages", token, `{"idempotency_key":"harness-empty-reasoning","content":"Think"}`)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("send status=%d body=%s", response.StatusCode, body)
	}
	waitFor(t, "wrapped streaming reasoning", func() bool {
		_, detailBody := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations/"+conversation.ID, token, "")
		var detail protocol.ConversationDetail
		if json.Unmarshal([]byte(detailBody), &detail) != nil || len(detail.Messages) < 2 {
			return false
		}
		return detail.Messages[1].Content == "<think>private reasoning"
	})
	fake.emit(`{"type":"session.idle","properties":{"sessionID":"ses_1"}}`)
	waitFor(t, "closed reasoning", func() bool {
		_, detailBody := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations/"+conversation.ID, token, "")
		var detail protocol.ConversationDetail
		if json.Unmarshal([]byte(detailBody), &detail) != nil || len(detail.Messages) < 2 {
			return false
		}
		return detail.Messages[1].Status == protocol.MessageComplete && detail.Messages[1].Content == "<think>private reasoning</think>"
	})
}

func TestHarnessReplacementIsPersisted(t *testing.T) {
	httpServer, fake, token := harnessTestServer(t)
	_, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
	var conversation protocol.Conversation
	if err := json.Unmarshal([]byte(body), &conversation); err != nil {
		t.Fatal(err)
	}
	// A part replaced in place is a non-appenditive rewrite: the corrected
	// content must be persisted wholesale, not silently dropped.
	fake.queueTurn(
		`{"type":"message.updated","properties":{"sessionID":"ses_1","info":{"id":"msg_1","sessionID":"ses_1","role":"assistant"}}}`,
		`{"type":"message.part.updated","properties":{"sessionID":"ses_1","part":{"id":"prt_1","sessionID":"ses_1","messageID":"msg_1","type":"text","text":"abc"},"time":1}}`,
		`{"type":"message.part.updated","properties":{"sessionID":"ses_1","part":{"id":"prt_1","sessionID":"ses_1","messageID":"msg_1","type":"text","text":"xyz"},"time":1}}`,
		`{"type":"message.part.delta","properties":{"sessionID":"ses_1","messageID":"msg_1","partID":"prt_1","field":"text","delta":"!"}}`,
		`{"type":"session.idle","properties":{"sessionID":"ses_1"}}`,
	)
	response, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations/"+conversation.ID+"/messages", token, `{"idempotency_key":"harness-replace","content":"Rewrite"}`)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("send status=%d body=%s", response.StatusCode, body)
	}
	waitFor(t, "completion", func() bool {
		_, detail := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations/"+conversation.ID, token, "")
		var parsed protocol.ConversationDetail
		if err := json.Unmarshal([]byte(detail), &parsed); err != nil {
			return false
		}
		if len(parsed.Messages) < 2 {
			return false
		}
		assistant := parsed.Messages[1]
		return assistant.Status == protocol.MessageComplete && assistant.Content == "xyz!"
	})
	// The rewrite must surface as a full-snapshot replacement event.
	waitForEvent(t, httpServer.URL, token, func(eventType string, payload json.RawMessage) bool {
		if eventType != "message.content" {
			return false
		}
		var contentPayload struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(payload, &contentPayload); err != nil {
			return false
		}
		return contentPayload.Content == "xyz"
	})
}

func TestHarnessOutputLimitAbortsTurn(t *testing.T) {
	httpServer, fake, token := harnessTestServer(t)
	_, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
	var conversation protocol.Conversation
	if err := json.Unmarshal([]byte(body), &conversation); err != nil {
		t.Fatal(err)
	}
	fake.queueTurn(
		`{"type":"message.updated","properties":{"sessionID":"ses_1","info":{"id":"msg_1","sessionID":"ses_1","role":"assistant"}}}`,
		fmt.Sprintf(`{"type":"message.part.updated","properties":{"sessionID":"ses_1","part":{"id":"prt_1","sessionID":"ses_1","messageID":"msg_1","type":"text","text":%q},"time":1}}`, strings.Repeat("a", 5*1024*1024)),
	)
	response, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations/"+conversation.ID+"/messages", token, `{"idempotency_key":"harness-limit","content":"Blow up"}`)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("send status=%d body=%s", response.StatusCode, body)
	}
	// The output-cap failure must abort the still-running OpenCode turn so a
	// new job cannot be accepted while the old turn keeps mutating the
	// session server-side.
	waitForEvent(t, httpServer.URL, token, func(eventType string, payload json.RawMessage) bool {
		if eventType != "job.failed" {
			return false
		}
		return strings.Contains(string(payload), "output limit")
	})
	if fake.abortCount() != 1 {
		t.Fatalf("abort count = %d, want 1", fake.abortCount())
	}
}

func TestHarnessStreamEndAbortsTurn(t *testing.T) {
	// The trailing empty payload is the turn-script sentinel that ends the
	// SSE response mid-turn, exercising the stream-end failure path.
	script := []string{
		`{"type":"message.updated","properties":{"sessionID":"ses_1","info":{"id":"msg_1","sessionID":"ses_1","role":"assistant"}}}`,
		`{"type":"message.part.updated","properties":{"sessionID":"ses_1","part":{"id":"prt_1","sessionID":"ses_1","messageID":"msg_1","type":"text","text":"partial"},"time":1}}`,
		"",
	}
	t.Run("abort confirmed", func(t *testing.T) {
		httpServer, fake, token := harnessTestServer(t)
		_, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
		var conversation protocol.Conversation
		if err := json.Unmarshal([]byte(body), &conversation); err != nil {
			t.Fatal(err)
		}
		fake.queueTurn(script...)
		response, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations/"+conversation.ID+"/messages", token, `{"idempotency_key":"harness-stream-end","content":"Cut off"}`)
		if response.StatusCode != http.StatusAccepted {
			t.Fatalf("send status=%d body=%s", response.StatusCode, body)
		}
		waitFor(t, "abort", func() bool { return fake.abortCount() == 1 })
		waitForEvent(t, httpServer.URL, token, func(eventType string, payload json.RawMessage) bool {
			return eventType == "job.failed" && strings.Contains(string(payload), "event stream ended")
		})
	})
	t.Run("abort refused", func(t *testing.T) {
		httpServer, fake, token := harnessTestServer(t)
		_, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
		var conversation protocol.Conversation
		if err := json.Unmarshal([]byte(body), &conversation); err != nil {
			t.Fatal(err)
		}
		fake.setFailAbort(true)
		fake.queueTurn(script...)
		response, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations/"+conversation.ID+"/messages", token, `{"idempotency_key":"harness-stream-end-refused","content":"Cut off"}`)
		if response.StatusCode != http.StatusAccepted {
			t.Fatalf("send status=%d body=%s", response.StatusCode, body)
		}
		waitFor(t, "abort attempt", func() bool { return fake.abortCount() == 1 })
		// An unconfirmed abort must be recorded in the job error so the
		// failure cannot be misread as a clean stop.
		waitForEvent(t, httpServer.URL, token, func(eventType string, payload json.RawMessage) bool {
			return eventType == "job.failed" && strings.Contains(string(payload), "abort unconfirmed")
		})
	})
}

func TestHarnessPromptFailureAttemptsAbort(t *testing.T) {
	httpServer, fake, token := harnessTestServer(t)
	_, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
	var conversation protocol.Conversation
	if err := json.Unmarshal([]byte(body), &conversation); err != nil {
		t.Fatal(err)
	}
	fake.setFailPrompt(true)
	response, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations/"+conversation.ID+"/messages", token, `{"idempotency_key":"harness-prompt-failure","content":"May have started"}`)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("send status=%d body=%s", response.StatusCode, body)
	}
	waitFor(t, "abort after ambiguous prompt failure", func() bool { return fake.abortCount() == 1 })
	waitForEvent(t, httpServer.URL, token, func(eventType string, payload json.RawMessage) bool {
		return eventType == "job.failed" && strings.Contains(string(payload), "OpenCode prompt failed")
	})
}

func TestHarnessBusyDeleteDoesNotDeleteLiveSession(t *testing.T) {
	httpServer, fake, token := harnessTestServer(t)
	_, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
	var conversation protocol.Conversation
	if err := json.Unmarshal([]byte(body), &conversation); err != nil {
		t.Fatal(err)
	}
	fake.queueTurn(`{"type":"message.updated","properties":{"sessionID":"ses_1","info":{"id":"msg_1","sessionID":"ses_1","role":"assistant"}}}`)
	response, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations/"+conversation.ID+"/messages", token, `{"idempotency_key":"harness-busy-delete","content":"Keep working"}`)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("send status=%d body=%s", response.StatusCode, body)
	}
	var send protocol.SendMessageResponse
	if err := json.Unmarshal([]byte(body), &send); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "prompt submission", func() bool { return fake.promptCount() == 1 })
	response, body = harnessRequest(t, httpServer.URL, http.MethodDelete, "/v1/conversations/"+conversation.ID, token, "")
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("delete status=%d body=%s, want 409", response.StatusCode, body)
	}
	if deleted := fake.deleteSessionIDs(); len(deleted) != 0 {
		t.Fatalf("busy conversation deleted its live OpenCode session: %v", deleted)
	}
	response, body = harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/jobs/"+send.Job.ID+"/stop", token, "")
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("stop status=%d body=%s", response.StatusCode, body)
	}
}

func TestHarnessDeleteKeepsRecordWhenOpenCodeIsUnavailable(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	conversation, err := db.CreateHarnessConversation(context.Background(), "opencode/ollama/qwen3:8b", "Harness chat", "ses_unavailable")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(context.Background(), "test", db, &controlledRuntime{release: make(chan struct{})}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	_, token, err := db.CreateDevice(context.Background(), "phone")
	if err != nil {
		t.Fatal(err)
	}

	response, body := harnessRequest(t, httpServer.URL, http.MethodDelete, "/v1/conversations/"+conversation.ID, token, "")
	if response.StatusCode != http.StatusBadGateway {
		t.Fatalf("delete status=%d body=%s, want 502", response.StatusCode, body)
	}
	var errorPayload protocol.ErrorResponse
	if err := json.Unmarshal([]byte(body), &errorPayload); err != nil {
		t.Fatal(err)
	}
	if errorPayload.Code != "harness_delete_failed" {
		t.Fatalf("delete error=%q body=%s", errorPayload.Code, body)
	}
	response, _ = harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations/"+conversation.ID, token, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("conversation was removed while its OpenCode session could not be deleted: status=%d", response.StatusCode)
	}
}

func TestHarnessDeleteConversationDeletesSession(t *testing.T) {
	httpServer, fake, token := harnessTestServer(t)

	// A harness conversation owns a server-side OpenCode session: deleting
	// the conversation must remove that session too, not just the record.
	_, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
	var first protocol.Conversation
	if err := json.Unmarshal([]byte(body), &first); err != nil {
		t.Fatal(err)
	}
	response, body := harnessRequest(t, httpServer.URL, http.MethodDelete, "/v1/conversations/"+first.ID, token, "")
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%s", response.StatusCode, body)
	}
	if deleted := fake.deleteSessionIDs(); len(deleted) != 1 || deleted[0] != "ses_1" {
		t.Fatalf("deleted sessions = %q, want [ses_1]", deleted)
	}
	response, _ = harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations/"+first.ID, token, "")
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("conversation survives delete: status=%d", response.StatusCode)
	}

	t.Run("session delete failure keeps the conversation", func(t *testing.T) {
		_, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"opencode/ollama/qwen3:8b"}`)
		var conversation protocol.Conversation
		if err := json.Unmarshal([]byte(body), &conversation); err != nil {
			t.Fatal(err)
		}
		fake.setFailSessionDelete(true)
		response, body := harnessRequest(t, httpServer.URL, http.MethodDelete, "/v1/conversations/"+conversation.ID, token, "")
		if response.StatusCode != http.StatusBadGateway {
			t.Fatalf("delete status=%d body=%s", response.StatusCode, body)
		}
		var errPayload protocol.ErrorResponse
		if err := json.Unmarshal([]byte(body), &errPayload); err != nil {
			t.Fatal(err)
		}
		if errPayload.Code != "harness_delete_failed" {
			t.Fatalf("error code=%q body=%s", errPayload.Code, body)
		}
		response, _ = harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations/"+conversation.ID, token, "")
		if response.StatusCode != http.StatusOK {
			t.Fatalf("conversation deleted despite session failure: status=%d", response.StatusCode)
		}
	})

	t.Run("runtime conversations never touch the session endpoint", func(t *testing.T) {
		before := fake.deleteSessionIDs()
		_, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations", token, `{"model_id":"test-model"}`)
		var conversation protocol.Conversation
		if err := json.Unmarshal([]byte(body), &conversation); err != nil {
			t.Fatal(err)
		}
		response, body := harnessRequest(t, httpServer.URL, http.MethodDelete, "/v1/conversations/"+conversation.ID, token, "")
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("delete status=%d body=%s", response.StatusCode, body)
		}
		if deleted := fake.deleteSessionIDs(); len(deleted) != len(before) {
			t.Fatalf("runtime delete touched the session endpoint: %q", deleted)
		}
	})
}

// harnessTestServerWithConnector is harnessTestServer plus the connector Server
// handle, so observe-mode tests can drive reconciliation deterministically
// instead of racing the background poll loop.
func harnessTestServerWithConnector(t *testing.T) (*httptest.Server, *fakeOpenCode, string, *Server) {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	fake := newFakeOpenCode()
	opencode := httptest.NewServer(fake.handler())
	t.Cleanup(opencode.Close)
	client, err := harness.NewOpenCode(opencode.URL, "/project", "", "")
	if err != nil {
		t.Fatal(err)
	}
	root, cancel := context.WithCancel(context.Background())
	server, err := NewServer(root, "test", db, &controlledRuntime{release: make(chan struct{})}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server.AttachOpenCode(client, true)
	t.Cleanup(func() {
		cancel()
		close(fake.done)
	})
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	_, token, err := db.CreateDevice(context.Background(), "phone")
	if err != nil {
		t.Fatal(err)
	}
	return httpServer, fake, token, server
}

func observeTranscript(running string) string {
	return `[
		{"info":{"id":"msg_user1","role":"user","time":{"created":1789000100000}},"parts":[{"id":"p1","type":"text","text":"run the tests"}]},
		{"info":{"id":"msg_asst1","role":"assistant","time":{"created":1789000101000}},"parts":[{"id":"p2","type":"text","text":"` + running + `"},{"id":"p3","type":"reasoning","text":"checking"}]}
	]`
}

func TestObserveModeMirrorsActiveSessionAndGatesWrites(t *testing.T) {
	httpServer, fake, token, connector := harnessTestServerWithConnector(t)
	const sessionID = "ses_observed_terminal"
	observedID := observedConversationPrefix + sessionID
	fake.seedObserved(sessionID, "Terminal refactor", "/project", "ollama", "qwen3:8b", 1_789_000_100_000)
	fake.setObservedStatus(sessionID, "busy")
	fake.setObservedTranscript(sessionID, observeTranscript("Running "))
	connector.harness.reconcileObserved(context.Background())
	connector.harness.flushObserved(context.Background())

	response, body := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations", token, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("listing status=%d body=%s", response.StatusCode, body)
	}
	var listing struct {
		Conversations []struct {
			ID          string `json:"id"`
			Backend     string `json:"backend"`
			ActiveSite  string `json:"activeSite"`
			ObserveOnly bool   `json:"observeOnly"`
		} `json:"conversations"`
	}
	if err := json.Unmarshal([]byte(body), &listing); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, conversation := range listing.Conversations {
		if conversation.ID != observedID {
			continue
		}
		found = true
		if conversation.ActiveSite != "harness" || !conversation.ObserveOnly || conversation.Backend != "harness" {
			t.Fatalf("observe marker wrong: %+v", conversation)
		}
	}
	if !found {
		t.Fatalf("observed conversation missing from listing: %s", body)
	}

	response, body = harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations/"+observedID, token, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("detail status=%d body=%s", response.StatusCode, body)
	}
	var detail protocol.ConversationDetail
	if err := json.Unmarshal([]byte(body), &detail); err != nil {
		t.Fatal(err)
	}
	if len(detail.Messages) != 2 {
		t.Fatalf("observed transcript has %d messages, want 2", len(detail.Messages))
	}
	if got := detail.Messages[1].Content; !strings.Contains(got, "Running ") || !strings.Contains(got, "\u003cthink\u003echecking\u003c/think\u003e") {
		t.Fatalf("observed content not rendered like the controlled path: %q", got)
	}

	// A content change mirrors as a non-appenditive full-snapshot event.
	fake.setObservedTranscript(sessionID, observeTranscript("Running the tests "))
	connector.harness.flushObserved(context.Background())
	waitForEvent(t, httpServer.URL, token, func(eventType string, payload json.RawMessage) bool {
		if eventType != "message.content" {
			return false
		}
		var content struct {
			ConversationID string `json:"conversation_id"`
			GenerationID   string `json:"generation_id"`
			Content        string `json:"content"`
		}
		if json.Unmarshal(payload, &content) != nil {
			return false
		}
		return content.ConversationID == observedID && content.GenerationID == "msg_asst1" && strings.Contains(content.Content, "Running the tests ")
	})

	// Observing alone never attaches: a write without take_over is refused.
	response, body = harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations/"+observedID+"/messages", token, `{"idempotency_key":"observe-1","content":"hi"}`)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("write to observed session status=%d body=%s", response.StatusCode, body)
	}
	var failure protocol.ErrorResponse
	if err := json.Unmarshal([]byte(body), &failure); err != nil {
		t.Fatal(err)
	}
	if failure.Code != "session_active_elsewhere" {
		t.Fatalf("write gate code=%q want session_active_elsewhere", failure.Code)
	}
	if fake.abortCount() != 0 {
		t.Fatalf("observing or gating aborted the harness turn %d times", fake.abortCount())
	}
}

func TestObserveModePrunesWhenSessionGoesIdle(t *testing.T) {
	httpServer, fake, token, connector := harnessTestServerWithConnector(t)
	const sessionID = "ses_observed_idle"
	observedID := observedConversationPrefix + sessionID
	fake.seedObserved(sessionID, "Idle soon", "/project", "ollama", "qwen3:8b", 1_789_000_200_000)
	fake.setObservedStatus(sessionID, "busy")
	fake.setObservedTranscript(sessionID, observeTranscript("working"))
	connector.harness.reconcileObserved(context.Background())
	connector.harness.flushObserved(context.Background())

	fake.setObservedStatus(sessionID, "idle")
	connector.harness.reconcileObserved(context.Background())

	_, body := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations", token, "")
	if strings.Contains(body, observedID) {
		t.Fatalf("idle session still listed: %s", body)
	}
	response, body := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations/"+observedID, token, "")
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("pruned observed detail status=%d body=%s", response.StatusCode, body)
	}
	waitForEvent(t, httpServer.URL, token, func(eventType string, payload json.RawMessage) bool {
		if eventType != "conversation.deleted" {
			return false
		}
		var deleted struct {
			ID string `json:"id"`
		}
		return json.Unmarshal(payload, &deleted) == nil && deleted.ID == observedID
	})
}

func TestObserveModeTakeOverNeverKillsInFlightTurn(t *testing.T) {
	httpServer, fake, token, connector := harnessTestServerWithConnector(t)
	const sessionID = "ses_observed_busy"
	observedID := observedConversationPrefix + sessionID
	fake.seedObserved(sessionID, "Busy session", "/project", "ollama", "qwen3:8b", 1_789_000_300_000)
	fake.setObservedStatus(sessionID, "busy")
	fake.setObservedTranscript(sessionID, observeTranscript("half done"))
	connector.harness.reconcileObserved(context.Background())
	connector.harness.flushObserved(context.Background())

	response, body := harnessRequest(t, httpServer.URL, http.MethodPost, "/v1/conversations/"+observedID+"/messages", token, `{"idempotency_key":"takeover-1","content":"continue please","take_over":true}`)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("take-over status=%d body=%s", response.StatusCode, body)
	}
	if fake.abortCount() != 0 {
		t.Fatalf("take-over aborted the in-flight harness turn %d times", fake.abortCount())
	}
	// The observed listing entry is gone; the store-owned conversation reuses
	// the same ID without the observe marker.
	_, listing := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations", token, "")
	if strings.Count(listing, observedID) != 1 {
		t.Fatalf("take-over conversation should appear exactly once: %s", listing)
	}
	var conversations []protocol.Conversation
	var wrapper struct {
		Conversations []protocol.Conversation `json:"conversations"`
	}
	if err := json.Unmarshal([]byte(listing), &wrapper); err != nil {
		t.Fatal(err)
	}
	conversations = wrapper.Conversations
	found := false
	for _, conversation := range conversations {
		if conversation.ID != observedID {
			continue
		}
		found = true
		if conversation.ObserveOnly || conversation.ActiveSite != "" || conversation.Backend != protocol.BackendHarness {
			t.Fatalf("taken-over conversation still marked observed: %+v", conversation)
		}
	}
	if !found {
		t.Fatalf("taken-over conversation missing: %s", listing)
	}
	// History mirrored at take-over plus the accepted prompt/placeholder.
	_, detailBody := harnessRequest(t, httpServer.URL, http.MethodGet, "/v1/conversations/"+observedID, token, "")
	var detail protocol.ConversationDetail
	if err := json.Unmarshal([]byte(detailBody), &detail); err != nil {
		t.Fatal(err)
	}
	if len(detail.Messages) != 4 {
		t.Fatalf("taken-over transcript has %d messages, want 4: %s", len(detail.Messages), detailBody)
	}
	if detail.Messages[0].ID != "msg_user1" || detail.Messages[1].ID != "msg_asst1" {
		t.Fatalf("mirrored history order lost: %+v", detail.Messages)
	}
}
