// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// withV1Detection serves the API-generation detection surface a real
// v1.18.x server answers: 404 at the v2 info route and healthy at the v1
// health route (healthVersion "" leaves the health route to the wrapped
// handler). A wrapped handler never sees the info probe. Detection probes
// are server-global and carry no directory or auth expectations.
func withV1Detection(healthVersion string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/info":
			http.NotFound(w, r)
			return
		case r.URL.Path == "/global/health" && healthVersion != "":
			fmt.Fprintf(w, `{"healthy":true,"version":%q}`, healthVersion)
			return
		}
		next(w, r)
	}
}

func TestOpenCodeSessionContract(t *testing.T) {
	// healthVersion "": this server serves its own health route.
	server := httptest.NewServer(withV1Detection("", func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok || username != "opencode" || password != "secret" {
			t.Errorf("basic auth = %q %q %v", username, password, ok)
		}
		// Identity probes are server-global and carry no workspace
		// directory; every other route on this fake does.
		probeRoute := r.URL.Path == "/global/health"
		if got := r.URL.Query().Get("directory"); got != "/project" && !probeRoute {
			t.Errorf("directory = %q", got)
		}
		switch {
		case r.URL.Path == "/global/health":
			fmt.Fprint(w, `{"healthy":true,"version":"1.0.0"}`)
		case r.URL.Path == "/session" && r.Method == http.MethodPost:
			fmt.Fprint(w, `{"id":"session-1","title":"Localzie task"}`)
		case r.URL.Path == "/session/session-1/prompt_async":
			var body struct {
				Parts []map[string]string `json:"parts"`
				Model struct {
					ProviderID string `json:"providerID"`
					ModelID    string `json:"modelID"`
				} `json:"model"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if len(body.Parts) != 1 || body.Parts[0]["text"] != "Run tests" || body.Model.ProviderID != "anthropic" || body.Model.ModelID != "claude-sonnet-4" {
				t.Errorf("prompt body = %+v", body)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.URL.Path == "/session/session-1/abort":
			fmt.Fprint(w, `true`)
		case r.URL.Path == "/session/session-1/diff":
			fmt.Fprint(w, `[{"file":"main.go"}]`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewOpenCode(server.URL, "/project", "", "secret")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if version, err := client.Health(ctx); err != nil || version != "1.0.0" {
		t.Fatalf("version=%q err=%v", version, err)
	}
	session, err := client.CreateSession(ctx, "Localzie task")
	if err != nil || session.ID != "session-1" {
		t.Fatalf("session=%+v err=%v", session, err)
	}
	if err := client.Prompt(ctx, session.ID, "Run tests", "anthropic/claude-sonnet-4"); err != nil {
		t.Fatal(err)
	}
	if err := client.Abort(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	diff, err := client.Diff(ctx, session.ID)
	if err != nil || !json.Valid(diff) {
		t.Fatalf("diff=%s err=%v", diff, err)
	}
}

func TestOpenCodeRejectsNonLoopbackServer(t *testing.T) {
	if _, err := NewOpenCode("http://192.0.2.10:4096", "/project", "", ""); err == nil {
		t.Fatal("non-loopback server unexpectedly accepted")
	}
}

func TestOpenCodeConfigProvidersContract(t *testing.T) {
	server := httptest.NewServer(withV1Detection("1.18.31", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/config/providers" {
			fmt.Fprint(w, `{"providers":[{"id":"ollama","name":"Ollama","source":"config","options":{"baseURL":"http://localhost:11434"},"models":{"qwen3:8b":{"id":"qwen3:8b","providerID":"ollama","name":"Qwen3 8B","status":"active","capabilities":{"reasoning":true,"toolcall":true}}}},{"id":"anthropic","name":"Anthropic","source":"config","options":{},"models":{"claude-sonnet-4":{"id":"claude-sonnet-4","providerID":"anthropic","name":"Claude Sonnet 4","status":"active","capabilities":{}}}}]}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	client, err := NewOpenCode(server.URL, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	providers, err := client.ConfigProviders(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 2 {
		t.Fatalf("providers=%d", len(providers))
	}
	ollama := providers[0].Models["qwen3:8b"]
	if !ollama.Capabilities.Reasoning || !ollama.Capabilities.ToolCall || ollama.Status != "active" {
		t.Fatalf("ollama model info: %+v", ollama)
	}
	if providers[0].Options.BaseURL != "http://localhost:11434" {
		t.Fatalf("ollama baseURL: %+v", providers[0].Options)
	}
	if providers[1].Models["claude-sonnet-4"].Name != "Claude Sonnet 4" {
		t.Fatalf("anthropic model info: %+v", providers[1].Models["claude-sonnet-4"])
	}
}

func TestOpenCodeEventDecoding(t *testing.T) {
	messageUpdated := OpenCodeEvent{
		Type:       "message.updated",
		Properties: json.RawMessage(`{"sessionID":"ses_1","info":{"id":"msg_1","sessionID":"ses_1","role":"assistant"}}`),
	}
	message, err := messageUpdated.Message()
	if err != nil {
		t.Fatal(err)
	}
	if message.ID != "msg_1" || message.SessionID != "ses_1" || message.Role != "assistant" {
		t.Fatalf("message=%+v", message)
	}

	partUpdated := OpenCodeEvent{
		Type:       "message.part.updated",
		Properties: json.RawMessage(`{"sessionID":"ses_1","part":{"id":"prt_1","sessionID":"ses_1","messageID":"msg_1","type":"reasoning","text":"hmm"},"time":1}`),
	}
	part, err := partUpdated.Part()
	if err != nil {
		t.Fatal(err)
	}
	if part.ID != "prt_1" || part.MessageID != "msg_1" || part.Type != "reasoning" || part.Text != "hmm" {
		t.Fatalf("part=%+v", part)
	}

	partDeltaEvent := OpenCodeEvent{
		Type:       "message.part.delta",
		Properties: json.RawMessage(`{"sessionID":"ses_1","messageID":"msg_1","partID":"prt_1","field":"text","delta":" more"}`),
	}
	sessionID, messageID, partID, field, delta, err := partDeltaEvent.PartDelta()
	if err != nil || sessionID != "ses_1" || messageID != "msg_1" || partID != "prt_1" || field != "text" || delta != " more" {
		t.Fatalf("delta=%q %q %q %q %q err=%v", sessionID, messageID, partID, field, delta, err)
	}

	idle := OpenCodeEvent{Type: "session.idle", Properties: json.RawMessage(`{"sessionID":"ses_1"}`)}
	if idle.SessionID() != "ses_1" {
		t.Fatalf("idle session=%q", idle.SessionID())
	}

	sessionError := OpenCodeEvent{Type: "session.error", Properties: json.RawMessage(`{"sessionID":"ses_1","error":{"name":"APIError","data":{"message":"rate limited"}}}`)}
	if text := sessionError.ErrorText(); text != "APIError: rate limited" {
		t.Fatalf("session error=%q", text)
	}

	// Properties shapes must tolerate unknown fields without failing.
	unknown := OpenCodeEvent{Type: "permission.updated", Properties: json.RawMessage(`{"sessionID":"ses_1","permission":{}}`)}
	if unknown.SessionID() != "ses_1" {
		t.Fatalf("unknown event session=%q", unknown.SessionID())
	}
}
