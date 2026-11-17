// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package harness

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOpenCodeV2DetectionAndCatalog(t *testing.T) {
	var saw []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		saw = append(saw, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery)
		switch {
		case r.URL.Path == "/api/info" && r.Method == http.MethodGet:
			fmt.Fprint(w, `{"version":"2.2.5","pid":7,"urls":["http://127.0.0.1:4096"],"paths":{"tmp":"/t"},"capabilities":{"persistentPty":true}}`)
		case r.URL.Path == "/api/provider" && r.Method == http.MethodGet:
			if got := r.URL.Query().Get("directory"); got != "" {
				t.Errorf("v2 catalog request must not carry the legacy directory parameter, got %q", got)
			}
			fmt.Fprint(w, `{"location":{"directory":"/srv"},"data":[{"id":"ollama","name":"Ollama (local)","activation":"enabled","package":"p","settings":{"baseURL":"http://127.0.0.1:11434/v1"}}]}`)
		case r.URL.Path == "/api/model" && r.Method == http.MethodGet:
			if r.URL.Query().Get("types") == "image" {
				fmt.Fprint(w, `{"location":{"directory":"/srv"},"data":[{"id":"flux-3-image-klein","modelID":"flux-3-image-klein","providerID":"openrouter","name":"FLUX.3","type":"image","capabilities":{"tools":false,"input":["text"],"output":["image"]},"status":"active","enabled":true}]}`)
				return
			}
			fmt.Fprint(w, `{"location":{"directory":"/srv"},"data":[{"id":"qwen3-coder","modelID":"qwen3-coder","providerID":"ollama","name":"Qwen3 Coder","type":"chat","capabilities":{"tools":true,"input":["text","image"],"output":["text"]},"status":"active","enabled":true},{"id":"mpro","modelID":"mpro","providerID":"ollama","name":"M Pro","type":"chat","capabilities":{"tools":true,"input":["text"],"output":["text"]},"status":"deprecated","enabled":false}]}`)
		default:
			// A v2 server serves its web app on unknown routes.
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, "<!doctype html>")
		}
	}))
	defer server.Close()

	client, err := NewOpenCode(server.URL, "/srv", "", "")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	version, err := client.Health(ctx)
	if err != nil || version != "2.2.5" {
		t.Fatalf("health version=%q err=%v", version, err)
	}
	if generation, err := client.Generation(ctx); err != nil || generation != OpenCodeV2 {
		t.Fatalf("generation=%q err=%v", generation, err)
	}
	providers, err := client.ConfigProviders(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 2 {
		t.Fatalf("providers=%d", len(providers))
	}
	var ollama, openrouter *OpenCodeProvider
	for i := range providers {
		switch providers[i].ID {
		case "ollama":
			ollama = &providers[i]
		case "openrouter":
			openrouter = &providers[i]
		}
	}
	if ollama == nil || ollama.Options.BaseURL != "http://127.0.0.1:11434/v1" {
		t.Fatalf("ollama provider=%+v", ollama)
	}
	if _, ok := ollama.Models["qwen3-coder"]; !ok {
		t.Fatalf("ollama models=%+v", ollama.Models)
	}
	// Deprecated/disabled models are never offered.
	if _, ok := ollama.Models["mpro"]; ok {
		t.Fatalf("deprecated model offered: %+v", ollama.Models["mpro"])
	}
	if openrouter == nil {
		t.Fatalf("image model's provider missing: %+v", providers)
	}

	// A credential wall surfaces as such and never selects a generation.
	wall := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Basic "+basicFor("opencode", "secret") {
			fmt.Fprint(w, `{"version":"2.2.5","pid":1,"urls":["http://h:1"],"paths":{"tmp":"/t"}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"_tag":"UnauthorizedError","message":"Authentication required"}`)
	}))
	defer wall.Close()
	if _, err := NewOpenCode(wall.URL, "/srv", "", ""); err != nil {
		t.Fatal(err)
	}
	wrongClient, _ := NewOpenCode(wall.URL, "/srv", "", "wrong")
	if _, err := wrongClient.Health(ctx); !errors.Is(err, ErrOpenCodeRequiresCredential) {
		t.Fatalf("wall error=%v", err)
	}
	rightClient, _ := NewOpenCode(wall.URL, "/srv", "", "secret")
	if _, err := rightClient.Health(ctx); err != nil {
		t.Fatalf("authenticated wall health: %v", err)
	}

	// v1 mutation surfaces fail on a v2 server without issuing v1 routes.
	saw = nil
	v2Client, _ := NewOpenCode(server.URL, "/srv", "", "")
	if _, err := v2Client.GlobalSessions(ctx); !errors.Is(err, ErrOpenCodeV2NotMigrated) {
		t.Fatalf("globalsessions err=%v", err)
	}
	if err := v2Client.Prompt(ctx, "ses_x", "hi", ""); !errors.Is(err, ErrOpenCodeV2NotMigrated) {
		t.Fatalf("prompt err=%v", err)
	}
	if err := v2Client.Events(ctx, nil, nil); !errors.Is(err, ErrOpenCodeV2NotMigrated) {
		t.Fatalf("events err=%v", err)
	}
	// The only requests the v2 server saw were v2 info probes: no v1
	// session, event, status, or health route ever left the connector.
	for _, request := range saw {
		switch request {
		case "GET /api/info?",
			"GET /api/provider?",
			"GET /api/model?",
			"GET /api/model?types=image":
		default:
			t.Fatalf("unexpected request against a v2 server: %q (all: %v)", request, saw)
		}
	}
}

func TestOpenCodeV1UsernameProbeOrder(t *testing.T) {
	// A v1 server protected with OPENCODE_SERVER_USERNAME: the fixed v2
	// username hits a JSON wall, and the configured username authenticates.
	var attempts []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, _, ok := r.BasicAuth()
		authenticated := ok && username == "alice"
		attempts = append(attempts, r.Header.Get("Authorization"))
		switch r.URL.Path {
		case "/api/info":
			if !authenticated {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				fmt.Fprint(w, `{"_tag":"UnauthorizedError"}`)
				return
			}
			// A v1.18.x router has no v2 route.
			http.NotFound(w, r)
		case "/global/health":
			if !authenticated {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				fmt.Fprint(w, `{}`)
				return
			}
			fmt.Fprint(w, `{"healthy":true,"version":"1.18.31"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewOpenCode(server.URL, "/srv", "alice", "secret")
	if err != nil {
		t.Fatal(err)
	}
	version, err := client.Health(context.Background())
	if err != nil || version != "1.18.31" {
		t.Fatalf("version=%q err=%v", version, err)
	}
	if len(attempts) < 2 {
		t.Fatalf("attempts=%v", attempts)
	}
	if got := "Basic " + basicFor("opencode", "secret"); attempts[0] != got {
		t.Fatalf("first probe sent %q, want the fixed username", attempts[0])
	}
	if got := "Basic " + basicFor("alice", "secret"); attempts[len(attempts)-1] != got {
		t.Fatalf("last probe sent %q, want the stored username", attempts[len(attempts)-1])
	}
}

func TestOpenCodeV2NothingDecodableIsNotDetected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<!doctype html>")
	}))
	defer server.Close()
	client, err := NewOpenCode(server.URL, "/srv", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Health(context.Background()); err == nil {
		t.Fatal("a server answering only HTML must never be detected")
	}
}

func basicFor(username, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
}
