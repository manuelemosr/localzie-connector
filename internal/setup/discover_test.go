// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package setup

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/manuelemosr/localzie-connector/internal/ziticonn"
)

func TestHealthProbeRequiresCorrectSchemaAndDoesNotFollowRedirect(t *testing.T) {
	for _, test := range []struct {
		name, body string
		status     int
		want       bool
	}{{"healthy", `{"healthy":true}`, 200, true}, {"wrong API", `{"ok":true}`, 200, false}, {"not healthy", `{"healthy":false}`, 200, false}, {"unauthorized", `{"healthy":true}`, 401, false}} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(test.status); fmt.Fprint(w, test.body) }))
			defer server.Close()
			if got := health(context.Background(), server.URL, "healthy", "", ""); got != test.want {
				t.Fatalf("health=%v", got)
			}
		})
	}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("followed redirect") }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 302) }))
	defer redirect.Close()
	if health(context.Background(), redirect.URL, "healthy", "", "") {
		t.Fatal("accepted redirect")
	}
}

func TestDiscoveryRetainsConfiguredIDsAndDoesNotDuplicateRuntime(t *testing.T) {
	existing := Service{Endpoint: ziticonn.Endpoint{ID: "saved", Name: "Saved models", Kind: "openai-compatible", URL: "http://127.0.0.1:1234", InstanceID: "persistent"}}
	found := map[string]Service{"saved": existing}
	addService(found, Service{Endpoint: ziticonn.Endpoint{ID: "new", Name: "LM Studio", Kind: "openai-compatible", URL: "http://127.0.0.1:1234"}})
	if len(found) != 1 || found["saved"].Endpoint.InstanceID != "persistent" {
		t.Fatal("discovery replaced the configured runtime identity")
	}
}

func TestDescriptorMustPointToLocalMachine(t *testing.T) {
	for _, raw := range []string{"https://example.com:1234", "http://user:secret@127.0.0.1:1234", "http://127.0.0.1:1234?next=elsewhere", "http://127.0.0.1:99999"} {
		if loopbackURL(raw) {
			t.Fatalf("accepted %s", raw)
		}
	}
	for _, raw := range []string{"http://127.0.0.1:4096", "http://localhost:4096", "http://0.0.0.0:4096", "http://[::1]:4096"} {
		if !loopbackURL(raw) {
			t.Fatalf("rejected %s", raw)
		}
	}
}
