// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestDefaultDiscoveryTargets(t *testing.T) {
	got := make([]string, 0, len(discoveryTargets))
	for _, target := range discoveryTargets {
		got = append(got, target.runtime+" "+target.base)
	}
	want := []string{
		"openai-compatible http://127.0.0.1:11434",
		"openai-compatible http://127.0.0.1:1234",
		"openai-compatible http://127.0.0.1:8080",
		"openai-compatible http://127.0.0.1:8000",
		"openai-compatible http://127.0.0.1:1337",
		"openai-compatible http://127.0.0.1:6767",
		"openai-compatible http://127.0.0.1:5000",
		"openai-compatible http://127.0.0.1:5001",
		"openai-compatible http://127.0.0.1:4891",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("discovery targets = %#v, want %#v", got, want)
	}
}

func TestOpenAIModelsSchemaValidation(t *testing.T) {
	for _, test := range []struct {
		body  string
		valid bool
	}{
		{`{"object":"list","data":[{"id":"model-a"}]}`, true},
		{`{"data":[]}`, true},
		{`{"object":"not-list","data":[{"id":"model-a"}]}`, false},
		{`{"data":[{}]}`, false},
		{`{"data":{"id":"model-a"}}`, false},
		{`not json`, false},
	} {
		if got := validOpenAIModelsSchema([]byte(test.body)); got != test.valid {
			t.Errorf("%s: got %v", test.body, got)
		}
	}
}

func TestDiscoveryRejectsRedirectAndOversizedResponses(t *testing.T) {
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://example.invalid", http.StatusFound)
	}))
	defer redirect.Close()
	if probeOpenAICompatible(context.Background(), redirect.URL, "") {
		t.Fatal("accepted redirect")
	}
	large := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(make([]byte, discoveryBodyLimit+1)) }))
	defer large.Close()
	if probeOpenAICompatible(context.Background(), large.URL, "") {
		t.Fatal("accepted oversized response")
	}
}

func TestDiscoveryOnlyAllowsLiteralLoopback(t *testing.T) {
	for _, raw := range []string{"http://localhost:1234", "http://127.0.0.2:1234", "https://127.0.0.1:1234", "http://127.0.0.1:1234/path"} {
		if validDiscoveryBase(raw) {
			t.Errorf("accepted %q", raw)
		}
	}
	if !validDiscoveryBase("http://127.0.0.1:1234") {
		t.Fatal("rejected allowed endpoint")
	}
}

func TestDiscoveryCollapsesDuplicateRuntimeAndURLTargets(t *testing.T) {
	original := discoveryTargets
	t.Cleanup(func() { discoveryTargets = original })
	server := openAIModelServer(t, "model-a")
	defer server.Close()
	discoveryTargets = []struct{ runtime, base string }{{"openai-compatible", server.URL}, {"openai-compatible", server.URL}}
	got, err := Discover(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != (Candidate{Runtime: "openai-compatible", URL: server.URL}) {
		t.Fatalf("candidates = %#v", got)
	}
}

func TestProbeDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(100 * time.Millisecond) }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if probeOpenAICompatible(ctx, server.URL, "") {
		t.Fatal("accepted expired probe")
	}
}

func TestDiscoverNoneOneAndMultipleInDeterministicOrder(t *testing.T) {
	original := discoveryTargets
	t.Cleanup(func() { discoveryTargets = original })
	bad := httptest.NewServer(http.NotFoundHandler())
	defer bad.Close()
	discoveryTargets = []struct{ runtime, base string }{{"openai-compatible", bad.URL}}
	if got, _ := Discover(context.Background(), ""); len(got) != 0 {
		t.Fatalf("none = %#v", got)
	}

	good := openAIModelServer(t, "model-a")
	defer good.Close()
	discoveryTargets = []struct{ runtime, base string }{{"openai-compatible", good.URL}}
	if got, _ := Discover(context.Background(), ""); len(got) != 1 || got[0].URL != good.URL {
		t.Fatalf("one = %#v", got)
	}

	good2 := openAIModelServer(t, "model-b")
	defer good2.Close()
	discoveryTargets = []struct{ runtime, base string }{{"openai-compatible", good2.URL}, {"openai-compatible", good.URL}}
	got, err := Discover(context.Background(), "")
	if err != nil || len(got) != 2 {
		t.Fatalf("multiple = %#v, %v", got, err)
	}
	if got[0].URL > got[1].URL {
		t.Fatalf("not sorted: %#v", got)
	}
}

func TestDiscoveryAuthRequiredNeedsConfiguredKey(t *testing.T) {
	original := discoveryTargets
	t.Cleanup(func() { discoveryTargets = original })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"object":"list","data":[]}`))
	}))
	defer server.Close()
	discoveryTargets = []struct{ runtime, base string }{{"openai-compatible", server.URL}}
	if got, _ := Discover(context.Background(), ""); len(got) != 0 {
		t.Fatalf("accepted unauthenticated: %#v", got)
	}
	if got, _ := Discover(context.Background(), "test-key"); len(got) != 1 {
		t.Fatalf("rejected configured key: %#v", got)
	}
}

func openAIModelServer(t *testing.T, id string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"` + id + `"}]}`))
	}))
}
