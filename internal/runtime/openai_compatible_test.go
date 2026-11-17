// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/manuelemosr/localzie-connector/internal/protocol"
)

func TestOpenAICompatibleModelsAndSSE(t *testing.T) {
	var chatRequest map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer local-secret" {
			t.Errorf("authorization = %q", got)
		}
		switch r.URL.Path {
		case "/v1/models":
			fmt.Fprint(w, `{"data":[{"id":"local-model"}]}`)
		case "/v1/chat/completions":
			if err := json.NewDecoder(r.Body).Decode(&chatRequest); err != nil {
				t.Errorf("decode chat request: %v", err)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"Local\"}}]}\n\n")
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\" answer\"}}]}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	adapter, err := NewOpenAICompatible(server.URL, "local-secret")
	if err != nil {
		t.Fatal(err)
	}
	models, err := adapter.Models(context.Background())
	if err != nil || len(models) != 1 || models[0].ID != "local-model" {
		t.Fatalf("models=%+v err=%v", models, err)
	}
	var output strings.Builder
	err = adapter.Stream(context.Background(), StreamRequest{
		Model:           "local-model",
		Messages:        []protocol.Message{{Role: protocol.RoleUser, Content: "Hello"}},
		MaxOutputTokens: 24,
	}, func(delta string) error {
		output.WriteString(delta)
		return nil
	})
	if err != nil || output.String() != "Local answer" {
		t.Fatalf("output=%q err=%v", output.String(), err)
	}
	if chatRequest["max_tokens"] != float64(24) {
		t.Fatalf("max_tokens = %#v", chatRequest["max_tokens"])
	}
}
