// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/manuelemosr/localzie-connector/internal/protocol"
	"github.com/manuelemosr/localzie-connector/internal/runtime"
)

type staticRuntime struct{}

func (staticRuntime) Name() string { return "lazy" }
func (staticRuntime) Models(context.Context) ([]protocol.Model, error) {
	return []protocol.Model{{ID: "lazy-model", Name: "Lazy model", Runtime: "lazy", Ready: true}}, nil
}
func (staticRuntime) Stream(context.Context, runtime.StreamRequest, func(string) error) error {
	return nil
}

func TestExplicitRuntimeOverridesAutoDiscovery(t *testing.T) {
	adapter, err := makeRuntime("openai-compatible", "http://127.0.0.1:9999", "")
	if err != nil {
		t.Fatal(err)
	}
	if adapter.Name() != "openai-compatible" {
		t.Fatalf("runtime = %q", adapter.Name())
	}
	adapter, err = makeRuntime("auto", "http://127.0.0.1:9999", "")
	if err != nil {
		t.Fatal(err)
	}
	if adapter.Name() != "openai-compatible" {
		t.Fatalf("auto URL override runtime = %q", adapter.Name())
	}
}

func TestLazyRuntimeRecoversWhenRuntimeAppearsLater(t *testing.T) {
	runtimeReady := false
	attempts := 0
	lazy := newLazyRuntime("no supported local runtime found", func() (runtime.Adapter, error) {
		attempts++
		if !runtimeReady {
			return nil, errors.New("no supported local runtime found")
		}
		return staticRuntime{}, nil
	})
	lazy.retryTTL = 0
	ctx := context.Background()

	if _, err := lazy.Models(ctx); err == nil {
		t.Fatal("expected failure before the runtime appears")
	}
	if lazy.Name() != "unavailable" {
		t.Fatalf("pre-discovery name = %q", lazy.Name())
	}

	runtimeReady = true
	models, err := lazy.Models(ctx)
	if err != nil || len(models) != 1 || models[0].ID != "lazy-model" {
		t.Fatalf("models=%v err=%v", models, err)
	}
	if lazy.Name() != "lazy" {
		t.Fatalf("post-discovery name = %q", lazy.Name())
	}

	// A resolved adapter is pinned even if discovery would fail again.
	runtimeReady = false
	if _, err := lazy.Models(ctx); err != nil {
		t.Fatalf("resolved adapter must be reused: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("discovery attempts = %d", attempts)
	}

	if err := lazy.Stream(ctx, runtime.StreamRequest{}, func(string) error { return nil }); err != nil {
		t.Fatalf("stream through pinned adapter: %v", err)
	}
}

func TestLazyRuntimeRespectsRetryTTL(t *testing.T) {
	attempts := 0
	lazy := newLazyRuntime("none", func() (runtime.Adapter, error) {
		attempts++
		return nil, errors.New("none")
	})
	ctx := context.Background()
	if _, err := lazy.Models(ctx); err == nil {
		t.Fatal("expected first failure")
	}
	if _, err := lazy.Models(ctx); err == nil {
		t.Fatal("expected second failure")
	}
	if attempts != 1 {
		t.Fatalf("TTL must gate retries, attempts = %d", attempts)
	}
}

func TestAdvertisedAddressesRespectSpecificListenHost(t *testing.T) {
	got := advertisedAddresses("127.0.0.1:9443", 9443)
	want := []string{"https://127.0.0.1:9443"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("advertised addresses = %q, want %q", got, want)
	}
}

func TestListenPort(t *testing.T) {
	port, err := listenPort(":9443")
	if err != nil || port != 9443 {
		t.Fatalf("listen port = %d, error = %v", port, err)
	}
}

func TestOpenCodeFlagsRequireEachOther(t *testing.T) {
	if _, err := makeOpenCodeClient("http://127.0.0.1:4096", "", "", ""); err == nil || !strings.Contains(err.Error(), "--harness-dir") {
		t.Fatalf("URL without directory error = %v", err)
	}
	if _, err := makeOpenCodeClient("", "", "", "/tmp"); err == nil || !strings.Contains(err.Error(), "--opencode-url") {
		t.Fatalf("directory without URL error = %v", err)
	}
	client, err := makeOpenCodeClient("", "", "", "")
	if err != nil || client != nil {
		t.Fatalf("no flags = (%v, %v), want (nil, nil)", client, err)
	}
}
