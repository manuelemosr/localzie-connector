// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package ziticonn

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestExplicitInferenceStopSurvivesDetachAndIsDeviceScoped(t *testing.T) {
	root, cancelRoot := context.WithCancel(context.Background())
	done := make(chan struct{})
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("X-Localzie-Inference") != "" || r.Header.Get("X-Localzie-Token") != "" {
			t.Error("internal inference credentials reached upstream")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, ": connected\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(done)
	}))
	t.Cleanup(func() { cancelRoot(); upstream.Close() })
	_, db, err := OpenState(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	_, owner, err := db.CreateDevice(root, "owner")
	if err != nil {
		t.Fatal(err)
	}
	_, other, err := db.CreateDevice(root, "other")
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(Handler(root, Config{Endpoints: []Endpoint{{ID: "models", Kind: "openai-compatible", URL: upstream.URL}}}, db))
	t.Cleanup(func() { cancelRoot(); front.Close() })
	stop := func(token, id string) int {
		t.Helper()
		request, _ := http.NewRequest("POST", front.URL+"/localzie/inference/models/"+id+"/stop", nil)
		request.Header.Set("X-Localzie-Token", token)
		response, e := (&http.Client{Timeout: time.Second}).Do(request)
		if e != nil {
			t.Fatal(e)
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, response.Body)
		return response.StatusCode
	}
	clientContext, detach := context.WithCancel(context.Background())
	defer detach()
	request, _ := http.NewRequestWithContext(clientContext, "POST", front.URL+"/api/models/v1/chat/completions", strings.NewReader(`{}`))
	request.Header.Set("X-Localzie-Token", owner)
	request.Header.Set("X-Localzie-Inference", "operation-1")
	response, err := (&http.Client{Timeout: 3 * time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 {
		t.Fatal(response.Status)
	}
	detach()
	response.Body.Close()
	select {
	case <-done:
		t.Fatal("phone detach cancelled accepted inference")
	case <-time.After(80 * time.Millisecond):
	}
	if status := stop("invalid-token", "operation-1"); status != 401 {
		t.Fatal(status)
	}
	if status := stop(other, "operation-1"); status != 200 {
		t.Fatal(status)
	}
	select {
	case <-done:
		t.Fatal("another device stopped the owner's inference")
	case <-time.After(80 * time.Millisecond):
	}
	if status := stop(owner, "operation-1"); status != 200 {
		t.Fatal(status)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("explicit Stop did not cancel upstream")
	}
	if status := stop(owner, "operation-1"); status != 200 {
		t.Fatal("Stop was not idempotent", status)
	}
	// Stop may arrive before URLSession sends the first planner request.
	if status := stop(owner, "not-started"); status != 200 {
		t.Fatal(status)
	}
	late, _ := http.NewRequest("POST", front.URL+"/api/models/v1/chat/completions", strings.NewReader(`{}`))
	late.Header.Set("X-Localzie-Token", owner)
	late.Header.Set("X-Localzie-Inference", "not-started")
	rejected, err := http.DefaultClient.Do(late)
	if err != nil {
		t.Fatal(err)
	}
	defer rejected.Body.Close()
	if rejected.StatusCode != 409 || calls.Load() != 1 {
		t.Fatalf("stopped operation restarted: status %d calls %d", rejected.StatusCode, calls.Load())
	}
}
