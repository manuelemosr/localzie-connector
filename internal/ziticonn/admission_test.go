// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package ziticonn

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/netutil"
)

type admissionBody struct {
	io.ReadCloser
	once    sync.Once
	started chan<- struct{}
}

func (b *admissionBody) Read(p []byte) (int, error) {
	b.once.Do(func() { b.started <- struct{}{} })
	return b.ReadCloser.Read(p)
}

func stalledRequest(t *testing.T, address, path string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if _, err = fmt.Fprintf(c, "POST %s HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\nContent-Type: application/json\r\n\r\n{", path); err != nil {
		t.Fatal(err)
	}
	return c
}
func readAdmissionResponse(t *testing.T, c net.Conn, timeout time.Duration) *http.Response {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(timeout))
	r, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Body.Close() })
	return r
}
func TestStalledPairingsLeaveCapacityForAuthenticatedRequests(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }))
	defer upstream.Close()
	_, db, err := OpenState(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, token, err := db.CreateDevice(context.Background(), "paired test phone")
	if err != nil {
		t.Fatal(err)
	}
	handler := Handler(context.Background(), Config{Endpoints: []Endpoint{{ID: "model", Kind: "openai-compatible", URL: upstream.URL}}}, db)
	started := make(chan struct{}, maxPendingPairings)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = &admissionBody{ReadCloser: r.Body, started: started}
		handler.ServeHTTP(w, r)
	}))
	server.Listener = netutil.LimitListener(server.Listener, 64)
	server.Start()
	defer server.Close()
	address := strings.TrimPrefix(server.URL, "http://")
	for i := 0; i < maxPendingPairings; i++ {
		stalledRequest(t, address, "/localzie/pair")
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("pairing body read did not start")
		}
	}
	// Keep the excess clients open after rejection. A body-drain bug would let
	// these consume the remaining 60 connections despite the handler's limit.
	for i := maxPendingPairings; i < 64; i++ {
		c := stalledRequest(t, address, "/localzie/pair")
		r := readAdmissionResponse(t, c, time.Second)
		if r.StatusCode != http.StatusTooManyRequests || !r.Close {
			t.Fatalf("excess pairing: status %d close=%v", r.StatusCode, r.Close)
		}
	}
	client := &http.Client{Timeout: time.Second}
	request, _ := http.NewRequest("GET", server.URL+"/api/model/health", nil)
	request.Header.Set("X-Localzie-Token", token)
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("paired request starved: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal(response.Status)
	}
}
func TestPairingBodyDeadlineAndUnauthenticatedDrain(t *testing.T) {
	_, db, err := OpenState(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	server := httptest.NewServer(Handler(context.Background(), Config{}, db))
	defer server.Close()
	address := strings.TrimPrefix(server.URL, "http://")
	c := stalledRequest(t, address, "/localzie/pair")
	if response := readAdmissionResponse(t, c, pairingBodyTimeout+time.Second); response.StatusCode != http.StatusRequestTimeout || !response.Close {
		t.Fatalf("pairing timeout: %d close=%v", response.StatusCode, response.Close)
	}
	// Invalid API authentication must reject immediately without draining its
	// unfinished body, rather than occupying another connection until timeout.
	unauthorized := stalledRequest(t, address, "/api/model/health")
	if response := readAdmissionResponse(t, unauthorized, time.Second); response.StatusCode != http.StatusUnauthorized || !response.Close {
		t.Fatalf("unauthorized: %d close=%v", response.StatusCode, response.Close)
	}
}

func TestAuthenticatedUploadClearsAdmissionDeadline(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "body read failed", 400)
			return
		}
		if string(body) != "slow authenticated body" {
			t.Errorf("body=%q", body)
		}
		w.Write([]byte("ok"))
	}))
	defer upstream.Close()
	_, db, err := OpenState(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, token, err := db.CreateDevice(context.Background(), "paired test phone")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(Handler(context.Background(), Config{Endpoints: []Endpoint{{ID: "model", Kind: "openai-compatible", URL: upstream.URL}}}, db))
	server.Config.ReadTimeout = 100 * time.Millisecond
	server.Start()
	defer server.Close()
	reader, writer := io.Pipe()
	defer reader.Close()
	request, _ := http.NewRequest("POST", server.URL+"/api/model/slow", reader)
	request.Header.Set("X-Localzie-Token", token)
	go func() {
		time.Sleep(300 * time.Millisecond)
		_, _ = io.WriteString(writer, "slow authenticated body")
		writer.Close()
	}()
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("authenticated body inherited admission deadline: %s", response.Status)
	}
}
