// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package ziticonn

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/coder/websocket"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// This opt-in test crosses the configured OpenZiti fabric; credentials remain in
// private files outside the checkout. It invokes only a synthetic local API.
func TestZitiFabricPinnedTLSAndDetachedGeneration(t *testing.T) {
	home, phone, service := os.Getenv("LOCALZIE_ZITI_TEST_HOME"), os.Getenv("LOCALZIE_ZITI_TEST_PHONE"), os.Getenv("LOCALZIE_ZITI_TEST_SERVICE")
	if home == "" || phone == "" || service == "" {
		t.Skip("OpenZiti test identities not supplied")
	}
	completed := make(chan bool, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/ws" {
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			for {
				kind, message, err := conn.Read(r.Context())
				if err != nil {
					return
				}
				if conn.Write(r.Context(), kind, message) != nil {
					return
				}
			}
		}
		if r.URL.Path == "/v1/chat/completions" {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: first\n\n")
			w.(http.Flusher).Flush()
			select {
			case <-time.After(150 * time.Millisecond):
				completed <- true
			case <-r.Context().Done():
				completed <- false
			}
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		fmt.Fprint(w, `{"data":[{"id":"localzie-ziti-fixture"}]}`)
	}))
	defer upstream.Close()
	dir := t.TempDir()
	id, db, err := OpenState(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	pairing, _, err := db.CreatePairing(context.Background(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Serve(root, Config{IdentityFile: home, Service: service, Endpoints: []Endpoint{{ID: "model", Kind: "openai-compatible", URL: upstream.URL}, {ID: "hermes", Kind: "hermes", URL: upstream.URL}}}, dir)
	}()
	z, err := NewContext(root, phone)
	if err != nil {
		t.Fatal("load phone identity")
	}
	defer z.Close()
	if err = z.Authenticate(); err != nil {
		t.Fatal("phone authentication failed")
	}
	transport := func(pin string) *http.Transport {
		return &http.Transport{DialTLSContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var conn net.Conn
			var err error
			for attempt := 0; attempt < 15; attempt++ {
				conn, err = z.DialContext(ctx, service)
				if err == nil {
					break
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(200 * time.Millisecond):
				}
			}
			if err != nil {
				return nil, fmt.Errorf("dial pilot service: %w", err)
			}
			secure := tls.Client(conn, &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true, VerifyConnection: func(state tls.ConnectionState) error {
				actual := sha256.Sum256(state.PeerCertificates[0].Raw)
				if hex.EncodeToString(actual[:]) != pin {
					return fmt.Errorf("home certificate pin mismatch")
				}
				return nil
			}})
			if err = secure.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, err
			}
			return secure, nil
		}}
	}
	tr := transport(id.Fingerprint)
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 20 * time.Second}
	resp, err := client.Get("https://home/api/model/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatal("unpaired identity was accepted")
	}
	req, _ := http.NewRequest("POST", "https://home/localzie/pair", strings.NewReader(`{"token":"`+pairing+`","name":"integration"}`))
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	if result["token"] == "" {
		t.Fatal("pairing failed", resp.StatusCode)
	}
	req, _ = http.NewRequest("GET", "https://home/api/model/v1/models", nil)
	req.Header.Set("X-Localzie-Token", result["token"])
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), "localzie-ziti-fixture") {
		t.Fatal("native API did not cross the fabric")
	}
	wrong := transport(strings.Repeat("0", 64))
	defer wrong.CloseIdleConnections()
	if r, e := (&http.Client{Transport: wrong, Timeout: 10 * time.Second}).Get("https://home/api/model/v1/models"); e == nil {
		r.Body.Close()
		t.Fatal("wrong home pin accepted")
	}
	chatCtx, detach := context.WithCancel(root)
	req, _ = http.NewRequestWithContext(chatCtx, "POST", "https://home/api/model/v1/chat/completions", strings.NewReader(`{"stream":true}`))
	req.Header.Set("X-Localzie-Token", result["token"])
	req.Header.Set("X-Localzie-Inference", "synthetic-remote-turn")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("inference request was not accepted: HTTP %d", resp.StatusCode)
	}
	first := make([]byte, 5)
	if _, err = io.ReadFull(resp.Body, first); err != nil {
		t.Fatal(err)
	}
	detach()
	resp.Body.Close()
	select {
	case ok := <-completed:
		if !ok {
			t.Fatal("phone detach cancelled accepted inference")
		}
	case <-root.Done():
		t.Fatal("generation did not finish")
	}
	ws, _, err := websocket.Dial(root, "wss://home/api/hermes/api/ws", &websocket.DialOptions{HTTPClient: client, HTTPHeader: http.Header{"X-Localzie-Token": []string{result["token"]}}})
	if err != nil {
		t.Fatal("websocket through private connector:", err)
	}
	defer ws.CloseNow()
	if err = ws.Write(root, websocket.MessageText, []byte("synthetic harness frame")); err != nil {
		t.Fatal(err)
	}
	_, frame, err := ws.Read(root)
	if err != nil || string(frame) != "synthetic harness frame" {
		t.Fatal("websocket frame did not round-trip")
	}
	devices, err := db.ListDevices(root)
	if err != nil || len(devices) != 1 {
		t.Fatal("paired device missing")
	}
	if err = db.RevokeDevice(root, devices[0].ID); err != nil {
		t.Fatal(err)
	}
	revokeCtx, stop := context.WithTimeout(root, 5*time.Second)
	defer stop()
	if _, _, err = ws.Read(revokeCtx); err == nil || revokeCtx.Err() != nil {
		t.Fatal("revoked live websocket was not disconnected promptly")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(12 * time.Second):
		t.Fatal("connector did not shut down")
	}
}
