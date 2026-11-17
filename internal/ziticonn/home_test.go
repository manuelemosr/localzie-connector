// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package ziticonn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	qrcode "github.com/skip2/go-qrcode"
)

func TestPilotRemoteSetupBundleFitsOneQRWithoutLANHints(t *testing.T) {
	dir := t.TempDir()
	jwt := filepath.Join(dir, "phone.jwt")
	// The pilot's enrollment JWT is 1,011 bytes. Larger bundles retain the
	// printed terminal-code fallback rather than claiming unbounded QR capacity.
	if err := os.WriteFile(jwt, []byte("header."+strings.Repeat("a", 1024)+".signature"), 0600); err != nil {
		t.Fatal(err)
	}
	code, err := Pair(Config{Name: "My home computer", Service: "home", Endpoints: []Endpoint{
		{ID: "opencode", Name: "Mac OpenCode", Kind: "opencode"},
		{ID: "codex", Name: "Mac Codex", Kind: "codex"},
	}}, dir, jwt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := qrcode.Encode(code, qrcode.Low, 1024); err != nil {
		t.Fatal("ordinary remote setup cannot be scanned", err)
	}
	if strings.Contains(code, "\n") {
		t.Fatal("setup code must remain one copyable line")
	}
}

func TestHomeTrustSurvivesLeafRenewalAndRejectsOtherHomes(t *testing.T) {
	dir := t.TempDir()
	id, db, e := OpenState(dir)
	if e != nil {
		t.Fatal(e)
	}
	db.Close()
	legacy, e := tls.LoadX509KeyPair(id.CertificatePath, id.PrivateKeyPath)
	if e != nil {
		t.Fatal(e)
	}
	cfg, e := homeTLS(dir, legacy)
	if e != nil {
		t.Fatal(e)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "home-ca.crt"))
	block, _ := pem.Decode(data)
	anchor, e := x509.ParseCertificate(block.Bytes)
	if e != nil {
		t.Fatal(e)
	}
	pool := x509.NewCertPool()
	pool.AddCert(anchor)
	leaf, e := cfg.GetCertificate(&tls.ClientHelloInfo{ServerName: "localhost"})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = leaf.Leaf.Verify(x509.VerifyOptions{DNSName: "localhost", Roots: pool}); e != nil {
		t.Fatal(e)
	}
	if _, e = leaf.Leaf.Verify(x509.VerifyOptions{DNSName: "attacker.local", Roots: pool}); e == nil {
		t.Fatal("wrong hostname trusted")
	}
	if time.Until(leaf.Leaf.NotAfter) > 91*24*time.Hour {
		t.Fatal("leaf lifetime too long")
	}
	leaf.Leaf.NotAfter = time.Now().Add(-time.Hour)
	renewed, e := cfg.GetCertificate(&tls.ClientHelloInfo{ServerName: "localhost"})
	if e != nil {
		t.Fatal(e)
	}
	if renewed == leaf {
		t.Fatal("expired leaf not renewed")
	}
	if _, e = renewed.Leaf.Verify(x509.VerifyOptions{DNSName: "localhost", Roots: pool}); e != nil {
		t.Fatal(e)
	}
	old, e := cfg.GetCertificate(&tls.ClientHelloInfo{})
	if e != nil || string(old.Certificate[0]) != string(legacy.Certificate[0]) {
		t.Fatal("old client compatibility lost")
	}
	restarted, e := homeTLS(dir, legacy)
	if e != nil {
		t.Fatal(e)
	}
	after, e := restarted.GetCertificate(&tls.ClientHelloInfo{ServerName: "localhost"})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = after.Leaf.Verify(x509.VerifyOptions{DNSName: "localhost", Roots: pool}); e != nil {
		t.Fatal("home trust lost across restart", e)
	}
}

func TestRequestRecoveryNeverDispatchesTwice(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("first"))
		w.(http.Flusher).Flush()
		<-release
		w.Write([]byte("last"))
	}))
	defer upstream.Close()
	dir := t.TempDir()
	_, db, e := OpenState(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	_, token, e := db.CreateDevice(context.Background(), "phone")
	if e != nil {
		t.Fatal(e)
	}
	c := Config{stateDir: dir, Endpoints: []Endpoint{{ID: "models", Kind: "openai-compatible", URL: upstream.URL}}}
	handler := Handler(context.Background(), c, db)
	server := httptest.NewServer(handler)
	defer server.Close()
	request := func(body string) *http.Response {
		r, _ := http.NewRequest("POST", server.URL+"/api/models/v1/chat/completions", strings.NewReader(body))
		r.Header.Set("X-Localzie-Token", token)
		r.Header.Set("X-Localzie-Request", "request-one")
		r.Header.Set("X-Localzie-Inference", "turn-one")
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	first := request(`{"prompt":"hello"}`)
	b := make([]byte, 5)
	if _, e = io.ReadFull(first.Body, b); e != nil {
		t.Fatal(e)
	}
	first.Body.Close()
	close(release)
	second := request(`{"prompt":"hello"}`)
	body, e := io.ReadAll(second.Body)
	second.Body.Close()
	if e != nil || string(body) != "firstlast" || calls.Load() != 1 {
		t.Fatal("reply duplicated or lost", string(body), calls.Load(), e)
	}
	conflict := request(`{"prompt":"different"}`)
	conflict.Body.Close()
	if conflict.StatusCode != 409 || calls.Load() != 1 {
		t.Fatal("request ID collision dispatched")
	}
	// A fresh handler/process can return the completed receipt without forwarding.
	restored := httptest.NewServer(Handler(context.Background(), c, db))
	defer restored.Close()
	r, _ := http.NewRequest("POST", restored.URL+"/api/models/v1/chat/completions", strings.NewReader(`{"prompt":"hello"}`))
	r.Header.Set("X-Localzie-Token", token)
	r.Header.Set("X-Localzie-Request", "request-one")
	r.Header.Set("X-Localzie-Inference", "turn-one")
	r.Header.Set("X-Localzie-After", "5")
	response, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	body, _ = io.ReadAll(response.Body)
	if string(body) != "last" || calls.Load() != 1 {
		t.Fatal("restart replay failed", string(body), calls.Load())
	}
}

func TestRemoteUpgradeKeepsTheSameAuthenticatedPhone(t *testing.T) {
	_, db, e := OpenState(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	id, token, e := db.CreateDevice(context.Background(), "phone")
	if e != nil {
		t.Fatal(e)
	}
	secret, _, e := db.CreatePairing(context.Background(), time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	after, credential, e := db.UpgradePairing(context.Background(), secret, "phone", token)
	if e != nil || after != id || credential != token {
		t.Fatal("controller identity changed", e)
	}
	if _, _, e = db.UpgradePairing(context.Background(), secret, "phone", token); e == nil {
		t.Fatal("pairing secret reused")
	}
	phones, _ := db.ListDevices(context.Background())
	if len(phones) != 1 {
		t.Fatal("extra phone created")
	}
}

func TestServeReadyRequiresSuccessfulStartup(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	called := false
	config := Config{LocalOnly: true, Name: "Home", Endpoints: []Endpoint{{ID: "codex", Name: "Codex", Kind: "codex", Directory: t.TempDir()}}}
	err := ServeWithReady(context.Background(), config, t.TempDir(), func() error { called = true; return nil })
	if err == nil || called {
		t.Fatal("advertised readiness before the selected harness could start")
	}
}
