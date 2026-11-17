// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package ziticonn

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPinnedApplicationAuthorizationAndLocalAllowlist(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("X-Localzie-Token") != "" {
			t.Error("device token reached upstream")
		}
		if r.Header.Get("Authorization") != "Bearer local-only-key" {
			t.Error("local environment credential was not applied")
		}
		if r.URL.Path != "/v1/models" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.Write([]byte(`{"data":[]}`))
	}))
	defer upstream.Close()
	t.Setenv("LOCALZIE_TEST_KEY", "local-only-key")
	_, db, err := OpenState(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	config := Config{Service: "home", Endpoints: []Endpoint{{ID: "models", Kind: "openai-compatible", URL: upstream.URL, APIKeyEnv: "LOCALZIE_TEST_KEY"}}}
	handler := Handler(context.Background(), config, db)
	request := func(method, path, token, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("X-Localzie-Token", token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if w := request("GET", "/api/models/v1/models", "controller-authorized-but-unpaired", ""); w.Code != 401 || calls.Load() != 0 {
		t.Fatal("Ziti-only caller reached home API")
	}
	secret, _, err := db.CreatePairing(context.Background(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]string{"token": secret, "name": "phone"})
	paired := request("POST", "/localzie/pair", "", string(payload))
	if paired.Code != 200 {
		t.Fatal(paired.Code)
	}
	var response map[string]string
	json.Unmarshal(paired.Body.Bytes(), &response)
	if w := request("POST", "/localzie/pair", "", string(payload)); w.Code != 401 {
		t.Fatal("pairing secret replayed")
	}
	if w := request("GET", "/api/models/v1/models", response["token"], ""); w.Code != 200 || calls.Load() != 1 {
		t.Fatal("paired request failed", w.Code)
	}
	if w := request("GET", "/api/arbitrary/v1/models", response["token"], ""); w.Code != 404 || calls.Load() != 1 {
		t.Fatal("unconfigured service reached upstream")
	}
	devices, _ := db.ListDevices(context.Background())
	if len(devices) != 1 {
		t.Fatal(devices)
	}
	if err := db.RevokeDevice(context.Background(), devices[0].ID); err != nil {
		t.Fatal(err)
	}
	if w := request("GET", "/api/models/v1/models", response["token"], ""); w.Code != 401 || calls.Load() != 1 {
		t.Fatal("revoked phone reached home API")
	}
}

func TestConfigurationRejectsNetworkWideDestinationsAndSecrets(t *testing.T) {
	for _, address := range []string{"http://example.com:80", "http://192.168.1.1:80", "http://localhost:1234", "http://user:secret@127.0.0.1:1234", "http://127.0.0.1:1234?target=other"} {
		t.Run(address, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "connector.json")
			b, _ := json.Marshal(Config{IdentityFile: "identity.json", Name: "home", Service: "home", Endpoints: []Endpoint{{ID: "models", Name: "Models", Kind: "openai-compatible", URL: address}}})
			os.WriteFile(path, b, 0600)
			if _, err := Load(path); err == nil {
				t.Fatal("unsafe destination accepted")
			}
		})
	}
}

func TestPairingBundleDoesNotDiscloseUpstreamDetails(t *testing.T) {
	dir := t.TempDir()
	jwt := filepath.Join(dir, "phone.jwt")
	os.WriteFile(jwt, []byte("header.claims.signature"), 0600)
	code, err := Pair(Config{Name: "home", Service: "home", Endpoints: []Endpoint{{ID: "models", Name: "Models", Kind: "openai-compatible", URL: "http://127.0.0.1:1234", APIKeyEnv: "PRIVATE_ENV"}}}, dir, jwt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(code, "LZ1:") {
		t.Fatal("no version marker")
	}
	// The endpoint public projection intentionally omits credential source names.
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(code, "LZ1:"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte("PRIVATE_ENV")) || bytes.Contains(b, []byte("127.0.0.1")) || bytes.Contains(b, []byte("api_key_env")) {
		t.Fatal("private endpoint config leaked")
	}
}

func TestHermesPasswordLoginUsesOnlyHomeCredentials(t *testing.T) {
	t.Setenv("LOCALZIE_TEST_HERMES_PASSWORD", "home-only-password")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var form map[string]string
		if err := json.NewDecoder(r.Body).Decode(&form); err != nil {
			t.Error(err)
		}
		if form["username"] != "home-owner" || form["password"] != "home-only-password" || form["provider"] != "basic" {
			t.Error("incorrect native Hermes login form")
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("X-Localzie-Token") != "" {
			t.Error("transport credentials leaked to Hermes")
		}
		w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()
	_, db, err := OpenState(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, token, err := db.CreateDevice(context.Background(), "phone")
	if err != nil {
		t.Fatal(err)
	}
	h := Handler(context.Background(), Config{Endpoints: []Endpoint{{ID: "hermes", Kind: "hermes", URL: upstream.URL, Username: "home-owner", PasswordEnv: "LOCALZIE_TEST_HERMES_PASSWORD"}}}, db)
	r := httptest.NewRequest("POST", "/api/hermes/auth/password-login", strings.NewReader(`{"provider":"basic","username":"placeholder","password":""}`))
	r.Header.Set("X-Localzie-Token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
}
