// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package ziticonn

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDiscoveryKeepsOpenCodeCredentialsHomeAndCodexUsesNoURL(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "opencode")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_STATE_HOME", dir)
	t.Setenv("LOCALZIE_DISCOVERED_OPENCODE_CODE", "")
	if err := os.WriteFile(filepath.Join(state, "service.json"), []byte(`{"url":"http://0.0.0.0:4096","password":"home-only-secret"}`), 0600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "connector.json")
	raw := `{"identity_file":"home.json","service":"home","name":"Home","endpoints":[{"id":"code","name":"OpenCode","kind":"opencode"},{"id":"codex","name":"Codex","kind":"codex","directory":` + strconvQuote(dir) + `}]}`
	if err := os.WriteFile(config, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(config)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Endpoints[0].URL != "http://127.0.0.1:4096" || os.Getenv(loaded.Endpoints[0].PasswordEnv) != "home-only-secret" {
		t.Fatal("local discovery did not resolve the service")
	}
	jwt := filepath.Join(dir, "phone.jwt")
	_ = os.WriteFile(jwt, []byte("header.claims.signature"), 0600)
	code, err := Pair(loaded, filepath.Join(dir, "state"), jwt)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(code, "LZ1:"))
	for _, private := range []string{"home-only-secret", dir, "127.0.0.1", "password_env", "LOCALZIE_DISCOVERED"} {
		if strings.Contains(string(payload), private) {
			t.Fatalf("private value leaked in pairing payload: %q", private)
		}
	}
	loaded.Endpoints[1].URL = "http://127.0.0.1:4000"
	bad, _ := json.Marshal(loaded)
	_ = os.WriteFile(config, bad, 0600)
	if _, err := Load(config); err == nil {
		t.Fatal("Codex accepted an arbitrary server URL")
	}
}
func strconvQuote(value string) string { b, _ := json.Marshal(value); return string(b) }
