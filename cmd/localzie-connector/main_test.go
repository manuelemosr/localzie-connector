// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise the published CLI without a real runtime or OpenZiti credentials.
func TestTerminalPairingProducesCopyableCodeOnly(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "connector.json")
	err := os.WriteFile(config, []byte(`{"name":"Test home","local_only":true,"endpoints":[{"id":"models","name":"Models","kind":"openai-compatible","url":"http://127.0.0.1:1234"}]}`), 0600)
	if err != nil {
		t.Fatal(err)
	}
	oldArgs, oldOut := os.Args, os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Args, os.Stdout = oldArgs, oldOut; reader.Close(); writer.Close() })
	os.Args = []string{"localzie-connector", "pair", "--config", config, "--data-dir", filepath.Join(dir, "state")}
	os.Stdout = writer
	if err := run(); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	os.Stdout = oldOut
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[1], "LZ1:") {
		t.Fatal("expected instructions and a single copyable setup-code line")
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(lines[1], "LZ1:"))
	if err != nil {
		t.Fatal(err)
	}
	var code struct {
		Name       string `json:"name"`
		Token      string `json:"token"`
		Enrollment string `json:"enrollment"`
	}
	if json.Unmarshal(raw, &code) != nil || code.Name != "Test home" || code.Token == "" || code.Enrollment != "" {
		t.Fatal("invalid local setup bundle")
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".png") {
			t.Fatal("terminal pairing generated a QR image")
		}
	}
}

func TestInvalidCommandsDoNotInitializeState(t *testing.T) {
	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })
	for _, args := range [][]string{{"unknown"}, {"pair", "--qr", "unexpected.png"}, {"serve", "unexpected"}} {
		os.Args = append([]string{"localzie-connector"}, args...)
		if err := run(); err == nil {
			t.Fatalf("invalid arguments accepted: %v", args)
		}
	}
}
