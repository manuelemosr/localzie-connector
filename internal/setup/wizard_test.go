// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package setup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/ziticonn"
)

func wizardFixture(t *testing.T) (Wizard, Options, *bytes.Buffer) {
	t.Helper()
	dir := t.TempDir()
	options := Options{ConfigPath: filepath.Join(dir, "private", "connector.json"), DataDir: filepath.Join(dir, "private"), Name: "My computer"}
	services := []Service{{Endpoint: ziticonn.Endpoint{ID: "models", Name: "Models", Kind: "openai-compatible", URL: "http://127.0.0.1:1234"}}, {Endpoint: ziticonn.Endpoint{ID: "codex", Name: "Codex", Kind: "codex", Directory: dir}}}
	wizard := Wizard{
		Discover: func(context.Context, []ziticonn.Endpoint, string) ([]Service, error) { return services, nil },
		Select: func(context.Context, io.Reader, io.Writer, []Service, []string, bool) ([]string, error) {
			return []string{"models"}, nil
		},
		Pair: func(config ziticonn.Config, dir, jwt string) (string, error) {
			if len(config.Endpoints) != 1 || config.Endpoints[0].ID != "models" {
				t.Fatal("unselected service was enabled")
			}
			return "LZ1:synthetic", nil
		},
		Serve: func(ctx context.Context, config ziticonn.Config, dir string, ready func() error) error {
			return ready()
		},
	}
	return wizard, options, &bytes.Buffer{}
}

func TestSetupSelectsPersistsPairsAndServesInOneInvocation(t *testing.T) {
	wizard, options, output := wizardFixture(t)
	if err := wizard.Run(context.Background(), strings.NewReader(""), output, options); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(options.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	var saved ziticonn.Config
	if json.Unmarshal(raw, &saved) != nil || len(saved.Endpoints) != 1 || saved.Endpoints[0].ID != "models" {
		t.Fatal("unexpected persisted selection")
	}
	file, _ := os.Stat(options.ConfigPath)
	dir, _ := os.Stat(options.DataDir)
	if file.Mode().Perm() != 0600 || dir.Mode().Perm() != 0700 {
		t.Fatal("setup state is not private")
	}
	if !strings.Contains(output.String(), "LZ1:synthetic") || !strings.Contains(output.String(), "Keep this terminal open") {
		t.Fatal("phone setup or serving instructions missing")
	}
	if strings.Contains(string(raw), "synthetic") {
		t.Fatal("pairing secret persisted in configuration")
	}
}

func TestCancelAndStartFailureKeepPreviousSetupAndDoNotPrintCode(t *testing.T) {
	for _, failure := range []string{"selection", "start", "pair"} {
		t.Run(failure, func(t *testing.T) {
			wizard, options, output := wizardFixture(t)
			if err := os.MkdirAll(options.DataDir, 0700); err != nil {
				t.Fatal(err)
			}
			previous := []byte(`{"name":"Existing home","local_only":true,"endpoints":[{"id":"models","name":"Models","kind":"openai-compatible","url":"http://127.0.0.1:1234"}]}`)
			if err := os.WriteFile(options.ConfigPath, previous, 0600); err != nil {
				t.Fatal(err)
			}
			switch failure {
			case "selection":
				wizard.Select = func(context.Context, io.Reader, io.Writer, []Service, []string, bool) ([]string, error) {
					return nil, context.Canceled
				}
			case "start":
				wizard.Serve = func(context.Context, ziticonn.Config, string, func() error) error { return errors.New("port in use") }
			case "pair":
				wizard.Pair = func(ziticonn.Config, string, string) (string, error) { return "", errors.New("enrollment unavailable") }
			}
			if err := wizard.Run(context.Background(), strings.NewReader(""), output, options); err == nil {
				t.Fatal("expected setup failure")
			}
			after, _ := os.ReadFile(options.ConfigPath)
			if !bytes.Equal(after, previous) {
				t.Fatal("failed setup changed saved connection")
			}
			if strings.Contains(output.String(), "LZ1:") || strings.Contains(output.String(), "Ready on") {
				t.Fatal("failed setup advertised a usable connection")
			}
		})
	}
}

func TestSetupPreservesRemoteHomeAndEndpointIdentity(t *testing.T) {
	wizard, options, output := wizardFixture(t)
	if err := os.MkdirAll(options.DataDir, 0700); err != nil {
		t.Fatal(err)
	}
	previous := []byte(`{"name":"Existing home","identity_file":"home.json","service":"my-home","endpoints":[{"id":"stable-models","instanceID":"existing-instance","name":"Existing models","kind":"openai-compatible","url":"http://127.0.0.1:1234"}]}`)
	if err := os.WriteFile(options.ConfigPath, previous, 0600); err != nil {
		t.Fatal(err)
	}
	wizard.Discover = func(_ context.Context, old []ziticonn.Endpoint, _ string) ([]Service, error) {
		return []Service{{Endpoint: old[0]}}, nil
	}
	wizard.Select = func(_ context.Context, _ io.Reader, _ io.Writer, _ []Service, selected []string, _ bool) ([]string, error) {
		return selected, nil
	}
	wizard.Pair = func(config ziticonn.Config, dir, jwt string) (string, error) {
		if config.LocalOnly || config.Service != "my-home" || config.Endpoints[0].InstanceID != "existing-instance" {
			t.Fatal("remote identity changed")
		}
		return "LZ1:synthetic", nil
	}
	if err := wizard.Run(context.Background(), strings.NewReader(""), output, options); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(options.ConfigPath)
	if !bytes.Contains(raw, []byte(`"identity_file": "home.json"`)) {
		t.Fatal("relative identity path was changed")
	}
}

func TestUnsafeUpstreamIsRejectedBeforePairing(t *testing.T) {
	wizard, options, output := wizardFixture(t)
	wizard.Discover = func(context.Context, []ziticonn.Endpoint, string) ([]Service, error) {
		return []Service{{Endpoint: ziticonn.Endpoint{ID: "models", Name: "Unsafe", Kind: "openai-compatible", URL: "http://example.com:80"}}}, nil
	}
	wizard.Pair = func(ziticonn.Config, string, string) (string, error) {
		t.Fatal("unsafe upstream reached pairing")
		return "", nil
	}
	if err := wizard.Run(context.Background(), strings.NewReader(""), output, options); err == nil {
		t.Fatal("unsafe upstream accepted")
	}
	if _, err := os.Stat(options.ConfigPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unsafe setup was saved")
	}
}

func TestAccessibleCheckboxSelection(t *testing.T) {
	services := []Service{{Endpoint: ziticonn.Endpoint{ID: "one", Name: "First"}}, {Endpoint: ziticonn.Endpoint{ID: "two", Name: "Second"}}}
	output := &bytes.Buffer{}
	selected, err := selectServices(context.Background(), strings.NewReader("1\n0\n"), output, services, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0] != "one" {
		t.Fatalf("unexpected checkbox choices: %v", selected)
	}
}

func TestAccessibleEOFDoesNotSaveOrLoop(t *testing.T) {
	_, err := selectServices(context.Background(), strings.NewReader(""), &bytes.Buffer{}, []Service{{Endpoint: ziticonn.Endpoint{ID: "one", Name: "First"}}}, nil, true)
	if err == nil {
		t.Fatal("closed input must cancel setup")
	}
}

func TestExistingRemotePhoneDoesNotNeedAnotherEnrollment(t *testing.T) {
	dir := t.TempDir()
	_, db, err := ziticonn.OpenState(dir)
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := db.CreateDevice(context.Background(), "Test phone")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	config := ziticonn.Config{Service: "my-home"}
	code, err := preparePair(config, dir, "")
	if err != nil || code != "" {
		t.Fatal("existing phone required a new enrollment")
	}
	_, db, err = ziticonn.OpenState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.RevokeDevice(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if _, err = preparePair(config, dir, ""); err == nil {
		t.Fatal("revoked phone counted as a usable pairing")
	}
}

func TestAccessibleCancellationDoesNotWaitForAnotherLine(t *testing.T) {
	input, writer := io.Pipe()
	defer input.Close()
	defer writer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	reading := make(chan struct{})
	observed := &readStarted{Reader: input, started: reading}
	go func() {
		_, err := selectAccessible(ctx, observed, io.Discard, []Service{{Endpoint: ziticonn.Endpoint{ID: "one", Name: "First"}}}, nil)
		done <- err
	}()
	select {
	case <-reading:
	case <-time.After(time.Second):
		t.Fatal("setup never started reading terminal input")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled setup kept waiting for terminal input")
	}
}

type readStarted struct {
	io.Reader
	started chan struct{}
	once    sync.Once
}

func (r *readStarted) Read(value []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	return r.Reader.Read(value)
}
