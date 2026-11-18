// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package setup

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/ziticonn"
	"github.com/openziti/sdk-golang/ziti"
	"github.com/openziti/sdk-golang/ziti/enroll"
)

func TestRemoteSetupDoesNotPrintCodeBeforeBinding(t *testing.T) {
	wizard, options, output := wizardFixture(t)
	options.Remote = true
	if err := os.MkdirAll(options.DataDir, 0700); err != nil {
		t.Fatal(err)
	}
	privateFixture(t, options.DataDir, "remote-preview.json", []byte(`{"url":"https://example.test/phone-enrollment","service":"my-home","grant":"`+strings.Repeat("a", 64)+`"}`))
	privateFixture(t, options.DataDir, "remote-preview-home.json", []byte(`{}`))
	wizard.Serve = func(_ context.Context, config ziticonn.Config, dir string, ready func() error) error {
		if config.LocalOnly || !config.RequireRemoteReady || config.Service != "my-home" {
			t.Fatal("managed setup did not require a registered remote listener")
		}
		return context.DeadlineExceeded
	}
	err := wizard.Run(context.Background(), strings.NewReader(""), output, options)
	if err == nil || strings.Contains(output.String(), "LZ1:") {
		t.Fatal("unavailable remote service advertised a pairing code")
	}
	if _, err := os.Stat(options.ConfigPath); !os.IsNotExist(err) {
		t.Fatal("failed setup persisted a changed configuration")
	}
}

func TestPhoneEnrollmentRejectsMismatchedService(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost || req.Header.Get("Authorization") != "Bearer "+strings.Repeat("a", 64) {
			t.Error("enrollment used the wrong credentials or method")
		}
		_, _ = io.WriteString(w, `{"service":"different-home","jwt":"header.payload.signature"}`)
	}))
	defer server.Close()
	preview := remotePreview{URL: server.URL, Service: "my-home", Grant: strings.Repeat("a", 64)}
	if _, err := preview.phoneJWT(context.Background(), server.Client()); err == nil {
		t.Fatal("possible cross-home enrollment accepted")
	}
}

// This opt-in test pairs a brand new phone identity using the setup wizard's
// code and only the OpenZiti route. The API and preview home are disposable.
func TestManagedPreviewPairsWithoutLAN(t *testing.T) {
	fixture := os.Getenv("LOCALZIE_PREVIEW_TEST_DIR")
	if fixture == "" {
		t.Skip("operator-managed preview test home not supplied")
	}
	dir := t.TempDir()
	for _, name := range []string{"remote-preview.json", "remote-preview-home.json"} {
		data, err := os.ReadFile(filepath.Join(fixture, name))
		if err != nil {
			t.Fatal(err)
		}
		privateFixture(t, dir, name, data)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"data":[{"id":"managed-preview-test"}]}`)
	}))
	defer upstream.Close()
	wizard := New()
	wizard.Discover = func(context.Context, []ziticonn.Endpoint, string) ([]Service, error) {
		return []Service{{Endpoint: ziticonn.Endpoint{ID: "models", Name: "Test model", Kind: "openai-compatible", URL: upstream.URL}}}, nil
	}
	wizard.Select = func(context.Context, io.Reader, io.Writer, []Service, []string, bool) ([]string, error) {
		return []string{"models"}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	finished := make(chan error, 1)
	go func() {
		err := wizard.Run(ctx, strings.NewReader(""), writer, Options{ConfigPath: filepath.Join(dir, "connector.json"), DataDir: dir, Name: "Test home", Remote: true})
		writer.Close()
		finished <- err
	}()
	defer func() { cancel(); reader.Close(); <-finished }()
	codes := make(chan string, 1)
	go func() {
		defer close(codes)
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			if strings.HasPrefix(scanner.Text(), "LZ1:") {
				codes <- scanner.Text()
			}
		}
	}()
	var code string
	select {
	case code = <-codes:
	case <-ctx.Done():
		t.Fatal("timed out waiting for managed setup")
	}
	if code == "" {
		t.Fatal("managed setup did not produce a remote code")
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(code, "LZ1:"))
	var bundle ziticonn.Pairing
	if err != nil || json.Unmarshal(payload, &bundle) != nil || bundle.Enrollment == "" || len(bundle.Addresses) != 0 {
		t.Fatal("setup did not produce a route-only pairing bundle")
	}
	roots := x509.NewCertPool()
	der, err := base64.StdEncoding.DecodeString(bundle.Authority)
	if err != nil {
		t.Fatal("invalid paired certificate authority")
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal("invalid paired certificate authority")
	}
	roots.AddCert(cert)
	claims, parsed, err := enroll.ParseToken(bundle.Enrollment)
	if err != nil {
		t.Fatal("invalid phone enrollment")
	}
	var keyAlgorithm ziti.KeyAlgVar
	_ = keyAlgorithm.Set("EC")
	identity, err := enroll.Enroll(enroll.EnrollmentFlags{Token: claims, JwtToken: parsed, JwtString: bundle.Enrollment, KeyAlg: keyAlgorithm})
	if err != nil {
		t.Fatal("phone enrollment did not succeed")
	}
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		t.Fatal("phone identity could not be encoded")
	}
	privateFixture(t, dir, "phone-identity.json", identityJSON)
	phone, err := ziticonn.NewContext(ctx, filepath.Join(dir, "phone-identity.json"))
	if err != nil {
		t.Fatal("phone did not load its network identity")
	}
	defer phone.Close()
	if err := phone.Authenticate(); err != nil {
		t.Fatal("phone could not authenticate to the remote network")
	}
	transport := &http.Transport{DialTLSContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		conn, err := phone.DialContext(ctx, bundle.Service)
		if err != nil {
			return nil, err
		}
		secure := tls.Client(conn, &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: "localhost"})
		if err := secure.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, err
		}
		return secure, nil
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	body, _ := json.Marshal(map[string]string{"token": bundle.Token, "name": "Test phone"})
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://localhost/localzie/pair", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal("phone could not reach pinned home over OpenZiti")
	}
	var paired struct{ Token string }
	err = json.NewDecoder(response.Body).Decode(&paired)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || paired.Token == "" {
		t.Fatalf("home refused new remote phone: HTTP %d", response.StatusCode)
	}
	request, _ = http.NewRequestWithContext(ctx, http.MethodGet, "https://localhost/api/models/v1/models", nil)
	request.Header.Set("X-Localzie-Token", paired.Token)
	response, err = client.Do(request)
	if err != nil {
		t.Fatal("could not use the paired phone credential")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK || !bytes.Contains(raw, []byte("managed-preview-test")) {
		t.Fatalf("remote model unavailable: HTTP %d", response.StatusCode)
	}
}

func privateFixture(t *testing.T, dir, name string, content []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), content, 0600); err != nil {
		t.Fatal(err)
	}
}
