// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package setup

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
)

// Exercise the complete setup -> serving -> authenticated phone flow without a
// real phone, harness account, inference call or remote infrastructure.
func TestOneSetupInvocationPairsPhoneAndReachesSelectedAPI(t *testing.T) {
	var address string
	interfaces, _ := net.Interfaces()
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || strings.HasPrefix(iface.Name, "utun") || strings.HasPrefix(iface.Name, "tun") {
			continue
		}
		ips, _ := iface.Addrs()
		for _, raw := range ips {
			ip, _, err := net.ParseCIDR(raw.String())
			if err == nil && ip.To4() != nil && ip.IsPrivate() {
				address = ip.String()
				break
			}
		}
		if address != "" {
			break
		}
	}
	if address == "" {
		t.Skip("no private LAN interface is available")
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(address, "0"))
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Errorf("unexpected upstream request: %s", r.URL.Path)
		}
		fmt.Fprint(w, `{"data":[{"id":"synthetic-model"}]}`)
	}))
	defer upstream.Close()
	dir := t.TempDir()
	config := ziticonn.Config{Name: "Test computer", LocalOnly: true, LANPort: port, Endpoints: []ziticonn.Endpoint{{ID: "models", Name: "Test models", Kind: "openai-compatible", URL: upstream.URL}}}
	raw, _ := json.Marshal(config)
	configPath := filepath.Join(dir, "connector.json")
	if err = os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	output := &bytes.Buffer{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wizard := New()
	wizard.Discover = func(context.Context, []ziticonn.Endpoint, string) ([]Service, error) {
		return []Service{{Endpoint: config.Endpoints[0]}}, nil
	}
	wizard.Select = func(context.Context, io.Reader, io.Writer, []Service, []string, bool) ([]string, error) {
		return []string{"models"}, nil
	}
	wizard.Serve = func(ctx context.Context, c ziticonn.Config, dir string, ready func() error) error {
		return ziticonn.ServeWithReady(ctx, c, dir, func() error {
			if err := ready(); err != nil {
				return err
			}
			defer cancel()
			var pairing ziticonn.Pairing
			for _, line := range strings.Split(output.String(), "\n") {
				if strings.HasPrefix(line, "LZ1:") {
					raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(line, "LZ1:"))
					if err != nil {
						return err
					}
					if err = json.Unmarshal(raw, &pairing); err != nil {
						return err
					}
				}
			}
			if pairing.Token == "" {
				return fmt.Errorf("setup did not provide a code after serving started")
			}
			ca, err := os.ReadFile(filepath.Join(dir, "home-ca.crt"))
			if err != nil {
				return err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(ca) {
				return fmt.Errorf("no valid home authority")
			}
			transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool, ServerName: "localhost"}, Proxy: nil, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(address, fmt.Sprint(port)))
			}}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
			raw, _ := json.Marshal(map[string]string{"token": pairing.Token, "name": "Synthetic phone"})
			response, err := client.Post("https://localhost/localzie/pair", "application/json", bytes.NewReader(raw))
			if err != nil {
				return err
			}
			var phone struct {
				Token string `json:"token"`
			}
			err = json.NewDecoder(response.Body).Decode(&phone)
			response.Body.Close()
			if err != nil || response.StatusCode != 200 || phone.Token == "" {
				return fmt.Errorf("phone pairing did not succeed")
			}
			request, _ := http.NewRequestWithContext(ctx, "GET", "https://localhost/api/models/v1/models", nil)
			request.Header.Set("X-Localzie-Token", phone.Token)
			request.Header.Set("X-Localzie-Endpoint", pairing.Endpoints[0].InstanceID)
			response, err = client.Do(request)
			if err != nil {
				return err
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != 200 || !bytes.Contains(body, []byte("synthetic-model")) {
				return fmt.Errorf("selected service was not reachable after one setup command")
			}
			return nil
		})
	}
	if err = wizard.Run(ctx, strings.NewReader(""), output, Options{ConfigPath: configPath, DataDir: dir}); err != nil {
		t.Fatal(err)
	}
}
