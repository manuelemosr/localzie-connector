// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package setup

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	modelruntime "github.com/manuelemosr/localzie-connector/internal/runtime"
	"github.com/manuelemosr/localzie-connector/internal/ziticonn"
)

type Service struct {
	Endpoint ziticonn.Endpoint
	Detail   string
}

// Discover reads existing local service information and probes only a fixed
// loopback allowlist. It never starts inference, follows redirects or scans LAN.
func Discover(ctx context.Context, previous []ziticonn.Endpoint, keyEnv string) ([]Service, error) {
	found := make(map[string]Service)
	for _, endpoint := range previous {
		found[endpoint.ID] = Service{endpoint, "Previously connected"}
	}
	candidates, err := modelruntime.Discover(ctx, os.Getenv(keyEnv))
	if err != nil {
		return nil, err
	}
	names := map[string]string{"11434": "Ollama", "1234": "LM Studio", "8080": "Local model server", "8000": "vLLM", "1337": "Jan", "6767": "Jan", "5000": "Local model server", "5001": "KoboldCpp", "4891": "GPT4All"}
	for _, candidate := range candidates {
		address, _ := url.Parse(candidate.URL)
		endpoint := ziticonn.Endpoint{ID: "models-" + address.Port(), Name: names[address.Port()], Kind: "openai-compatible", URL: candidate.URL, APIKeyEnv: keyEnv}
		if endpoint.Name == "" {
			endpoint.Name = "Local models"
		}
		addService(found, Service{endpoint, "Running on this computer"})
	}
	// OpenCode's own descriptor discovers both its address and existing auth.
	state := os.Getenv("XDG_STATE_HOME")
	if state == "" {
		home, _ := os.UserHomeDir()
		state = filepath.Join(home, ".local", "state")
	}
	if raw, err := os.ReadFile(filepath.Join(state, "opencode", "service.json")); err == nil && len(raw) <= 64<<10 {
		var descriptor struct {
			URL string `json:"url"`
		}
		if json.Unmarshal(raw, &descriptor) == nil && loopbackURL(descriptor.URL) {
			addService(found, Service{ziticonn.Endpoint{ID: "opencode", Name: "OpenCode", Kind: "opencode"}, "Running on this computer"})
		}
	} else if health(ctx, "http://127.0.0.1:4096/global/health", "healthy", "opencode", os.Getenv("OPENCODE_SERVER_PASSWORD")) {
		endpoint := ziticonn.Endpoint{ID: "opencode", Name: "OpenCode", Kind: "opencode", URL: "http://127.0.0.1:4096"}
		if os.Getenv("OPENCODE_SERVER_PASSWORD") != "" {
			endpoint.Username = "opencode"
			endpoint.PasswordEnv = "OPENCODE_SERVER_PASSWORD"
		}
		addService(found, Service{endpoint, "Running on this computer"})
	}
	if os.Getenv("HERMES_USERNAME") != "" && os.Getenv("HERMES_PASSWORD") != "" {
		for _, port := range []string{"9119", "8642"} {
			address := "http://127.0.0.1:" + port
			if health(ctx, address+"/api/health", "ok", os.Getenv("HERMES_USERNAME"), os.Getenv("HERMES_PASSWORD")) {
				addService(found, Service{ziticonn.Endpoint{ID: "hermes-" + port, Name: "Hermes", Kind: "hermes", URL: address, Username: os.Getenv("HERMES_USERNAME"), PasswordEnv: "HERMES_PASSWORD"}, "Running on this computer"})
			}
		}
	}

	if _, err := exec.LookPath("codex"); err == nil {
		dir, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		addService(found, Service{ziticonn.Endpoint{ID: "codex", Name: "Codex", Kind: "codex", Directory: dir}, "Installed on this computer"})
	}
	result := make([]Service, 0, len(found))
	for _, service := range found {
		result = append(result, service)
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i].Endpoint, result[j].Endpoint
		if a.Name == b.Name {
			return a.ID < b.ID
		}
		return a.Name < b.Name
	})
	return result, nil
}

func addService(found map[string]Service, service Service) {
	for _, existing := range found {
		a, b := existing.Endpoint, service.Endpoint
		if a.ID == b.ID || (a.Kind == b.Kind && (a.URL == b.URL || b.Kind == "codex" || b.Kind == "opencode" && (a.URL == "" || b.URL == ""))) {
			return
		}
	}
	found[service.Endpoint.ID] = service
}

func loopbackURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	host := u.Hostname()
	port, err := strconv.Atoi(u.Port())
	return err == nil && port > 0 && port <= 65535 && (u.Scheme == "http" || u.Scheme == "https") && (host == "127.0.0.1" || host == "::1" || host == "localhost" || host == "0.0.0.0" || host == "::") && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

func health(ctx context.Context, address, field, user, password string) bool {
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return false
	}
	if password != "" {
		req.SetBasicAuth(user, password)
	}
	response, err := client.Do(req)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 8193))
	if err != nil || len(body) > 8192 {
		return false
	}
	var result map[string]json.RawMessage
	if json.Unmarshal(body, &result) != nil {
		return false
	}
	return strings.TrimSpace(string(result[field])) == "true"
}
