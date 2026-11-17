// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// Candidate is a locally verified runtime. URLs are always from the fixed
// loopback allowlist below; discovery never accepts an address from a response
// or configuration file.
type Candidate struct {
	Runtime string `json:"runtime"`
	URL     string `json:"url"`
}

const (
	discoveryOverallTimeout = 2 * time.Second
	discoveryProbeTimeout   = 500 * time.Millisecond
	discoveryBodyLimit      = 64 * 1024
)

var discoveryTargets = []struct {
	runtime string
	base    string
}{
	{"openai-compatible", "http://127.0.0.1:11434"}, // Ollama's OpenAI-compatible API
	{"openai-compatible", "http://127.0.0.1:1234"},  // LM Studio
	{"openai-compatible", "http://127.0.0.1:8080"},  // llama.cpp / LocalAI
	{"openai-compatible", "http://127.0.0.1:8000"},  // vLLM
	{"openai-compatible", "http://127.0.0.1:1337"},  // Jan
	{"openai-compatible", "http://127.0.0.1:6767"},  // Jan CLI
	{"openai-compatible", "http://127.0.0.1:5000"},  // text-generation-webui
	{"openai-compatible", "http://127.0.0.1:5001"},  // KoboldCpp
	{"openai-compatible", "http://127.0.0.1:4891"},  // GPT4All
}

// Discover concurrently probes a deliberately small set of literal loopback
// addresses. It neither resolves names nor follows redirects, and it disables
// proxy environment variables. apiKey is optional and is only ever used in an
// Authorization header; callers must obtain it from an environment variable.
func Discover(ctx context.Context, apiKey string) ([]Candidate, error) {
	ctx, cancel := context.WithTimeout(ctx, discoveryOverallTimeout)
	defer cancel()

	results := make(chan *Candidate, len(discoveryTargets))
	var group sync.WaitGroup
	for _, target := range discoveryTargets {
		target := target
		group.Add(1)
		go func() {
			defer group.Done()
			probeCtx, stop := context.WithTimeout(ctx, discoveryProbeTimeout)
			defer stop()
			if probeOpenAICompatible(probeCtx, target.base, apiKey) {
				results <- &Candidate{Runtime: target.runtime, URL: target.base}
			}
		}()
	}
	group.Wait()
	close(results)

	unique := make(map[string]Candidate)
	for candidate := range results {
		unique[candidate.Runtime+"\x00"+candidate.URL] = *candidate
	}
	candidates := make([]Candidate, 0, len(unique))
	for _, candidate := range unique {
		candidates = append(candidates, candidate)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].URL == candidates[j].URL {
			return candidates[i].Runtime < candidates[j].Runtime
		}
		return candidates[i].URL < candidates[j].URL
	})
	return candidates, nil
}

func discoveryClient(port string) *http.Client {
	return &http.Client{
		Timeout: 0, // Context supplies the per-probe deadline.
		Transport: &http.Transport{
			Proxy: nil,
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				if network != "tcp" || address != net.JoinHostPort("127.0.0.1", port) {
					return nil, fmt.Errorf("runtime discovery only permits 127.0.0.1:%s", port)
				}
				return (&net.Dialer{}).DialContext(ctx, network, address)
			},
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func probeOpenAICompatible(ctx context.Context, base, apiKey string) bool {
	if !validDiscoveryBase(base) {
		return false
	}
	return getJSON(ctx, base, "/v1/models", apiKey, validOpenAIModelsSchema)
}

func getJSON(ctx context.Context, base, path, apiKey string, validate func([]byte) bool) bool {
	u, err := url.Parse(base)
	if err != nil || u.Hostname() != "127.0.0.1" {
		return false
	}
	client := discoveryClient(u.Port())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+path, nil)
	if err != nil {
		return false
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, discoveryBodyLimit+1))
	return err == nil && len(body) <= discoveryBodyLimit && validate(body)
}

func validDiscoveryBase(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.Port() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	return u.Path == "" || u.Path == "/"
}

func isJSONArray(raw json.RawMessage) bool {
	var values []json.RawMessage
	return json.Unmarshal(raw, &values) == nil
}

func validOpenAIModelsSchema(body []byte) bool {
	var value struct {
		Object string          `json:"object"`
		Data   json.RawMessage `json:"data"`
	}
	if json.Unmarshal(body, &value) != nil || value.Data == nil || !isJSONArray(value.Data) {
		return false
	}
	var models []struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(value.Data, &models) != nil {
		return false
	}
	for _, model := range models {
		if strings.TrimSpace(model.ID) == "" {
			return false
		}
	}
	return value.Object == "" || value.Object == "list"
}
