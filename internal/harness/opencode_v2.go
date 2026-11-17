// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package harness

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// OpenCode v2 server API support (pinned 2.2.5,
// docs/integrations.md). V2 replaces the v1 routes
// wholesale, so this client detects the attached server's API generation
// once and answers version-specific requests, normalizing v2 catalogs into
// the existing connector types. V1 routes are never issued against a v2
// server, and v2 surfaces whose translation lands in a later compatibility
// slice fail honestly instead of crossing generations or faking success.

// OpenCodeAPIGeneration identifies which server API a connection speaks.
type OpenCodeAPIGeneration string

const (
	OpenCodeV1 OpenCodeAPIGeneration = "v1"
	OpenCodeV2 OpenCodeAPIGeneration = "v2"
)

// ErrOpenCodeV2NotMigrated marks connector operations whose v2 translation
// lands in a later compatibility slice.
var ErrOpenCodeV2NotMigrated = errors.New("this OpenCode operation is not yet supported against an OpenCode v2 server")

// OpenCodeV2Username is the fixed v2 basic-auth username. V2 servers reject
// every other username (verified against the pinned auth source and a live
// 401), so a configured custom username must never be sent to them.
const OpenCodeV2Username = "opencode"

// ErrOpenCodeRequiresCredential reports a credential wall: the service
// identifies as OpenCode, but the configured credentials did not
// authenticate it.
var ErrOpenCodeRequiresCredential = errors.New("OpenCode server rejected the credentials")

type openCodeV2ServerInfo struct {
	Version string   `json:"version"`
	PID     int      `json:"pid"`
	URLs    []string `json:"urls"`
	Paths   struct {
		Tmp string `json:"tmp"`
	} `json:"paths"`
}

// valid reports the documented required skeleton. Another service's 200
// JSON without it never claims v2.
func (s openCodeV2ServerInfo) valid() bool {
	return s.Version != "" && s.PID >= 0 && len(s.URLs) > 0 && s.Paths.Tmp != ""
}

type openCodeV2ModelInfo struct {
	ID           string `json:"id"`
	ProviderID   string `json:"providerID"`
	Name         string `json:"name"`
	Type         string `json:"type"`
	Status       string `json:"status"`
	Enabled      *bool  `json:"enabled"`
	Capabilities *struct {
		Input  []string `json:"input"`
		Output []string `json:"output"`
	} `json:"capabilities"`
}

type openCodeV2Provider struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Settings *struct {
		BaseURL string `json:"baseURL"`
	} `json:"settings"`
}

type openCodeV2Envelope[T any] struct {
	Data []T `json:"data"`
}

// openCodeProbeOutcome classifies one probe reply per the pinned contracts.
type openCodeProbeOutcome int

const (
	outcomeUnknown openCodeProbeOutcome = iota
	outcomeV2
	outcomeV1
	outcomeUnhealthy
	outcomeRequiresCredential
	outcomeAbsent
	outcomeMalformed
	outcomeServerError
)

func openCodeClassifyInfoResponse(status int, contentType string, body []byte) (openCodeProbeOutcome, openCodeV2ServerInfo) {
	switch {
	case status >= 200 && status < 300:
		var info openCodeV2ServerInfo
		if err := json.Unmarshal(body, &info); err != nil || !info.valid() {
			return outcomeMalformed, info
		}
		return outcomeV2, info
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		// The pinned v2 error envelope is JSON. An HTML or plain-text
		// wall is some other service; it must not fall through to "try
		// v1", so it identifies nothing.
		if openCodeIsJSONBody(contentType, body) {
			return outcomeRequiresCredential, openCodeV2ServerInfo{}
		}
		return outcomeMalformed, openCodeV2ServerInfo{}
	case status == http.StatusNotFound:
		return outcomeAbsent, openCodeV2ServerInfo{}
	default:
		return outcomeServerError, openCodeV2ServerInfo{}
	}
}

func openCodeClassifyHealthResponse(status int, body []byte) (openCodeProbeOutcome, string) {
	switch {
	case status >= 200 && status < 300:
		// A v2 catch-all serving HTML on the health route answers 200;
		// only the documented shape is v1 evidence.
		var health struct {
			Healthy bool   `json:"healthy"`
			Version string `json:"version"`
		}
		if err := json.Unmarshal(body, &health); err != nil {
			return outcomeMalformed, ""
		}
		if !health.Healthy {
			return outcomeUnhealthy, ""
		}
		return outcomeV1, health.Version
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return outcomeRequiresCredential, ""
	case status == http.StatusNotFound:
		return outcomeAbsent, ""
	default:
		return outcomeServerError, ""
	}
}

func openCodeIsJSONBody(contentType string, body []byte) bool {
	if !strings.Contains(strings.ToLower(contentType), "json") {
		return false
	}
	trimmed := strings.TrimSpace(string(body))
	return strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[")
}

// probeGeneration runs the detection sequence with one username. The v1
// health route is consulted only when the v2 info route says nothing
// (absent or undecodable), never after a credential wall or a server error.
func (o *OpenCodeClient) probeGeneration(ctx context.Context, username string) (openCodeProbeOutcome, string, error) {
	status, contentType, body, err := o.probeRequest(ctx, "/api/info", username)
	if err != nil {
		return outcomeUnknown, "", err
	}
	infoOutcome, info := openCodeClassifyInfoResponse(status, contentType, body)
	switch infoOutcome {
	case outcomeV2:
		return outcomeV2, info.Version, nil
	case outcomeRequiresCredential:
		return outcomeRequiresCredential, "", nil
	case outcomeAbsent, outcomeMalformed:
		// A healthy:false v1 server is still v1; Health surfaces the
		// unhealthy state. Anything else says nothing usable.
		status, _, body, err := o.probeRequest(ctx, "/global/health", username)
		if err != nil {
			return outcomeUnknown, "", err
		}
		healthOutcome, version := openCodeClassifyHealthResponse(status, body)
		switch healthOutcome {
		case outcomeV1, outcomeUnhealthy, outcomeRequiresCredential:
			return healthOutcome, version, nil
		default:
			return outcomeMalformed, "", nil
		}
	default:
		return infoOutcome, "", nil
	}
}

// probeRequest issues one read-only probe with the given username. It never
// carries the workspace directory: identity routes are server-global.
func (o *OpenCodeClient) probeRequest(ctx context.Context, path, username string) (int, string, []byte, error) {
	endpoint := *o.base
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return 0, "", nil, err
	}
	if o.password != "" && username != "" {
		req.SetBasicAuth(username, o.password)
	}
	resp, err := o.client.Do(req)
	if err != nil {
		return 0, "", nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return 0, "", nil, err
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), body, nil
}

// ensureGeneration returns the detected generation, discovering it once and
// caching it for the client's lifetime. Reconnecting rebuilds the client;
// within one client an accepted mutation is never retried against a
// different generation.
func (o *OpenCodeClient) ensureGeneration(ctx context.Context) (OpenCodeAPIGeneration, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.generation != "" {
		return o.generation, nil
	}
	generation, err := o.detect(ctx)
	if err != nil {
		return "", err
	}
	o.generation = generation
	// V2 servers accept only their fixed username; keep the username the
	// probe authenticated with so later requests match.
	if generation == OpenCodeV2 {
		o.authUser = OpenCodeV2Username
	}
	return generation, nil
}

// authUsername returns the username basic-auth requests must carry.
func (o *OpenCodeClient) authUsername() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.authUser
}

// detect probes the fixed v2 username first and the configured custom v1
// username second, so a v1 server with OPENCODE_SERVER_USERNAME still
// answers while v2 never sees a rejected username.
func (o *OpenCodeClient) detect(ctx context.Context) (OpenCodeAPIGeneration, error) {
	candidates := []string{OpenCodeV2Username}
	if o.username != "" && o.username != OpenCodeV2Username {
		candidates = append(candidates, o.username)
	}
	sawWall := false
	for _, candidate := range candidates {
		outcome, _, err := o.probeGeneration(ctx, candidate)
		if err != nil {
			return "", err
		}
		switch outcome {
		case outcomeV2:
			return OpenCodeV2, nil
		case outcomeV1, outcomeUnhealthy:
			return OpenCodeV1, nil
		case outcomeRequiresCredential:
			sawWall = true
		case outcomeAbsent, outcomeMalformed, outcomeServerError:
		}
	}
	if sawWall {
		return "", ErrOpenCodeRequiresCredential
	}
	return "", errors.New("OpenCode server did not answer as a supported OpenCode server")
}

// Generation reports the attached server's API generation, detecting it on
// first use.
func (o *OpenCodeClient) Generation(ctx context.Context) (OpenCodeAPIGeneration, error) {
	return o.ensureGeneration(ctx)
}

// generationSnapshot reports the cached generation without probing; empty
// until detection has run.
func (o *OpenCodeClient) generationSnapshot() OpenCodeAPIGeneration {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.generation
}

// requireV1 gates connector surfaces whose v2 translation lands in a later
// slice: a v1 route is never issued against a v2 server, and an
// unimplemented v2 surface is never faked as working.
func (o *OpenCodeClient) requireV1(ctx context.Context) error {
	generation, err := o.ensureGeneration(ctx)
	if err != nil {
		return err
	}
	if generation == OpenCodeV2 {
		return ErrOpenCodeV2NotMigrated
	}
	return nil
}

// v2Health reports the v2 server version from its server info route.
func (o *OpenCodeClient) v2Health(ctx context.Context) (string, error) {
	var info openCodeV2ServerInfo
	if err := o.rawJSON(ctx, http.MethodGet, "/api/info", nil, nil, &info); err != nil {
		return "", err
	}
	if !info.valid() {
		return "", errors.New("OpenCode v2 server info did not match the documented structure")
	}
	return info.Version, nil
}

// v2ConfigProviders lists the v2 catalog normalized into the connector's
// provider/model shape so existing routing works unchanged. Only active,
// enabled models are offered; provider settings carry the endpoint base URL
// that drives locality. Reasoning capability has no v2 chat flag yet, so it
// is never claimed (a later slice reads it from model compatibility).
func (o *OpenCodeClient) v2ConfigProviders(ctx context.Context) ([]OpenCodeProvider, error) {
	var providerReply openCodeV2Envelope[openCodeV2Provider]
	if err := o.rawJSON(ctx, http.MethodGet, "/api/provider", nil, nil, &providerReply); err != nil {
		return nil, err
	}
	providers := make([]OpenCodeProvider, 0, len(providerReply.Data))
	index := map[string]int{}
	for _, provider := range providerReply.Data {
		entry := OpenCodeProvider{ID: provider.ID, Name: provider.Name, Models: map[string]OpenCodeModelInfo{}}
		if provider.Settings != nil {
			entry.Options.BaseURL = provider.Settings.BaseURL
		}
		index[provider.ID] = len(providers)
		providers = append(providers, entry)
	}
	// The chat type is the default; the image query adds image-capable
	// models so their capability flags reach the phone.
	for _, query := range []url.Values{nil, {"types": {"image"}}} {
		var modelReply openCodeV2Envelope[openCodeV2ModelInfo]
		if err := o.rawJSON(ctx, http.MethodGet, "/api/model", query, nil, &modelReply); err != nil {
			return nil, err
		}
		for _, model := range modelReply.Data {
			if (model.Status != "" && model.Status != "active") || (model.Enabled != nil && !*model.Enabled) {
				continue
			}
			at, ok := index[model.ProviderID]
			if !ok {
				providers = append(providers, OpenCodeProvider{ID: model.ProviderID, Name: model.ProviderID, Models: map[string]OpenCodeModelInfo{}})
				at = len(providers) - 1
				index[model.ProviderID] = at
			}
			if providers[at].Models == nil {
				providers[at].Models = map[string]OpenCodeModelInfo{}
			}
			providers[at].Models[model.ID] = OpenCodeModelInfo{
				ID:         model.ID,
				ProviderID: model.ProviderID,
				Name:       model.Name,
				Status:     "active",
			}
		}
	}
	return providers, nil
}
