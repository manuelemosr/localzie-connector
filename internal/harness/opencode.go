// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type OpenCodeClient struct {
	base      *url.URL
	directory string
	// username is the configured credential (immutable); authUser is the
	// username actually sent, replaced by the fixed v2 username when
	// detection discovers a v2 server.
	username string
	authUser string
	password string
	client   *http.Client

	// mu guards the detected API generation and the auth username:
	// detection replaces a configured custom username with the fixed v2
	// username, and request builders read both under the same lock.
	mu         sync.Mutex
	generation OpenCodeAPIGeneration
}

type OpenCodeSession struct {
	ID        string `json:"id"`
	Title     string `json:"title,omitempty"`
	Directory string `json:"directory,omitempty"`
}

func NewOpenCode(rawURL, directory, username, password string) (*OpenCodeClient, error) {
	base, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, errors.New("OpenCode URL must use http or https")
	}
	host := base.Hostname()
	if host != "localhost" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return nil, errors.New("OpenCode server must use a loopback address")
		}
	}
	if username == "" {
		username = "opencode"
	}
	return &OpenCodeClient{base: base, directory: directory, username: username, authUser: username, password: password, client: &http.Client{Timeout: 30 * time.Second}}, nil
}

// Health reports the attached server's version, detecting the API
// generation on first use. A v2 server answers on its info route; the v1
// health route is never issued against it.
func (o *OpenCodeClient) Health(ctx context.Context) (string, error) {
	generation, err := o.ensureGeneration(ctx)
	if err != nil {
		return "", err
	}
	if generation == OpenCodeV2 {
		return o.v2Health(ctx)
	}
	var response struct {
		Healthy bool   `json:"healthy"`
		Version string `json:"version"`
	}
	if err := o.doJSON(ctx, http.MethodGet, "/global/health", nil, &response); err != nil {
		return "", err
	}
	if !response.Healthy {
		return response.Version, errors.New("OpenCode reported unhealthy")
	}
	return response.Version, nil
}

// URL reports the configured server base URL.
func (o *OpenCodeClient) URL() string { return o.base.String() }

// OpenCodeSessionGlobal is one entry of the global root-session listing
// (GET /experimental/session, verified v1.18.31): Session.Info fields plus the
// project it belongs to, sorted by most recently updated.
type OpenCodeSessionGlobal struct {
	ID        string                `json:"id"`
	Title     string                `json:"title,omitempty"`
	Directory string                `json:"directory,omitempty"`
	ParentID  string                `json:"parentID,omitempty"`
	Model     *OpenCodeSessionModel `json:"model,omitempty"`
	Time      OpenCodeSessionTime   `json:"time,omitempty"`
}

type OpenCodeSessionModel struct {
	ID         string `json:"id"`
	ProviderID string `json:"providerID"`
}

type OpenCodeSessionTime struct {
	Created int64 `json:"created,omitempty"`
	Updated int64 `json:"updated,omitempty"`
}

// OpenCodeSessionStatus is one entry of GET /session/status: type is "idle",
// "busy", or "retry" (verified v1.18.31 server status surface).
type OpenCodeSessionStatus struct {
	Type    string `json:"type"`
	Message string `json:"message,omitempty"`
}

// GlobalSessions lists root sessions across projects, most recently updated
// first. The vendor paginates by a strict time cursor; active sessions are the
// most recently updated, so one generous page is enough to observe them.
func (o *OpenCodeClient) GlobalSessions(ctx context.Context) ([]OpenCodeSessionGlobal, error) {
	if err := o.requireV1(ctx); err != nil {
		return nil, err
	}
	var sessions []OpenCodeSessionGlobal
	query := url.Values{}
	query.Set("roots", "true")
	query.Set("limit", "200")
	return sessions, o.doJSONQuery(ctx, http.MethodGet, "/experimental/session", query, nil, &sessions)
}

// SessionStatuses fetches the directory-scoped session status map. The status
// route carries no session ID, so the server workspace-routes it by the
// directory query; an empty directory uses the client's configured one.
func (o *OpenCodeClient) SessionStatuses(ctx context.Context, directory string) (map[string]OpenCodeSessionStatus, error) {
	if err := o.requireV1(ctx); err != nil {
		return nil, err
	}
	statuses := map[string]OpenCodeSessionStatus{}
	return statuses, o.doJSONDirectory(ctx, http.MethodGet, "/session/status", directory, nil, &statuses)
}

// OpenCodeMessageInfo is the message metadata of GET /session/:id/message.
type OpenCodeMessageInfo struct {
	ID        string              `json:"id"`
	SessionID string              `json:"sessionID,omitempty"`
	Role      string              `json:"role"`
	Time      OpenCodeMessageTime `json:"time,omitempty"`
}

type OpenCodeMessageTime struct {
	Created   int64 `json:"created,omitempty"`
	Completed int64 `json:"completed,omitempty"`
}

// OpenCodeMessageEntry is one message with its ordered parts, the shape of
// GET /session/:id/message.
type OpenCodeMessageEntry struct {
	Info  OpenCodeMessageInfo       `json:"info"`
	Parts []OpenCodeMessagePartInfo `json:"parts,omitempty"`
}

// OpenCodeMessagePartInfo models the part fields the connector translates for
// observed sessions. Unknown part types are ignored by callers.
type OpenCodeMessagePartInfo struct {
	ID        string `json:"id"`
	MessageID string `json:"messageID,omitempty"`
	SessionID string `json:"sessionID,omitempty"`
	Type      string `json:"type"`
	Text      string `json:"text,omitempty"`
	Synthetic bool   `json:"synthetic,omitempty"`
	Ignored   bool   `json:"ignored,omitempty"`
	Tool      string `json:"tool,omitempty"`
}

// SessionMessages fetches a session's authoritative transcript in order.
func (o *OpenCodeClient) SessionMessages(ctx context.Context, sessionID string) ([]OpenCodeMessageEntry, error) {
	if err := o.requireV1(ctx); err != nil {
		return nil, err
	}
	var entries []OpenCodeMessageEntry
	return entries, o.doJSON(ctx, http.MethodGet, "/session/"+url.PathEscape(sessionID)+"/message", nil, &entries)
}

func (o *OpenCodeClient) Sessions(ctx context.Context) ([]OpenCodeSession, error) {
	if err := o.requireV1(ctx); err != nil {
		return nil, err
	}
	var sessions []OpenCodeSession
	return sessions, o.doJSON(ctx, http.MethodGet, "/session", nil, &sessions)
}

func (o *OpenCodeClient) CreateSession(ctx context.Context, title string) (OpenCodeSession, error) {
	var session OpenCodeSession
	if err := o.requireV1(ctx); err != nil {
		return session, err
	}
	err := o.doJSON(ctx, http.MethodPost, "/session", map[string]string{"title": title}, &session)
	if err == nil && session.ID == "" {
		err = errors.New("OpenCode returned an empty session id")
	}
	return session, err
}

// Prompt submits one user turn asynchronously. modelID is the OpenCode
// "provider/model" identifier; empty selects the server's default model.
func (o *OpenCodeClient) Prompt(ctx context.Context, sessionID, prompt, modelID string) error {
	if err := o.requireV1(ctx); err != nil {
		return err
	}
	body := map[string]any{"parts": []map[string]string{{"type": "text", "text": prompt}}}
	if provider, model, ok := strings.Cut(modelID, "/"); ok && provider != "" && model != "" {
		body["model"] = map[string]string{"providerID": provider, "modelID": model}
	}
	return o.doJSON(ctx, http.MethodPost, "/session/"+url.PathEscape(sessionID)+"/prompt_async", body, nil)
}

func (o *OpenCodeClient) Abort(ctx context.Context, sessionID string) error {
	if err := o.requireV1(ctx); err != nil {
		return err
	}
	var result bool
	if err := o.doJSON(ctx, http.MethodPost, "/session/"+url.PathEscape(sessionID)+"/abort", map[string]any{}, &result); err != nil {
		return err
	}
	if !result {
		return errors.New("OpenCode did not confirm abort")
	}
	return nil
}

func (o *OpenCodeClient) Diff(ctx context.Context, sessionID string) (json.RawMessage, error) {
	if err := o.requireV1(ctx); err != nil {
		return nil, err
	}
	var result json.RawMessage
	return result, o.doJSON(ctx, http.MethodGet, "/session/"+url.PathEscape(sessionID)+"/diff", nil, &result)
}

// DeleteSession removes the session and its server-side transcript. The
// connector treats "already gone" as success because the user-visible goal
// is that the session no longer exists.
func (o *OpenCodeClient) DeleteSession(ctx context.Context, sessionID string) error {
	if err := o.requireV1(ctx); err != nil {
		return err
	}
	err := o.doJSON(ctx, http.MethodDelete, "/session/"+url.PathEscape(sessionID), nil, nil)
	if err != nil && strings.HasPrefix(err.Error(), "OpenCode HTTP 404") {
		return nil
	}
	return err
}

// OpenCodeModelInfo is one model entry from GET /config/providers. Status is
// "active" when the provider can currently serve the model.
type OpenCodeModelInfo struct {
	ID           string `json:"id"`
	ProviderID   string `json:"providerID"`
	Name         string `json:"name"`
	Status       string `json:"status"`
	Capabilities struct {
		Reasoning bool `json:"reasoning"`
		ToolCall  bool `json:"toolcall"`
	} `json:"capabilities"`
}

// OpenCodeProvider is one provider entry from GET /config/providers.
// Options.BaseURL is present for custom providers and drives locality.
type OpenCodeProvider struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Source  string `json:"source"`
	Options struct {
		BaseURL string `json:"baseURL"`
	} `json:"options"`
	Models map[string]OpenCodeModelInfo `json:"models"`
}

// ConfigProviders returns the providers and models the OpenCode server can
// currently serve. On a v2 server the model and provider catalogs are
// translated into this same shape, so connector routing is generation-
// agnostic.
func (o *OpenCodeClient) ConfigProviders(ctx context.Context) ([]OpenCodeProvider, error) {
	generation, err := o.ensureGeneration(ctx)
	if err != nil {
		return nil, err
	}
	if generation == OpenCodeV2 {
		return o.v2ConfigProviders(ctx)
	}
	var response struct {
		Providers []OpenCodeProvider `json:"providers"`
	}
	if err := o.doJSON(ctx, http.MethodGet, "/config/providers", nil, &response); err != nil {
		return nil, err
	}
	return response.Providers, nil
}

// OpenCodeEvent is one envelope of the server's SSE /event stream.
type OpenCodeEvent struct {
	Type       string          `json:"type"`
	Properties json.RawMessage `json:"properties"`
}

// OpenCodeMessage is the message info carried by message.updated events.
// Content parts arrive separately through message.part.* events.
type OpenCodeMessage struct {
	ID        string `json:"id"`
	SessionID string `json:"sessionID"`
	Role      string `json:"role"`
}

// OpenCodeMessagePart is one part of a message. Only the subset Localzie
// translates is modeled; unknown part types are ignored by callers.
type OpenCodeMessagePart struct {
	ID        string `json:"id"`
	SessionID string `json:"sessionID"`
	MessageID string `json:"messageID"`
	Type      string `json:"type"`
	Text      string `json:"text"`
}

// Message decodes the message info of a message.updated event.
func (e OpenCodeEvent) Message() (OpenCodeMessage, error) {
	var properties struct {
		SessionID string          `json:"sessionID"`
		Info      OpenCodeMessage `json:"info"`
	}
	err := json.Unmarshal(e.Properties, &properties)
	if properties.Info.SessionID == "" {
		properties.Info.SessionID = properties.SessionID
	}
	return properties.Info, err
}

// Part decodes the part of a message.part.updated event.
func (e OpenCodeEvent) Part() (OpenCodeMessagePart, error) {
	var properties struct {
		SessionID string              `json:"sessionID"`
		Part      OpenCodeMessagePart `json:"part"`
	}
	err := json.Unmarshal(e.Properties, &properties)
	if properties.Part.SessionID == "" {
		properties.Part.SessionID = properties.SessionID
	}
	return properties.Part, err
}

// PartDelta decodes a message.part.delta event. field is "text" for the
// content Localzie streams.
func (e OpenCodeEvent) PartDelta() (sessionID, messageID, partID, field, delta string, err error) {
	var properties struct {
		SessionID string `json:"sessionID"`
		MessageID string `json:"messageID"`
		PartID    string `json:"partID"`
		Field     string `json:"field"`
		Delta     string `json:"delta"`
	}
	err = json.Unmarshal(e.Properties, &properties)
	return properties.SessionID, properties.MessageID, properties.PartID, properties.Field, properties.Delta, err
}

// SessionID decodes the session payload shared by the session lifecycle
// events (idle, error, deleted, updated).
func (e OpenCodeEvent) SessionID() string {
	var properties struct {
		SessionID string `json:"sessionID"`
	}
	_ = json.Unmarshal(e.Properties, &properties)
	return properties.SessionID
}

// ErrorText decodes the error payload of a session.error event.
func (e OpenCodeEvent) ErrorText() string {
	var properties struct {
		Error struct {
			Name string `json:"name"`
			Data struct {
				Message string `json:"message"`
			} `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(e.Properties, &properties); err != nil {
		return ""
	}
	if properties.Error.Data.Message != "" {
		return properties.Error.Name + ": " + properties.Error.Data.Message
	}
	return properties.Error.Name
}

// Events subscribes to the server's SSE event stream. onConnected fires once
// the response is established, before any event is delivered; callers submit
// work only after it so no early event is lost. The stream has no replay
// cursor: when it ends, callers must reconcile from authoritative session
// state.
func (o *OpenCodeClient) Events(ctx context.Context, onConnected func(), onEvent func(json.RawMessage) error) error {
	if err := o.requireV1(ctx); err != nil {
		return err
	}
	req, err := o.request(ctx, http.MethodGet, "/event", nil)
	if err != nil {
		return err
	}
	client := *o.client
	client.Timeout = 0
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return openCodeResponseError(resp)
	}
	if onConnected != nil {
		onConnected()
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	var data []string
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			if len(data) > 0 {
				if err := onEvent(json.RawMessage(strings.Join(data, "\n"))); err != nil {
					return err
				}
				data = data[:0]
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
}

func (o *OpenCodeClient) doJSON(ctx context.Context, method, path string, body, result any) error {
	return o.doJSONDirectory(ctx, method, path, "", body, result)
}

// rawJSON issues one request without the workspace directory query, for
// server-global routes.
func (o *OpenCodeClient) rawJSON(ctx context.Context, method, path string, query url.Values, body, result any) error {
	req, err := o.requestQuery(ctx, method, path, "", query, body)
	if err != nil {
		return err
	}
	resp, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return openCodeResponseError(resp)
	}
	if result == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

func (o *OpenCodeClient) doJSONDirectory(ctx context.Context, method, path, directory string, body, result any) error {
	req, err := o.requestDirectory(ctx, method, path, directory, body)
	if err != nil {
		return err
	}
	resp, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return openCodeResponseError(resp)
	}
	if result == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

func (o *OpenCodeClient) doJSONQuery(ctx context.Context, method, path string, query url.Values, body, result any) error {
	req, err := o.requestQuery(ctx, method, path, o.directory, query, body)
	if err != nil {
		return err
	}
	resp, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return openCodeResponseError(resp)
	}
	if result == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

func (o *OpenCodeClient) request(ctx context.Context, method, path string, body any) (*http.Request, error) {
	return o.requestDirectory(ctx, method, path, "", body)
}

func (o *OpenCodeClient) requestDirectory(ctx context.Context, method, path, directory string, body any) (*http.Request, error) {
	if directory == "" {
		directory = o.directory
	}
	return o.requestQuery(ctx, method, path, directory, nil, body)
}

func (o *OpenCodeClient) requestQuery(ctx context.Context, method, path, directory string, query url.Values, body any) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	endpoint := *o.base
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	values := endpoint.Query()
	for key, entries := range query {
		for _, entry := range entries {
			values.Add(key, entry)
		}
	}
	if directory != "" {
		// The v2 workspace selector is the deepObject location parameter;
		// its legacy bare `directory=` is accepted but ignored (verified
		// against the pinned schema and a live v2.2.5 server). The
		// generation is always detected before any data request runs, so
		// the selector never crosses generations.
		if o.generationSnapshot() == OpenCodeV2 {
			values.Set("location[directory]", directory)
		} else {
			values.Set("directory", directory)
		}
	}
	endpoint.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if o.password != "" {
		// The auth username is read through the lock: detection may
		// replace a configured custom username with the fixed v2 username,
		// and a concurrent request must observe one or the other whole.
		req.SetBasicAuth(o.authUsername(), o.password)
	}
	return req, nil
}

func openCodeResponseError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("OpenCode HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}
