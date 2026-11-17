// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/harness"
	"github.com/manuelemosr/localzie-connector/internal/protocol"
	"github.com/manuelemosr/localzie-connector/internal/runtime"
	"github.com/manuelemosr/localzie-connector/internal/store"
)

type Server struct {
	version     string
	connectorID string
	store       *store.Store
	runtime     runtime.Adapter
	jobs        *JobManager
	harness     *HarnessManager
	logger      *slog.Logger
	mux         *http.ServeMux
	root        context.Context
	sendMu      sync.Mutex
}

type deviceContextKey struct{}

func NewServer(root context.Context, version string, s *store.Store, adapter runtime.Adapter, logger *slog.Logger) (*Server, error) {
	id, err := s.ConnectorID(root)
	if err != nil {
		return nil, err
	}
	server := &Server{version: version, connectorID: id, store: s, runtime: adapter, jobs: NewJobManager(root, s, adapter), logger: logger, mux: http.NewServeMux(), root: root}
	server.routes()
	server.jobs.BackfillTitles()
	return server, nil
}
func (s *Server) Handler() http.Handler { return s.securityHeaders(s.mux) }

// RemoteHandler shares the same identity and APIs but never grants LAN auto-pairing.
func (s *Server) RemoteHandler() http.Handler {
	return s.securityHeaders(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/pair/local" {
			writeError(w, http.StatusForbidden, "local_pairing_remote_forbidden", "automatic local pairing is unavailable remotely")
			return
		}
		s.mux.ServeHTTP(w, r)
	}))
}
func (s *Server) ConnectorID() string { return s.connectorID }

// AttachOpenCode enables the harness backend against an already-running
// OpenCode server. allowUnverified opts in to conversation usage before the
// pinned-version compatibility fixtures have passed; without it the server
// reports the attachment but rejects harness conversations and hides its
// models. It must be called before the server starts serving.
func (s *Server) AttachOpenCode(client *harness.OpenCodeClient, allowUnverified bool) {
	manager := NewHarnessManager(client, allowUnverified, s.logger, s.root, s.store)
	s.harness = manager
	s.jobs.harness = manager
	manager.StartObserving()
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /v1/health", s.health)
	s.mux.HandleFunc("POST /v1/pair", s.pair)
	s.mux.HandleFunc("POST /v1/pair/local", s.pairLocal)
	s.mux.Handle("GET /v1/models", s.auth(http.HandlerFunc(s.models)))
	s.mux.Handle("GET /v1/harnesses", s.auth(http.HandlerFunc(s.harnesses)))
	s.mux.Handle("GET /v1/snapshot", s.auth(http.HandlerFunc(s.snapshot)))
	s.mux.Handle("GET /v1/conversations", s.auth(http.HandlerFunc(s.conversations)))
	s.mux.Handle("POST /v1/conversations", s.auth(http.HandlerFunc(s.createConversation)))
	s.mux.Handle("GET /v1/conversations/{id}", s.auth(http.HandlerFunc(s.conversation)))
	s.mux.Handle("DELETE /v1/conversations/{id}", s.auth(http.HandlerFunc(s.deleteConversation)))
	s.mux.Handle("POST /v1/conversations/{id}/messages", s.auth(http.HandlerFunc(s.sendMessage)))
	s.mux.Handle("POST /v1/jobs/{id}/stop", s.auth(http.HandlerFunc(s.stop)))
	s.mux.Handle("GET /v1/events", s.auth(http.HandlerFunc(s.events)))
	s.mux.Handle("GET /v1/devices", s.auth(http.HandlerFunc(s.devices)))
	s.mux.Handle("DELETE /v1/devices/{id}", s.auth(http.HandlerFunc(s.revokeDevice)))
}
func (s *Server) harnesses(w http.ResponseWriter, r *http.Request) {
	installations := harness.NewProber().All(r.Context())
	if s.harness.Enabled() {
		status := s.harness.Status(r.Context())
		for index := range installations {
			if installations[index].ID == "opencode" {
				installations[index].Server = &status
				// An explicitly attached API is detected even without a CLI binary.
				// Installed still describes the independent local CLI probe.
				installations[index].IntegrationStatus = "detected_unverified"
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"harnesses": installations})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	features := []string{"durable_jobs", "event_replay", "snapshot", "device_revocation", "conversation_deletion", "automatic_titles", "local_auto_pairing", "short_pairing_codes"}
	if s.harness.Allowed() {
		features = append(features, "harness_sessions")
	}
	writeJSON(w, http.StatusOK, protocol.Health{ProtocolVersion: protocol.Version, Service: "connector", Version: s.version, Features: features, Time: time.Now().UTC()})
}
func (s *Server) snapshot(w http.ResponseWriter, r *http.Request) {
	snapshot, err := s.store.Snapshot(r.Context())
	if err != nil {
		s.internal(w, err)
		return
	}
	snapshot.Conversations = mergeObservedConversations(snapshot.Conversations, s.harness.ObservedSummaries())
	writeJSON(w, http.StatusOK, snapshot)
}
func (s *Server) pair(w http.ResponseWriter, r *http.Request) {
	var req protocol.PairRequest
	if !decode(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.DeviceName) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "device_name is required")
		return
	}
	deviceID, token, err := s.store.ConsumePairing(r.Context(), req.PairingToken, req.DeviceName)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid_pairing", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, protocol.PairResponse{DeviceID: deviceID, DeviceToken: token, ConnectorID: s.connectorID})
}
func (s *Server) pairLocal(w http.ResponseWriter, r *http.Request) {
	if !isPrivateNetworkRequest(r) {
		writeError(w, http.StatusForbidden, "local_pairing_only", "automatic pairing is only available on a private local network")
		return
	}
	var req protocol.LocalPairRequest
	if !decode(w, r, &req) {
		return
	}
	deviceID, token, err := s.store.CreateDevice(r.Context(), req.DeviceName)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, protocol.PairResponse{DeviceID: deviceID, DeviceToken: token, ConnectorID: s.connectorID})
}

func isPrivateNetworkRequest(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast())
}
func (s *Server) models(w http.ResponseWriter, r *http.Request) {
	runtimeModels, runtimeErr := s.runtime.Models(r.Context())
	var harnessModels []protocol.Model
	var harnessErr error
	if s.harness.Allowed() {
		harnessModels, harnessErr = s.harness.Models(r.Context())
	}
	switch {
	case runtimeErr != nil && !s.harness.Enabled():
		writeError(w, http.StatusBadGateway, "runtime_unavailable", runtimeErr.Error())
	case runtimeErr != nil && harnessErr != nil:
		writeError(w, http.StatusBadGateway, "runtime_unavailable", runtimeErr.Error())
	case runtimeErr != nil:
		// The model runtime is down but the harness surface is usable on its
		// own; surface that instead of failing the whole endpoint.
		s.logger.Warn("runtime model listing failed; returning harness models only", "error", runtimeErr)
		writeJSON(w, http.StatusOK, map[string]any{"models": harnessModels})
	case harnessErr != nil:
		s.logger.Warn("harness model listing failed; returning runtime models only", "error", harnessErr)
		writeJSON(w, http.StatusOK, map[string]any{"models": runtimeModels})
	default:
		writeJSON(w, http.StatusOK, map[string]any{"models": append(runtimeModels, harnessModels...)})
	}
}
func (s *Server) conversations(w http.ResponseWriter, r *http.Request) {
	items, err := s.store.ListConversations(r.Context())
	if err != nil {
		s.internal(w, err)
		return
	}
	items = mergeObservedConversations(items, s.harness.ObservedSummaries())
	writeJSON(w, http.StatusOK, map[string]any{"conversations": items})
}
func (s *Server) createConversation(w http.ResponseWriter, r *http.Request) {
	var req protocol.CreateConversationRequest
	if !decode(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.ModelID) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "model_id is required")
		return
	}
	if IsOpenCodeModelID(req.ModelID) {
		if !s.harness.Enabled() {
			writeError(w, http.StatusConflict, "harness_unavailable", "this model requires an OpenCode server; start one and attach it with the connector's --opencode-url flag")
			return
		}
		if !s.harness.Allowed() {
			writeError(w, http.StatusConflict, "harness_unverified", "OpenCode support is unverified and disabled; restart the connector with --harness-allow-unverified to use it")
			return
		}
		client := s.harness.Client()
		sessionCtx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		session, err := client.CreateSession(sessionCtx, strings.TrimSpace(req.Title))
		cancel()
		if err != nil {
			writeError(w, http.StatusBadGateway, "harness_unavailable", "OpenCode session could not be created: "+err.Error())
			return
		}
		c, err := s.store.CreateHarnessConversation(r.Context(), req.ModelID, req.Title, session.ID)
		if err != nil {
			s.internal(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, c)
		return
	}
	c, err := s.store.CreateConversation(r.Context(), req.ModelID, req.Title)
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, c)
}
func (s *Server) conversation(w http.ResponseWriter, r *http.Request) {
	detail, err := s.store.Conversation(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		if observed, ok := s.harness.ObservedConversation(r.PathValue("id")); ok {
			writeJSON(w, http.StatusOK, observed)
			return
		}
		writeError(w, http.StatusNotFound, "not_found", "conversation not found")
		return
	}
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}
func (s *Server) deleteConversation(w http.ResponseWriter, r *http.Request) {
	// Serialize the preflight and deletion with message acceptance. Otherwise a
	// job can become active after the preflight, or an already-busy harness
	// session can be deleted before Store.DeleteConversation returns ErrBusy.
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if err := s.store.EnsureConversationDeletable(r.Context(), r.PathValue("id")); err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			writeError(w, http.StatusNotFound, "not_found", "conversation not found")
		case errors.Is(err, store.ErrBusy):
			writeError(w, http.StatusConflict, "conversation_busy", "stop the active response before deleting this conversation")
		default:
			s.internal(w, err)
		}
		return
	}
	// Harness conversations own a server-side session. Best-effort delete
	// before the store removal: a failure keeps the Localzie record intact
	// so the user can retry, which is the safer of the two leak directions.
	detail, err := s.store.Conversation(r.Context(), r.PathValue("id"))
	if err != nil {
		s.internal(w, err)
		return
	}
	if detail.Conversation.Backend == protocol.BackendHarness && detail.Conversation.HarnessSessionID != "" {
		if !s.harness.Enabled() {
			writeError(w, http.StatusBadGateway, "harness_delete_failed", "the OpenCode session cannot be deleted while the harness is unavailable; reconnect it and retry")
			return
		}
		client := s.harness.Client()
		sessionCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		sessionErr := client.DeleteSession(sessionCtx, detail.Conversation.HarnessSessionID)
		cancel()
		if sessionErr != nil {
			s.logger.Warn("OpenCode session delete failed; keeping the conversation", "session_id", detail.Conversation.HarnessSessionID, "error", sessionErr)
			writeError(w, http.StatusBadGateway, "harness_delete_failed", "the OpenCode session could not be deleted: "+sessionErr.Error())
			return
		}
	}
	err = s.store.DeleteConversation(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "conversation not found")
	case errors.Is(err, store.ErrBusy):
		writeError(w, http.StatusConflict, "conversation_busy", "stop the active response before deleting this conversation")
	case err != nil:
		s.internal(w, err)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}
func (s *Server) sendMessage(w http.ResponseWriter, r *http.Request) {
	var req protocol.SendMessageRequest
	if !decode(w, r, &req) {
		return
	}
	// Keep idempotency lookup and job acceptance together. A duplicate retry
	// returns its original persisted job without accepting a new one.
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if existing, err := s.store.JobForIdempotencyKey(r.Context(), req.IdempotencyKey); err == nil {
		writeJSON(w, http.StatusAccepted, protocol.SendMessageResponse{Job: existing, Duplicate: true})
		return
	} else if !errors.Is(err, store.ErrNotFound) {
		s.internal(w, err)
		return
	}
	// An observed session is being driven elsewhere on the harness. Writing
	// requires an explicit take-over; observing itself never attaches.
	var takeover *ObservedTakeover
	if _, observed := s.harness.ObservedConversation(r.PathValue("id")); observed {
		if !req.TakeOver {
			writeError(w, http.StatusConflict, "session_active_elsewhere", "this session is active on your computer; take it over to send from here")
			return
		}
		result, err := s.harness.TakeOverObserved(r.Context(), r.PathValue("id"))
		if err != nil {
			writeError(w, http.StatusConflict, "session_active_elsewhere", "this session is active on your computer; reconnect and take it over again")
			return
		}
		conversation, err := s.store.CreateHarnessConversationWithID(r.Context(), r.PathValue("id"), result.ModelID, result.Title, result.SessionID)
		if err != nil {
			s.internal(w, err)
			return
		}
		if err := s.store.UpsertHarnessMirrorMessages(r.Context(), conversation.ID, result.Transcript); err != nil {
			s.internal(w, err)
			return
		}
		takeover = &result
	}
	// Runtime and harness jobs both start thinking; there is no model-load
	// probe to resolve a different acceptance phase.
	req.InitialPhase = protocol.JobThinking
	job, duplicate, err := s.store.AcceptMessage(r.Context(), r.PathValue("id"), req)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "conversation not found")
	case errors.Is(err, store.ErrBusy):
		writeError(w, http.StatusConflict, "connector_busy", "another generation is active")
	case err != nil:
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
	default:
		if !duplicate {
			if takeover != nil {
				// A harness turn that was in flight when the take-over
				// arrived must finish before this prompt is submitted; the
				// job runner waits for idle and mirrors its result.
				job.AwaitIdle = takeover.Busy
				job.HarnessDirectory = takeover.Directory
			}
			s.jobs.Start(job)
		}
		writeJSON(w, http.StatusAccepted, protocol.SendMessageResponse{Job: job, Duplicate: duplicate})
	}
}
func (s *Server) stop(w http.ResponseWriter, r *http.Request) {
	err := s.jobs.Stop(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "active job not found")
		return
	}
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "canceling"})
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "stream_unsupported", "streaming unsupported")
		return
	}
	cursor, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	deviceID, _ := r.Context().Value(deviceContextKey{}).(string)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")
	// Flush an authenticated stream acknowledgement immediately. Some clients do
	// not expose response bytes until the first SSE frame arrives.
	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()
	ticker := time.NewTicker(300 * time.Millisecond)
	keepalive := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			if !s.store.DeviceActive(r.Context(), deviceID) {
				return
			}
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case <-ticker.C:
			// Authentication at accept time is insufficient for a persistent
			// stream: revocation must stop future event disclosure promptly.
			if !s.store.DeviceActive(r.Context(), deviceID) {
				return
			}
			items, err := s.store.EventsAfter(r.Context(), cursor, 100)
			if err != nil {
				return
			}
			for _, event := range items {
				envelope := map[string]any{"cursor": event.Cursor, "type": event.Type, "entity_id": event.EntityID, "payload": json.RawMessage(event.Payload), "created_at": event.CreatedAt}
				data, _ := json.Marshal(envelope)
				fmt.Fprintf(w, "id: %d\ndata: %s\n\n", event.Cursor, data)
				cursor = event.Cursor
			}
			if len(items) > 0 {
				flusher.Flush()
			}
		}
	}
}

func (s *Server) devices(w http.ResponseWriter, r *http.Request) {
	devices, err := s.store.ListDevices(r.Context())
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"devices": devices, "current_device_id": r.Context().Value(deviceContextKey{})})
}

func (s *Server) revokeDevice(w http.ResponseWriter, r *http.Request) {
	if err := s.store.RevokeDevice(r.Context(), r.PathValue("id")); errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "active device not found")
		return
	} else if err != nil {
		s.internal(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := bearerToken(r)
		deviceID, ok := s.store.AuthenticateDevice(r.Context(), token)
		if !ok {
			writeError(w, http.StatusUnauthorized, "unauthorized", "valid device token required")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), deviceContextKey{}, deviceID)))
	})
}
func bearerToken(r *http.Request) string {
	parts := strings.Fields(r.Header.Get("Authorization"))
	if len(parts) != 2 || parts[0] != "Bearer" {
		return ""
	}
	return parts[1]
}
func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}
func (s *Server) internal(w http.ResponseWriter, err error) {
	s.logger.Error("request failed", "error", err)
	writeError(w, http.StatusInternalServerError, "internal_error", "request failed")
}
func decode(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must contain one JSON value")
		return false
	}
	return true
}
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, protocol.ErrorResponse{Code: code, Message: message})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
