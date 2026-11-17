// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/harness"
	"github.com/manuelemosr/localzie-connector/internal/protocol"
	"github.com/manuelemosr/localzie-connector/internal/store"
)

const harnessModelCacheTTL = 30 * time.Second

const (
	// observedConversationPrefix namespaces mirrored harness sessions away from
	// store-owned conversation IDs; it stays stable so the phone's conversation
	// identity survives reconnects.
	observedConversationPrefix = "opencode-observed-"
	observedReconcileInterval  = 2 * time.Second
	observedFlushInterval      = 400 * time.Millisecond
	observedIdlePollInterval   = 750 * time.Millisecond
)

// errObservedNotFound reports a write against a session the connector is not
// currently mirroring.
var errObservedNotFound = errors.New("observed session not found")

// observedSession is the in-memory mirror of one harness session another
// client is driving. It is deliberately not a store-owned conversation: the
// harness stays authoritative and no SQLite record exists for it.
type observedSession struct {
	conversationID string
	sessionID      string
	title          string
	directory      string
	modelID        string
	busy           bool
	updatedAt      time.Time
	messages       []protocol.Message
	lastContent    map[string]string
	// mirrorConversationID is set after a take-over while the previously
	// in-flight turn is still running: its continued output is written into
	// the adopted store-owned conversation instead of the observed listing.
	mirrorConversationID string
}

func (s *observedSession) summary() protocol.Conversation {
	return protocol.Conversation{
		ID:          s.conversationID,
		Title:       s.title,
		ModelID:     s.modelID,
		Backend:     protocol.BackendHarness,
		ActiveSite:  "harness",
		ObserveOnly: true,
		CreatedAt:   s.updatedAt,
		UpdatedAt:   s.updatedAt,
	}
}

// ObservedTakeover is the connector-internal result of taking over an observed
// session: the harness identity to adopt plus the transcript already mirrored,
// and whether a turn was still running when the take-over arrived.
type ObservedTakeover struct {
	SessionID  string
	Title      string
	ModelID    string
	Directory  string
	Busy       bool
	Transcript []protocol.Message
}

// HarnessManager owns the connector's optional OpenCode attachment. It is a
// proof-of-concept integration: model surface, session-backed conversations,
// and streaming translation. The pinned-version compatibility fixtures in
// docs/integrations.md have not yet passed, so the harness keeps its
// detected_unverified integration status to API clients.
type HarnessManager struct {
	client *harness.OpenCodeClient
	logger *slog.Logger

	allowUnverified bool

	root  context.Context
	store *store.Store

	mu        sync.Mutex
	models    []protocol.Model
	fetchedAt time.Time

	observeOnce   sync.Once
	observeCancel context.CancelFunc
	observeMu     sync.Mutex
	observed      map[string]*observedSession
	observedDirty map[string]struct{}
}

func NewHarnessManager(client *harness.OpenCodeClient, allowUnverified bool, logger *slog.Logger, root context.Context, st *store.Store) *HarnessManager {
	return &HarnessManager{
		client:          client,
		logger:          logger,
		allowUnverified: allowUnverified,
		root:            root,
		store:           st,
		observed:        map[string]*observedSession{},
		observedDirty:   map[string]struct{}{},
	}
}

// Enabled reports whether a harness backend is attached. A nil manager is a
// disabled manager, so callers may invoke Enabled on a nil receiver.
func (m *HarnessManager) Enabled() bool { return m != nil && m.client != nil }

// Allowed reports whether the attached harness may be used for conversations.
// The compatibility fixtures in docs/integrations.md have not run,
// so usage requires the connector's explicit unverified-override flag.
func (m *HarnessManager) Allowed() bool { return m.Enabled() && m.allowUnverified }

// Client exposes the transport client to the job runner and conversation
// creation path.
func (m *HarnessManager) Client() *harness.OpenCodeClient {
	if !m.Enabled() {
		return nil
	}
	return m.client
}

// Models returns the OpenCode server's models with a short cache. Model IDs
// are namespaced as opencode/<provider>/<model> so they can never collide
// with runtime model IDs, and the connector routes on that prefix.
func (m *HarnessManager) Models(ctx context.Context) ([]protocol.Model, error) {
	if !m.Enabled() {
		return nil, fmt.Errorf("harness backend is not attached")
	}
	m.mu.Lock()
	if len(m.models) > 0 && time.Since(m.fetchedAt) < harnessModelCacheTTL {
		cached := m.models
		m.mu.Unlock()
		return cached, nil
	}
	m.mu.Unlock()
	providers, err := m.client.ConfigProviders(ctx)
	if err != nil {
		m.mu.Lock()
		stale := m.models
		m.mu.Unlock()
		if len(stale) > 0 {
			m.logger.Warn("harness model refresh failed; serving the last list", "error", err)
			return stale, nil
		}
		return nil, err
	}
	models := make([]protocol.Model, 0, 16)
	for _, provider := range providers {
		for modelID, info := range provider.Models {
			if info.Status != "" && info.Status != "active" {
				continue
			}
			name := strings.TrimSpace(info.Name)
			if name == "" {
				name = modelID
			}
			model := protocol.Model{
				ID:         OpenCodeModelID(provider.ID, modelID),
				Name:       name,
				Runtime:    "opencode",
				Ready:      true,
				Locality:   opencodeProviderLocality(provider),
				CanRelease: boolPointer(false),
			}
			if info.Capabilities.Reasoning {
				model.ReasoningMode = protocol.ReasoningOptional
				model.SupportsReasoning = &info.Capabilities.Reasoning
			}
			models = append(models, model)
		}
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	m.mu.Lock()
	m.models = models
	m.fetchedAt = time.Now()
	m.mu.Unlock()
	return models, nil
}

// Status reports the attached server's health for GET /v1/harnesses.
func (m *HarnessManager) Status(ctx context.Context) harness.ServerStatus {
	status := harness.ServerStatus{Attached: true}
	if !m.Enabled() {
		return status
	}
	status.URL = m.client.URL()
	version, err := m.client.Health(ctx)
	if err != nil {
		status.Problem = err.Error()
		return status
	}
	status.Healthy = true
	status.Version = version
	if models, err := m.Models(ctx); err == nil {
		status.ModelCount = len(models)
	}
	return status
}

// OpenCodeModelID builds the connector-scoped model ID for an OpenCode
// provider/model pair.
func OpenCodeModelID(provider, modelID string) string {
	return "opencode/" + provider + "/" + modelID
}

// IsOpenCodeModelID reports whether a model ID belongs to the harness
// surface. Conversation creation routes on this predicate.
func IsOpenCodeModelID(modelID string) bool {
	return strings.HasPrefix(modelID, "opencode/")
}

// mergeObservedConversations appends mirrored observed sessions to a
// store-owned listing, newest first. A conversation whose store-owned twin
// already exists (an in-progress or completed take-over) is never duplicated.
func mergeObservedConversations(owned, observed []protocol.Conversation) []protocol.Conversation {
	if len(observed) == 0 {
		return owned
	}
	existing := make(map[string]struct{}, len(owned))
	for _, conversation := range owned {
		existing[conversation.ID] = struct{}{}
	}
	merged := owned
	for _, conversation := range observed {
		if _, ok := existing[conversation.ID]; ok {
			continue
		}
		merged = append(merged, conversation)
	}
	sort.SliceStable(merged, func(i, j int) bool { return merged[i].UpdatedAt.After(merged[j].UpdatedAt) })
	return merged
}

// StartObserving begins mirroring harness sessions another client is driving.
// It is a no-op until the harness is attached and allowed. The loop is scoped
// to the connector's root context, so shutdown stops it.
func (m *HarnessManager) StartObserving() {
	if !m.Allowed() {
		return
	}
	m.observeOnce.Do(func() {
		ctx, cancel := context.WithCancel(m.root)
		m.observeCancel = cancel
		go m.observeLoop(ctx)
	})
}

// StopObserving ends the mirror loop and drops the in-memory registry.
func (m *HarnessManager) StopObserving() {
	m.observeMu.Lock()
	cancel := m.observeCancel
	m.observeCancel = nil
	m.observed = map[string]*observedSession{}
	m.observedDirty = map[string]struct{}{}
	m.observeMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (m *HarnessManager) observeLoop(ctx context.Context) {
	m.reconcileObserved(ctx)
	reconcile := time.NewTicker(observedReconcileInterval)
	flush := time.NewTicker(observedFlushInterval)
	defer reconcile.Stop()
	defer flush.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-reconcile.C:
			m.reconcileObserved(ctx)
		case <-flush.C:
			m.flushObserved(ctx)
		}
	}
}

// ObservedSummaries lists the currently mirrored sessions as additive
// observe-only conversation summaries. It performs no network work.
func (m *HarnessManager) ObservedSummaries() []protocol.Conversation {
	if !m.Enabled() {
		return nil
	}
	m.observeMu.Lock()
	summaries := make([]protocol.Conversation, 0, len(m.observed))
	for _, session := range m.observed {
		if session.mirrorConversationID != "" {
			continue
		}
		summaries = append(summaries, session.summary())
	}
	m.observeMu.Unlock()
	sort.Slice(summaries, func(i, j int) bool { return summaries[i].UpdatedAt.After(summaries[j].UpdatedAt) })
	return summaries
}

// ObservedConversation returns the mirrored transcript of one observed
// session. The second result is false for store-owned or unknown IDs, so
// callers fall back to the store.
func (m *HarnessManager) ObservedConversation(id string) (protocol.ConversationDetail, bool) {
	if !m.Enabled() {
		return protocol.ConversationDetail{}, false
	}
	m.observeMu.Lock()
	defer m.observeMu.Unlock()
	for _, session := range m.observed {
		if session.conversationID != id || session.mirrorConversationID != "" {
			continue
		}
		messages := append([]protocol.Message{}, session.messages...)
		return protocol.ConversationDetail{Conversation: session.summary(), Messages: messages}, true
	}
	return protocol.ConversationDetail{}, false
}

// TakeOverObserved adopts an observed conversation for Localzie control. An
// in-flight turn is never killed: while it runs, the session keeps mirroring
// its output into the adopted conversation until it reaches idle.
func (m *HarnessManager) TakeOverObserved(ctx context.Context, conversationID string) (ObservedTakeover, error) {
	if !m.Enabled() {
		return ObservedTakeover{}, errObservedNotFound
	}
	m.observeMu.Lock()
	var session *observedSession
	for _, candidate := range m.observed {
		if candidate.conversationID == conversationID && candidate.mirrorConversationID == "" {
			session = candidate
			break
		}
	}
	if session == nil {
		m.observeMu.Unlock()
		return ObservedTakeover{}, errObservedNotFound
	}
	result := ObservedTakeover{
		SessionID:  session.sessionID,
		Title:      session.title,
		ModelID:    session.modelID,
		Directory:  session.directory,
		Busy:       session.busy,
		Transcript: append([]protocol.Message{}, session.messages...),
	}
	if session.busy {
		session.mirrorConversationID = conversationID
	} else {
		delete(m.observed, session.sessionID)
		delete(m.observedDirty, session.sessionID)
	}
	m.observeMu.Unlock()
	m.publishObservedConversationEvent(ctx, "conversation.updated", conversationID)
	return result, nil
}

// WaitForIdle blocks until a taken-over session's in-flight turn reaches idle,
// then mirrors its final transcript into the adopted conversation. It never
// aborts or mutates the harness turn.
func (m *HarnessManager) WaitForIdle(ctx context.Context, sessionID, directory, conversationID string) {
	if !m.Enabled() {
		return
	}
	for {
		if ctx.Err() != nil {
			return
		}
		statuses, err := m.client.SessionStatuses(ctx, directory)
		if err == nil {
			status, present := statuses[sessionID]
			// OpenCode omits idle sessions from the map; absence from a map
			// that answered therefore means idle, not "unknown".
			if !present || status.Type == "idle" {
				m.finishMirror(ctx, sessionID, conversationID)
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(observedIdlePollInterval):
		}
	}
}

func (m *HarnessManager) finishMirror(ctx context.Context, sessionID, conversationID string) {
	if conversationID != "" {
		if entries, err := m.client.SessionMessages(ctx, sessionID); err == nil {
			_ = m.store.UpsertHarnessMirrorMessages(ctx, conversationID, renderObservedTranscript(conversationID, entries, false))
		}
	}
	m.observeMu.Lock()
	if session := m.observed[sessionID]; session != nil && session.mirrorConversationID == conversationID {
		delete(m.observed, sessionID)
		delete(m.observedDirty, sessionID)
	}
	m.observeMu.Unlock()
}

func (m *HarnessManager) reconcileObserved(ctx context.Context) {
	if !m.Enabled() {
		return
	}
	if !m.Allowed() {
		m.pruneObserved(ctx)
		return
	}
	owned := map[string]bool{}
	if m.store != nil {
		if ids, err := m.store.HarnessSessionIDs(ctx); err == nil {
			owned = ids
		}
	}
	sessions, err := m.client.GlobalSessions(ctx)
	if err != nil {
		// A failed listing keeps the registry: transient failure must not read
		// as "every session ended".
		return
	}
	byID := map[string]harness.OpenCodeSessionGlobal{}
	directories := map[string]struct{}{}
	for _, session := range sessions {
		if session.ParentID != "" {
			continue
		}
		byID[session.ID] = session
		if session.Directory != "" {
			directories[session.Directory] = struct{}{}
		}
	}
	statuses := map[string]harness.OpenCodeSessionStatus{}
	for directory := range directories {
		fetched, statusErr := m.client.SessionStatuses(ctx, directory)
		if statusErr != nil {
			continue
		}
		for id, status := range fetched {
			statuses[id] = status
		}
	}
	active := map[string]harness.OpenCodeSessionGlobal{}
	for id, status := range statuses {
		if status.Type != "busy" && status.Type != "retry" {
			continue
		}
		session, present := byID[id]
		if !present {
			continue
		}
		active[id] = session
	}
	m.applyObservedReconcile(ctx, active, owned)
}

func (m *HarnessManager) applyObservedReconcile(ctx context.Context, active map[string]harness.OpenCodeSessionGlobal, owned map[string]bool) {
	var added []string
	var prunedObserved []string
	m.observeMu.Lock()
	for id, session := range active {
		if existing, ok := m.observed[id]; ok {
			existing.title = observedTitle(session)
			existing.directory = session.Directory
			existing.modelID = observedModelID(session)
			existing.busy = true
			existing.updatedAt = observedUpdatedAt(session)
			continue
		}
		if owned[id] {
			continue
		}
		mirror := &observedSession{
			conversationID: observedConversationPrefix + id,
			sessionID:      id,
			title:          observedTitle(session),
			directory:      session.Directory,
			modelID:        observedModelID(session),
			busy:           true,
			updatedAt:      observedUpdatedAt(session),
			lastContent:    map[string]string{},
		}
		m.observed[id] = mirror
		m.observedDirty[id] = struct{}{}
		added = append(added, mirror.conversationID)
	}
	for id, session := range m.observed {
		if _, ok := active[id]; ok {
			continue
		}
		if session.mirrorConversationID == "" {
			prunedObserved = append(prunedObserved, session.conversationID)
		}
		delete(m.observed, id)
		delete(m.observedDirty, id)
	}
	m.observeMu.Unlock()
	for _, id := range added {
		m.publishObservedConversationEvent(ctx, "conversation.updated", id)
	}
	for _, id := range prunedObserved {
		m.publishObservedConversationEvent(ctx, "conversation.deleted", id)
	}
}

func (m *HarnessManager) pruneObserved(ctx context.Context) {
	m.observeMu.Lock()
	var pruned []string
	for id, session := range m.observed {
		if session.mirrorConversationID == "" {
			pruned = append(pruned, session.conversationID)
		}
		delete(m.observed, id)
		delete(m.observedDirty, id)
	}
	m.observeMu.Unlock()
	for _, id := range pruned {
		m.publishObservedConversationEvent(ctx, "conversation.deleted", id)
	}
}

func (m *HarnessManager) flushObserved(ctx context.Context) {
	type target struct {
		sessionID            string
		conversationID       string
		mirrorConversationID string
		busy                 bool
	}
	m.observeMu.Lock()
	targets := make([]target, 0, len(m.observed))
	for id, session := range m.observed {
		targets = append(targets, target{id, session.conversationID, session.mirrorConversationID, session.busy})
	}
	m.observeMu.Unlock()
	for _, target := range targets {
		m.refreshObserved(ctx, target.sessionID, target.conversationID, target.mirrorConversationID, target.busy)
	}
}

func (m *HarnessManager) refreshObserved(ctx context.Context, sessionID, conversationID, mirrorConversationID string, busy bool) {
	entries, err := m.client.SessionMessages(ctx, sessionID)
	if err != nil {
		m.observeMu.Lock()
		if _, ok := m.observed[sessionID]; ok {
			m.observedDirty[sessionID] = struct{}{}
		}
		m.observeMu.Unlock()
		return
	}
	messages := renderObservedTranscript(conversationID, entries, busy)
	var changed []protocol.Message
	added := false
	m.observeMu.Lock()
	session := m.observed[sessionID]
	if session == nil {
		m.observeMu.Unlock()
		return
	}
	if session.lastContent == nil {
		session.lastContent = map[string]string{}
	}
	for _, message := range messages {
		previous, seen := session.lastContent[message.ID]
		if seen && previous != message.Content {
			changed = append(changed, message)
		}
		if !seen {
			added = true
		}
		session.lastContent[message.ID] = message.Content
	}
	session.messages = messages
	m.observeMu.Unlock()
	if mirrorConversationID != "" {
		_ = m.store.UpsertHarnessMirrorMessages(ctx, mirrorConversationID, messages)
	}
	for _, message := range changed {
		m.publishObservedContentEvent(ctx, conversationID, message)
	}
	if added {
		m.observeMu.Lock()
		current := m.observed[sessionID]
		m.observeMu.Unlock()
		if current != nil {
			entity := current.conversationID
			if current.mirrorConversationID != "" {
				entity = current.mirrorConversationID
			}
			m.publishObservedConversationEvent(ctx, "conversation.updated", entity)
		}
	}
}

// publishObservedConversationEvent announces a listing-level change for an
// observed or mirrored conversation. The payload only needs the conversation
// ID: clients reconcile structure from the authoritative snapshot.
func (m *HarnessManager) publishObservedConversationEvent(ctx context.Context, eventType, conversationID string) {
	_ = m.store.PublishObservedEvent(ctx, eventType, conversationID, map[string]string{"id": conversationID})
}

// publishObservedContentEvent publishes the full-snapshot message.content
// event the protocol already defines for non-appenditive harness updates.
func (m *HarnessManager) publishObservedContentEvent(ctx context.Context, conversationID string, message protocol.Message) {
	_ = m.store.PublishObservedEvent(ctx, "message.content", message.ID, map[string]string{
		"conversation_id": conversationID,
		"generation_id":   message.ID,
		"content":         message.Content,
	})
}

func observedTitle(session harness.OpenCodeSessionGlobal) string {
	if title := strings.TrimSpace(session.Title); title != "" {
		return title
	}
	return "OpenCode session"
}

func observedModelID(session harness.OpenCodeSessionGlobal) string {
	if session.Model != nil && session.Model.ID != "" {
		return OpenCodeModelID(session.Model.ProviderID, session.Model.ID)
	}
	return "opencode/"
}

func observedUpdatedAt(session harness.OpenCodeSessionGlobal) time.Time {
	if session.Time.Updated > 0 {
		return time.UnixMilli(session.Time.Updated).UTC()
	}
	if session.Time.Created > 0 {
		return time.UnixMilli(session.Time.Created).UTC()
	}
	return time.Now().UTC()
}

// renderObservedTranscript turns a session transcript into the same inline
// content shape the controlled harness path renders: text as-is, reasoning
// wrapped in think tags, tool and unknown parts skipped. Observed sessions
// never use delta assembly.
func renderObservedTranscript(conversationID string, entries []harness.OpenCodeMessageEntry, busy bool) []protocol.Message {
	messages := make([]protocol.Message, 0, len(entries))
	for _, entry := range entries {
		role := protocol.RoleAssistant
		if entry.Info.Role == "user" {
			role = protocol.RoleUser
		}
		var content strings.Builder
		for _, part := range entry.Parts {
			switch part.Type {
			case "text":
				if part.Synthetic || part.Ignored {
					continue
				}
				content.WriteString(part.Text)
			case "reasoning":
				if part.Text == "" {
					continue
				}
				content.WriteString("\u003cthink\u003e")
				content.WriteString(part.Text)
				content.WriteString("</think>")
			}
		}
		messages = append(messages, protocol.Message{
			ID:             entry.Info.ID,
			ConversationID: conversationID,
			Role:           role,
			Content:        content.String(),
			Status:         protocol.MessageComplete,
			CreatedAt:      observedMessageTime(entry),
		})
	}
	if busy {
		for index := len(messages) - 1; index >= 0; index-- {
			if messages[index].Role == protocol.RoleAssistant {
				messages[index].Status = protocol.MessageStreaming
				break
			}
		}
	}
	return messages
}

func observedMessageTime(entry harness.OpenCodeMessageEntry) time.Time {
	if entry.Info.Time.Created > 0 {
		return time.UnixMilli(entry.Info.Time.Created).UTC()
	}
	return time.Now().UTC()
}

// opencodeProviderLocality classifies a provider. A custom provider whose
// base URL points at loopback fronts a user-run model server; known local
// provider names are local; everything else is conservatively reported as
// cloud so the phone never implies data stays on the user's computer when it may not.
func opencodeProviderLocality(provider harness.OpenCodeProvider) string {
	if provider.Options.BaseURL != "" {
		if parsed, err := url.Parse(provider.Options.BaseURL); err == nil {
			if host := parsed.Hostname(); host == "localhost" || net.ParseIP(host).IsLoopback() {
				return "local"
			}
		}
	}
	switch strings.ToLower(provider.ID) {
	case "ollama", "lmstudio", "llamacpp", "llama-cpp", "local", "openai-compatible":
		return "local"
	default:
		return "cloud"
	}
}
