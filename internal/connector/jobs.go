// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/harness"
	"github.com/manuelemosr/localzie-connector/internal/protocol"
	"github.com/manuelemosr/localzie-connector/internal/runtime"
	"github.com/manuelemosr/localzie-connector/internal/store"
)

const (
	maxJobDuration = 30 * time.Minute
	maxOutputBytes = 4 * 1024 * 1024
	maxTitleBytes  = 1024
)

var ErrOutputLimit = errors.New("generation exceeded the output limit")

type JobManager struct {
	root    context.Context
	store   *store.Store
	runtime runtime.Adapter
	harness *HarnessManager
	// harnessMu serializes harness work the way runtimeMu serializes runtime
	// work. The connector-wide single active job already prevents overlap;
	// the lock keeps that invariant explicit for the harness path too.
	harnessMu sync.Mutex
	runtimeMu sync.Mutex
	mu        sync.Mutex
	active    map[string]context.CancelFunc
}

func NewJobManager(root context.Context, store *store.Store, adapter runtime.Adapter) *JobManager {
	return &JobManager{root: root, store: store, runtime: adapter, active: map[string]context.CancelFunc{}}
}

func (m *JobManager) BackfillTitles() {
	go func() {
		conversations, err := m.store.ConversationsNeedingGeneratedTitle(m.root)
		if err != nil {
			return
		}
		for _, conversation := range conversations {
			if m.root.Err() != nil {
				return
			}
			m.generateTitle(conversation.ID, conversation.ModelID)
		}
	}()
}

func (m *JobManager) Start(job protocol.Job) {
	ctx, cancel := context.WithTimeout(m.root, maxJobDuration)
	m.mu.Lock()
	m.active[job.ID] = cancel
	m.mu.Unlock()
	go func() {
		defer func() { m.mu.Lock(); delete(m.active, job.ID); m.mu.Unlock(); cancel() }()
		m.run(ctx, job)
	}()
}

func (m *JobManager) Stop(ctx context.Context, jobID string) error {
	m.mu.Lock()
	cancel, ok := m.active[jobID]
	m.mu.Unlock()
	if !ok {
		return store.ErrNotFound
	}
	if err := m.store.MarkJobCanceling(ctx, jobID); err != nil {
		return err
	}
	cancel()
	return nil
}

func (m *JobManager) run(ctx context.Context, job protocol.Job) {
	if ctx.Err() != nil {
		_ = m.store.MarkJobCanceled(context.Background(), job.ID)
		return
	}
	if err := m.store.MarkJobRunning(context.Background(), job.ID); err != nil {
		if errors.Is(err, store.ErrInvalidState) && ctx.Err() != nil {
			_ = m.store.MarkJobCanceled(context.Background(), job.ID)
		}
		return
	}
	detail, err := m.store.Conversation(context.Background(), job.ConversationID)
	if err != nil {
		_ = m.store.MarkJobFailed(context.Background(), job.ID, err)
		return
	}
	if detail.Conversation.Backend == protocol.BackendHarness {
		m.runHarnessJob(ctx, job, detail)
		return
	}
	rawOutputBytes := 0
	// A runtime is never trusted to honor a disabled reasoning request. Keep
	// the persisted assistant message free of fallback <think> output unless
	// this generation explicitly opted in (required-reasoning models are sent
	// with true by the client).
	allowReasoning := job.AllowReasoning != nil && *job.AllowReasoning
	thinkingFilter := runtime.ThinkContentFilter{}
	appendPersisted := func(delta string) error {
		if delta == "" {
			return nil
		}
		return m.store.AppendDelta(context.Background(), job.ID, delta)
	}
	appendVisible := func(delta string) error {
		if delta == "" {
			return nil
		}
		// Count before filtering. A disabled response may buffer every byte
		// until completion to defend against a later orphan </think>, but it
		// must not evade the output cap.
		rawOutputBytes += len(delta)
		if rawOutputBytes > maxOutputBytes {
			return ErrOutputLimit
		}
		if !allowReasoning {
			delta = thinkingFilter.Filter(delta)
		}
		return appendPersisted(delta)
	}
	m.runtimeMu.Lock()
	err = m.runtime.Stream(ctx, runtime.StreamRequest{Model: job.ModelID, Messages: detail.Messages, AllowReasoning: job.AllowReasoning}, appendVisible)
	if err == nil && !allowReasoning {
		err = appendPersisted(thinkingFilter.Flush())
	}
	m.runtimeMu.Unlock()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		_ = m.store.MarkJobFailed(context.Background(), job.ID, errors.New("generation exceeded the 30 minute job limit"))
		return
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		_ = m.store.MarkJobCanceled(context.Background(), job.ID)
		return
	}
	if err != nil {
		_ = m.store.MarkJobFailed(context.Background(), job.ID, fmt.Errorf("generation failed: %w", err))
		return
	}
	if err := m.store.MarkJobCompleted(context.Background(), job.ID); err != nil {
		return
	}
	m.generateTitle(job.ConversationID, job.ModelID)
}

// runHarnessJob drives an accepted job through an OpenCode session. The
// submission and event translation are proof-of-concept scope: text and
// reasoning stream to the phone, tool activity and approvals do not yet.
func (m *JobManager) runHarnessJob(ctx context.Context, job protocol.Job, detail protocol.ConversationDetail) {
	if !m.harness.Enabled() {
		_ = m.store.MarkJobFailed(context.Background(), job.ID, errors.New("harness backend is not attached to this connector"))
		return
	}
	if !m.harness.Allowed() {
		_ = m.store.MarkJobFailed(context.Background(), job.ID, errors.New("harness conversations require the connector's --harness-allow-unverified flag"))
		return
	}
	sessionID := detail.Conversation.HarnessSessionID
	if sessionID == "" {
		_ = m.store.MarkJobFailed(context.Background(), job.ID, errors.New("conversation has no harness session"))
		return
	}
	prompt := ""
	for _, message := range detail.Messages {
		if message.ID == job.UserMessageID {
			prompt = message.Content
			break
		}
	}
	if prompt == "" {
		_ = m.store.MarkJobFailed(context.Background(), job.ID, errors.New("user message for this job is missing"))
		return
	}
	m.harnessMu.Lock()
	defer m.harnessMu.Unlock()

	if job.AwaitIdle {
		// A take-over must never kill a turn already running on the harness:
		// wait for it to reach idle (the observe mirror writes its result into
		// this conversation) before subscribing, so the previous turn's events
		// cannot be mistaken for this job's output. A turn started by another
		// client in the sub-second window between this idle check and the
		// prompt below could still be misattributed; the same residual race the
		// normal controlled path carries, bounded to that window.
		m.harness.WaitForIdle(ctx, sessionID, job.HarnessDirectory, job.ConversationID)
		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				_ = m.store.MarkJobFailed(context.Background(), job.ID, errors.New("generation exceeded the 30 minute job limit"))
			} else {
				_ = m.store.MarkJobCanceled(context.Background(), job.ID)
			}
			return
		}
	}
	// Subscribe before submitting so the first assistant updates cannot race
	// past an unconnected event stream. OpenCode's SSE has no replay cursor.
	events := make(chan harness.OpenCodeEvent, 64)
	connected := make(chan error, 1)
	streamDone := make(chan error, 1)
	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()
	client := m.harness.Client()
	fail := func(cause error) {
		_ = m.store.MarkJobFailed(context.Background(), job.ID, cause)
	}
	// Prompt submission is asynchronous, so any response error is ambiguous:
	// OpenCode may already have accepted and started the turn. Keep this helper
	// available before Prompt and use it for every failure after submission is
	// attempted, not only failures observed later on the event stream.
	abortTurn := func() string {
		abortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := client.Abort(abortCtx, sessionID); err != nil {
			return " (OpenCode turn abort unconfirmed: " + err.Error() + ")"
		}
		return ""
	}
	failAborted := func(cause error) {
		if note := abortTurn(); note != "" {
			cause = fmt.Errorf("%w%s", cause, note)
		}
		_ = m.store.MarkJobFailed(context.Background(), job.ID, cause)
	}
	go func() {
		streamDone <- client.Events(streamCtx, func() { connected <- nil }, func(raw json.RawMessage) error {
			var event harness.OpenCodeEvent
			if err := json.Unmarshal(raw, &event); err != nil || event.Type == "" {
				// One malformed envelope must not drop the stream.
				return nil
			}
			select {
			case events <- event:
				return nil
			case <-streamCtx.Done():
				return streamCtx.Err()
			}
		})
	}()
	select {
	case <-connected:
	case err := <-streamDone:
		_ = m.store.MarkJobFailed(context.Background(), job.ID, fmt.Errorf("OpenCode event stream failed: %w", err))
		return
	case <-ctx.Done():
		_ = m.store.MarkJobFailed(context.Background(), job.ID, errors.New("job context ended before the harness stream connected"))
		return
	}
	// The selected harness model travels with each turn so the server runs
	// the model the phone chose rather than its configured default.
	harnessModel := strings.TrimPrefix(job.ModelID, "opencode/")
	if err := client.Prompt(ctx, sessionID, prompt, harnessModel); err != nil {
		failAborted(fmt.Errorf("OpenCode prompt failed: %w", err))
		return
	}

	// OpenCode streams content as separate part events: message.updated
	// announces message infos without content, message.part.updated carries
	// part snapshots, and message.part.delta carries incremental text. Track
	// the parts of assistant messages in arrival order and emit rendered
	// suffixes into the store, mirroring runtime deltas. Reasoning parts are
	// wrapped in think tags so phones render them with the existing thinking
	// preview; tool and unknown parts are skipped for now.
	type trackedPart struct {
		id, kind, text string
		closed         bool
	}
	var parts []trackedPart
	partIndex := map[string]int{}
	assistantMessages := map[string]bool{}
	emitted := ""
	rawOutputBytes := 0
	turnStarted := false
	// Reasoning parts render wrapped in think tags, but the closing tag is
	// emitted lazily: while a reasoning part is still growing and nothing
	// follows it, appending its deltas stays a pure suffix append. The iOS
	// Markdown renderer already tolerates an unterminated <think> while
	// streaming.
	anyLaterNonEmpty := func(index int) bool {
		for j := index + 1; j < len(parts); j++ {
			if parts[j].text != "" {
				return true
			}
		}
		return false
	}
	render := func(final bool) string {
		var builder strings.Builder
		for _, part := range parts {
			switch part.kind {
			case "text":
				builder.WriteString(part.text)
			case "reasoning":
				if part.text == "" {
					continue
				}
				builder.WriteString("<think>")
				builder.WriteString(part.text)
				if part.closed || final {
					builder.WriteString("</think>")
				}
			}
		}
		return builder.String()
	}
	refreshClosers := func() {
		for j := range parts {
			if parts[j].kind == "reasoning" && anyLaterNonEmpty(j) {
				parts[j].closed = true
			}
		}
	}
	appendPartDelta := func(delta string) error {
		rawOutputBytes += len(delta)
		if rawOutputBytes > maxOutputBytes {
			return ErrOutputLimit
		}
		if err := m.store.AppendDelta(context.Background(), job.ID, delta); err != nil {
			return err
		}
		emitted += delta
		return nil
	}
	persistFull := func(full string) error {
		if delta, ok := suffixDelta(emitted, full); ok {
			if delta == "" {
				return nil
			}
			return appendPartDelta(delta)
		}
		// A replacement persists the whole corrected content; only its net
		// growth counts against the output cap so snapshot-driven streams
		// are not re-counted on every rewrite.
		if growth := len(full) - len(emitted); growth > 0 {
			rawOutputBytes += growth
			if rawOutputBytes > maxOutputBytes {
				return ErrOutputLimit
			}
		}
		if err := m.store.SetMessageContent(context.Background(), job.ID, full); err != nil {
			return err
		}
		emitted = full
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			note := abortTurn()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				fail(fmt.Errorf("generation exceeded the 30 minute job limit%s", note))
				return
			}
			if note != "" {
				fail(fmt.Errorf("stop was requested but the OpenCode turn state is uncertain%s", note))
				return
			}
			_ = m.store.MarkJobCanceled(context.Background(), job.ID)
			return
		case err := <-streamDone:
			failAborted(fmt.Errorf("OpenCode event stream ended before the turn finished: %w", err))
			return
		case event := <-events:
			switch event.Type {
			case "message.updated":
				message, err := event.Message()
				if err != nil || message.SessionID != sessionID {
					continue
				}
				turnStarted = true
				if message.Role == "assistant" {
					assistantMessages[message.ID] = true
				}
			case "message.part.updated":
				part, err := event.Part()
				if err != nil || part.SessionID != sessionID || !assistantMessages[part.MessageID] {
					continue
				}
				if part.Type != "text" && part.Type != "reasoning" {
					continue
				}
				oldText := ""
				oldKind := ""
				if index, ok := partIndex[part.ID]; ok {
					oldText = parts[index].text
					oldKind = parts[index].kind
					parts[index].kind = part.Type
					parts[index].text = part.Text
				} else {
					partIndex[part.ID] = len(parts)
					parts = append(parts, trackedPart{id: part.ID, kind: part.Type, text: part.Text})
				}
				index := partIndex[part.ID]
				partDelta, appenditive := suffixDelta(oldText, part.Text)
				if appenditive && oldText != "" && oldKind == part.Type && !anyLaterNonEmpty(index) && (part.Type == "text" || !parts[index].closed) {
					err = appendPartDelta(partDelta)
				} else {
					refreshClosers()
					err = persistFull(render(false))
				}
				if err != nil {
					failAborted(err)
					return
				}
			case "message.part.delta":
				deltaSession, deltaMessage, deltaPart, deltaField, deltaText, err := event.PartDelta()
				if err != nil || deltaSession != sessionID || !assistantMessages[deltaMessage] || deltaField != "text" || deltaText == "" {
					continue
				}
				if index, ok := partIndex[deltaPart]; ok {
					wasEmpty := parts[index].text == ""
					parts[index].text += deltaText
					if !anyLaterNonEmpty(index) && (parts[index].kind == "text" || !parts[index].closed) {
						if parts[index].kind == "reasoning" && wasEmpty {
							err = appendPartDelta("<think>" + deltaText)
						} else {
							err = appendPartDelta(deltaText)
						}
					} else {
						refreshClosers()
						err = persistFull(render(false))
					}
					if err != nil {
						failAborted(err)
						return
					}
				}
			case "session.idle":
				if event.SessionID() != sessionID {
					continue
				}
				// The stream also carries idles from earlier turns and from
				// other sessions on the same server. Only an idle after this
				// turn started means completion.
				if !turnStarted {
					continue
				}
				// Titles for harness conversations come from the first user
				// message at acceptance; runtime title generation is skipped.
				if final := render(true); final != emitted {
					if err := persistFull(final); err != nil {
						failAborted(err)
						return
					}
				}
				_ = m.store.MarkJobCompleted(context.Background(), job.ID)
				return
			case "session.error":
				if event.SessionID() != sessionID {
					continue
				}
				cause := "OpenCode reported a session error"
				if text := event.ErrorText(); text != "" {
					cause += ": " + text
				}
				fail(errors.New(cause))
				return
			}
		}
	}
}

// suffixDelta returns the appended suffix of next relative to previous. It
// reports false when the content changed non-appenditively, in which case the
// caller resyncs without emitting.
func suffixDelta(previous, next string) (string, bool) {
	if next == previous {
		return "", true
	}
	if strings.HasPrefix(next, previous) {
		return next[len(previous):], true
	}
	return "", false
}

func (m *JobManager) generateTitle(conversationID, modelID string) {
	detail, err := m.store.Conversation(context.Background(), conversationID)
	if err != nil || len(detail.Messages) < 2 {
		return
	}
	if shouldDeferTitle(detail.Messages) {
		return
	}
	prompt := "Create a concise 3 to 6 word title that summarizes the conversation below. " +
		"Return only the title, with no quotes, label, or ending punctuation.\n\n" +
		titleTranscript(detail.Messages)
	titleContext, cancel := context.WithTimeout(m.root, 30*time.Second)
	defer cancel()
	var output strings.Builder
	m.runtimeMu.Lock()
	err = m.runtime.Stream(titleContext, runtime.StreamRequest{
		Model: modelID,
		Messages: []protocol.Message{
			{Role: protocol.RoleUser, Content: prompt},
			{Role: protocol.RoleAssistant, Content: "Title:"},
		},
		MaxOutputTokens:   24,
		AllowReasoning:    boolPointer(false),
		SuppressReasoning: true,
	}, func(delta string) error {
		if output.Len()+len(delta) > maxTitleBytes {
			return ErrOutputLimit
		}
		_, writeErr := output.WriteString(delta)
		return writeErr
	})
	m.runtimeMu.Unlock()
	if err != nil {
		return
	}
	if title := cleanGeneratedTitle(output.String()); title != "" {
		_ = m.store.UpdateConversationTitle(context.Background(), conversationID, title)
	}
}

func boolPointer(value bool) *bool { return &value }

func shouldDeferTitle(messages []protocol.Message) bool {
	userMessages := 0
	firstPrompt := ""
	for _, message := range messages {
		if message.Role == protocol.RoleUser && strings.TrimSpace(message.Content) != "" {
			userMessages++
			if firstPrompt == "" {
				firstPrompt = strings.ToLower(strings.Trim(strings.TrimSpace(message.Content), "!?.,"))
			}
		}
	}
	if userMessages > 1 {
		return false
	}
	switch firstPrompt {
	case "hi", "hey", "hello", "yo", "sup", "howdy", "good morning", "good afternoon", "good evening":
		return true
	default:
		return false
	}
}

func titleTranscript(messages []protocol.Message) string {
	const maxRunes = 6000
	var transcript strings.Builder
	for _, message := range messages {
		content := strings.TrimSpace(message.Content)
		if content == "" {
			continue
		}
		role := "Assistant"
		if message.Role == protocol.RoleUser {
			role = "User"
		}
		fmt.Fprintf(&transcript, "%s:\n%s\n\n", role, content)
	}
	runes := []rune(transcript.String())
	if len(runes) > maxRunes {
		runes = runes[:maxRunes]
	}
	return strings.TrimSpace(string(runes))
}

func cleanGeneratedTitle(value string) string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "<think>") {
		if end := strings.LastIndex(value, "</think>"); end >= 0 {
			value = value[end+len("</think>"):]
		}
	}
	value = strings.TrimSpace(value)
	if line, _, found := strings.Cut(value, "\n"); found {
		value = line
	}
	lower := strings.ToLower(value)
	for _, prefix := range []string{"title:", "conversation title:"} {
		if strings.HasPrefix(lower, prefix) {
			value = strings.TrimSpace(value[len(prefix):])
			break
		}
	}
	value = strings.Trim(value, " \t\r\n\"'`#*._-")
	lower = strings.ToLower(value)
	for _, prefix := range []string{"okay ", "okay,", "we are ", "we need ", "i need ", "i should ", "let me ", "the user "} {
		if strings.HasPrefix(lower, prefix) {
			return ""
		}
	}
	words := strings.Fields(value)
	if len(words) > 8 {
		words = words[:8]
	}
	value = strings.Join(words, " ")
	runes := []rune(value)
	if len(runes) > 64 {
		value = strings.TrimSpace(string(runes[:64])) + "…"
	}
	return strings.TrimRight(value, " .!?")
}
