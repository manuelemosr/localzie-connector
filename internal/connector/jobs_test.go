// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/protocol"
	"github.com/manuelemosr/localzie-connector/internal/runtime"
	"github.com/manuelemosr/localzie-connector/internal/store"
)

type titleRuntime struct {
	output  string
	request runtime.StreamRequest
}

type streamingRuntime struct {
	deltas  []string
	request runtime.StreamRequest
}

type blockingReasoningRuntime struct {
	started chan struct{}
	release chan struct{}
}

type oversizedReasoningRuntime struct{ callbackError error }

func (f *titleRuntime) Name() string                                     { return "title-test" }
func (f *titleRuntime) Models(context.Context) ([]protocol.Model, error) { return nil, nil }
func (f *titleRuntime) Stream(_ context.Context, request runtime.StreamRequest, onDelta func(string) error) error {
	f.request = request
	return onDelta(f.output)
}

func (f *streamingRuntime) Name() string                                     { return "streaming-test" }
func (f *streamingRuntime) Models(context.Context) ([]protocol.Model, error) { return nil, nil }
func (f *streamingRuntime) Stream(_ context.Context, request runtime.StreamRequest, onDelta func(string) error) error {
	f.request = request
	for _, delta := range f.deltas {
		if err := onDelta(delta); err != nil {
			return err
		}
	}
	return nil
}

func (f *blockingReasoningRuntime) Name() string                                     { return "blocking-reasoning-test" }
func (f *blockingReasoningRuntime) Models(context.Context) ([]protocol.Model, error) { return nil, nil }
func (f *blockingReasoningRuntime) Stream(_ context.Context, _ runtime.StreamRequest, onDelta func(string) error) error {
	if err := onDelta("private chain</think>"); err != nil {
		return err
	}
	close(f.started)
	<-f.release
	return onDelta("Visible answer")
}

func (f *oversizedReasoningRuntime) Name() string { return "oversized-reasoning-test" }
func (f *oversizedReasoningRuntime) Models(context.Context) ([]protocol.Model, error) {
	return nil, nil
}
func (f *oversizedReasoningRuntime) Stream(_ context.Context, _ runtime.StreamRequest, onDelta func(string) error) error {
	f.callbackError = onDelta("<think>" + strings.Repeat("x", maxOutputBytes))
	return f.callbackError
}

func TestGenerateTitleUsesFirstCompletedExchange(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	conversation, _ := db.CreateConversation(ctx, "model", "")
	job, _, _ := db.AcceptMessage(ctx, conversation.ID, protocol.SendMessageRequest{IdempotencyKey: "title", Content: "What is YOLO in machine learning?"})
	_ = db.MarkJobRunning(ctx, job.ID)
	_ = db.AppendDelta(ctx, job.ID, "YOLO is a real-time object detection architecture.")
	_ = db.MarkJobCompleted(ctx, job.ID)
	before, _ := db.Conversation(ctx, conversation.ID)

	titleModel := &titleRuntime{output: "Title: Real-Time Object Detection.\n"}
	manager := NewJobManager(ctx, db, titleModel)
	manager.generateTitle(job.ConversationID, job.ModelID)

	after, err := db.Conversation(ctx, conversation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Conversation.Title != "Real-Time Object Detection" {
		t.Fatalf("title = %q", after.Conversation.Title)
	}
	if !after.Conversation.UpdatedAt.Equal(before.Conversation.UpdatedAt) {
		t.Fatalf("title generation changed last-message time from %v to %v", before.Conversation.UpdatedAt, after.Conversation.UpdatedAt)
	}
	pending, err := db.ConversationsNeedingGeneratedTitle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("generated conversation still needs a title: %+v", pending)
	}
	if titleModel.request.MaxOutputTokens != 24 {
		t.Fatalf("title request controls = %+v", titleModel.request)
	}
	if titleModel.request.AllowReasoning == nil || *titleModel.request.AllowReasoning {
		t.Fatalf("title request must explicitly disable reasoning: %+v", titleModel.request)
	}
	if !titleModel.request.SuppressReasoning {
		t.Fatalf("title request must suppress persisted reasoning: %+v", titleModel.request)
	}
	if len(titleModel.request.Messages) != 2 ||
		titleModel.request.Messages[1].Role != protocol.RoleAssistant ||
		titleModel.request.Messages[1].Content != "Title:" {
		t.Fatalf("title request does not constrain the response with an assistant prefix: %+v", titleModel.request.Messages)
	}
	if !strings.Contains(titleModel.request.Messages[0].Content, "What is YOLO") ||
		!strings.Contains(titleModel.request.Messages[0].Content, "real-time object detection") {
		t.Fatalf("title prompt does not contain the exchange: %q", titleModel.request.Messages[0].Content)
	}
}

func TestTitleWaitsPastGreetingForConversationContext(t *testing.T) {
	messages := []protocol.Message{
		{Role: protocol.RoleUser, Content: "Hey!"},
		{Role: protocol.RoleAssistant, Content: "How can I help?"},
	}
	if !shouldDeferTitle(messages) {
		t.Fatal("greeting-only exchange should defer its title")
	}
	messages = append(messages,
		protocol.Message{Role: protocol.RoleUser, Content: "Who are you?"},
		protocol.Message{Role: protocol.RoleAssistant, Content: "I am your local assistant."},
	)
	if shouldDeferTitle(messages) {
		t.Fatal("conversation with a substantive follow-up should be titled")
	}
	transcript := titleTranscript(messages)
	if !strings.Contains(transcript, "Who are you?") || !strings.Contains(transcript, "local assistant") {
		t.Fatalf("transcript omitted later context: %q", transcript)
	}
}

func TestCleanGeneratedTitle(t *testing.T) {
	got := cleanGeneratedTitle("<think>reasoning</think>\nConversation title: Local Models for Object Detection.\nExtra")
	if got != "Local Models for Object Detection" {
		t.Fatalf("clean title = %q", got)
	}
	if got := cleanGeneratedTitle("Okay, so I need to create a concise title"); got != "" {
		t.Fatalf("reasoning preamble accepted as title: %q", got)
	}
	if got := cleanGeneratedTitle("Who Are You?\n<think>reasoning</think>"); got != "Who Are You" {
		t.Fatalf("title punctuation was not cleaned: %q", got)
	}
}

func TestJobManagerPersistsThinkingOnlyWhenGenerationExplicitlyEnablesIt(t *testing.T) {
	for _, test := range []struct {
		name           string
		allowReasoning *bool
		deltas         []string
		wantContent    string
	}{
		{"non-reasoning model", nil, []string{"<thi", "nk>private chain</think>Visible", " answer"}, "Visible answer"},
		{"reasoning disabled", boolPointer(false), []string{"<thi", "nk>private chain</think>Visible", " answer"}, "Visible answer"},
		{"disabled orphan closing tag", boolPointer(false), []string{"private chain</th", "ink>Visible", " answer"}, "Visible answer"},
		{"reasoning enabled", boolPointer(true), []string{"<thi", "nk>private chain</think>Visible", " answer"}, "<think>private chain</think>Visible answer"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			conversation, err := db.CreateConversation(context.Background(), "model", "New chat")
			if err != nil {
				t.Fatal(err)
			}
			job, _, err := db.AcceptMessage(context.Background(), conversation.ID, protocol.SendMessageRequest{
				IdempotencyKey: test.name,
				Content:        "Hi",
				AllowReasoning: test.allowReasoning,
			})
			if err != nil {
				t.Fatal(err)
			}
			adapter := &streamingRuntime{deltas: test.deltas}
			manager := NewJobManager(context.Background(), db, adapter)
			manager.run(context.Background(), job)

			detail, err := db.Conversation(context.Background(), conversation.ID)
			if err != nil {
				t.Fatal(err)
			}
			assistant := detail.Messages[len(detail.Messages)-1]
			if assistant.Content != test.wantContent {
				t.Fatalf("persisted assistant content = %q, want %q", assistant.Content, test.wantContent)
			}
			if adapter.request.AllowReasoning != test.allowReasoning {
				t.Fatalf("runtime reasoning request = %p, want %p", adapter.request.AllowReasoning, test.allowReasoning)
			}
			events, err := db.EventsAfter(context.Background(), 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			var deltaCount int
			for _, event := range events {
				if event.Type != "message.delta" {
					continue
				}
				var payload struct {
					ConversationID string `json:"conversation_id"`
					GenerationID   string `json:"generation_id"`
				}
				if err := json.Unmarshal(event.Payload, &payload); err != nil {
					t.Fatal(err)
				}
				if payload.ConversationID != conversation.ID || payload.GenerationID != job.ID {
					t.Fatalf("delta payload = %+v", payload)
				}
				deltaCount++
			}
			if deltaCount == 0 {
				t.Fatal("generation did not emit a delta event")
			}
		})
	}
}

func TestHiddenReasoningKeepsJobInThinkingPhase(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conversation, err := db.CreateConversation(context.Background(), "model", "New chat")
	if err != nil {
		t.Fatal(err)
	}
	disabled := false
	job, _, err := db.AcceptMessage(context.Background(), conversation.ID, protocol.SendMessageRequest{IdempotencyKey: "phase-before-filter", Content: "Hi", AllowReasoning: &disabled})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &blockingReasoningRuntime{started: make(chan struct{}), release: make(chan struct{})}
	manager := NewJobManager(context.Background(), db, adapter)
	manager.Start(job)
	select {
	case <-adapter.started:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not produce its first raw delta")
	}
	stored, err := db.Job(context.Background(), job.ID)
	if err != nil || stored.Phase != protocol.JobThinking {
		t.Fatalf("job after first hidden delta = %+v, err=%v", stored, err)
	}
	close(adapter.release)
	waitForJobCompletion(t, db, job.ID)
	detail, err := db.Conversation(context.Background(), conversation.ID)
	if err != nil || detail.Messages[len(detail.Messages)-1].Content != "Visible answer" {
		t.Fatalf("visible assistant content = %+v, err=%v", detail.Messages, err)
	}
}

func TestDisabledReasoningCountsHiddenBytesTowardOutputLimit(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conversation, err := db.CreateConversation(context.Background(), "model", "New chat")
	if err != nil {
		t.Fatal(err)
	}
	disabled := false
	job, _, err := db.AcceptMessage(context.Background(), conversation.ID, protocol.SendMessageRequest{IdempotencyKey: "hidden-limit", Content: "Hi", AllowReasoning: &disabled})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &oversizedReasoningRuntime{}
	manager := NewJobManager(context.Background(), db, adapter)
	manager.run(context.Background(), job)
	if !errors.Is(adapter.callbackError, ErrOutputLimit) {
		t.Fatalf("hidden reasoning callback error = %v, want ErrOutputLimit", adapter.callbackError)
	}
	stored, err := db.Job(context.Background(), job.ID)
	if err != nil || stored.Status != protocol.JobFailed || !strings.Contains(stored.Error, ErrOutputLimit.Error()) {
		t.Fatalf("oversized hidden response job = %+v, err=%v", stored, err)
	}
}

func waitForJobCompletion(t *testing.T, db *store.Store, jobID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, err := db.Job(context.Background(), jobID)
		if err == nil && job.Status == protocol.JobCompleted {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job did not complete")
}
