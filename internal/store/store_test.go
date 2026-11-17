// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/protocol"
)

func TestPairingTokenIsSingleUse(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	token, _, err := s.CreatePairing(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	deviceID, deviceToken, err := s.ConsumePairing(ctx, token, "Manuel's iPhone")
	if err != nil {
		t.Fatal(err)
	}
	if authenticatedID, ok := s.AuthenticateDevice(ctx, deviceToken); !ok || authenticatedID != deviceID {
		t.Fatalf("device did not authenticate: id=%q ok=%v", authenticatedID, ok)
	}
	if _, _, err := s.ConsumePairing(ctx, token, "Second phone"); !errors.Is(err, ErrInvalidPairing) {
		t.Fatalf("second consumption error = %v, want %v", err, ErrInvalidPairing)
	}
	devices, err := s.ListDevices(ctx)
	if err != nil || len(devices) != 1 || devices[0].ID != deviceID {
		t.Fatalf("devices=%+v err=%v", devices, err)
	}
	if err := s.RevokeDevice(ctx, deviceID); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.AuthenticateDevice(ctx, deviceToken); ok {
		t.Fatal("revoked device still authenticates")
	}
}

func TestShortPairingCodeAndAutomaticLocalDevice(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	code, expires, err := s.CreatePairingCode(ctx, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[A-HJ-NP-Z2-9]{6}$`).MatchString(code) {
		t.Fatalf("pairing code = %q", code)
	}
	if time.Until(expires) < 9*time.Minute {
		t.Fatalf("pairing expiry = %v", expires)
	}
	if _, _, err := s.ConsumePairing(ctx, code, "Code iPhone"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ConsumePairing(ctx, code, "Replay iPhone"); !errors.Is(err, ErrInvalidPairing) {
		t.Fatalf("replayed pairing code error = %v", err)
	}
	deviceID, deviceToken, err := s.CreateDevice(ctx, "Local iPhone")
	if err != nil {
		t.Fatal(err)
	}
	if authenticatedID, ok := s.AuthenticateDevice(ctx, deviceToken); !ok || authenticatedID != deviceID {
		t.Fatalf("automatic local device did not authenticate: id=%q ok=%v", authenticatedID, ok)
	}
}

func TestEmptyCollectionsEncodeAsArrays(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	devices, err := s.ListDevices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if devices == nil || len(devices) != 0 {
		t.Fatalf("devices = %#v, want a non-nil empty slice", devices)
	}

	conversations, err := s.ListConversations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if conversations == nil || len(conversations) != 0 {
		t.Fatalf("conversations = %#v, want a non-nil empty slice", conversations)
	}
}

func TestAcceptMessageIsDurableIdempotentAndSerialized(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	conversation, err := s.CreateConversation(ctx, "qwen3:8b", "")
	if err != nil {
		t.Fatal(err)
	}
	request := protocol.SendMessageRequest{IdempotencyKey: "send-1", Content: "Explain this repository"}
	job, duplicate, err := s.AcceptMessage(ctx, conversation.ID, request)
	if err != nil || duplicate {
		t.Fatalf("first accept: job=%+v duplicate=%v err=%v", job, duplicate, err)
	}
	replayed, duplicate, err := s.AcceptMessage(ctx, conversation.ID, request)
	if err != nil || !duplicate || replayed.ID != job.ID {
		t.Fatalf("replay: job=%+v duplicate=%v err=%v", replayed, duplicate, err)
	}
	_, _, err = s.AcceptMessage(ctx, conversation.ID, protocol.SendMessageRequest{IdempotencyKey: "send-2", Content: "Another request"})
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("parallel accept error = %v, want %v", err, ErrBusy)
	}
	if err := s.MarkJobRunning(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendDelta(ctx, job.ID, "A partial "); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendDelta(ctx, job.ID, "answer."); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkJobCompleted(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkJobFailed(ctx, job.ID, errors.New("late failure")); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("late transition error = %v, want %v", err, ErrInvalidState)
	}
	detail, err := s.Conversation(ctx, conversation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := detail.Messages[len(detail.Messages)-1]; got.Content != "A partial answer." || got.Status != protocol.MessageComplete {
		t.Fatalf("assistant message = %+v", got)
	}
	events, err := s.EventsAfter(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 6 {
		t.Fatalf("event count = %d, want 6", len(events))
	}
	if events[len(events)-1].Type != "job.completed" {
		t.Fatalf("last event = %q", events[len(events)-1].Type)
	}
	var deltaEvents int
	for _, event := range events {
		if event.Type != "message.delta" {
			continue
		}
		var payload struct {
			ConversationID string `json:"conversation_id"`
			GenerationID   string `json:"generation_id"`
			Delta          string `json:"delta"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.ConversationID != conversation.ID || payload.GenerationID != job.ID || payload.Delta == "" {
			t.Fatalf("delta payload = %+v", payload)
		}
		deltaEvents++
	}
	if deltaEvents != 2 {
		t.Fatalf("delta event count = %d, want 2", deltaEvents)
	}
	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Cursor != events[len(events)-1].Cursor || snapshot.ActiveJob != nil || len(snapshot.Conversations) != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestSetMessageContentReplacesStreamingContent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	conversation, err := s.CreateConversation(ctx, "test-model", "New chat")
	if err != nil {
		t.Fatal(err)
	}
	job, duplicate, err := s.AcceptMessage(ctx, conversation.ID, protocol.SendMessageRequest{IdempotencyKey: "replace-content", Content: "Explain"})
	if err != nil || duplicate {
		t.Fatalf("accept: job=%+v duplicate=%v err=%v", job, duplicate, err)
	}
	if err := s.MarkJobRunning(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMessageContent(ctx, job.ID, "first replacement"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMessageContent(ctx, job.ID, "second replacement"); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendDelta(ctx, job.ID, " plus more"); err != nil {
		t.Fatal(err)
	}
	detail, err := s.Conversation(ctx, conversation.ID)
	if err != nil {
		t.Fatal(err)
	}
	assistant := detail.Messages[len(detail.Messages)-1]
	if assistant.Content != "second replacement plus more" {
		t.Fatalf("content = %q", assistant.Content)
	}
	events, err := s.EventsAfter(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var contentEvents int
	for _, event := range events {
		if event.Type != "message.content" {
			continue
		}
		var payload struct {
			ConversationID string `json:"conversation_id"`
			GenerationID   string `json:"generation_id"`
			Content        string `json:"content"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.ConversationID != conversation.ID || payload.GenerationID != job.ID || payload.Content == "" {
			t.Fatalf("content payload = %+v", payload)
		}
		contentEvents++
	}
	if contentEvents != 2 {
		t.Fatalf("message.content event count = %d, want 2", contentEvents)
	}
}

func TestAcceptMessagePersistsOptionalReasoningPreference(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()
	ctx := context.Background()
	conversation, err := s.CreateConversation(ctx, "test-model", "New chat")
	if err != nil {
		t.Fatal(err)
	}
	allowReasoning := false
	job, duplicate, err := s.AcceptMessage(ctx, conversation.ID, protocol.SendMessageRequest{
		IdempotencyKey: "reasoning-off",
		Content:        "Answer directly",
		AllowReasoning: &allowReasoning,
	})
	if err != nil || duplicate || job.AllowReasoning == nil || *job.AllowReasoning {
		t.Fatalf("job=%+v duplicate=%v err=%v", job, duplicate, err)
	}
	stored, err := s.Job(ctx, job.ID)
	if err != nil || stored.AllowReasoning == nil || *stored.AllowReasoning {
		t.Fatalf("stored=%+v err=%v", stored, err)
	}
}

func TestJobPhasePersistsInEventsIdempotencyAndSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "localzie.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	conversation, err := s.CreateConversation(ctx, "test-model", "New chat")
	if err != nil {
		t.Fatal(err)
	}
	request := protocol.SendMessageRequest{IdempotencyKey: "phase-job", Content: "Hello"}
	job, duplicate, err := s.AcceptMessage(ctx, conversation.ID, request)
	if err != nil || duplicate || job.Phase != protocol.JobThinking {
		t.Fatalf("accepted job=%+v duplicate=%v err=%v", job, duplicate, err)
	}
	if replay, duplicate, err := s.AcceptMessage(ctx, conversation.ID, request); err != nil || !duplicate || replay.Phase != protocol.JobThinking {
		t.Fatalf("idempotent replay=%+v duplicate=%v err=%v", replay, duplicate, err)
	}
	if err := s.MarkJobRunning(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.SetJobPhase(ctx, job.ID, protocol.JobPreparingModel); err != nil {
		t.Fatal(err)
	}
	stored, err := s.Job(ctx, job.ID)
	if err != nil || stored.Phase != protocol.JobPreparingModel {
		t.Fatalf("stored job=%+v err=%v", stored, err)
	}
	snapshot, err := s.Snapshot(ctx)
	if err != nil || snapshot.ActiveJob == nil || snapshot.ActiveJob.Phase != protocol.JobPreparingModel {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	events, err := s.EventsAfter(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Type != "job.phase" {
		t.Fatalf("last event=%q", last.Type)
	}
	var payload protocol.Job
	if err := json.Unmarshal(last.Payload, &payload); err != nil || payload.Phase != protocol.JobPreparingModel {
		t.Fatalf("phase payload=%+v err=%v", payload, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	snapshot, err = s.Snapshot(ctx)
	if err != nil || snapshot.ActiveJob == nil || snapshot.ActiveJob.Phase != protocol.JobPreparingModel {
		t.Fatalf("restart snapshot=%+v err=%v", snapshot, err)
	}
}

func TestMigrateJobPhaseDefaultsExistingRowsToThinking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacy.Exec(`CREATE TABLE jobs (id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, user_message_id TEXT NOT NULL, assistant_message_id TEXT NOT NULL, model_id TEXT NOT NULL, allow_reasoning INTEGER, status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
		INSERT INTO jobs(id,conversation_id,user_message_id,assistant_message_id,model_id,status,error,created_at,updated_at) VALUES('legacy','conversation','user','assistant','model','running','','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	job, err := s.Job(context.Background(), "legacy")
	if err != nil || job.Phase != protocol.JobThinking {
		t.Fatalf("migrated job=%+v err=%v", job, err)
	}
}

func TestConversationMetadataTracksLatestMessageAndDeletionCascades(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	conversation, err := s.CreateConversation(ctx, "qwen3:4b", "")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	job, _, err := s.AcceptMessage(ctx, conversation.ID, protocol.SendMessageRequest{
		IdempotencyKey: "metadata-send",
		Content:        "Explain real-time object detection",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteConversation(ctx, conversation.ID); !errors.Is(err, ErrBusy) {
		t.Fatalf("delete active conversation error = %v, want %v", err, ErrBusy)
	}
	items, err := s.ListConversations(ctx)
	if err != nil || len(items) != 1 {
		t.Fatalf("conversations = %+v, err = %v", items, err)
	}
	if items[0].Title != "Explain real-time object detection" || !items[0].UpdatedAt.After(conversation.UpdatedAt) {
		t.Fatalf("metadata after send = %+v", items[0])
	}
	afterSend := items[0].UpdatedAt
	if err := s.MarkJobRunning(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if err := s.MarkJobCompleted(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	items, _ = s.ListConversations(ctx)
	if !items[0].UpdatedAt.After(afterSend) {
		t.Fatalf("completion time %v did not advance past send time %v", items[0].UpdatedAt, afterSend)
	}
	lastMessageTime := items[0].UpdatedAt
	if err := s.UpdateConversationTitle(ctx, conversation.ID, "Real-Time Object Detection"); err != nil {
		t.Fatal(err)
	}
	items, _ = s.ListConversations(ctx)
	if items[0].Title != "Real-Time Object Detection" || !items[0].UpdatedAt.Equal(lastMessageTime) {
		t.Fatalf("metadata after title update = %+v, want unchanged message time %v", items[0], lastMessageTime)
	}
	if err := s.DeleteConversation(ctx, conversation.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Conversation(ctx, conversation.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("conversation lookup after delete = %v, want %v", err, ErrNotFound)
	}
	if _, err := s.Job(ctx, job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("job lookup after delete = %v, want %v", err, ErrNotFound)
	}
	items, _ = s.ListConversations(ctx)
	if len(items) != 0 {
		t.Fatalf("conversations after delete = %+v", items)
	}
	events, err := s.EventsAfter(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if events[len(events)-1].Type != "conversation.deleted" {
		t.Fatalf("last event = %q, want conversation.deleted", events[len(events)-1].Type)
	}
}

func TestRecoverInterruptedPreservesPartialOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "localzie.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	conversation, _ := s.CreateConversation(ctx, "model", "Recovery")
	job, _, _ := s.AcceptMessage(ctx, conversation.ID, protocol.SendMessageRequest{IdempotencyKey: "recovery", Content: "Long task"})
	if err := s.MarkJobRunning(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendDelta(ctx, job.ID, "work so far"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.RecoverInterrupted(ctx); err != nil {
		t.Fatal(err)
	}
	recoveredJob, err := reopened.Job(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredJob.Status != protocol.JobInterrupted || recoveredJob.Error != "connector restarted" {
		t.Fatalf("recovered job = %+v", recoveredJob)
	}
	detail, _ := reopened.Conversation(ctx, conversation.ID)
	assistant := detail.Messages[len(detail.Messages)-1]
	if assistant.Content != "work so far" || assistant.Status != protocol.MessageInterrupted {
		t.Fatalf("partial output was not preserved: %+v", assistant)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "localzie.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
