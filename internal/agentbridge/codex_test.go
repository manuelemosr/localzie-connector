// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package agentbridge

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("LOCALZIE_FAKE_CODEX") == "1" {
		fakeCodex()
		return
	}
	os.Exit(m.Run())
}
func fakeCodex() {
	var writeMu sync.Mutex
	write := func(v any) { writeMu.Lock(); defer writeMu.Unlock(); _ = json.NewEncoder(os.Stdout).Encode(v) }
	complete := func() {
		write(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": "thread-1", "item": map[string]any{"id": "assistant-1", "type": "agentMessage", "text": "connector test OK"}}})
		write(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1", "status": "completed", "items": []any{}}}})
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params map[string]any  `json:"params"`
		}
		_ = json.Unmarshal(scanner.Bytes(), &msg)
		if len(msg.ID) == 0 {
			continue
		}
		if msg.Method == "" {
			write(map[string]any{"method": "serverRequest/resolved", "params": map[string]any{"threadId": "thread-1", "requestId": msg.ID}})
			complete()
			continue
		}
		thread := map[string]any{"id": "thread-1", "name": "Fixture", "model": "fixture-model", "cwd": "/tmp", "createdAt": 1, "updatedAt": 2, "turns": []any{}}
		if id, ok := msg.Params["threadId"].(string); ok {
			thread["id"] = id
		}
		var result any = map[string]any{}
		switch msg.Method {
		case "model/list":
			result = map[string]any{"data": []any{map[string]any{"id": "fixture-model", "model": "fixture-model", "displayName": "Fixture", "supportedReasoningEfforts": []any{map[string]any{"reasoningEffort": "none"}, map[string]any{"reasoningEffort": "medium"}}}}}
		case "thread/list":
			result = map[string]any{"data": []any{thread}}
		case "thread/turns/list":
			result = map[string]any{"data": []any{}}
			if file := os.Getenv("LOCALZIE_FAKE_HISTORY"); file != "" && msg.Params["threadId"] == "thread-1" {
				phase, _ := os.ReadFile(file)
				status := "inProgress"
				if string(phase) == "done" {
					status = "completed"
				}
				result = map[string]any{"data": []any{map[string]any{"id": "external-turn", "status": status, "items": []any{map[string]any{"id": "external-message", "type": "agentMessage", "text": string(phase)}}}}}
			}
		case "thread/read", "thread/start", "thread/resume":
			result = map[string]any{"thread": thread}
		case "turn/start":
			result = map[string]any{"turn": map[string]any{"id": "turn-1", "status": "inProgress"}}
		}
		write(map[string]any{"id": msg.ID, "result": result})
		if msg.Method == "turn/start" {
			write(map[string]any{"method": "turn/started", "params": map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1", "status": "inProgress"}}})
			mode := os.Getenv("LOCALZIE_FAKE_MODE")
			if mode == "question" {
				write(map[string]any{"id": 41, "method": "item/tool/requestUserInput", "params": map[string]any{"threadId": "thread-1", "questions": []any{map[string]any{"id": "q1", "header": "Choice", "question": "Which one?", "options": []any{map[string]any{"label": "A"}}}}}})
			} else if mode == "permission" {
				write(map[string]any{"id": 42, "method": "item/commandExecution/requestApproval", "params": map[string]any{"threadId": "thread-1"}})
			} else {
				write(map[string]any{"method": "item/agentMessage/delta", "params": map[string]any{"threadId": "thread-1", "itemId": "assistant-1", "delta": "connector "}})
				go func() { time.Sleep(150 * time.Millisecond); complete() }()
			}
		}
	}
}
func fixture(t *testing.T, mode string) *Codex {
	t.Helper()
	directory := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(binary, filepath.Join(directory, "codex")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("LOCALZIE_FAKE_CODEX", "1")
	t.Setenv("LOCALZIE_FAKE_MODE", mode)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c, err := StartCodex(ctx, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}
func awaitSession(t *testing.T, c *Codex, predicate func(Session) bool) Session {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s, err := c.Load(context.Background(), "thread-1")
		if err != nil {
			t.Fatal(err)
		}
		if predicate(s) {
			return s
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Codex state did not reach expected condition")
	return Session{}
}
func TestStreamingContinuesAfterPhoneDetachAndSubmissionIsIdempotent(t *testing.T) {
	c := fixture(t, "")
	ctx := context.Background()
	if _, err := c.Models(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Create(ctx, "fixture-model"); err != nil {
		t.Fatal(err)
	}
	_, detach := c.Subscribe("phone-1")
	input := Submission{Text: "Hello", Model: "fixture-model", ID: "submission-1"}
	if err := c.Send(ctx, "thread-1", "phone-1", input); err != nil {
		t.Fatal(err)
	}
	live := awaitSession(t, c, func(s Session) bool { return len(s.Messages) > 0 })
	if live.Messages[0].Text != "connector " {
		t.Fatalf("not a live delta: %q", live.Messages[0].Text)
	}
	detach()
	done := awaitSession(t, c, func(s Session) bool { return s.Activity == "completed" })
	if done.Messages[0].Text != "connector test OK" {
		t.Fatal("lost authoritative completed item")
	}
	if err := c.Send(ctx, "thread-1", "phone-1", input); err != nil {
		t.Fatal("idempotent retry failed", err)
	}
}
func TestQuestionIsNeverAutoAnswered(t *testing.T) {
	c := fixture(t, "question")
	ctx := context.Background()
	_, _ = c.Create(ctx, "fixture-model")
	_, detach := c.Subscribe("phone-1")
	defer detach()
	if err := c.Send(ctx, "thread-1", "phone-1", Submission{Text: "Ask", ID: "q"}); err != nil {
		t.Fatal(err)
	}
	pending := awaitSession(t, c, func(s Session) bool { return len(s.Pending) == 1 })
	time.Sleep(100 * time.Millisecond)
	still, _ := c.Load(ctx, "thread-1")
	if still.Activity != "awaitingInput" {
		t.Fatal("question was automatically resolved")
	}
	if err := c.Reply(ctx, "thread-1", "phone-1", Answer{ID: pending.Pending[0].ID, Answers: [][]string{{"A"}}}); err != nil {
		t.Fatal(err)
	}
	awaitSession(t, c, func(s Session) bool { return s.Activity == "completed" })
}
func TestPermissionWaitsWithoutControllerAndResumesOnAttach(t *testing.T) {
	c := fixture(t, "permission")
	ctx := context.Background()
	_, _ = c.Create(ctx, "fixture-model")
	if err := c.Send(ctx, "thread-1", "phone-1", Submission{Text: "Tool", ID: "p"}); err != nil {
		t.Fatal(err)
	}
	awaitSession(t, c, func(s Session) bool { return len(s.Pending) == 1 })
	time.Sleep(100 * time.Millisecond)
	s, _ := c.Load(ctx, "thread-1")
	if s.Activity != "awaitingInput" {
		t.Fatal("permission approved without controller")
	}
	_, detach := c.Subscribe("phone-1")
	defer detach()
	awaitSession(t, c, func(s Session) bool { return s.Activity == "completed" })
}
func TestObservedExternalTurnRefreshesWithoutTakingOwnership(t *testing.T) {
	file := filepath.Join(t.TempDir(), "external-state")
	_ = os.WriteFile(file, []byte("running"), 0600)
	t.Setenv("LOCALZIE_FAKE_HISTORY", file)
	c := fixture(t, "")
	ctx := context.Background()
	first, err := c.Load(ctx, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	if first.Activity != "waiting" {
		t.Fatal("external active turn was not observed")
	}
	if err = c.Send(ctx, "thread-1", "phone", Submission{ID: "must-wait", Text: "Hello"}); err == nil {
		t.Fatal("external active turn was taken over")
	}
	_ = os.WriteFile(file, []byte("done"), 0600)
	finished, err := c.Load(ctx, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	if finished.Activity != "idle" || finished.Messages[0].Text != "done" {
		t.Fatal("external completion was stuck in the cache")
	}
}

func TestLiveCodexReadOnlyDiscovery(t *testing.T) {
	if os.Getenv("LOCALZIE_CODEX_LIVE") != "1" {
		t.Skip("native Codex smoke is opt-in")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	c, err := StartCodex(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	models, err := c.Models(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(models) == 0 {
		t.Fatal("no Codex models")
	}
	sessions, err := c.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Native Codex models: %d; saved sessions: %d\n", len(models), len(sessions))
	if len(sessions) > 0 {
		if _, err := c.Load(ctx, sessions[0].ID); err != nil {
			t.Fatal("saved Codex history could not be loaded through its API", err)
		}
	}
	if os.Getenv("LOCALZIE_CODEX_INFERENCE") != "1" {
		return
	}
	session, err := c.Create(ctx, models[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	_, detach := c.Subscribe("smoke-test")
	defer detach()
	if err = c.Send(ctx, session.ID, "smoke-test", Submission{ID: "native-smoke", Model: models[0].ID, Text: "Reply exactly: Localzie Codex connector OK. Do not use tools, read files, change files or spawn agents."}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		value, e := c.Load(ctx, session.ID)
		if e != nil {
			t.Fatal(e)
		}
		if value.Activity == "failed" {
			t.Fatal(value.Error)
		}
		if value.Activity == "completed" {
			found := false
			for _, m := range value.Messages {
				found = found || strings.Contains(m.Text, "Localzie Codex connector OK")
			}
			if !found {
				t.Fatal("native reply did not match smoke prompt")
			}
			fmt.Println("Native Codex streaming turn completed successfully")
			c.Close()
			reopened, e := StartCodex(ctx, t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			defer reopened.Close()
			loaded, e := reopened.Load(ctx, session.ID)
			if e != nil {
				t.Fatal(e)
			}
			found = false
			for _, item := range loaded.Messages {
				found = found || strings.Contains(item.Text, "Localzie Codex connector OK")
			}
			if !found {
				t.Fatal("native history did not survive app-server restart")
			}
			fmt.Println("Native Codex transcript recovered after connector restart")
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("native Codex inference timeout")
}

func TestSendRefreshesOtherExternallyObservedTurnBeforeGlobalBusyGate(t *testing.T) {
	file := filepath.Join(t.TempDir(), "external-state")
	if err := os.WriteFile(file, []byte("running"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LOCALZIE_FAKE_HISTORY", file)
	c := fixture(t, "")
	ctx := context.Background()
	if _, err := c.Load(ctx, "thread-1"); err != nil {
		t.Fatal(err)
	}
	input := Submission{ID: "new-chat-send", Text: "Continue in another chat", Model: "fixture-model"}
	if err := c.Send(ctx, "thread-2", "phone", input); err == nil {
		t.Fatal("active external turn was ignored")
	}
	if err := os.WriteFile(file, []byte("done"), 0600); err != nil {
		t.Fatal(err)
	}
	// Do not reopen thread-1: Send itself must retire its stale busy state.
	if err := c.Send(ctx, "thread-2", "phone", input); err != nil {
		t.Fatalf("finished external turn blocked the new chat: %v", err)
	}
}
