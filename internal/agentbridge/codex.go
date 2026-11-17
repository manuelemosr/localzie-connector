// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package agentbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type nativeTurn struct {
	ID     string           `json:"id"`
	Status string           `json:"status"`
	Items  []map[string]any `json:"items"`
}
type nativeThread struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	Preview     string       `json:"preview"`
	Model       string       `json:"model"`
	Cwd         string       `json:"cwd"`
	Created     int64        `json:"createdAt"`
	Updated     int64        `json:"updatedAt"`
	Turns       []nativeTurn `json:"turns"`
	HistoryMode string       `json:"historyMode"`
	Status      struct {
		Type string `json:"type"`
	} `json:"status"`
}
type pendingRequest struct {
	id        json.RawMessage
	method    string
	questions []Question
	resolved  chan struct{}
	sent      bool
}
type codexSession struct {
	session   Session
	turn      string
	loaded    bool
	pending   map[string]pendingRequest
	sent      map[string]bool
	lastEvent time.Time
	ownsTurn  bool
}
type Codex struct {
	process     *rpc
	root        context.Context
	directory   string
	operations  sync.Mutex
	mu          sync.Mutex
	sessions    map[string]*codexSession
	subscribers map[chan Session]string
	models      []Model
	efforts     map[string][]string
}

func StartCodex(ctx context.Context, directory string) (*Codex, error) {
	if directory == "" {
		directory, _ = os.Getwd()
	}
	directory, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(directory)
	if err != nil || !info.IsDir() {
		return nil, errors.New("Codex needs an existing project directory")
	}
	c := &Codex{root: ctx, directory: directory, sessions: map[string]*codexSession{}, subscribers: map[chan Session]string{}, efforts: map[string][]string{}}
	c.process, err = startRPC(ctx, directory, c.event)
	if err != nil {
		return nil, err
	}
	if err = c.process.call(ctx, "initialize", map[string]any{"clientInfo": map[string]string{"name": "localzie", "title": "Localzie", "version": "0.2.0"}, "capabilities": map[string]bool{"experimentalApi": true}}, nil); err != nil {
		c.Close()
		return nil, err
	}
	if err = c.process.write(map[string]any{"method": "initialized"}); err != nil {
		c.Close()
		return nil, err
	}
	go func() {
		<-c.process.done
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, s := range c.sessions {
			if s.turn != "" {
				s.turn = ""
				s.session.Activity = "failed"
				s.session.Error = "Codex app-server stopped; restart the connector"
				c.publishLocked(s)
			}
		}
	}()
	return c, nil
}
func (c *Codex) Close() {
	if c.process != nil {
		c.process.close()
	}
}
func (c *Codex) Models(ctx context.Context) ([]Model, error) {
	var result struct {
		Data []struct {
			ID      string `json:"id"`
			Model   string `json:"model"`
			Name    string `json:"displayName"`
			Hidden  bool   `json:"hidden"`
			Efforts []struct {
				Effort string `json:"reasoningEffort"`
			} `json:"supportedReasoningEfforts"`
		} `json:"data"`
		Next *string `json:"nextCursor"`
	}
	models := []Model{}
	efforts := map[string][]string{}
	var cursor *string
	for page := 0; page < 20; page++ {
		if err := c.process.call(ctx, "model/list", map[string]any{"limit": 100, "cursor": cursor}, &result); err != nil {
			return nil, err
		}
		for _, m := range result.Data {
			if m.Hidden {
				continue
			}
			id := m.Model
			if id == "" {
				id = m.ID
			}
			options := []string{}
			off := false
			for _, e := range m.Efforts {
				options = append(options, e.Effort)
				off = off || e.Effort == "none"
			}
			models = append(models, Model{id, m.Name, len(options) > 0, off})
			efforts[id] = options
		}
		if result.Next == nil {
			break
		}
		cursor = result.Next
	}
	c.mu.Lock()
	c.models = models
	c.efforts = efforts
	c.mu.Unlock()
	return models, nil
}
func (c *Codex) List(ctx context.Context) ([]Session, error) {
	result := []Session{}
	var cursor *string
	for page := 0; page < 20; page++ {
		var response struct {
			Data []nativeThread `json:"data"`
			Next *string        `json:"nextCursor"`
		}
		if err := c.process.call(ctx, "thread/list", map[string]any{"limit": 100, "cursor": cursor, "modelProviders": []string{}, "sourceKinds": []string{}}, &response); err != nil {
			return nil, err
		}
		c.mu.Lock()
		for _, thread := range response.Data {
			session := threadSummary(thread)
			if cached := c.sessions[thread.ID]; cached != nil {
				session.Activity = cached.session.Activity
				session.Controller = cached.session.Controller
			}
			result = append(result, session)
		}
		c.mu.Unlock()
		if response.Next == nil {
			break
		}
		cursor = response.Next
	}
	return result, nil
}
func threadSummary(t nativeThread) Session {
	title := t.Name
	if title == "" {
		title = t.Preview
	}
	if title == "" {
		title = "Codex chat"
	}
	if len([]rune(title)) > 120 {
		title = string([]rune(title)[:120])
	}
	activity := "idle"
	if t.Status.Type == "active" {
		activity = "waiting"
	}
	return Session{ID: t.ID, Title: title, Model: t.Model, Directory: t.Cwd, Created: t.Created, Updated: t.Updated, Activity: activity, Messages: []Message{}, Pending: []Pending{}}
}
func (c *Codex) hydrate(t nativeThread, loaded, refreshObserved bool) Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	old := c.sessions[t.ID]
	if old != nil && (old.ownsTurn || !refreshObserved && time.Since(old.lastEvent) < time.Second) {
		return cloneSession(old.session)
	} // Never overwrite live deltas with an older read.
	state := &codexSession{session: threadSummary(t), loaded: loaded, pending: map[string]pendingRequest{}, sent: map[string]bool{}}
	if old != nil {
		state.loaded = state.loaded || old.loaded
		state.pending = old.pending
		state.sent = old.sent
		state.session.Controller = old.session.Controller
	}
	for _, turn := range t.Turns {
		for _, item := range turn.Items {
			upsert(&state.session, item, turn.Status != "inProgress")
		}
		if turn.Status == "inProgress" {
			state.turn = turn.ID
			state.session.Activity = "waiting"
		}
	}
	if len(c.sessions) >= 16 {
		for id, entry := range c.sessions {
			if entry.turn == "" && len(entry.pending) == 0 {
				delete(c.sessions, id)
				break
			}
		}
	}
	c.sessions[t.ID] = state
	c.refreshPendingLocked(state)
	return cloneSession(state.session)
}
func (c *Codex) Load(ctx context.Context, id string) (Session, error) {
	return c.load(ctx, id, false)
}
func (c *Codex) load(ctx context.Context, id string, refreshObserved bool) (Session, error) {
	c.mu.Lock()
	cached := c.sessions[id]
	if cached != nil && cached.loaded && (cached.ownsTurn || !refreshObserved && time.Since(cached.lastEvent) < time.Second) {
		value := cloneSession(cached.session)
		c.mu.Unlock()
		return value, nil
	}
	c.mu.Unlock()
	var result struct {
		Thread nativeThread `json:"thread"`
	}
	if err := c.process.call(ctx, "thread/read", map[string]any{"threadId": id, "includeTurns": false}, &result); err != nil {
		return Session{}, err
	}
	var cursor *string
	for page := 0; page < 100; page++ {
		var history struct {
			Data []nativeTurn `json:"data"`
			Next *string      `json:"nextCursor"`
		}
		if err := c.process.call(ctx, "thread/turns/list", map[string]any{"threadId": id, "limit": 50, "itemsView": "full", "sortDirection": "asc", "cursor": cursor}, &history); err != nil {
			return Session{}, err
		}
		result.Thread.Turns = append(result.Thread.Turns, history.Data...)
		if history.Next == nil {
			return c.hydrate(result.Thread, false, refreshObserved), nil
		}
		cursor = history.Next
	}
	return Session{}, errors.New("This Codex chat exceeds the connector's history limit")
}
func (c *Codex) Create(ctx context.Context, model string) (Session, error) {
	c.operations.Lock()
	defer c.operations.Unlock()
	var result struct {
		Thread nativeThread `json:"thread"`
	}
	if err := c.process.call(ctx, "thread/start", map[string]any{"cwd": c.directory, "model": model, "approvalPolicy": "untrusted"}, &result); err != nil {
		return Session{}, err
	}
	result.Thread.Model = model
	return c.hydrate(result.Thread, true, false), nil
}
func (c *Codex) Send(ctx context.Context, id, device string, input Submission) error {
	c.operations.Lock()
	defer c.operations.Unlock()
	c.mu.Lock()
	state := c.sessions[id]
	if state != nil && state.sent[input.ID] {
		c.mu.Unlock()
		return nil
	}
	// Externally owned turns do not necessarily emit completion notifications
	// to this app-server. Refresh them before using the global busy gate.
	observed := []string{}
	for id, s := range c.sessions {
		if s.turn != "" && !s.ownsTurn {
			observed = append(observed, id)
		}
	}
	c.mu.Unlock()
	for _, id := range observed {
		if _, err := c.load(ctx, id, true); err != nil {
			return fmt.Errorf("could not refresh the active Codex chat: %w", err)
		}
	}
	c.mu.Lock()
	state = c.sessions[id]
	for _, s := range c.sessions {
		if s.turn != "" {
			c.mu.Unlock()
			return errors.New("Wait for the current Codex turn, or press Stop")
		}
	}
	loaded := state != nil && state.loaded
	c.mu.Unlock()
	if !loaded {
		observed, err := c.Load(ctx, id)
		if err != nil {
			return err
		}
		if observed.Activity != "idle" && observed.Activity != "completed" && observed.Activity != "stopped" && observed.Activity != "failed" {
			return errors.New("This Codex chat is still active on your computer. Wait for it to finish before continuing here")
		}
		var resumed struct {
			Thread nativeThread `json:"thread"`
		}
		if err := c.process.call(ctx, "thread/resume", map[string]any{"threadId": id, "approvalPolicy": "untrusted", "excludeTurns": true}, &resumed); err != nil {
			return err
		}
		c.mu.Lock()
		if existing := c.sessions[id]; existing != nil {
			existing.loaded = true
			if resumed.Thread.Model != "" {
				existing.session.Model = resumed.Thread.Model
			}
		}
		c.mu.Unlock()
	}
	c.mu.Lock()
	state = c.sessions[id]
	if state == nil {
		c.mu.Unlock()
		return errors.New("Codex session is unavailable")
	}
	if len(state.pending) > 0 {
		c.mu.Unlock()
		return errors.New("Answer the pending Codex question first")
	}
	if state.turn != "" {
		c.mu.Unlock()
		return errors.New("This Codex chat is still running")
	}
	state.session.Controller = device
	state.session.Model = input.Model
	params := map[string]any{"threadId": id, "model": input.Model, "clientUserMessageId": input.ClientID, "input": []map[string]string{{"type": "text", "text": input.Text}}}
	if input.Reasoning != nil {
		options := c.efforts[input.Model]
		pick := ""
		if *input.Reasoning {
			for _, e := range options {
				if e == "medium" || e == "high" {
					pick = e
					break
				}
			}
		} else {
			for _, e := range options {
				if e == "none" {
					pick = e
					break
				}
			}
		}
		if pick != "" {
			params["effort"] = pick
		}
	}
	state.session.Activity = "waiting"
	state.session.Error = ""
	state.turn = "accepting"
	state.ownsTurn = true
	c.mu.Unlock()
	var response struct {
		Turn nativeTurn `json:"turn"`
	}
	if err := c.process.call(ctx, "turn/start", params, &response); err != nil {
		c.mu.Lock()
		state.turn = ""
		state.ownsTurn = false
		state.session.Activity = "idle"
		c.mu.Unlock()
		return err
	}
	c.mu.Lock()
	state.sent[input.ID] = true
	if state.turn == "accepting" {
		state.turn = response.Turn.ID
	}
	c.publishLocked(state)
	c.mu.Unlock()
	return nil
}
func (c *Codex) Stop(ctx context.Context, id, device string) error {
	c.mu.Lock()
	s := c.sessions[id]
	turn := ""
	if s != nil {
		turn = s.turn
		s.session.Controller = device
	}
	c.mu.Unlock()
	if turn == "" {
		return nil
	}
	return c.process.call(ctx, "turn/interrupt", map[string]string{"threadId": id, "turnId": turn}, nil)
}
func (c *Codex) Delete(ctx context.Context, id string) error {
	c.operations.Lock()
	defer c.operations.Unlock()
	c.mu.Lock()
	s := c.sessions[id]
	busy := s != nil && s.turn != ""
	c.mu.Unlock()
	if busy {
		return errors.New("Stop the Codex turn before deleting this chat")
	}
	if err := c.process.call(ctx, "thread/delete", map[string]string{"threadId": id}, nil); err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.sessions, id)
	c.mu.Unlock()
	return nil
}
func (c *Codex) Reply(ctx context.Context, id, device string, answer Answer) error {
	c.mu.Lock()
	s := c.sessions[id]
	if s == nil {
		c.mu.Unlock()
		return errors.New("Codex session is unavailable")
	}
	s.lastEvent = time.Now()
	request, ok := s.pending[answer.ID]
	if !ok {
		c.mu.Unlock()
		return errors.New("This Codex question is no longer pending")
	}
	s.session.Controller = device
	c.mu.Unlock()
	var result any
	if request.method == "item/tool/requestUserInput" {
		if len(answer.Answers) != len(request.questions) {
			return errors.New("Answer every Codex question")
		}
		answers := map[string]any{}
		for i, q := range request.questions {
			if len(answer.Answers[i]) == 0 {
				return errors.New("Answer every Codex question")
			}
			answers[q.ID] = map[string]any{"answers": answer.Answers[i]}
		}
		result = map[string]any{"answers": answers}
	} else {
		decision := "decline"
		if answer.Decision == "once" {
			decision = "accept"
		}
		result = map[string]string{"decision": decision}
	}
	if err := c.process.respond(request.id, result); err != nil {
		return err
	}
	select {
	case <-request.resolved:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(15 * time.Second):
		return errors.New("Codex has not acknowledged this answer yet; reconnect to check its state")
	}
}
func (c *Codex) Subscribe(device string) (<-chan Session, func()) {
	ch := make(chan Session, 1)
	c.mu.Lock()
	c.subscribers[ch] = device
	for _, s := range c.sessions {
		if s.turn != "" || len(s.pending) > 0 {
			select {
			case ch <- cloneSession(s.session):
			default:
			}
		}
	}
	c.mu.Unlock()
	go c.approveControlled(device)
	return ch, func() { c.mu.Lock(); delete(c.subscribers, ch); c.mu.Unlock() }
}
func (c *Codex) publishLocked(s *codexSession) {
	s.lastEvent = time.Now()
	s.session.Updated = time.Now().Unix()
	for ch := range c.subscribers {
		select {
		case ch <- cloneSession(s.session):
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- cloneSession(s.session):
			default:
			}
		}
	}
}
func cloneSession(s Session) Session {
	s.Messages = append([]Message{}, s.Messages...)
	s.Pending = append([]Pending{}, s.Pending...)
	return s
}
func (c *Codex) refreshPendingLocked(s *codexSession) {
	s.session.Pending = []Pending{}
	for id, p := range s.pending {
		kind := "permission"
		title := "Codex tool permission"
		if p.method == "item/tool/requestUserInput" {
			kind = "question"
			title = "Codex has a question"
		}
		s.session.Pending = append(s.session.Pending, Pending{id, kind, title, p.questions})
	}
	if len(s.pending) > 0 {
		s.session.Activity = "awaitingInput"
	} else if s.turn != "" {
		s.session.Activity = "waiting"
	}
}
func (c *Codex) approveControlled(device string) {
	if device == "" {
		return
	}
	type approval struct {
		id      string
		request pendingRequest
		session *codexSession
	}
	approvals := []approval{}
	c.mu.Lock()
	live := false
	for _, d := range c.subscribers {
		live = live || d == device
	}
	if live {
		for _, s := range c.sessions {
			if s.session.Controller != device {
				continue
			}
			for id, p := range s.pending {
				if !p.sent && (p.method == "item/commandExecution/requestApproval" || p.method == "item/fileChange/requestApproval") {
					approvals = append(approvals, approval{id, p, s})
					p.sent = true
					s.pending[id] = p
				}
			}
		}
	}
	c.mu.Unlock()
	for _, a := range approvals {
		err := c.process.respond(a.request.id, map[string]string{"decision": "accept"})
		c.mu.Lock()
		if err != nil {
			a.request.sent = false
			a.session.pending[a.id] = a.request
		}
		c.refreshPendingLocked(a.session)
		c.publishLocked(a.session)
		c.mu.Unlock()
	}
}
func (c *Codex) event(m rpcMessage) {
	var p struct {
		ThreadID  string          `json:"threadId"`
		RequestID json.RawMessage `json:"requestId"`
		ItemID    string          `json:"itemId"`
		Delta     string          `json:"delta"`
		Item      map[string]any  `json:"item"`
		Turn      nativeTurn      `json:"turn"`
		Thread    nativeThread    `json:"thread"`
		Questions []struct {
			ID       string `json:"id"`
			Header   string `json:"header"`
			Question string `json:"question"`
			Options  []struct {
				Label string `json:"label"`
			} `json:"options"`
		} `json:"questions"`
	}
	if json.Unmarshal(m.Params, &p) != nil {
		return
	}
	if m.Method == "thread/started" {
		return
	}
	if p.ThreadID == "" {
		return
	}
	c.mu.Lock()
	s := c.sessions[p.ThreadID]
	if s == nil {
		c.mu.Unlock()
		return
	}
	if len(m.ID) > 0 {
		id := string(m.ID)
		questions := []Question{}
		for _, q := range p.Questions {
			options := []string{}
			for _, o := range q.Options {
				options = append(options, o.Label)
			}
			questions = append(questions, Question{q.ID, q.Header, q.Question, options})
		}
		if m.Method != "item/tool/requestUserInput" && m.Method != "item/commandExecution/requestApproval" && m.Method != "item/fileChange/requestApproval" {
			c.mu.Unlock()
			_ = c.process.write(map[string]any{"id": m.ID, "error": map[string]any{"code": -32601, "message": "This interaction is not supported by Localzie yet"}})
			return
		}
		s.pending[id] = pendingRequest{id: append(json.RawMessage{}, m.ID...), method: m.Method, questions: questions, resolved: make(chan struct{})}
		c.refreshPendingLocked(s)
		c.publishLocked(s)
		device := s.session.Controller
		c.mu.Unlock()
		go c.approveControlled(device)
		return
	}
	switch m.Method {
	case "serverRequest/resolved":
		if request, ok := s.pending[string(p.RequestID)]; ok {
			delete(s.pending, string(p.RequestID))
			close(request.resolved)
			c.refreshPendingLocked(s)
		}
	case "turn/started":
		s.turn = p.Turn.ID
		s.ownsTurn = true
		s.session.Activity = "waiting"
	case "turn/completed":
		s.turn = ""
		s.ownsTurn = false
		s.session.Activity = "completed"
		if p.Turn.Status == "interrupted" {
			s.session.Activity = "stopped"
		}
		if p.Turn.Status == "failed" {
			s.session.Activity = "failed"
			s.session.Error = "Codex could not finish this turn"
		}
		for _, item := range p.Turn.Items {
			upsert(&s.session, item, true)
		}
		for i := range s.session.Messages {
			s.session.Messages[i].Status = "completed"
		}
	case "item/started", "item/completed":
		upsert(&s.session, p.Item, m.Method == "item/completed")
		kind := text(p.Item["type"])
		if kind == "reasoning" {
			s.session.Activity = "reasoning"
		} else if kind == "agentMessage" {
			s.session.Activity = "writing"
		} else if kind != "userMessage" {
			s.session.Activity = "usingTools"
		}
	default:
		kind := ""
		activity := ""
		if m.Method == "item/agentMessage/delta" {
			kind = "text"
			activity = "writing"
		} else if strings.HasPrefix(m.Method, "item/reasoning/") && strings.HasSuffix(m.Method, "Delta") {
			kind = "reasoning"
			activity = "reasoning"
		} else if m.Method == "item/commandExecution/outputDelta" {
			kind = "tool"
			activity = "usingTools"
		}
		if kind != "" {
			found := false
			for i := range s.session.Messages {
				if s.session.Messages[i].ID == p.ItemID {
					s.session.Messages[i].Text += p.Delta
					found = true
					break
				}
			}
			if !found {
				s.session.Messages = append(s.session.Messages, Message{ID: p.ItemID, Role: "assistant", Kind: kind, Text: p.Delta, Status: "streaming"})
			}
			s.session.Activity = activity
		} else {
			c.mu.Unlock()
			return
		}
	}
	if len(s.pending) > 0 {
		s.session.Activity = "awaitingInput"
	}
	c.publishLocked(s)
	c.mu.Unlock()
}
func text(v any) string { s, _ := v.(string); return s }
func upsert(s *Session, item map[string]any, completed bool) {
	id := text(item["id"])
	if id == "" {
		return
	}
	kind := text(item["type"])
	m := Message{ID: id, Role: "assistant", Kind: "text", Status: "streaming"}
	if completed {
		m.Status = "completed"
	}
	switch kind {
	case "userMessage":
		if clientID := text(item["clientId"]); clientID != "" {
			m.ID = clientID
			id = clientID
		}
		m.Role = "user"
		m.Status = "completed"
		if content, ok := item["content"].([]any); ok {
			for _, raw := range content {
				if v, ok := raw.(map[string]any); ok {
					m.Text += text(v["text"])
				}
			}
		}
	case "agentMessage", "plan":
		m.Text = text(item["text"])
	case "reasoning":
		m.Kind = "reasoning"
		for _, key := range []string{"summary", "content"} {
			if parts, ok := item[key].([]any); ok {
				for _, part := range parts {
					m.Text += text(part) + "\n"
				}
				if m.Text != "" {
					break
				}
			}
		}
	case "commandExecution":
		m.Kind = "tool"
		m.Name = "Command"
		m.Text = text(item["command"]) + "\n" + text(item["aggregatedOutput"])
		if status := text(item["status"]); status != "inProgress" && status != "" {
			m.Status = "completed"
		}
	default:
		m.Kind = "tool"
		m.Name = kind
		body, _ := json.Marshal(item)
		m.Text = string(body)
	}
	for i := range s.Messages {
		if s.Messages[i].ID == id {
			s.Messages[i] = m
			return
		}
	}
	s.Messages = append(s.Messages, m)
}
