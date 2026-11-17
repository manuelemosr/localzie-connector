// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

// Package agentbridge translates native harness APIs into one phone contract.
// It never listens on TCP. ziticonn supplies its pinned TLS and authorization.
package agentbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type Model struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Reasoning bool   `json:"reasoning"`
	Off       bool   `json:"off"`
}
type Message struct {
	ID     string `json:"id"`
	Role   string `json:"role"`
	Kind   string `json:"kind"`
	Text   string `json:"text"`
	Name   string `json:"name,omitempty"`
	Status string `json:"status"`
}
type Question struct {
	ID      string   `json:"id"`
	Title   string   `json:"title"`
	Text    string   `json:"text"`
	Options []string `json:"options"`
}
type Pending struct {
	ID        string     `json:"id"`
	Kind      string     `json:"kind"`
	Title     string     `json:"title"`
	Questions []Question `json:"questions"`
}
type Session struct {
	ID         string    `json:"id"`
	Title      string    `json:"title"`
	Model      string    `json:"model"`
	Directory  string    `json:"directory"`
	Created    int64     `json:"created"`
	Updated    int64     `json:"updated"`
	Activity   string    `json:"activity"`
	Messages   []Message `json:"messages"`
	Pending    []Pending `json:"pending"`
	Error      string    `json:"error,omitempty"`
	Controller string    `json:"controller,omitempty"`
}
type Submission struct {
	Text      string `json:"text"`
	Model     string `json:"model"`
	ID        string `json:"id"`
	ClientID  string `json:"clientID"`
	Reasoning *bool  `json:"reasoning"`
}
type Answer struct {
	ID       string     `json:"id"`
	Answers  [][]string `json:"answers"`
	Decision string     `json:"decision"`
}

// Adapter is the harness boundary. Future adapters do not change the phone wire.
type Adapter interface {
	Models(context.Context) ([]Model, error)
	List(context.Context) ([]Session, error)
	Load(context.Context, string) (Session, error)
	Create(context.Context, string) (Session, error)
	Send(context.Context, string, string, Submission) error
	Stop(context.Context, string, string) error
	Delete(context.Context, string) error
	Reply(context.Context, string, string, Answer) error
	Subscribe(string) (<-chan Session, func())
	Close()
}
type deviceKey struct{}

func WithDevice(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, deviceKey{}, id)
}
func Handler(root context.Context, adapter Adapter) http.Handler {
	mux := http.NewServeMux()
	write := func(w http.ResponseWriter, v any, err error) {
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	decode := func(w http.ResponseWriter, r *http.Request, v any) error {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		return json.NewDecoder(r.Body).Decode(v)
	}
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		write(w, map[string]any{"name": "Codex", "version": 1, "capabilities": []string{"sessions", "streaming", "tools", "questions", "stop", "delete", "reasoning"}}, nil)
	})
	mux.HandleFunc("GET /models", func(w http.ResponseWriter, r *http.Request) { v, e := adapter.Models(r.Context()); write(w, v, e) })
	mux.HandleFunc("GET /sessions", func(w http.ResponseWriter, r *http.Request) {
		v, e := adapter.List(r.Context())
		device, _ := r.Context().Value(deviceKey{}).(string)
		for i := range v {
			v[i] = sessionForDevice(v[i], device)
		}
		write(w, v, e)
	})
	mux.HandleFunc("POST /sessions", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Model string `json:"model"`
		}
		if e := decode(w, r, &input); e != nil {
			write(w, nil, e)
			return
		}
		v, e := adapter.Create(root, input.Model)
		write(w, v, e)
	})
	mux.HandleFunc("GET /sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		v, e := adapter.Load(r.Context(), r.PathValue("id"))
		device, _ := r.Context().Value(deviceKey{}).(string)
		write(w, sessionForDevice(v, device), e)
	})
	mux.HandleFunc("DELETE /sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		write(w, struct{}{}, adapter.Delete(root, r.PathValue("id")))
	})
	for _, action := range []string{"send", "stop", "reply"} {
		mux.HandleFunc("POST /sessions/{id}/"+action, func(w http.ResponseWriter, r *http.Request) {
			device, _ := r.Context().Value(deviceKey{}).(string)
			if device == "" {
				write(w, nil, errors.New("Pair this phone before controlling Codex"))
				return
			}
			var err error
			switch action {
			case "send":
				var input Submission
				err = decode(w, r, &input)
				if err == nil {
					if strings.TrimSpace(input.Text) == "" || len(input.ID) > 128 || input.ID == "" {
						err = errors.New("A message and submission ID are required")
					} else {
						err = adapter.Send(root, r.PathValue("id"), device, input)
					}
				}
			case "stop":
				err = adapter.Stop(root, r.PathValue("id"), device)
			case "reply":
				var input Answer
				err = decode(w, r, &input)
				if err == nil {
					err = adapter.Reply(root, r.PathValue("id"), device, input)
				}
			}
			write(w, struct{}{}, err)
		})
	}
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		device, _ := r.Context().Value(deviceKey{}).(string)
		updates, close := adapter.Subscribe(device)
		defer close()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-store")
		controller := http.NewResponseController(w)
		if _, err := fmt.Fprint(w, ": connected\n\n"); err != nil {
			return
		}
		if controller.Flush() != nil {
			return
		}
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			select {
			case snapshot := <-updates:
				snapshot = sessionForDevice(snapshot, device)
				data, err := json.Marshal(snapshot)
				if err != nil {
					return
				}
				_ = controller.SetWriteDeadline(time.Now().Add(15 * time.Second))
				if _, err = fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
					return
				}
				if controller.Flush() != nil {
					return
				}
			case <-tick.C:
				_ = controller.SetWriteDeadline(time.Now().Add(15 * time.Second))
				if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
					return
				}
				if controller.Flush() != nil {
					return
				}
			case <-r.Context().Done():
				return
			case <-root.Done():
				return
			}
		}
	})
	return mux
}

func sessionForDevice(s Session, device string) Session {
	if device != "" && s.Controller == device {
		s.Controller = "phone"
	} else {
		s.Controller = "harness"
	}
	return s
}
