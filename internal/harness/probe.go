// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package harness

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"time"
)

type Capabilities struct {
	CreateSession        bool `json:"create_session"`
	ResumeOwnedSession   bool `json:"resume_owned_session"`
	ListExistingSessions bool `json:"list_existing_sessions"`
	StreamOutput         bool `json:"stream_output"`
	Cancel               bool `json:"cancel"`
	Approvals            bool `json:"approvals"`
	Diff                 bool `json:"diff"`
	AttachActiveTerminal bool `json:"attach_active_terminal"`
}

type Installation struct {
	ID                string       `json:"id"`
	Name              string       `json:"name"`
	Installed         bool         `json:"installed"`
	Version           string       `json:"version,omitempty"`
	IntegrationStatus string       `json:"integration_status"`
	Capabilities      Capabilities `json:"capabilities"`
	Problem           string       `json:"problem,omitempty"`
	// Server describes a connector-attached harness server. It is present
	// only when the connector was started with an explicit attachment.
	Server *ServerStatus `json:"server,omitempty"`
}

// ServerStatus reports the connector's view of an attached harness server.
// It is distinct from IntegrationStatus: an attached, healthy server still
// reports detected_unverified until the pinned-version compatibility fixtures
// pass.
type ServerStatus struct {
	Attached   bool   `json:"attached"`
	URL        string `json:"url,omitempty"`
	Healthy    bool   `json:"healthy"`
	Version    string `json:"version,omitempty"`
	Problem    string `json:"problem,omitempty"`
	ModelCount int    `json:"model_count"`
}

type commandRunner interface {
	LookPath(string) (string, error)
	Output(context.Context, string, ...string) ([]byte, error)
}

type systemRunner struct{}

func (systemRunner) LookPath(name string) (string, error) { return exec.LookPath(name) }
func (systemRunner) Output(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

type Prober struct{ runner commandRunner }

func NewProber() Prober { return Prober{runner: systemRunner{}} }

func (p Prober) All(ctx context.Context) []Installation {
	definitions := []struct {
		id, name, binary string
		capabilities     Capabilities
	}{
		{
			id: "opencode", name: "OpenCode", binary: "opencode",
			capabilities: Capabilities{CreateSession: true, ResumeOwnedSession: true, ListExistingSessions: true, StreamOutput: true, Cancel: true, Approvals: true, Diff: true, AttachActiveTerminal: true},
		},
	}
	result := make([]Installation, 0, len(definitions))
	for _, definition := range definitions {
		installation := Installation{
			ID: definition.id, Name: definition.name, IntegrationStatus: "not_installed", Capabilities: definition.capabilities,
		}
		path, err := p.runner.LookPath(definition.binary)
		if err != nil {
			if !errors.Is(err, exec.ErrNotFound) {
				installation.Problem = err.Error()
			}
			result = append(result, installation)
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		output, err := p.runner.Output(probeCtx, path, "--version")
		cancel()
		installation.Installed = true
		installation.IntegrationStatus = "detected_unverified"
		installation.Version = firstLine(string(output))
		if err != nil {
			installation.Problem = strings.TrimSpace(string(output))
			if installation.Problem == "" {
				installation.Problem = err.Error()
			}
		}
		result = append(result, installation)
	}
	return result
}

func firstLine(value string) string {
	value = strings.TrimSpace(value)
	if index := strings.IndexByte(value, '\n'); index >= 0 {
		return strings.TrimSpace(value[:index])
	}
	return value
}
