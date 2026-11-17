// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package harness

import (
	"context"
	"errors"
	"testing"
)

type fakeRunner struct{}

func (fakeRunner) LookPath(name string) (string, error) {
	if name == "opencode" {
		return "/test/opencode", nil
	}
	return "", errors.New("not found")
}
func (fakeRunner) Output(context.Context, string, ...string) ([]byte, error) {
	return []byte("opencode 1.2.3\nextra"), nil
}

type missingRunner struct{}

func (missingRunner) LookPath(string) (string, error) { return "", errors.New("not found") }
func (missingRunner) Output(context.Context, string, ...string) ([]byte, error) {
	return nil, errors.New("not found")
}

func TestProbeNeverClaimsDetectedVersionIsSupported(t *testing.T) {
	items := (Prober{runner: fakeRunner{}}).All(context.Background())
	if len(items) != 1 {
		t.Fatalf("items = %d", len(items))
	}
	if items[0].ID != "opencode" {
		t.Fatalf("harness = %q", items[0].ID)
	}
	if !items[0].Installed || items[0].Version != "opencode 1.2.3" || items[0].IntegrationStatus != "detected_unverified" {
		t.Fatalf("opencode probe = %+v", items[0])
	}
}

func TestProbeReportsMissingHarnessAsNotInstalled(t *testing.T) {
	items := (Prober{runner: missingRunner{}}).All(context.Background())
	if len(items) != 1 || items[0].ID != "opencode" || items[0].Installed || items[0].IntegrationStatus != "not_installed" {
		t.Fatalf("missing harness probe = %+v", items)
	}
}
