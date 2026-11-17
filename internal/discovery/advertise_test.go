// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"slices"
	"testing"
)

func TestTXTRecordsExposeOnlyConnectionMetadata(t *testing.T) {
	records := txtRecords("connector-id", "ABCDEF", []string{"https://192.168.1.10:9443", "https://[fd00::1]:9443"})
	for _, want := range []string{"v=1", "id=connector-id", "fp=abcdef", "url0=https://192.168.1.10:9443", "url1=https://[fd00::1]:9443"} {
		if !slices.Contains(records, want) {
			t.Fatalf("records %q do not contain %q", records, want)
		}
	}
}

func TestInstanceNameIsShortAndStable(t *testing.T) {
	if got := instanceName("12345678-abcd-ef00-1234-567890abcdef"); got != "Localzie-12345678" {
		t.Fatalf("instance name = %q", got)
	}
}
