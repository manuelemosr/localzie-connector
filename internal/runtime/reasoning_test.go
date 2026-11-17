// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package runtime

import "testing"

func TestThinkContentFilterSuppressesFragmentedThinkingWithoutDroppingAnswer(t *testing.T) {
	filter := ThinkContentFilter{}
	var got string
	for _, chunk := range []string{"Before <thi", "nk>private", " chain</think> after"} {
		got += filter.Filter(chunk)
	}
	got += filter.Flush()
	if want := "Before  after"; got != want {
		t.Fatalf("filtered output = %q, want %q", got, want)
	}
}

func TestThinkContentFilterSuppressesFragmentedOrphanClosingTag(t *testing.T) {
	filter := ThinkContentFilter{}
	var got string
	for _, chunk := range []string{"private chain</th", "ink>Visible", " answer"} {
		got += filter.Filter(chunk)
	}
	got += filter.Flush()
	if want := "Visible answer"; got != want {
		t.Fatalf("filtered output = %q, want %q", got, want)
	}
}

func TestThinkContentFilterRetainsIncompleteOrdinaryTagAtEnd(t *testing.T) {
	filter := ThinkContentFilter{}
	got := filter.Filter("2 < 3 <th") + filter.Flush()
	if want := "2 < 3 <th"; got != want {
		t.Fatalf("filtered output = %q, want %q", got, want)
	}
}
