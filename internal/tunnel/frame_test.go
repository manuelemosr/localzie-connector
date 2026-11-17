// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package tunnel

import "testing"

func TestFrameRoundTrip(t *testing.T) {
	raw, err := (Frame{Type: Data, StreamID: 1, Payload: []byte{1, 2}}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(raw)
	if err != nil || got.StreamID != 1 || string(got.Payload) != "\x01\x02" {
		t.Fatalf("%+v %v", got, err)
	}
}
func TestRejectMalformed(t *testing.T) {
	for _, b := range [][]byte{nil, {'L', 'Z', 1, 1, 0, 0, 0, 0}, {'L', 'Z', 2, 1, 0, 0, 0, 1}} {
		if _, err := Decode(b); err == nil {
			t.Fatalf("accepted %x", b)
		}
	}
}
