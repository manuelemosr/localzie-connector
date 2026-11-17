// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

// Package tunnel defines the versioned opaque frames carried by relay WebSockets.
package tunnel

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	Version        = 1
	HeaderSize     = 8
	MaxMessageSize = 65552
	MaxPayloadSize = MaxMessageSize - HeaderSize
)

type Type byte

const (
	Open       Type = 0x01
	Opened     Type = 0x02
	Data       Type = 0x03
	CloseWrite Type = 0x04
	Close      Type = 0x05
	Reset      Type = 0x06
	Ping       Type = 0x07
	Pong       Type = 0x08
)

var (
	ErrMalformed = errors.New("malformed tunnel frame")
	ErrOversized = errors.New("oversized tunnel frame")
	ErrVersion   = errors.New("unsupported tunnel version")
)

type Frame struct {
	Type     Type
	StreamID uint32
	Payload  []byte
}

func (f Frame) Encode() ([]byte, error) {
	if f.StreamID == 0 || !validType(f.Type) {
		return nil, ErrMalformed
	}
	if len(f.Payload) > MaxPayloadSize {
		return nil, ErrOversized
	}
	if (f.Type == Open || f.Type == Opened || f.Type == CloseWrite || f.Type == Close) && len(f.Payload) != 0 {
		return nil, ErrMalformed
	}
	b := make([]byte, HeaderSize+len(f.Payload))
	b[0], b[1], b[2], b[3] = 'L', 'Z', Version, byte(f.Type)
	binary.BigEndian.PutUint32(b[4:], f.StreamID)
	copy(b[HeaderSize:], f.Payload)
	return b, nil
}
func Decode(b []byte) (Frame, error) {
	if len(b) < HeaderSize {
		return Frame{}, fmt.Errorf("%w: short header", ErrMalformed)
	}
	if len(b) > MaxMessageSize {
		return Frame{}, ErrOversized
	}
	if b[0] != 'L' || b[1] != 'Z' {
		return Frame{}, ErrMalformed
	}
	if b[2] != Version {
		return Frame{}, ErrVersion
	}
	f := Frame{Type: Type(b[3]), StreamID: binary.BigEndian.Uint32(b[4:]), Payload: append([]byte(nil), b[HeaderSize:]...)}
	if f.StreamID == 0 || !validType(f.Type) || ((f.Type == Open || f.Type == Opened || f.Type == CloseWrite || f.Type == Close) && len(f.Payload) != 0) {
		return Frame{}, ErrMalformed
	}
	return f, nil
}
func validType(t Type) bool { return t >= Open && t <= Pong }
