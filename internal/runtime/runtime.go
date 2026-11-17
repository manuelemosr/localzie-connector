// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"github.com/manuelemosr/localzie-connector/internal/protocol"
)

type StreamRequest struct {
	Model           string
	Messages        []protocol.Message
	MaxOutputTokens int
	// AllowReasoning is nil for a model that does not support selectable
	// reasoning. Runtimes must not turn that into a reasoning request.
	// False explicitly disables optional reasoning; true enables it only for a
	// model that advertises support.
	AllowReasoning *bool
	// SuppressReasoning keeps internal callers (such as title generation) from
	// receiving persisted thinking text when a runtime requires thinking on.
	SuppressReasoning bool
}

type Adapter interface {
	Name() string
	Models(context.Context) ([]protocol.Model, error)
	Stream(context.Context, StreamRequest, func(string) error) error
}
