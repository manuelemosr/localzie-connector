// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package protocol

import "time"

const Version = 1

type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

type MessageStatus string

const (
	MessageComplete    MessageStatus = "complete"
	MessageStreaming   MessageStatus = "streaming"
	MessageInterrupted MessageStatus = "interrupted"
)

type JobStatus string

const (
	JobAccepted    JobStatus = "accepted"
	JobRunning     JobStatus = "running"
	JobCompleted   JobStatus = "completed"
	JobFailed      JobStatus = "failed"
	JobInterrupted JobStatus = "interrupted"
	JobCanceling   JobStatus = "canceling"
	JobCanceled    JobStatus = "canceled"
)

// JobPhase describes the active work before an assistant message has visible
// content. It is additive to JobStatus, which continues to describe the job
// lifecycle. Unknown runtime state is represented as thinking, never loading.
type JobPhase string

const (
	JobPreparingModel JobPhase = "preparing_model"
	JobThinking       JobPhase = "thinking"
)

type Health struct {
	ProtocolVersion int       `json:"protocol_version"`
	Service         string    `json:"service"`
	Version         string    `json:"version"`
	Features        []string  `json:"features"`
	Time            time.Time `json:"time"`
}

type Model struct {
	ID                string        `json:"id"`
	Name              string        `json:"name"`
	Runtime           string        `json:"runtime"`
	Ready             bool          `json:"ready"`
	Locality          string        `json:"locality"`
	SupportsReasoning *bool         `json:"supports_reasoning,omitempty"`
	ReasoningMode     ReasoningMode `json:"reasoning_mode,omitempty"`
	// MemoryState is additive: older and runtimes that cannot safely inspect
	// residency leave it empty rather than guessing it is unloaded.
	MemoryState MemoryState `json:"memory_state,omitempty"`
	CanRelease  *bool       `json:"can_release,omitempty"`
}

type MemoryState string

const (
	MemoryStateLoaded   MemoryState = "loaded"
	MemoryStateUnloaded MemoryState = "unloaded"
	MemoryStateUnknown  MemoryState = "unknown"
)

type ReasoningMode string

const (
	ReasoningNone     ReasoningMode = "none"
	ReasoningOptional ReasoningMode = "optional"
	ReasoningRequired ReasoningMode = "required"
)

type ConversationBackend string

const (
	BackendRuntime ConversationBackend = "runtime"
	BackendHarness ConversationBackend = "harness"
)

type Conversation struct {
	ID        string              `json:"id"`
	Title     string              `json:"title"`
	ModelID   string              `json:"model_id"`
	Backend   ConversationBackend `json:"backend,omitempty"`
	CreatedAt time.Time           `json:"created_at"`
	UpdatedAt time.Time           `json:"updated_at"`
	// ActiveSite and ObserveOnly mark a conversation the connector mirrors
	// for a harness session another client is driving. Both are additive and
	// omitted on store-owned rows, so older clients ignore them.
	ActiveSite  string `json:"activeSite,omitempty"`
	ObserveOnly bool   `json:"observeOnly,omitempty"`
	// HarnessSessionID is connector-internal routing state. It is never
	// accepted from or returned to API clients.
	HarnessSessionID string `json:"-"`
}

type Message struct {
	ID             string        `json:"id"`
	ConversationID string        `json:"conversation_id"`
	GenerationID   string        `json:"generation_id,omitempty"`
	Role           Role          `json:"role"`
	Content        string        `json:"content"`
	Status         MessageStatus `json:"status"`
	CreatedAt      time.Time     `json:"created_at"`
}

type ConversationDetail struct {
	Conversation Conversation `json:"conversation"`
	Messages     []Message    `json:"messages"`
}

type Snapshot struct {
	Cursor        int64          `json:"cursor"`
	Conversations []Conversation `json:"conversations"`
	ActiveJob     *Job           `json:"active_job,omitempty"`
}

type Device struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

type Job struct {
	ID                 string    `json:"id"`
	ConversationID     string    `json:"conversation_id"`
	UserMessageID      string    `json:"user_message_id"`
	AssistantMessageID string    `json:"assistant_message_id"`
	ModelID            string    `json:"model_id"`
	AllowReasoning     *bool     `json:"allow_reasoning,omitempty"`
	Status             JobStatus `json:"status"`
	Phase              JobPhase  `json:"phase"`
	Error              string    `json:"error,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
	// AwaitIdle and HarnessDirectory are connector-internal routing state set
	// only for a job accepted with take_over on a session whose previous turn
	// was still running. They are never accepted from or returned to clients.
	AwaitIdle        bool   `json:"-"`
	HarnessDirectory string `json:"-"`
}

type Event struct {
	Cursor    int64     `json:"cursor"`
	Type      string    `json:"type"`
	EntityID  string    `json:"entity_id"`
	Payload   any       `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}

type StoredEvent struct {
	Cursor    int64     `json:"cursor"`
	Type      string    `json:"type"`
	EntityID  string    `json:"entity_id"`
	Payload   []byte    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}

type PairRequest struct {
	PairingToken string `json:"pairing_token"`
	DeviceName   string `json:"device_name"`
}

type LocalPairRequest struct {
	DeviceName string `json:"device_name"`
}

type PairResponse struct {
	DeviceID    string `json:"device_id"`
	DeviceToken string `json:"device_token"`
	ConnectorID string `json:"connector_id"`
}

type CreateConversationRequest struct {
	ModelID string `json:"model_id"`
	Title   string `json:"title,omitempty"`
}

type SendMessageRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
	Content        string `json:"content"`
	AllowReasoning *bool  `json:"allow_reasoning,omitempty"`
	// TakeOver asks the connector to attach to an observed harness session and
	// make it the driver of that session. It is additive; absent means false.
	TakeOver bool `json:"take_over,omitempty"`
	// InitialPhase is connector-internal state resolved before accepting a job.
	// It is intentionally never accepted from or returned to API clients.
	InitialPhase JobPhase `json:"-"`
}

type SendMessageResponse struct {
	Job       Job  `json:"job"`
	Duplicate bool `json:"duplicate"`
}

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
