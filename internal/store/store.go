// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/manuelemosr/localzie-connector/internal/protocol"

	_ "modernc.org/sqlite"
)

var (
	ErrNotFound       = errors.New("not found")
	ErrBusy           = errors.New("connector busy")
	ErrInvalidState   = errors.New("invalid state transition")
	ErrInvalidPairing = errors.New("invalid or expired pairing token")
)

type Store struct{ db *sql.DB }

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`PRAGMA journal_mode=WAL`, `PRAGMA foreign_keys=ON`, `PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS conversations (id TEXT PRIMARY KEY, title TEXT NOT NULL, model_id TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE, generation_id TEXT NOT NULL DEFAULT '', role TEXT NOT NULL, content TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS messages_conversation_created ON messages(conversation_id, created_at)`,
		`CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE, user_message_id TEXT NOT NULL, assistant_message_id TEXT NOT NULL, model_id TEXT NOT NULL, allow_reasoning INTEGER, status TEXT NOT NULL, phase TEXT NOT NULL DEFAULT 'thinking', error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS jobs_status ON jobs(status)`,
		`CREATE TABLE IF NOT EXISTS idempotency (key TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE)`,
		`CREATE TABLE IF NOT EXISTS generated_conversation_titles (conversation_id TEXT PRIMARY KEY REFERENCES conversations(id) ON DELETE CASCADE, generated_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS events (cursor INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL, entity_id TEXT NOT NULL, payload BLOB NOT NULL, created_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS pairing_tokens (token_hash TEXT PRIMARY KEY, expires_at TEXT NOT NULL, used_at TEXT)`,
		`CREATE TABLE IF NOT EXISTS devices (id TEXT PRIMARY KEY, name TEXT NOT NULL, token_hash TEXT UNIQUE NOT NULL, created_at TEXT NOT NULL, revoked_at TEXT)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate store: %w", err)
		}
	}
	// Existing connector databases predate the optional per-message reasoning
	// preference. SQLite has no ADD COLUMN IF NOT EXISTS, so ignore its stable
	// duplicate-column error while surfacing all other migration failures.
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE jobs ADD COLUMN allow_reasoning INTEGER`); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
		return fmt.Errorf("migrate store: %w", err)
	}
	// Active-job phase was added after initial connector releases. Existing rows
	// deliberately default to thinking because their runtime load state is not
	// knowable after the fact.
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE jobs ADD COLUMN phase TEXT NOT NULL DEFAULT 'thinking'`); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
		return fmt.Errorf("migrate store: %w", err)
	}
	// Harness conversations record the OpenCode-owned session they drive.
	// Existing rows keep the runtime backend.
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE conversations ADD COLUMN backend TEXT NOT NULL DEFAULT 'runtime'`); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
		return fmt.Errorf("migrate store: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE conversations ADD COLUMN harness_session_id TEXT NOT NULL DEFAULT ''`); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
		return fmt.Errorf("migrate store: %w", err)
	}
	if _, err := s.ConnectorID(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) ConnectorID(ctx context.Context) (string, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='connector_id'`).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	id = newID()
	_, err = s.db.ExecContext(ctx, `INSERT OR IGNORE INTO metadata(key,value) VALUES('connector_id',?)`, id)
	if err != nil {
		return "", err
	}
	return s.ConnectorID(ctx)
}

func (s *Store) CreatePairing(ctx context.Context, ttl time.Duration) (string, time.Time, error) {
	raw := randomToken()
	expires := time.Now().UTC().Add(ttl)
	_, err := s.db.ExecContext(ctx, `INSERT INTO pairing_tokens(token_hash,expires_at) VALUES(?,?)`, hash(raw), stamp(expires))
	return raw, expires, err
}

func (s *Store) CreatePairingCode(ctx context.Context, ttl time.Duration) (string, time.Time, error) {
	expires := time.Now().UTC().Add(ttl)
	for range 10 {
		code := randomPairingCode()
		if _, err := s.db.ExecContext(ctx, `INSERT INTO pairing_tokens(token_hash,expires_at) VALUES(?,?)`, hash(code), stamp(expires)); err == nil {
			return code, expires, nil
		}
	}
	return "", time.Time{}, errors.New("could not allocate a pairing code")
}

// InvalidatePairing removes an unpublished pairing secret so it cannot be used
// after a relay publication failure.
func (s *Store) InvalidatePairing(ctx context.Context, token string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM pairing_tokens WHERE token_hash=? AND used_at IS NULL`, hash(token))
	return err
}

func (s *Store) CreateDevice(ctx context.Context, deviceName string) (string, string, error) {
	deviceName = strings.TrimSpace(deviceName)
	if deviceName == "" {
		return "", "", errors.New("device name is required")
	}
	deviceID, deviceToken := newID(), randomToken()
	_, err := s.db.ExecContext(ctx, `INSERT INTO devices(id,name,token_hash,created_at) VALUES(?,?,?,?)`, deviceID, deviceName, hash(deviceToken), stamp(time.Now().UTC()))
	if err != nil {
		return "", "", err
	}
	return deviceID, deviceToken, nil
}

func (s *Store) ConsumePairing(ctx context.Context, token, deviceName string) (string, string, error) {
	return s.consumePairing(ctx, token, deviceName, "")
}

// Upgrade connectivity without creating a second controller for this phone.
func (s *Store) UpgradePairing(ctx context.Context, token, deviceName, existingToken string) (string, string, error) {
	return s.consumePairing(ctx, token, deviceName, existingToken)
}
func (s *Store) consumePairing(ctx context.Context, token, deviceName, existingToken string) (string, string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback()
	var expires string
	var used sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT expires_at,used_at FROM pairing_tokens WHERE token_hash=?`, hash(token)).Scan(&expires, &used); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", ErrInvalidPairing
		}
		return "", "", err
	}
	expiresAt, err := parseStamp(expires)
	if err != nil || used.Valid || time.Now().UTC().After(expiresAt) {
		return "", "", ErrInvalidPairing
	}
	deviceID, deviceToken := newID(), randomToken()
	now := stamp(time.Now().UTC())
	if existingToken != "" {
		if err := tx.QueryRowContext(ctx, `SELECT id FROM devices WHERE token_hash=? AND revoked_at IS NULL`, hash(existingToken)).Scan(&deviceID); err != nil {
			return "", "", ErrInvalidPairing
		}
		deviceToken = existingToken
	} else if _, err := tx.ExecContext(ctx, `INSERT INTO devices(id,name,token_hash,created_at) VALUES(?,?,?,?)`, deviceID, strings.TrimSpace(deviceName), hash(deviceToken), now); err != nil {
		return "", "", err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE pairing_tokens SET used_at=? WHERE token_hash=? AND used_at IS NULL`, now, hash(token)); err != nil {
		return "", "", err
	}
	if err := tx.Commit(); err != nil {
		return "", "", err
	}
	return deviceID, deviceToken, nil
}

func (s *Store) AuthenticateDevice(ctx context.Context, token string) (string, bool) {
	if token == "" {
		return "", false
	}
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT id FROM devices WHERE token_hash=? AND revoked_at IS NULL`, hash(token)).Scan(&id)
	return id, err == nil
}

// DeviceActive is intentionally ID-based for long-lived authenticated
// transports.  It lets an already-open SSE response observe revocation without
// retaining or repeatedly handling the bearer secret.
func (s *Store) DeviceActive(ctx context.Context, id string) bool {
	if id == "" {
		return false
	}
	var found int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM devices WHERE id=? AND revoked_at IS NULL`, id).Scan(&found)
	return err == nil
}

func (s *Store) ListDevices(ctx context.Context) ([]protocol.Device, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,name,created_at,revoked_at FROM devices ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	devices := []protocol.Device{}
	for rows.Next() {
		var device protocol.Device
		var created string
		var revoked sql.NullString
		if err := rows.Scan(&device.ID, &device.Name, &created, &revoked); err != nil {
			return nil, err
		}
		device.CreatedAt, _ = parseStamp(created)
		if revoked.Valid {
			value, err := parseStamp(revoked.String)
			if err != nil {
				return nil, err
			}
			device.RevokedAt = &value
		}
		devices = append(devices, device)
	}
	return devices, rows.Err()
}

func (s *Store) RevokeDevice(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE devices SET revoked_at=? WHERE id=? AND revoked_at IS NULL`, stamp(time.Now().UTC()), id)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) CreateConversation(ctx context.Context, modelID, title string) (protocol.Conversation, error) {
	return s.createConversation(ctx, newID(), modelID, title, protocol.BackendRuntime, "")
}

// CreateHarnessConversation records the OpenCode-owned session that serves
// this conversation. The session is created before this call; the store owns
// only the durable association.
func (s *Store) CreateHarnessConversation(ctx context.Context, modelID, title, harnessSessionID string) (protocol.Conversation, error) {
	return s.createConversation(ctx, newID(), modelID, title, protocol.BackendHarness, harnessSessionID)
}

// CreateHarnessConversationWithID adopts an existing OpenCode session under a
// caller-chosen conversation ID. Taking over an observed session keeps its
// conversation ID so the phone never sees the conversation identity change;
// an already-adopted ID returns the existing conversation unchanged.
func (s *Store) CreateHarnessConversationWithID(ctx context.Context, id, modelID, title, harnessSessionID string) (protocol.Conversation, error) {
	if existing, err := s.Conversation(ctx, id); err == nil {
		return existing.Conversation, nil
	} else if !errors.Is(err, ErrNotFound) {
		return protocol.Conversation{}, err
	}
	return s.createConversation(ctx, id, modelID, title, protocol.BackendHarness, harnessSessionID)
}

func (s *Store) createConversation(ctx context.Context, id, modelID, title string, backend protocol.ConversationBackend, harnessSessionID string) (protocol.Conversation, error) {
	if strings.TrimSpace(title) == "" {
		title = "New chat"
	}
	now := time.Now().UTC()
	c := protocol.Conversation{ID: id, Title: strings.TrimSpace(title), ModelID: modelID, Backend: backend, HarnessSessionID: harnessSessionID, CreatedAt: now, UpdatedAt: now}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return protocol.Conversation{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO conversations(id,title,model_id,backend,harness_session_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?)`, c.ID, c.Title, c.ModelID, c.Backend, c.HarnessSessionID, stamp(now), stamp(now)); err != nil {
		return protocol.Conversation{}, err
	}
	if _, err := s.appendEvent(ctx, tx, "conversation.created", c.ID, c); err != nil {
		return protocol.Conversation{}, err
	}
	return c, tx.Commit()
}

// HarnessSessionIDs reports the OpenCode sessions a store-owned conversation
// already drives. Observed-session listing uses it to exclude sessions
// Localzie owns; the harness stays authoritative and no store record is
// created for observed sessions.
func (s *Store) HarnessSessionIDs(ctx context.Context) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT harness_session_id FROM conversations WHERE backend='harness' AND harness_session_id<>''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids[id] = true
	}
	return ids, rows.Err()
}

// UpsertHarnessMirrorMessages records transcript messages the harness owns
// into a store conversation, keyed by the harness message ID. Re-running it
// updates content in place instead of duplicating; created_at keeps the
// harness order (which predates the connector-accepted prompt). It never
// creates or deletes harness state.
func (s *Store) UpsertHarnessMirrorMessages(ctx context.Context, conversationID string, messages []protocol.Message) error {
	if len(messages) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, message := range messages {
		if message.ID == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO messages(id,conversation_id,generation_id,role,content,status,created_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET content=excluded.content,status=excluded.status`, message.ID, conversationID, message.GenerationID, message.Role, message.Content, message.Status, stamp(message.CreatedAt)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE conversations SET updated_at=? WHERE id=?`, stamp(time.Now().UTC()), conversationID); err != nil {
		return err
	}
	return tx.Commit()
}

// PublishObservedEvent appends a durable event for an observed or mirrored
// harness conversation that has no store-owned message rows of its own. It
// reuses the standard event pipeline so cursor replay works unchanged.
func (s *Store) PublishObservedEvent(ctx context.Context, eventType, entityID string, payload any) error {
	_, err := s.appendEvent(ctx, nil, eventType, entityID, payload)
	return err
}

func (s *Store) ListConversations(ctx context.Context) ([]protocol.Conversation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,title,model_id,backend,harness_session_id,created_at,updated_at FROM conversations ORDER BY updated_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []protocol.Conversation{}
	for rows.Next() {
		c, err := scanConversation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func (s *Store) Conversation(ctx context.Context, id string) (protocol.ConversationDetail, error) {
	c, err := scanConversation(s.db.QueryRowContext(ctx, `SELECT id,title,model_id,backend,harness_session_id,created_at,updated_at FROM conversations WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.ConversationDetail{}, ErrNotFound
	}
	if err != nil {
		return protocol.ConversationDetail{}, err
	}
	// rowid is the durable insertion order within a conversation. User and
	// assistant placeholders deliberately share one timestamp, so UUID order
	// would sometimes put the assistant before its prompt.
	rows, err := s.db.QueryContext(ctx, `SELECT id,conversation_id,generation_id,role,content,status,created_at FROM messages WHERE conversation_id=? ORDER BY created_at,rowid`, id)
	if err != nil {
		return protocol.ConversationDetail{}, err
	}
	defer rows.Close()
	detail := protocol.ConversationDetail{Conversation: c, Messages: []protocol.Message{}}
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return protocol.ConversationDetail{}, err
		}
		detail.Messages = append(detail.Messages, m)
	}
	return detail, rows.Err()
}

func (s *Store) AcceptMessage(ctx context.Context, conversationID string, request protocol.SendMessageRequest) (protocol.Job, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return protocol.Job{}, false, err
	}
	defer tx.Rollback()
	if request.IdempotencyKey == "" || strings.TrimSpace(request.Content) == "" {
		return protocol.Job{}, false, fmt.Errorf("idempotency_key and content are required")
	}
	if job, err := jobForKey(ctx, tx, request.IdempotencyKey); err == nil {
		return job, true, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return protocol.Job{}, false, err
	}
	var modelID, title string
	if err := tx.QueryRowContext(ctx, `SELECT model_id,title FROM conversations WHERE id=?`, conversationID).Scan(&modelID, &title); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return protocol.Job{}, false, ErrNotFound
		}
		return protocol.Job{}, false, err
	}
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE status IN ('accepted','running','canceling')`).Scan(&active); err != nil {
		return protocol.Job{}, false, err
	}
	if active > 0 {
		return protocol.Job{}, false, ErrBusy
	}
	now := time.Now().UTC()
	userID, assistantID, jobID := newID(), newID(), newID()
	if _, err := tx.ExecContext(ctx, `INSERT INTO messages(id,conversation_id,role,content,status,created_at) VALUES(?,?,?,?,?,?)`, userID, conversationID, protocol.RoleUser, strings.TrimSpace(request.Content), protocol.MessageComplete, stamp(now)); err != nil {
		return protocol.Job{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO messages(id,conversation_id,generation_id,role,content,status,created_at) VALUES(?,?,?,?,?,?,?)`, assistantID, conversationID, jobID, protocol.RoleAssistant, "", protocol.MessageStreaming, stamp(now)); err != nil {
		return protocol.Job{}, false, err
	}
	phase := request.InitialPhase
	if phase != protocol.JobPreparingModel && phase != protocol.JobThinking {
		phase = protocol.JobThinking
	}
	job := protocol.Job{ID: jobID, ConversationID: conversationID, UserMessageID: userID, AssistantMessageID: assistantID, ModelID: modelID, AllowReasoning: request.AllowReasoning, Status: protocol.JobAccepted, Phase: phase, CreatedAt: now, UpdatedAt: now}
	var allowReasoning any
	if request.AllowReasoning != nil {
		allowReasoning = *request.AllowReasoning
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO jobs(id,conversation_id,user_message_id,assistant_message_id,model_id,allow_reasoning,status,phase,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, job.ID, job.ConversationID, job.UserMessageID, job.AssistantMessageID, job.ModelID, allowReasoning, job.Status, job.Phase, stamp(now), stamp(now)); err != nil {
		return protocol.Job{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO idempotency(key,job_id) VALUES(?,?)`, request.IdempotencyKey, job.ID); err != nil {
		return protocol.Job{}, false, err
	}
	if title == "New chat" {
		title = titleFrom(request.Content)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE conversations SET title=?,updated_at=? WHERE id=?`, title, stamp(now), conversationID); err != nil {
		return protocol.Job{}, false, err
	}
	if _, err := s.appendEvent(ctx, tx, "job.accepted", job.ID, job); err != nil {
		return protocol.Job{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return protocol.Job{}, false, err
	}
	return job, false, nil
}

func (s *Store) MarkJobRunning(ctx context.Context, jobID string) error {
	return s.setJobStatus(ctx, jobID, protocol.JobRunning, "", protocol.MessageStreaming, "job.running", protocol.JobAccepted)
}

// SetJobPhase publishes a normal job event so clients can refresh an active
// snapshot without assigning lifecycle meaning to the phase itself.
func (s *Store) SetJobPhase(ctx context.Context, jobID string, phase protocol.JobPhase) error {
	if phase != protocol.JobPreparingModel && phase != protocol.JobThinking {
		return fmt.Errorf("invalid job phase %q", phase)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE jobs SET phase=?,updated_at=? WHERE id=? AND status='running'`, phase, stamp(time.Now().UTC()), jobID)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return classifyJobUpdate(ctx, tx, jobID)
	}
	job, err := jobByID(ctx, tx, jobID)
	if err != nil {
		return err
	}
	if _, err := s.appendEvent(ctx, tx, "job.phase", jobID, job); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) MarkJobCompleted(ctx context.Context, jobID string) error {
	return s.setJobStatus(ctx, jobID, protocol.JobCompleted, "", protocol.MessageComplete, "job.completed", protocol.JobRunning)
}
func (s *Store) MarkJobCanceled(ctx context.Context, jobID string) error {
	return s.setJobStatus(ctx, jobID, protocol.JobCanceled, "", protocol.MessageInterrupted, "job.canceled", protocol.JobCanceling)
}
func (s *Store) MarkJobFailed(ctx context.Context, jobID string, cause error) error {
	return s.setJobStatus(ctx, jobID, protocol.JobFailed, cause.Error(), protocol.MessageInterrupted, "job.failed", protocol.JobAccepted, protocol.JobRunning, protocol.JobCanceling)
}

func (s *Store) MarkJobCanceling(ctx context.Context, jobID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE jobs SET status=?,updated_at=? WHERE id=? AND status IN ('accepted','running')`, protocol.JobCanceling, stamp(time.Now().UTC()), jobID)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return classifyJobUpdate(ctx, tx, jobID)
	}
	job, err := jobByID(ctx, tx, jobID)
	if err != nil {
		return err
	}
	if _, err = s.appendEvent(ctx, tx, "job.canceling", jobID, job); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) setJobStatus(ctx context.Context, jobID string, status protocol.JobStatus, jobError string, messageStatus protocol.MessageStatus, eventType string, allowed ...protocol.JobStatus) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := stamp(time.Now().UTC())
	placeholders := strings.TrimRight(strings.Repeat("?,", len(allowed)), ",")
	query := `UPDATE jobs SET status=?,error=?,updated_at=? WHERE id=? AND status IN (` + placeholders + `)`
	args := []any{status, jobError, now, jobID}
	for _, current := range allowed {
		args = append(args, current)
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return classifyJobUpdate(ctx, tx, jobID)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE messages SET status=? WHERE generation_id=?`, messageStatus, jobID); err != nil {
		return err
	}
	job, err := jobByID(ctx, tx, jobID)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE conversations SET updated_at=? WHERE id=?`, now, job.ConversationID); err != nil {
		return err
	}
	if _, err := s.appendEvent(ctx, tx, eventType, jobID, job); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) UpdateConversationTitle(ctx context.Context, id, title string) error {
	title = strings.Join(strings.Fields(title), " ")
	if title == "" {
		return fmt.Errorf("title is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE conversations SET title=? WHERE id=?`, title, id)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return ErrNotFound
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO generated_conversation_titles(conversation_id,generated_at) VALUES(?,?)`, id, stamp(time.Now().UTC())); err != nil {
		return err
	}
	conversation, err := scanConversation(tx.QueryRowContext(ctx, `SELECT id,title,model_id,backend,harness_session_id,created_at,updated_at FROM conversations WHERE id=?`, id))
	if err != nil {
		return err
	}
	if _, err := s.appendEvent(ctx, tx, "conversation.updated", id, conversation); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ConversationsNeedingGeneratedTitle(ctx context.Context) ([]protocol.Conversation, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT c.id,c.title,c.model_id,c.backend,c.harness_session_id,c.created_at,c.updated_at
		FROM conversations c
		WHERE c.backend='runtime'
		AND NOT EXISTS (
			SELECT 1 FROM generated_conversation_titles g WHERE g.conversation_id=c.id
		)
		AND EXISTS (
			SELECT 1 FROM messages m
			WHERE m.conversation_id=c.id AND m.role='assistant' AND TRIM(m.content)<>''
		)
		ORDER BY c.updated_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []protocol.Conversation{}
	for rows.Next() {
		conversation, err := scanConversation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, conversation)
	}
	return result, rows.Err()
}

func (s *Store) DeleteConversation(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE conversation_id=? AND status IN ('accepted','running','canceling')`, id).Scan(&active); err != nil {
		return err
	}
	if active > 0 {
		return ErrBusy
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM conversations WHERE id=?`, id)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return ErrNotFound
	}
	if _, err := s.appendEvent(ctx, tx, "conversation.deleted", id, map[string]string{"id": id}); err != nil {
		return err
	}
	return tx.Commit()
}

// EnsureConversationDeletable performs the side-effect-free half of
// DeleteConversation. Callers that must remove an external resource first use
// this while holding their message-acceptance lock, so a busy conversation is
// rejected before its external session is touched.
func (s *Store) EnsureConversationDeletable(ctx context.Context, id string) error {
	var exists, active int
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM conversations WHERE id=?),
		       EXISTS(SELECT 1 FROM jobs WHERE conversation_id=? AND status IN ('accepted','running','canceling'))
	`, id, id).Scan(&exists, &active)
	if err != nil {
		return err
	}
	if exists == 0 {
		return ErrNotFound
	}
	if active != 0 {
		return ErrBusy
	}
	return nil
}

func (s *Store) AppendDelta(ctx context.Context, jobID, delta string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE messages SET content=content||? WHERE generation_id=? AND status=?`, delta, jobID, protocol.MessageStreaming)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return ErrNotFound
	}
	// Read from the same transaction after the streaming-message update so the
	// durable delta event is unambiguously tied to its conversation.
	var conversationID string
	if err := tx.QueryRowContext(ctx, `SELECT conversation_id FROM messages WHERE generation_id=? AND status=?`, jobID, protocol.MessageStreaming).Scan(&conversationID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	payload := map[string]string{"conversation_id": conversationID, "generation_id": jobID, "delta": delta}
	if _, err := s.appendEvent(ctx, tx, "message.delta", jobID, payload); err != nil {
		return err
	}
	return tx.Commit()
}

// SetMessageContent replaces the streaming assistant message's content
// wholesale and publishes a full-snapshot message.content event. It exists
// for harness streams whose next state is not an append of the previous one.
func (s *Store) SetMessageContent(ctx context.Context, jobID, content string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE messages SET content=? WHERE generation_id=? AND status=?`, content, jobID, protocol.MessageStreaming)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return ErrNotFound
	}
	var conversationID string
	if err := tx.QueryRowContext(ctx, `SELECT conversation_id FROM messages WHERE generation_id=? AND status=?`, jobID, protocol.MessageStreaming).Scan(&conversationID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	payload := map[string]string{"conversation_id": conversationID, "generation_id": jobID, "content": content}
	if _, err := s.appendEvent(ctx, tx, "message.content", jobID, payload); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Job(ctx context.Context, id string) (protocol.Job, error) {
	job, err := jobByID(ctx, s.db, id)
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.Job{}, ErrNotFound
	}
	return job, err
}

// JobForIdempotencyKey finds an earlier accepted message without creating a
// new job. Callers use this before side effects such as a model-load probe.
func (s *Store) JobForIdempotencyKey(ctx context.Context, key string) (protocol.Job, error) {
	job, err := jobForKey(ctx, s.db, key)
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.Job{}, ErrNotFound
	}
	return job, err
}

func (s *Store) RecoverInterrupted(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM jobs WHERE status IN ('accepted','running','canceling')`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		if err := s.setJobStatus(ctx, id, protocol.JobInterrupted, "connector restarted", protocol.MessageInterrupted, "job.interrupted", protocol.JobAccepted, protocol.JobRunning, protocol.JobCanceling); err != nil {
			return err
		}
	}
	return nil
}

func classifyJobUpdate(ctx context.Context, q queryer, jobID string) error {
	var found int
	if err := q.QueryRowContext(ctx, `SELECT 1 FROM jobs WHERE id=?`, jobID).Scan(&found); errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	return ErrInvalidState
}

func (s *Store) EventsAfter(ctx context.Context, cursor int64, limit int) ([]protocol.StoredEvent, error) {
	if limit < 1 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT cursor,type,entity_id,payload,created_at FROM events WHERE cursor>? ORDER BY cursor LIMIT ?`, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []protocol.StoredEvent
	for rows.Next() {
		var e protocol.StoredEvent
		var created string
		if err := rows.Scan(&e.Cursor, &e.Type, &e.EntityID, &e.Payload, &created); err != nil {
			return nil, err
		}
		e.CreatedAt, _ = parseStamp(created)
		events = append(events, e)
	}
	return events, rows.Err()
}

func (s *Store) Snapshot(ctx context.Context) (protocol.Snapshot, error) {
	conversations, err := s.ListConversations(ctx)
	if err != nil {
		return protocol.Snapshot{}, err
	}
	var cursor int64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(cursor),0) FROM events`).Scan(&cursor); err != nil {
		return protocol.Snapshot{}, err
	}
	result := protocol.Snapshot{Cursor: cursor, Conversations: conversations}
	row := s.db.QueryRowContext(ctx, `SELECT id,conversation_id,user_message_id,assistant_message_id,model_id,allow_reasoning,status,phase,error,created_at,updated_at FROM jobs WHERE status IN ('accepted','running','canceling') ORDER BY created_at LIMIT 1`)
	job, err := scanJob(row)
	if err == nil {
		result.ActiveJob = &job
	} else if !errors.Is(err, sql.ErrNoRows) {
		return protocol.Snapshot{}, err
	}
	return result, nil
}

func (s *Store) appendEvent(ctx context.Context, tx *sql.Tx, eventType, entityID string, payload any) (int64, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	var result sql.Result
	if tx != nil {
		result, err = tx.ExecContext(ctx, `INSERT INTO events(type,entity_id,payload,created_at) VALUES(?,?,?,?)`, eventType, entityID, data, stamp(time.Now().UTC()))
	} else {
		result, err = s.db.ExecContext(ctx, `INSERT INTO events(type,entity_id,payload,created_at) VALUES(?,?,?,?)`, eventType, entityID, data, stamp(time.Now().UTC()))
	}
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

type scanner interface{ Scan(...any) error }
type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func scanConversation(row scanner) (protocol.Conversation, error) {
	var c protocol.Conversation
	var created, updated string
	err := row.Scan(&c.ID, &c.Title, &c.ModelID, &c.Backend, &c.HarnessSessionID, &created, &updated)
	if c.Backend == "" {
		c.Backend = protocol.BackendRuntime
	}
	c.CreatedAt, _ = parseStamp(created)
	c.UpdatedAt, _ = parseStamp(updated)
	return c, err
}
func scanMessage(row scanner) (protocol.Message, error) {
	var m protocol.Message
	var created string
	err := row.Scan(&m.ID, &m.ConversationID, &m.GenerationID, &m.Role, &m.Content, &m.Status, &created)
	m.CreatedAt, _ = parseStamp(created)
	return m, err
}
func jobByID(ctx context.Context, q queryer, id string) (protocol.Job, error) {
	return scanJob(q.QueryRowContext(ctx, `SELECT id,conversation_id,user_message_id,assistant_message_id,model_id,allow_reasoning,status,phase,error,created_at,updated_at FROM jobs WHERE id=?`, id))
}
func scanJob(row scanner) (protocol.Job, error) {
	var j protocol.Job
	var created, updated string
	var allowReasoning sql.NullBool
	err := row.Scan(&j.ID, &j.ConversationID, &j.UserMessageID, &j.AssistantMessageID, &j.ModelID, &allowReasoning, &j.Status, &j.Phase, &j.Error, &created, &updated)
	if allowReasoning.Valid {
		j.AllowReasoning = new(bool)
		*j.AllowReasoning = allowReasoning.Bool
	}
	j.CreatedAt, _ = parseStamp(created)
	j.UpdatedAt, _ = parseStamp(updated)
	return j, err
}
func jobForKey(ctx context.Context, q queryer, key string) (protocol.Job, error) {
	var id string
	if err := q.QueryRowContext(ctx, `SELECT job_id FROM idempotency WHERE key=?`, key).Scan(&id); err != nil {
		return protocol.Job{}, err
	}
	return jobByID(ctx, q, id)
}

func titleFrom(content string) string {
	content = strings.Join(strings.Fields(content), " ")
	r := []rune(content)
	if len(r) > 52 {
		return string(r[:52]) + "…"
	}
	return content
}
func stamp(t time.Time) string               { return t.UTC().Format(time.RFC3339Nano) }
func parseStamp(v string) (time.Time, error) { return time.Parse(time.RFC3339Nano, v) }
func hash(v string) string                   { sum := sha256.Sum256([]byte(v)); return hex.EncodeToString(sum[:]) }
func randomToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
func randomPairingCode() string {
	const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	for index := range b {
		b[index] = alphabet[int(b[index])%len(alphabet)]
	}
	return string(b)
}
func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
