// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package ziticonn

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/manuelemosr/localzie-connector/internal/agentbridge"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Receipts and bounded temporary outputs live only on the home machine. A
// pending receipt found after restart is uncertain and is never dispatched again.
type acceptedRequestKey struct{}

type requestJournal struct {
	mu        sync.Mutex
	entries   map[string]*requestEntry
	dir       string
	lastPrune time.Time
}
type requestEntry struct {
	mu        sync.Mutex
	Digest    string      `json:"digest"`
	Status    int         `json:"status"`
	Header    http.Header `json:"header"`
	Done      bool        `json:"done"`
	Uncertain bool        `json:"uncertain"`
	Created   time.Time   `json:"created"`
	data      []byte
	changed   chan struct{}
	file      string
}

func newRequestJournal(dir string) *requestJournal {
	if dir != "" {
		dir = filepath.Join(dir, "requests")
		_ = os.MkdirAll(dir, 0700)
	}
	return &requestJournal{entries: map[string]*requestEntry{}, dir: dir}
}
func requestKey(device, id string) string {
	b := sha256.Sum256([]byte(device + "\x00" + id))
	return hex.EncodeToString(b[:])
}
func (j *requestJournal) get(key string) *requestEntry {
	if e := j.entries[key]; e != nil {
		return e
	}
	if j.dir == "" {
		return nil
	}
	b, err := os.ReadFile(filepath.Join(j.dir, key+".json"))
	if err != nil {
		return nil
	}
	e := &requestEntry{changed: make(chan struct{}), file: filepath.Join(j.dir, key)}
	if json.Unmarshal(b, e) != nil || time.Since(e.Created) > 30*time.Minute {
		return nil
	}
	e.data, _ = os.ReadFile(e.file + ".body")
	if !e.Done {
		e.Uncertain = true
		e.Done = true
	}
	j.entries[key] = e
	return e
}
func (e *requestEntry) persist() error {
	if e.file == "" {
		return nil
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if err = os.WriteFile(e.file+".tmp", b, 0600); err != nil {
		return err
	}
	return os.Rename(e.file+".tmp", e.file+".json")
}
func (e *requestEntry) signal()                { close(e.changed); e.changed = make(chan struct{}) }
func (e *requestEntry) HeaderMap() http.Header { return e.Header }

type journalWriter struct {
	e      *requestEntry
	cancel context.CancelFunc
}

func (w *journalWriter) Header() http.Header { return w.e.Header }
func (w *journalWriter) WriteHeader(code int) {
	w.e.mu.Lock()
	defer w.e.mu.Unlock()
	if w.e.Status == 0 {
		w.e.Status = code
		_ = w.e.persist()
		w.e.signal()
	}
}
func (w *journalWriter) Write(b []byte) (int, error) {
	w.WriteHeader(200)
	w.e.mu.Lock()
	defer w.e.mu.Unlock()
	if len(w.e.data)+len(b) > 4<<20 {
		w.e.Uncertain = true
		w.cancel()
		return 0, errors.New("response exceeded the 4 MiB recovery limit")
	}
	if w.e.file != "" {
		f, err := os.OpenFile(w.e.file+".body", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return 0, err
		}
		_, err = f.Write(b)
		if err == nil {
			err = f.Sync()
		}
		f.Close()
		if err != nil {
			return 0, err
		}
	}
	w.e.data = append(w.e.data, b...)
	w.e.signal()
	return len(b), nil
}
func (w *journalWriter) Flush() { w.WriteHeader(200) }

func (j *requestJournal) serve(root context.Context, w http.ResponseWriter, r *http.Request, device string, next http.Handler) {
	id := r.Header.Get("X-Localzie-Request")
	if !safeID.MatchString(id) {
		http.Error(w, "Invalid request ID", 400)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<20))
	if err != nil {
		http.Error(w, "Request body exceeds the recovery limit", 413)
		return
	}
	digestBytes := sha256.Sum256(append([]byte(r.Method+"\x00"+r.URL.RequestURI()+"\x00"), body...))
	digest := hex.EncodeToString(digestBytes[:])
	key := requestKey(device, id)
	j.mu.Lock()
	if j.dir != "" && time.Since(j.lastPrune) > time.Minute {
		files, _ := os.ReadDir(j.dir)
		for _, file := range files {
			if info, err := file.Info(); err == nil && time.Since(info.ModTime()) > 30*time.Minute {
				_ = os.Remove(filepath.Join(j.dir, file.Name()))
			}
		}
		j.lastPrune = time.Now()
	}
	// Expired outputs are removed before admission; bounded on disk and in memory.
	for k, e := range j.entries {
		e.mu.Lock()
		expired := e.Done && time.Since(e.Created) > 30*time.Minute
		e.mu.Unlock()
		if expired {
			delete(j.entries, k)
			if e.file != "" {
				os.Remove(e.file + ".json")
				os.Remove(e.file + ".body")
			}
		}
	}
	e := j.get(key)
	if e == nil {
		if created := r.Header.Get("X-Localzie-Created"); created != "" {
			stamp, err := strconv.ParseInt(created, 10, 64)
			if err != nil || time.Since(time.Unix(stamp, 0)) > 30*time.Minute || time.Until(time.Unix(stamp, 0)) > 2*time.Minute {
				j.mu.Unlock()
				http.Error(w, "This request expired. Check the chat before retrying.", 410)
				return
			}
		}
		if len(j.entries) >= 64 {
			var oldestKey string
			oldestTime := time.Now()
			for k, entry := range j.entries {
				entry.mu.Lock()
				eligible := entry.Done && entry.Created.Before(oldestTime)
				created := entry.Created
				entry.mu.Unlock()
				if eligible {
					oldestKey = k
					oldestTime = created
				}
			}
			if oldestKey != "" {
				old := j.entries[oldestKey]
				old.mu.Lock()
				old.Uncertain = true
				old.data = nil
				_ = old.persist()
				if old.file != "" {
					_ = os.Remove(old.file + ".body")
				}
				old.mu.Unlock()
				delete(j.entries, oldestKey)
			}
		}
		if len(j.entries) >= 64 {
			j.mu.Unlock()
			http.Error(w, "Too many recoverable requests", 429)
			return
		}
		e = &requestEntry{Digest: digest, Header: make(http.Header), Created: time.Now(), changed: make(chan struct{})}
		if j.dir != "" {
			e.file = filepath.Join(j.dir, key)
		}
		// Persist acceptance before dispatch. Storage failure cannot become a replay.
		if err = e.persist(); err != nil {
			j.mu.Unlock()
			http.Error(w, "Could not save request receipt", 503)
			return
		}
		j.entries[key] = e
		clone := r.Clone(context.WithValue(context.WithoutCancel(r.Context()), inferenceDeviceKey{}, device))
		clone.Body = io.NopCloser(bytes.NewReader(body))
		clone.Header.Del("X-Localzie-Request")
		clone.Header.Del("X-Localzie-After")
		clone.Header.Del("X-Localzie-Created")
		go func() {
			ctx, cancel := context.WithTimeout(root, 30*time.Minute)
			defer cancel()
			// Preserve authentication/controller identity while detaching the subscriber.
			ctx = copyDeviceContext(ctx, r.Context(), device)
			defer func() {
				e.mu.Lock()
				if recover() != nil {
					e.Uncertain = true
					e.Status = 502
				}
				if e.Status == 0 {
					e.Status = 200
				}
				e.Done = true
				_ = e.persist()
				e.signal()
				e.mu.Unlock()
			}()
			next.ServeHTTP(&journalWriter{e: e, cancel: cancel}, clone.WithContext(ctx))
		}()
	}
	j.mu.Unlock()
	e.mu.Lock()
	mismatch := e.Digest != digest
	uncertain := e.Uncertain
	e.mu.Unlock()
	if mismatch {
		http.Error(w, "Request ID was reused with different content", 409)
		return
	}
	if uncertain {
		http.Error(w, "Delivery is uncertain after connector restart. Check the native chat before retrying.", 409)
		return
	}
	offset, err := strconv.Atoi(r.Header.Get("X-Localzie-After"))
	if r.Header.Get("X-Localzie-After") == "" {
		offset = 0
		err = nil
	}
	if err != nil || offset < 0 || offset > 4<<20 {
		http.Error(w, "Invalid response cursor", 400)
		return
	}
	wroteHeader := false
	for {
		e.mu.Lock()
		status := e.Status
		done := e.Done
		changed := e.changed
		if !wroteHeader && status != 0 {
			for k, v := range e.Header {
				if k != "Content-Length" {
					w.Header()[k] = append([]string(nil), v...)
				}
			}
			w.Header().Set("X-Localzie-Replay", "1")
			w.Header().Set("X-Localzie-Complete", strconv.FormatBool(done))
			w.WriteHeader(status)
			wroteHeader = true
		}
		var data []byte
		if offset < len(e.data) {
			data = append([]byte(nil), e.data[offset:]...)
		}
		e.mu.Unlock()
		if len(data) > 0 {
			if _, err = w.Write(data); err != nil {
				return
			}
			offset += len(data)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		if done {
			return
		}
		select {
		case <-changed:
		case <-r.Context().Done():
			return
		case <-root.Done():
			return
		}
	}
}
func (j *requestJournal) status(w http.ResponseWriter, r *http.Request, device string) {
	id := r.PathValue("request")
	if !safeID.MatchString(id) {
		http.NotFound(w, r)
		return
	}
	j.mu.Lock()
	e := j.get(requestKey(device, id))
	j.mu.Unlock()
	if e == nil {
		http.NotFound(w, r)
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Done      bool `json:"done"`
		Size      int  `json:"size"`
		Uncertain bool `json:"uncertain"`
	}{e.Done, len(e.data), e.Uncertain})
}

func copyDeviceContext(root, request context.Context, device string) context.Context {
	return context.WithValue(context.WithValue(agentbridge.WithDevice(root, device), inferenceDeviceKey{}, device), acceptedRequestKey{}, true)
}
