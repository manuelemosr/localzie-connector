// Copyright 2026 Localzie contributors
// SPDX-License-Identifier: Apache-2.0

package ziticonn

import (
	"context"
	"errors"
	"sync"
	"time"
)

const inferenceLifetime = 30 * time.Minute

var errInferenceStopped = errors.New("this inference operation was explicitly stopped")
var errInferenceCapacity = errors.New("too many inference operations")

type inferenceDeviceKey struct{}
type inferenceKey struct{ device, endpoint, operation string }

// In-memory cancellation handles contain no prompts or transcripts. A stopped
// operation stays tombstoned for the job limit to cover Stop arriving before
// the first request or between planner/narration calls in the same phone turn.
type inferenceJobs struct {
	mu      sync.Mutex
	next    uint64
	active  map[inferenceKey]map[uint64]context.CancelFunc
	stopped map[inferenceKey]time.Time
}

func newInferenceJobs() *inferenceJobs {
	return &inferenceJobs{active: map[inferenceKey]map[uint64]context.CancelFunc{}, stopped: map[inferenceKey]time.Time{}}
}
func (j *inferenceJobs) pruneLocked() {
	now := time.Now()
	for key, until := range j.stopped {
		if !now.Before(until) {
			delete(j.stopped, key)
		}
	}
}
func (j *inferenceJobs) begin(root context.Context, key inferenceKey) (context.Context, func(), error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.pruneLocked()
	if _, ok := j.stopped[key]; ok {
		return nil, nil, errInferenceStopped
	}
	if len(j.active) >= 64 && j.active[key] == nil {
		return nil, nil, errInferenceCapacity
	}
	ctx, cancel := context.WithTimeout(root, inferenceLifetime)
	j.next++
	id := j.next
	if j.active[key] == nil {
		j.active[key] = map[uint64]context.CancelFunc{}
	}
	j.active[key][id] = cancel
	return ctx, func() {
		cancel()
		j.mu.Lock()
		defer j.mu.Unlock()
		delete(j.active[key], id)
		if len(j.active[key]) == 0 {
			delete(j.active, key)
		}
	}, nil
}
func (j *inferenceJobs) stop(key inferenceKey) error {
	j.mu.Lock()
	j.pruneLocked()
	if _, ok := j.stopped[key]; !ok && len(j.stopped) >= 1024 {
		j.mu.Unlock()
		return errInferenceCapacity
	}
	j.stopped[key] = time.Now().Add(inferenceLifetime)
	cancels := []context.CancelFunc{}
	for _, cancel := range j.active[key] {
		cancels = append(cancels, cancel)
	}
	j.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	return nil
}
