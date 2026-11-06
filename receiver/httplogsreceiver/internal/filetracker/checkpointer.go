// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package filetracker persists per-file delivery checkpoints for the
// httplogsreceiver. A checkpoint is the 1-based line number of the last line
// successfully handed to the next consumer; a resent file resumes from it.
package filetracker // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/filetracker"

import (
	"context"
	"encoding/json"
	"time"
)

// now is the clock used for the app-level sliding TTL; tests may override it.
var now = time.Now

// Checkpointer stores and retrieves per-file delivery checkpoints.
//
// Save must never let the stored value regress (max-wins): a concurrent or
// lagged replica writing an older position must not move the shared
// checkpoint backwards.
type Checkpointer interface {
	// Load returns the last recorded checkpoint (line count) for the file
	// identified by name, and whether an entry was found. A missing or
	// expired entry is reported as found == false.
	Load(ctx context.Context, name string) (lines int64, found bool, err error)
	// Save records a checkpoint for name. It is a no-op when the stored
	// value is already >= lines.
	Save(ctx context.Context, name string, lines int64) error
	// Delete removes the checkpoint entry for name (no-op if absent).
	Delete(ctx context.Context, name string) error
	// Close releases all resources held by the checkpointer.
	Close(ctx context.Context) error
}

// entry is the checkpoint value persisted by backends: the line count plus
// the last update timestamp, which lets backends without native key TTL
// honor the receiver's app-level sliding TTL.
type entry struct {
	Lines int64 `json:"lines"`
	// UpdatedMs is the time (Unix milliseconds) the entry was last updated.
	UpdatedMs int64 `json:"updated_ms"`
}

func encodeEntry(e entry) ([]byte, error) {
	return json.Marshal(e)
}

func decodeEntry(raw []byte) (entry, error) {
	var e entry
	if err := json.Unmarshal(raw, &e); err != nil {
		return entry{}, err
	}
	return e, nil
}

// expired reports whether e is older than ttl.
func (e entry) expired(ttl time.Duration, now time.Time) bool {
	if ttl <= 0 || e.UpdatedMs <= 0 {
		return false
	}
	return now.Sub(time.UnixMilli(e.UpdatedMs)) > ttl
}
