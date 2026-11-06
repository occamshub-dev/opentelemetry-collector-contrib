// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package filetracker

import (
	"context"
	"sync"
	"time"
)

// MemCheckpointer is an in-process Checkpointer. Entries are kept in a map
// with an application-level sliding TTL and, optionally, a bound on the
// number of tracked files (least recently updated are evicted).
//
// It is only safe for single-replica deployments.
type MemCheckpointer struct {
	mu       sync.RWMutex
	entries  map[string]entry
	entryTTL time.Duration
	maxFiles int
}

// NewMemCheckpointer returns an in-memory Checkpointer.
//
// entryTTL <= 0 disables expiry. maxFiles <= 0 means unlimited.
func NewMemCheckpointer(entryTTL time.Duration, maxFiles int) *MemCheckpointer {
	return &MemCheckpointer{
		entries:  make(map[string]entry),
		entryTTL: entryTTL,
		maxFiles: maxFiles,
	}
}

func (m *MemCheckpointer) Load(_ context.Context, name string) (int64, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.entries[name]
	if !ok || e.expired(m.entryTTL, now()) {
		return 0, false, nil
	}
	return e.Lines, true, nil
}

func (m *MemCheckpointer) Save(_ context.Context, name string, lines int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[name]; ok && !e.expired(m.entryTTL, now()) && e.Lines >= lines {
		return nil
	}
	if len(m.entries) >= m.maxFiles && m.maxFiles > 0 {
		m.evictLocked()
	}
	m.entries[name] = entry{Lines: lines, UpdatedMs: now().UnixMilli()}
	return nil
}

// Len returns the number of entries currently stored (expired entries are
// purged lazily and may still be counted).
func (m *MemCheckpointer) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}

func (m *MemCheckpointer) Delete(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, name)
	return nil
}

func (m *MemCheckpointer) Close(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = make(map[string]entry)
	return nil
}

// evictLocked drops the least recently updated entry. Callers must hold m.mu
// for writing.
func (m *MemCheckpointer) evictLocked() {
	if m.maxFiles <= 0 || len(m.entries) < m.maxFiles {
		return
	}
	var oldest string
	var oldestTime time.Time
	for name, e := range m.entries {
		t := time.UnixMilli(e.UpdatedMs)
		if oldest == "" || t.Before(oldestTime) {
			oldest, oldestTime = name, t
		}
	}
	delete(m.entries, oldest)
}
