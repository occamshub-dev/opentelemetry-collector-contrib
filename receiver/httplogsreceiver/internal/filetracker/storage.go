// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package filetracker

import (
	"context"
	"time"

	storage "go.opentelemetry.io/collector/extension/xextension/storage"
)

// storageCheckpointer implements Checkpointer on top of a
// storage.Client. Multiple replicas of the receiver sharing the same
// storage extension therefore share checkpoints.
//
// Save is best-effort max-wins: the current value is read and an older
// one never overwrites a newer one. Two racing stores both writing newer
// positions may interleave such that the last write wins; the contiguity
// of exported prefixes bounds that to duplication, never loss.
//
// The entry TTL (entry TTL in the receiver config) is honored here too,
// since pinned storage backends do not expose per-key TTL: an expired
// value is treated as absent.
type storageCheckpointer struct {
	client   storage.Client
	entryTTL time.Duration
}

// NewStorageCheckpointer wraps a storage.Client into a shared Checkpointer.
func NewStorageCheckpointer(client storage.Client, entryTTL time.Duration) Checkpointer {
	return &storageCheckpointer{client: client, entryTTL: entryTTL}
}

func (s *storageCheckpointer) Load(ctx context.Context, name string) (int64, bool, error) {
	raw, err := s.client.Get(ctx, name)
	if err != nil {
		return 0, false, err
	}
	if raw == nil {
		return 0, false, nil
	}
	e, err := decodeEntry(raw)
	if err != nil {
		// A corrupt entry is treated as absent; the receiver can reprocess
		// the file from line 1 (at-least-once).
		return 0, false, nil
	}
	if e.expired(s.entryTTL, now()) {
		return 0, false, nil
	}
	return e.Lines, true, nil
}

func (s *storageCheckpointer) Save(ctx context.Context, name string, lines int64) error {
	if cur, err := s.client.Get(ctx, name); err != nil {
		return err
	} else if cur != nil {
		if e, derr := decodeEntry(cur); derr == nil && !e.expired(s.entryTTL, now()) && e.Lines >= lines {
			return nil
		}
	}
	raw, err := encodeEntry(entry{Lines: lines, UpdatedMs: now().UnixMilli()})
	if err != nil {
		return err
	}
	return s.client.Set(ctx, name, raw)
}

func (s *storageCheckpointer) Delete(ctx context.Context, name string) error {
	return s.client.Delete(ctx, name)
}

func (s *storageCheckpointer) Close(ctx context.Context) error {
	return s.client.Close(ctx)
}
