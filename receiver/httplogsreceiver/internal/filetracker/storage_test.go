// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package filetracker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	storage "go.opentelemetry.io/collector/extension/xextension/storage"
)

// stubClient is an in-memory storage.Client with per-method error injection.
type stubClient struct {
	entries     map[string][]byte
	getErr      error
	setErr      error
	deleteErr   error
	closeErr    error
	getCalls    int
	setCalls    []string
	deleteCalls []string
	closed      bool
}

func newStubClient() *stubClient {
	return &stubClient{entries: make(map[string][]byte)}
}

func (s *stubClient) Get(ctx context.Context, key string) ([]byte, error) {
	s.getCalls++
	if s.getErr != nil {
		return nil, s.getErr
	}
	v, ok := s.entries[key]
	if !ok {
		return nil, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (s *stubClient) Set(ctx context.Context, key string, value []byte) error {
	if s.setErr != nil {
		return s.setErr
	}
	out := make([]byte, len(value))
	copy(out, value)
	s.entries[key] = out
	s.setCalls = append(s.setCalls, key)
	return nil
}

func (s *stubClient) Delete(ctx context.Context, key string) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	delete(s.entries, key)
	s.deleteCalls = append(s.deleteCalls, key)
	return nil
}

func (s *stubClient) Batch(ctx context.Context, ops ...*storage.Operation) error {
	for _, op := range ops {
		switch op.Type {
		case storage.Get:
			v, err := s.Get(ctx, op.Key)
			if err != nil {
				return err
			}
			op.Value = v
		case storage.Set:
			if err := s.Set(ctx, op.Key, op.Value); err != nil {
				return err
			}
		case storage.Delete:
			if err := s.Delete(ctx, op.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *stubClient) Close(ctx context.Context) error {
	s.closed = true
	return s.closeErr
}

func TestStorageLoadAbsent(t *testing.T) {
	cp := NewStorageCheckpointer(newStubClient(), time.Hour)
	ctx := context.Background()

	lines, found, err := cp.Load(ctx, "a.txt")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Zero(t, lines)
}

func TestStorageSaveLoadRoundTrip(t *testing.T) {
	stub := newStubClient()
	cp := NewStorageCheckpointer(stub, 0)
	ctx := context.Background()

	require.NoError(t, cp.Save(ctx, "a.txt", 7))
	// The stored value carries both the line count and a recent timestamp
	// (used for the app-level entry TTL).
	raw, ok := stub.entries["a.txt"]
	require.True(t, ok)
	lines, updatedMs, err := entryJSON(raw)
	require.NoError(t, err)
	assert.EqualValues(t, 7, lines)
	assert.Greater(t, updatedMs, int64(0))

	lines, found, err := cp.Load(ctx, "a.txt")
	require.NoError(t, err)
	assert.True(t, found)
	assert.EqualValues(t, 7, lines)
}

func TestStorageSaveMaxWins(t *testing.T) {
	stub := newStubClient()
	cp := NewStorageCheckpointer(stub, 0)
	ctx := context.Background()

	require.NoError(t, cp.Save(ctx, "a.txt", 50))
	// Older value must not overwrite.
	require.NoError(t, cp.Save(ctx, "a.txt", 40))
	lines, _, _ := cp.Load(ctx, "a.txt")
	assert.EqualValues(t, 50, lines)
	// Newer value overwrites.
	require.NoError(t, cp.Save(ctx, "a.txt", 60))
	lines, _, _ = cp.Load(ctx, "a.txt")
	assert.EqualValues(t, 60, lines)
}

func TestStorageSaveDoesNotRegressExpiredEntry(t *testing.T) {
	withFakeClock(t, baseTime, time.Minute)
	stub := newStubClient()
	cp := NewStorageCheckpointer(stub, time.Hour)
	ctx := context.Background()

	require.NoError(t, cp.Save(ctx, "a.txt", 50))
	for i := 0; i < 70; i++ {
		now()
	}
	// Past the TTL the stored value is no longer authoritative, so an older
	// value is stored again.
	require.NoError(t, cp.Save(ctx, "a.txt", 20))
	lines, found, err := cp.Load(ctx, "a.txt")
	require.NoError(t, err)
	assert.True(t, found)
	assert.EqualValues(t, 20, lines)
}

func TestStorageCorruptEntryTreatedAsAbsent(t *testing.T) {
	stub := newStubClient()
	stub.entries["a.txt"] = []byte("{not json")
	cp := NewStorageCheckpointer(stub, time.Hour)
	ctx := context.Background()

	lines, found, err := cp.Load(ctx, "a.txt")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Zero(t, lines)

	// A corrupt entry also does not block saving a fresh checkpoint.
	require.NoError(t, cp.Save(ctx, "a.txt", 3))
	lines, found, _ = cp.Load(ctx, "a.txt")
	assert.True(t, found)
	assert.EqualValues(t, 3, lines)
}

func TestStorageErrorPropagation(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*stubClient)
		run  func(ck Checkpointer) error
		want func(*stubClient) bool
	}{
		{
			name: "load",
			mut:  func(s *stubClient) { s.getErr = assert.AnError },
			run:  func(ck Checkpointer) error { _, _, err := ck.Load(context.Background(), "a.txt"); return err },
			want: func(s *stubClient) bool { return s.getCalls == 1 },
		},
		{
			name: "save",
			mut:  func(s *stubClient) { s.setErr = assert.AnError },
			run:  func(ck Checkpointer) error { return ck.Save(context.Background(), "a.txt", 1) },
			want: func(s *stubClient) bool { return len(s.setCalls) == 0 },
		},
		{
			name: "save with failing read",
			mut:  func(s *stubClient) { s.getErr = assert.AnError },
			run:  func(ck Checkpointer) error { return ck.Save(context.Background(), "a.txt", 1) },
			want: func(s *stubClient) bool { return len(s.setCalls) == 0 },
		},
		{
			name: "delete",
			mut:  func(s *stubClient) { s.deleteErr = assert.AnError },
			run:  func(ck Checkpointer) error { return ck.Delete(context.Background(), "a.txt") },
			want: func(s *stubClient) bool { return len(s.deleteCalls) == 0 },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newStubClient()
			tt.mut(stub)
			cp := NewStorageCheckpointer(stub, 0)

			require.ErrorIs(t, tt.run(cp), assert.AnError)
			assert.True(t, tt.want(stub))
		})
	}
}

func TestStorageDelete(t *testing.T) {
	stub := newStubClient()
	cp := NewStorageCheckpointer(stub, 0)
	ctx := context.Background()

	require.NoError(t, cp.Save(ctx, "a.txt", 1))
	require.NoError(t, cp.Delete(ctx, "a.txt"))

	_, found, _ := cp.Load(ctx, "a.txt")
	assert.False(t, found)
	assert.Equal(t, []string{"a.txt"}, stub.deleteCalls)
}

func TestStorageCloseDelegates(t *testing.T) {
	stub := newStubClient()
	cp := NewStorageCheckpointer(stub, 0)
	ctx := context.Background()

	require.NoError(t, cp.Close(ctx))
	assert.True(t, stub.closed)

	stub.closeErr = assert.AnError
	require.ErrorIs(t, cp.Close(ctx), assert.AnError)
}

// entryJSON is a helper to inspect the raw value stored by the backend.
func entryJSON(raw []byte) (int64, int64, error) {
	var e struct {
		Lines     int64 `json:"lines"`
		UpdatedMs int64 `json:"updated_ms"`
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		return 0, 0, err
	}
	return e.Lines, e.UpdatedMs, nil
}
