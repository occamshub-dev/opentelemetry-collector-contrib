// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package filetracker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var baseTime = time.Date(2026, time.November, 5, 12, 0, 0, 0, time.UTC)

// withFakeClock pins the package clock to t0 and advances it by step for
// each call, simulating time passing between operations.
func withFakeClock(t *testing.T, t0 time.Time, step time.Duration) {
	t.Helper()
	orig := now
	var cur time.Time
	now = func() time.Time {
		out := cur
		if cur.IsZero() {
			cur = t0
			return t0
		}
		cur = cur.Add(step)
		return out
	}
	t.Cleanup(func() { now = orig })
}

func TestMemSaveLoadRoundTrip(t *testing.T) {
	m := NewMemCheckpointer(0, 0)
	ctx := context.Background()

	_, found, err := m.Load(ctx, "a.txt")
	require.NoError(t, err)
	assert.False(t, found)

	require.NoError(t, m.Save(ctx, "a.txt", 42))
	lines, found, err := m.Load(ctx, "a.txt")
	require.NoError(t, err)
	assert.True(t, found)
	assert.EqualValues(t, 42, lines)
}

func TestMemSaveMaxWins(t *testing.T) {
	m := NewMemCheckpointer(0, 0)
	ctx := context.Background()

	require.NoError(t, m.Save(ctx, "a.txt", 100))
	require.NoError(t, m.Save(ctx, "a.txt", 50)) // regression must be dropped

	lines, found, err := m.Load(ctx, "a.txt")
	require.NoError(t, err)
	assert.True(t, found)
	assert.EqualValues(t, 100, lines)

	// Equal and newer values are accepted; older ones are not.
	require.NoError(t, m.Save(ctx, "a.txt", 100))
	require.NoError(t, m.Save(ctx, "a.txt", 200))
	lines, _, _ = m.Load(ctx, "a.txt")
	assert.EqualValues(t, 200, lines)
}

func TestMemTTLExpiry(t *testing.T) {
	withFakeClock(t, baseTime, time.Minute)
	m := NewMemCheckpointer(time.Hour, 0)
	ctx := context.Background()

	require.NoError(t, m.Save(ctx, "a.txt", 10))

	// Within the TTL: visible.
	_, found, _ := m.Load(ctx, "a.txt")
	assert.True(t, found)

	// Past the TTL: treated as absent...
	for i := 0; i < 70; i++ {
		now()
	}
	_, found, _ = m.Load(ctx, "a.txt")
	assert.False(t, found)

	// ...and an expired checkpoint never blocks regression: after expiry the
	// older line count is accepted again (resumed from the beginning).
	_ = now()
	require.NoError(t, m.Save(ctx, "a.txt", 2))
	lines, found, _ := m.Load(ctx, "a.txt")
	assert.True(t, found)
	assert.EqualValues(t, 2, lines)
}

func TestMemTTLDisabledWhenNonPositive(t *testing.T) {
	m := NewMemCheckpointer(0, 0)
	ctx := context.Background()

	require.NoError(t, m.Save(ctx, "a.txt", 10))
	lines, found, _ := m.Load(ctx, "a.txt")
	assert.True(t, found)
	assert.EqualValues(t, 10, lines)
}

func TestMemEvictionLeastRecentlyUpdated(t *testing.T) {
	withFakeClock(t, baseTime, time.Second)
	m := NewMemCheckpointer(0, 2)
	ctx := context.Background()

	require.NoError(t, m.Save(ctx, "a.txt", 1))
	now()
	require.NoError(t, m.Save(ctx, "b.txt", 1))
	now()
	require.NoError(t, m.Save(ctx, "c.txt", 1)) // evicts a.txt
	now()
	// Touch b so that c is the oldest.
	require.NoError(t, m.Save(ctx, "b.txt", 2))
	now()
	require.NoError(t, m.Save(ctx, "d.txt", 1)) // evicts c.txt

	assert.Equal(t, 2, m.Len())

	_, aFound, _ := m.Load(ctx, "a.txt")
	assert.False(t, aFound)
	_, cFound, _ := m.Load(ctx, "c.txt")
	assert.False(t, cFound)
	bLines, bFound, _ := m.Load(ctx, "b.txt")
	assert.True(t, bFound)
	assert.EqualValues(t, 2, bLines)
	_, dFound, _ := m.Load(ctx, "d.txt")
	assert.True(t, dFound)
}

func TestMemSaveAfterEvictionReplacesOldest(t *testing.T) {
	withFakeClock(t, baseTime, time.Second)
	m := NewMemCheckpointer(0, 1)
	ctx := context.Background()

	require.NoError(t, m.Save(ctx, "a.txt", 1))
	require.NoError(t, m.Save(ctx, "b.txt", 2))

	_, aFound, _ := m.Load(ctx, "a.txt")
	assert.False(t, aFound)
	lines, bFound, _ := m.Load(ctx, "b.txt")
	assert.True(t, bFound)
	assert.EqualValues(t, 2, lines)
}

func TestMemDelete(t *testing.T) {
	m := NewMemCheckpointer(0, 0)
	ctx := context.Background()

	require.NoError(t, m.Save(ctx, "a.txt", 1))
	require.NoError(t, m.Delete(ctx, "a.txt"))

	_, found, _ := m.Load(ctx, "a.txt")
	assert.False(t, found)

	// Deleting an absent key is a no-op.
	assert.NoError(t, m.Delete(ctx, "nope"))
}

func TestMemCloseClears(t *testing.T) {
	m := NewMemCheckpointer(0, 0)
	ctx := context.Background()

	require.NoError(t, m.Save(ctx, "a.txt", 1))
	require.NoError(t, m.Close(ctx))

	_, found, _ := m.Load(ctx, "a.txt")
	assert.False(t, found)
	assert.Equal(t, 0, m.Len())
}
