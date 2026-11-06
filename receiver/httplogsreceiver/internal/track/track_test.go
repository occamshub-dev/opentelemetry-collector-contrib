// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package track

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testBody is "line1\nline22\nline333\n": newline positions (0-based) are
// 5, 12, 20 and the total size is 21 bytes.
const testBody = "line1\nline22\nline333\n"

func TestPassthroughNoLinesSkipped(t *testing.T) {
	r := NewReader(strings.NewReader(testBody), 0)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, testBody, string(out))

	lines, ok := r.LineCount(21)
	require.True(t, ok)
	assert.EqualValues(t, 3, lines)
}

func TestLineCountPartialAnchors(t *testing.T) {
	r := NewReader(strings.NewReader(testBody), 0)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, testBody, string(out))

	// Anchors at various offsets: a line is counted once its terminating
	// newline (position 5, 12, 20) is at or before the anchor.
	for _, tc := range []struct {
		anchor int64
		want   int64
	}{
		{0, 0}, {5, 1}, {6, 1}, {11, 1}, {12, 2}, {19, 2}, {20, 3}, {21, 3},
	} {
		assert.EqualValues(t, tc.want, countAt(r, tc.anchor))
	}
}

func TestLineCountAnchorOutOfBounds(t *testing.T) {
	r := NewReader(strings.NewReader(testBody), 0)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, testBody, string(out))

	// Offset beyond the bytes passed through: cannot anchor.
	lines, ok := r.LineCount(22)
	assert.False(t, ok)
	assert.Zero(t, lines)
}

func TestLineCountBackwardsAnchor(t *testing.T) {
	r := NewReader(strings.NewReader(testBody), 0)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, testBody, string(out))

	_, ok := r.LineCount(12)
	require.True(t, ok)
	// A backwards anchor is not comparable with stream positions.
	lines, ok := r.LineCount(6)
	assert.False(t, ok)
	assert.Zero(t, lines)
}

func TestSkipLines(t *testing.T) {
	r := NewReader(strings.NewReader(testBody), 2)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "line333\n", string(out))

	// The post-skip stream is 8 bytes long ("line333\n"); before the
	// terminating newline is reached the line is not yet complete, and at
	// its position it is.
	lines, ok := r.LineCount(6)
	require.True(t, ok)
	assert.EqualValues(t, 0, lines)

	lines, ok = r.LineCount(7)
	require.True(t, ok)
	assert.EqualValues(t, 1, lines)
}

// TestSkipLinesAcrossBuffers forces doSkip to read the source in more than
// one chunk (chunks are skipBufSize = 64 KiB).
func TestSkipLinesAcrossBuffers(t *testing.T) {
	const total = 2000
	const skip = 1500
	lines := make([]string, total)
	for i := range lines {
		lines[i] = "line" + strings.Repeat("x", 400) + "\n"
	}
	src := strings.Join(lines, "")

	r := NewReader(strings.NewReader(src), skip)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, strings.Join(lines[skip:], ""), string(out))

	// 500 lines are left.
	n, ok := r.LineCount(int64(len(out)))
	require.True(t, ok)
	assert.EqualValues(t, total-skip, n)
}

func TestSkipNoTrailingNewlineCountedOnceTerminated(t *testing.T) {
	// A line without a trailing newline is not a "complete" line: it is
	// neither counted toward the checkpoint nor skippable.
	r := NewReader(strings.NewReader("a\nb\nc"), 2)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "c", string(out))

	lines, ok := r.LineCount(1)
	require.True(t, ok)
	assert.Zero(t, lines)
}

func TestStaleCheckpointSticky(t *testing.T) {
	r := NewReader(strings.NewReader("a\nb\nc"), 5)

	var out []byte
	buf := make([]byte, 16)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil {
			require.ErrorIs(t, err, ErrStaleCheckpoint)
			break
		}
		if n == 0 {
			t.Fatal("zero-size read without error")
		}
	}
	assert.Empty(t, out, "a stale reader must not pass any data through")
	assert.True(t, r.Stale())

	// Sticky: every subsequent read keeps reporting the stale condition.
	for i := 0; i < 3; i++ {
		n, err := r.Read(buf)
		assert.Zero(t, n)
		require.ErrorIs(t, err, ErrStaleCheckpoint)
	}
	assert.True(t, r.Stale())
}

func TestStaleCheckpointDetail(t *testing.T) {
	r := NewReader(bytes.NewReader([]byte("a\nb\nc")), 5)
	_, err := io.ReadAll(r)
	require.ErrorIs(t, err, ErrStaleCheckpoint)
	assert.Contains(t, err.Error(), "the file contains 2 complete lines")
}

func TestSkipZeroIsTransparent(t *testing.T) {
	r := NewReader(strings.NewReader(""), 0)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Empty(t, out)
	assert.False(t, r.Stale())
}

func TestNegativeSkipClamped(t *testing.T) {
	r := NewReader(strings.NewReader(testBody), -3)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, testBody, string(out))
}

func countAt(r *Reader, anchor int64) int64 {
	n, ok := r.LineCount(anchor)
	if !ok {
		return -1
	}
	return n
}

// zeroReader is a misbehaving reader: it always returns (0, nil).
type zeroReader struct{}

func (zeroReader) Read([]byte) (int, error) { return 0, nil }

func TestMisbehavingSourceReadZeroNil(t *testing.T) {
	r := NewReader(zeroReader{}, 1)
	_, err := io.ReadAll(r)
	assert.ErrorIs(t, err, ErrStaleCheckpoint)
	assert.True(t, r.Stale())
}
