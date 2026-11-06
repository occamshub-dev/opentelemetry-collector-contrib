// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package track provides the line-tracking reader used by the
// httplogsreceiver to resume deliveries of a file from its last
// successfully exported line.
package track // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/track"

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// skipBufSize is the scratch buffer used while discarding the skipped
// prefix of the stream.
const skipBufSize = 64 * 1024

// ErrStaleCheckpoint is returned by Reader.Read when the source ends before
// all of the lines requested for skipping have been consumed. It indicates
// that the recorded checkpoint exceeds the number of lines in the (resent)
// file.
var ErrStaleCheckpoint = errors.New("stale checkpoint: the file ends before all checkpointed lines could be skipped")

// Reader is an io.Reader that optionally discards the first skip lines of
// the source stream (during its first Read), then passes data through
// unchanged while recording the position of every newline byte of the
// post-skip stream.
//
// If the source ends before the skip target is reached, the reader becomes
// permanently stale: every subsequent Read keeps returning the
// ErrStaleCheckpoint error instead of passing data through, so callers can
// detect the condition (errors.Is, or Stale) at any point downstream.
//
// LineCount(offset) reports how many complete lines (newline-terminated) of
// the post-skip stream end at or before the given offset. Counting is
// anchored at the offset — the number of bytes the *consumer* of the stream
// (e.g. a streaming decoder) has consumed — not at the number of bytes this
// reader has happened to pass through. Decoders buffer ahead of what they
// hand out as decoded batches, so counting pass-through bytes would credit
// batches with lines that have not been decoded yet and could advance a
// checkpoint past not-yet-exported data. Anchoring at the consumed offset
// guarantees the line count never runs ahead of the data actually decoded.
//
// A new line is counted only once it is newline-terminated; a final line of
// the file without a trailing newline is therefore not counted (a resend
// may reprocess it — duplication, never loss).
type Reader struct {
	src      io.Reader
	skip     int64
	skipping bool
	stale    bool
	staleErr error     // the error returned by Read once the reader is stale
	rest     io.Reader // the stream to read from once skipping is done

	// Newline positions in the post-skip stream are kept in a rolling
	// window: positions at or below the last LineCount anchor are folded
	// into finalCount and dropped, so memory stays bounded by the
	// decoder's read-ahead (its buffer size), not by the file size.
	window      []int64
	windowStart int64 // LineCount anchor the window was truncated at
	finalCount  int64 // newline count finalized up to windowStart
	seen        int64 // total bytes passed through the reader
}

// NewReader returns a Reader over src that, on first use, discards the
// first skip complete lines of src and then passes the remainder through.
// With skip == 0 it is a transparent pass-through.
func NewReader(src io.Reader, skip int64) *Reader {
	if skip < 0 {
		skip = 0
	}
	return &Reader{
		src:      src,
		skip:     skip,
		skipping: skip > 0,
		rest:     src,
	}
}

// Read implements io.Reader.
func (r *Reader) Read(p []byte) (int, error) {
	if r.stale {
		return 0, r.staleErr
	}
	if r.skipping {
		if err := r.doSkip(); err != nil {
			return 0, err
		}
	}
	n, err := r.rest.Read(p)
	if n > 0 {
		pos := r.seen
		for i := 0; i < n; i++ {
			if p[i] == '\n' {
				r.window = append(r.window, pos+int64(i))
			}
		}
		r.seen += int64(n)
	}
	return n, err
}

// doSkip discards r.skip complete lines from the source stream. Any bytes
// after the skipped prefix that were already pulled from src are kept in
// the reader (r.rest) so nothing is lost.
func (r *Reader) doSkip() error {
	r.skipping = false
	remaining := r.skip
	buf := make([]byte, skipBufSize)
	for {
		n, rerr := r.src.Read(buf)
		last := -1
		for i := 0; i < n && remaining > 0; i++ {
			if buf[i] == '\n' {
				remaining--
				last = i
			}
		}
		if remaining == 0 {
			// Drop bytes through the last skipped newline, keep the rest.
			left := n - last - 1
			if left > 0 {
				residual := make([]byte, left)
				copy(residual, buf[last+1:])
				r.rest = io.MultiReader(bytes.NewReader(residual), r.src)
				return nil
			}
			r.rest = r.src
			return nil
		}
		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				return rerr
			}
			// EOF before all requested lines were skipped: the file is
			// shorter than the checkpoint expects. Report the condition on
			// the first Read rather than silently truncating the stream.
			r.setStale(fmt.Sprintf("the file contains %d complete lines, skipping required %d", r.skip-remaining, r.skip))
			return r.staleErr
		}
		if n == 0 {
			// Misbehaving reader (0, nil); treat as end of stream.
			r.setStale(fmt.Sprintf("the reader returned no data while %d lines were left to skip", remaining))
			return r.staleErr
		}
	}
}

// setStale marks the reader as permanently stale and records the error to
// report on subsequent reads.
func (r *Reader) setStale(detail string) {
	r.stale = true
	r.staleErr = fmt.Errorf("%w: %s", ErrStaleCheckpoint, detail)
}

// Stale reports whether the source ended before the requested lines could
// be skipped (see ErrStaleCheckpoint).
func (r *Reader) Stale() bool {
	return r.stale
}

// LineCount returns the number of complete lines of the post-skip stream
// whose terminating newline lies at or before offset, or false if the count
// cannot be anchored: offset exceeds the bytes this reader has passed
// through (the consumer's offset is not byte-based, e.g. a non-byte
// offset decoder), or is below the last anchor.
//
// Anchors must be monotonically non-decreasing, as is the case for
// streaming decoders as they consume the stream.
func (r *Reader) LineCount(offset int64) (int64, bool) {
	if offset > r.seen {
		return 0, false
	}
	if offset < r.windowStart {
		// Anchors only move forward; a backwards anchor means the
		// consumer's offset is not comparable to stream positions.
		return 0, false
	}
	i := 0
	for i < len(r.window) && r.window[i] <= offset {
		i++
	}
	r.finalCount += int64(i)
	if i < len(r.window) {
		r.window = r.window[i:]
	} else {
		r.window = nil
	}
	r.windowStart = offset
	return r.finalCount, true
}

// Seen returns the total number of bytes passed through the reader.
func (r *Reader) Seen() int64 {
	return r.seen
}
