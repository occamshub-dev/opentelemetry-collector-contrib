// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package httplogsreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.opentelemetry.io/collector/component"
	storage "go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/filetracker"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/track"
)

// fileLineKey is the record attribute stamped on each log record carrying
// its 1-based line number within the delivered file.
const fileLineKey = "file.line"

// fileTracker owns the shared checkpoint backend of a receiver instance and
// establishes a per-request file session from the request's common
// attributes. It is nil when file tracking is disabled, in which case every
// request is processed exactly as before.
type fileTracker struct {
	cfg          FileTrackingConfig
	checkpointer filetracker.Checkpointer
	backend      string
	telemetry    *metadata.TelemetryBuilder
	logger       *zap.Logger
}

// newFileTracker builds the file tracker for a receiver instance, or nil
// when file tracking is disabled. With a configured storage extension the
// checkpoints are shared across all replicas through that extension;
// otherwise an in-process checkpointer is used.
func newFileTracker(ctx context.Context, config *Config, set receiver.Settings, host component.Host) (*fileTracker, error) {
	ft := &fileTracker{
		cfg:    config.FileTracking,
		logger: set.Logger,
	}
	if !config.FileTracking.Enabled {
		return nil, nil
	}
	telemetry, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("failed to create file tracking telemetry: %w", err)
	}
	ft.telemetry = telemetry

	if config.StorageID != nil {
		extension, ok := host.GetExtensions()[*config.StorageID]
		if !ok {
			return nil, fmt.Errorf("storage extension %q not found", config.StorageID)
		}
		storageExtension, ok := extension.(storage.Extension)
		if !ok {
			return nil, fmt.Errorf("extension %q is not a storage extension", config.StorageID)
		}
		// Make storage immune to component renames that add underscores to the component type.
		// This is a workaround for https://github.com/open-telemetry/opentelemetry-collector/issues/14988.
		normalizedComponentType := strings.ReplaceAll(set.ID.Type().String(), "_", "")
		normalizedComponentID := component.MustNewIDWithName(normalizedComponentType, set.ID.Name())
		client, err := storageExtension.GetClient(ctx, component.KindReceiver, normalizedComponentID, "")
		if err != nil {
			return nil, fmt.Errorf("failed to get storage client: %w", err)
		}
		ft.checkpointer = filetracker.NewStorageCheckpointer(client, config.FileTracking.EntryTTL)
		ft.backend = "storage"
		return ft, nil
	}

	ft.checkpointer = filetracker.NewMemCheckpointer(config.FileTracking.EntryTTL, config.FileTracking.MaxTrackedFiles)
	ft.backend = "inmem"
	ft.logger.Warn(
		"File tracking is running in single-replica in-memory mode; "+
			"configure a 'storage' extension for horizontally scaled deployments, otherwise checkpoints are not shared across replicas",
		zap.String("receiver", set.ID.String()),
	)
	ft.observeTrackedFiles(ctx)
	return ft, nil
}

// Shutdown releases the tracker's resources. It is safe to call on a nil
// tracker (file tracking disabled).
func (ft *fileTracker) Shutdown(ctx context.Context) {
	if ft == nil {
		return
	}
	if ft.checkpointer != nil {
		_ = ft.checkpointer.Close(ctx)
	}
	if ft.telemetry != nil {
		ft.telemetry.Shutdown()
	}
}

// fileSession is the per-request tracking state for one delivered file.
//
// The tracking math follows the decoder: for every decoded batch, the number
// of new lines is anchored at the decoder's consumed-byte offset (see
// track.Reader), so the count can never run ahead of the data actually
// decoded. A checkpoint is saved only after the batch has been forwarded
// successfully.
type fileSession struct {
	tracker *fileTracker
	name    string
	// base is the number of lines already exported for this file; a fresh
	// file has base 0. Record lines are base+1, base+2, …; checkpoints are
	// base + lines decoded so far.
	base int64
	// reader wraps the decompressed body: it skips the base already-exported
	// lines, then records the position of every newline it passes through.
	reader *track.Reader

	linesSoFar    int64 // lines of the post-skip stream consumed by batches processed so far
	anchorLost    bool  // the decoder offset cannot be anchored to the stream; see markBatch
	anchorWarned  bool
	skipAccounted bool // the skipped_lines_total metric has been accounted for this session
}

// establish resolves the file name of the delivery from the request's common
// attributes and loads its checkpoint. It returns the (possibly wrapped)
// request body to pass to the consumer, and a nil session when file tracking
// is disabled for this request (tracking off, or no file name present).
//
// When loading the checkpoint fails, on_error "fail" returns the status code
// and error to respond to the sender; on_error "degrade" processes the file
// untracked (at-least-once preserved).
func (ft *fileTracker) establish(ctx context.Context, body io.Reader, commonAttributes map[string]string) (io.Reader, *fileSession, int, error) {
	if ft == nil {
		return body, nil, 0, nil
	}
	var name string
	if commonAttributes != nil {
		name = commonAttributes[ft.cfg.FileAttribute]
	}
	if name == "" {
		ft.logger.Debug(
			"File tracking is enabled, but the request does not carry the file name attribute; processing without tracking",
			zap.String("attribute", ft.cfg.FileAttribute),
		)
		return body, nil, 0, nil
	}

	base, found, err := ft.checkpointer.Load(ctx, name)
	if err != nil {
		ft.countStoreError(ctx, "load")
		ft.logger.Warn(
			"Failed to load the file tracking checkpoint; behavior depends on on_error",
			zap.String("file", name),
			zap.String("on_error", ft.cfg.OnError),
			zap.Error(err),
		)
		if ft.cfg.OnError == onErrorPolicyFail {
			return body, nil, http.StatusServiceUnavailable,
				fmt.Errorf("the file tracking checkpoint store is unavailable (on_error=%q): %w", ft.cfg.OnError, err)
		}
		return body, nil, 0, nil
	}

	session := &fileSession{tracker: ft, name: name, base: base, reader: track.NewReader(body, base)}
	if found {
		ft.logger.Debug(
			"Resuming tracked file from its last exported line",
			zap.String("file", name),
			zap.Int64("checkpoint", base),
			zap.String("backend", ft.backend),
		)
	}
	return session.reader, session, 0, nil
}

// markBatch anchors the decoder at its current byte offset after a batch is
// decoded, and reports the bookkeeping for that batch:
//
//   - stampFrom is the file.line value of the batch's first record (valid
//     when stampable);
//   - checkpoint is the checkpoint to save once the batch has been forwarded
//     successfully;
//   - stampable is true when the 1:1 line-to-record correspondence is
//     verified for the batch (records == new lines).
//
// stampable is false both when the 1:1 guard fails and when the decoder
// offset cannot be anchored to the stream (non-byte-based or non-advancing
// offsets); in the latter case checkpoint bookkeeping is suspended for the
// rest of this request.
func (s *fileSession) markBatch(offset int64, records int) (stampFrom, checkpoint int64, stampable bool) {
	before := s.linesSoFar
	lines, anchored := s.reader.LineCount(offset)
	if !anchored {
		s.anchorLost = true
		if !s.anchorWarned {
			s.anchorWarned = true
			s.tracker.logger.Warn(
				"The decoder offset cannot be anchored to the decompressed stream "+
					"(it is not byte-based or it is not advancing monotonically); "+
					"file checkpointing and file.line stamping are disabled for this request",
				zap.String("file", s.name),
			)
		}
		return 0, 0, false
	}
	s.linesSoFar = lines
	stampable = int64(records) == lines-before
	if !stampable {
		s.tracker.logger.Debug(
			"Batch does not match the 1:1 line-to-record guard; forwarding without the file.line attribute",
			zap.String("file", s.name),
			zap.Int("records", records),
			zap.Int64("lines", lines-before),
		)
	}
	return s.base + before + 1, s.base + lines, stampable
}

// prepareWholeBody anchors at the end of the stream for a whole-body decode
// (one batch of the entire file) and reports the stamping bookkeeping. The
// return values and semantics mirror markBatch.
func (s *fileSession) prepareWholeBody(records int) (stampFrom, checkpoint int64, stampable bool) {
	total, anchored := s.reader.LineCount(s.reader.Seen())
	if !anchored {
		s.anchorLost = true
		if !s.anchorWarned {
			s.anchorWarned = true
			s.tracker.logger.Warn(
				"The whole-body path could not anchor the decompressed stream "+
					"for file tracking; checkpointing and file.line stamping are disabled for this request",
				zap.String("file", s.name),
			)
		}
		return 0, 0, false
	}
	s.linesSoFar = total
	stampable = int64(records) == total
	return s.base + 1, s.base + total, stampable
}

// saveCheckpoint persists the checkpoint after the batch it covers has been
// forwarded successfully. With on_error "fail" it reports the error through
// statusCode http.StatusServiceUnavailable (the sender retries once the store
// is back); with on_error "degrade" it logs and carries on.
//
// When the decoder offset could not be anchored (see markBatch), the
// checkpoint is not written: it would not reflect an exported prefix of the
// file.
func (s *fileSession) saveCheckpoint(ctx context.Context, checkpoint int64) (int, error) {
	if s.anchorLost {
		return http.StatusOK, nil
	}
	if err := s.tracker.checkpointer.Save(ctx, s.name, checkpoint); err != nil {
		s.tracker.countStoreError(ctx, "save")
		s.tracker.logger.Warn(
			"Failed to save the file tracking checkpoint; behavior depends on on_error",
			zap.String("file", s.name),
			zap.Int64("checkpoint", checkpoint),
			zap.String("on_error", s.tracker.cfg.OnError),
			zap.Error(err),
		)
		if s.tracker.cfg.OnError == onErrorPolicyFail {
			return http.StatusServiceUnavailable,
				fmt.Errorf("failed to save the file tracking checkpoint (on_error=%q): %w", s.tracker.cfg.OnError, err)
		}
		return 0, nil
	}
	s.tracker.observeTrackedFiles(ctx)
	return 0, nil
}

// handleStale applies the stale_checkpoint policy: the delivered file ends
// before the recorded checkpoint (it was shrunk, truncated, or a different
// file reusing the name). "reject" answers 400; "reset" clears the entry and
// answers 503 so the sender's retry is processed as a fresh delivery from
// line 1.
func (s *fileSession) handleStale(ctx context.Context) (int, error) {
	ft := s.tracker
	ft.telemetry.HttplogsReceiverFiletrackingResumedFilesTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("result", "stale"),
	))
	switch ft.cfg.StaleCheckpoint {
	case staleCheckpointReset:
		ft.logger.Warn(
			"Delivered file is shorter than its tracked checkpoint; clearing the entry, asking the sender to resend",
			zap.String("file", s.name),
			zap.Int64("checkpoint", s.base),
		)
		if err := ft.checkpointer.Delete(ctx, s.name); err != nil {
			ft.logger.Error("Failed to clear the stale checkpoint entry", zap.String("file", s.name), zap.Error(err))
		} else {
			ft.observeTrackedFiles(ctx)
		}
		return http.StatusServiceUnavailable, fmt.Errorf(
			"file %q is shorter than its tracked checkpoint (%d lines); the checkpoint entry was cleared, resend the full file", s.name, s.base,
		)
	default:
		ttl := ft.cfg.EntryTTL
		if ttl <= 0 {
			return http.StatusBadRequest, fmt.Errorf(
				"file %q is shorter than its tracked checkpoint (%d lines); deliver the full file or clear the tracking entry manually", s.name, s.base,
			)
		}
		return http.StatusBadRequest, fmt.Errorf(
			"file %q is shorter than its tracked checkpoint (%d lines); the tracking entry will expire after %v, deliver the full file or clear the entry manually",
			s.name, s.base, ttl,
		)
	}
}

// stale reports whether err (carried upstream from the stream or the decoder)
// is the stale-checkpoint condition of this session. A nil session reports
// false.
func (s *fileSession) stale(err error) bool {
	return s != nil && s.reader != nil && (s.reader.Stale() || errors.Is(err, track.ErrStaleCheckpoint))
}

// countSkipped records the number of already-exported lines skipped by this
// delivery. It is called once the skip has proven itself (the first batch
// decoded after the skipped prefix, or EOF straight away), and at most once
// per session.
func (s *fileSession) countSkipped(ctx context.Context) {
	if s.base <= 0 || s.skipAccounted {
		return
	}
	s.skipAccounted = true
	s.tracker.telemetry.HttplogsReceiverFiletrackingSkippedLinesTotal.Add(ctx, s.base)
}

// markOutcome records the outcome of a fully successful delivery.
func (s *fileSession) markOutcome(ctx context.Context) {
	result := "fresh"
	if s.base > 0 {
		result = "resumed"
	}
	s.tracker.telemetry.HttplogsReceiverFiletrackingResumedFilesTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("result", result),
	))
}

// delivered is called on the 200 path once a tracked delivery has completed.
func (s *fileSession) delivered(ctx context.Context) {
	s.countSkipped(ctx)
	s.markOutcome(ctx)
}

// countStoreError records a failed checkpoint store operation.
func (ft *fileTracker) countStoreError(ctx context.Context, op string) {
	ft.telemetry.HttplogsReceiverFiletrackingErrorsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("op", op),
		attribute.String("backend", ft.backend),
		attribute.String("on_error", ft.cfg.OnError),
	))
}

// countLineAttrSkipped records a log batch forwarded without the file.line
// attribute because the 1:1 line-to-record guard did not hold.
func (ft *fileTracker) countLineAttrSkipped(ctx context.Context) {
	ft.telemetry.HttplogsReceiverFiletrackingLineAttrBatchesSkippedTotal.Add(ctx, 1)
}

// observeTrackedFiles refreshes the tracked_files gauge (in-memory backend
// only — it is the capacity signal against max_tracked_files).
func (ft *fileTracker) observeTrackedFiles(ctx context.Context) {
	if mem, ok := ft.checkpointer.(*filetracker.MemCheckpointer); ok && ft.telemetry != nil {
		ft.telemetry.HttplogsReceiverFiletrackingTrackedFiles.Record(ctx, int64(mem.Len()))
	}
}

// stampFileLine sets the file.line record attribute on every record of logs,
// assigning the 1-based line numbers first, first+1, … in stream order. It
// overrides any file.line the encoding extension may have set: the receiver
// is the component that observes the physical line order.
func stampFileLine(logs plog.Logs, first int64) {
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		rl := logs.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				sl.LogRecords().At(k).Attributes().PutInt(fileLineKey, first)
				first++
			}
		}
	}
}
