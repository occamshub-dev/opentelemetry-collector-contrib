// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package httplogsreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/unmarshaler/cwlog"
)

const defaultLogsEncoding = cwlog.TypeStr

// logsConsumer implements the httpLogsConsumer
// to use a logs consumer and unmarshaler.
type logsConsumer struct {
	config   *Config
	settings receiver.Settings

	// consumer passes the translated logs on to the
	// next consumer.
	consumer consumer.Logs
	// unmarshaler is the configured plog.Unmarshaler
	// to use when processing the records.
	unmarshaler plog.Unmarshaler
	// tracker holds the per-file delivery checkpoints of this receiver
	// instance; it is nil when file tracking is disabled.
	tracker   *fileTracker
	telemetry *metadata.TelemetryBuilder
	newest    atomic.Int64
}

var _ httpLogsConsumer = (*logsConsumer)(nil)

// newLogsReceiver creates a new instance of the receiver
// with a logsConsumer.
func newLogsReceiver(
	config *Config,
	set receiver.Settings,
	nextConsumer consumer.Logs,
) (receiver.Logs, error) {
	c := &logsConsumer{
		config:   config,
		settings: set,
		consumer: nextConsumer,
	}
	return newHttpLogsReceiver(set, config, c)
}

// Start sets the consumer's log unmarshaler to either a built-in
// unmarshaler or one loaded from an encoding extension, and initializes the
// file tracker (no-op when file tracking is disabled).
func (c *logsConsumer) Start(ctx context.Context, host component.Host) error {
	tracker, err := newFileTracker(ctx, c.config, c.settings, host)
	if err != nil {
		return fmt.Errorf("failed to initialize file tracking: %w", err)
	}
	c.tracker = tracker

	encoding := c.config.Encoding
	if encoding == "" {
		encoding = c.config.RecordType
		if encoding == "" {
			encoding = defaultLogsEncoding
		}
	}
	if encoding == cwlog.TypeStr {
		c.settings.Logger.Warn(
			"The built-in \"cwlogs\" encoding is deprecated and will be removed in a future version. " +
				"Use the \"aws_logs_encoding\" encoding extension with format \"cloudwatch\" instead.",
		)
		c.unmarshaler = cwlog.NewUnmarshaler(c.settings.Logger, c.settings.BuildInfo)
	} else {
		unmarshaler, err := loadEncodingExtension[plog.Unmarshaler](host, encoding, "logs")
		if err != nil {
			return fmt.Errorf("failed to load encoding extension: %w", err)
		}
		c.unmarshaler = unmarshaler
	}
	return nil
}

// Consume deserializes the (decompressed) request body and sends the
// resulting plog.Logs to the next consumer.
//
// If the configured unmarshaler provides a streaming decoder, batches are
// decoded and forwarded as they are produced from the body, so the receiver
// never holds more than one decoder batch in memory. Otherwise (and when the
// unmarshaler does not implement streaming), the whole body is unmarshalled
// at once.
func (c *logsConsumer) Consume(ctx context.Context, body io.Reader, commonAttributes map[string]string) (int, error) {
	body, session, statusCode, err := c.tracker.establish(ctx, body, commonAttributes)
	if err != nil {
		return statusCode, err
	}
	if session != nil {
		session.telemetry = c.telemetry
	}
	if factory, ok := c.unmarshaler.(encoding.LogsDecoderFactory); ok {
		decoder, err := factory.NewLogsDecoder(body, decoderOptions(c.config)...)
		if err == nil {
			return c.consumeDecodedLogs(ctx, decoder, session, commonAttributes)
		}
		// The extension accepted this reader but does not stream the given
		// format (or its stream could not be started). Buffer the body and
		// unmarshal it in one piece instead.
		c.settings.Logger.Warn(
			"Streaming decode not available for encoding; falling back to whole-body unmarshalling.",
			zap.Error(err),
		)
	}
	return c.consumeWholeBody(ctx, body, session, commonAttributes)
}

// consumeDecodedLogs forwards each batch produced by a streaming decoder to
// the next consumer as it is decoded. When the request carries a file
// tracking session, each batch is anchored at the decoder's offset, stamped
// with file.line (when the 1:1 guard holds), and its checkpoint is saved
// only after the batch was forwarded successfully.
func (c *logsConsumer) consumeDecodedLogs(
	ctx context.Context,
	decoder encoding.LogsDecoder,
	session *fileSession,
	commonAttributes map[string]string,
) (int, error) {
	for {
		logs, err := decoder.DecodeLogs()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				c.recordUnparseable()
				if session.stale(err) {
					return session.handleStale(ctx)
				}
				return http.StatusBadRequest, fmt.Errorf("decoding logs from the request body: %w", err)
			}
			// Some decoders deliver the last partial batch together with
			// io.EOF, so it must still be forwarded.
			if logs.LogRecordCount() > 0 {
				if statusCode, ferr := c.forwardTrackedLogs(ctx, decoder.Offset(), logs, session, commonAttributes); ferr != nil {
					return statusCode, ferr
				}
			}
			break
		}
		if logs.LogRecordCount() > 0 {
			if statusCode, ferr := c.forwardTrackedLogs(ctx, decoder.Offset(), logs, session, commonAttributes); ferr != nil {
				return statusCode, ferr
			}
		}
	}
	if session != nil {
		session.delivered(ctx)
	}
	return http.StatusOK, nil
}

// consumeWholeBody reads the entire request body and unmarshalls it in one
// piece with the configured unmarshaler.
func (c *logsConsumer) consumeWholeBody(
	ctx context.Context,
	body io.Reader,
	session *fileSession,
	commonAttributes map[string]string,
) (int, error) {
	raw, err := io.ReadAll(body)
	if err != nil {
		if session.stale(err) {
			return session.handleStale(ctx)
		}
		return http.StatusBadRequest, err
	}
	if session != nil && len(raw) == 0 {
		// The delivery carries nothing beyond the file's checkpoint: the
		// tracking reader skipped every line, so there is nothing to decode.
		// Short-circuit instead of running an empty body through the
		// unmarshaler, which several formats reject outright.
		session.delivered(ctx)
		return http.StatusOK, nil
	}
	logs, err := c.unmarshaler.UnmarshalLogs(raw)
	if err != nil {
		c.recordUnparseable()
		return http.StatusBadRequest, err
	}
	c.recordLinesRead(session, logs.LogRecordCount())
	var checkpoint int64
	if session != nil {
		stampFrom, cp, stampable := session.prepareWholeBody(logs.LogRecordCount())
		if stampable {
			stampFileLine(logs, stampFrom)
		} else {
			session.tracker.countLineAttrSkipped(ctx)
		}
		checkpoint = cp
	}
	if statusCode, err := c.forwardLogs(ctx, logs, commonAttributes); err != nil {
		return statusCode, err
	}
	if session != nil {
		session.forwarded = true
		if sc, serr := session.saveCheckpoint(ctx, checkpoint); serr != nil {
			return sc, serr
		}
		session.delivered(ctx)
	}
	return http.StatusOK, nil
}

// forwardTrackedLogs anchors a decoded batch at the decoder's byte offset,
// stamps file.line when the 1:1 line-to-record guard holds, forwards the
// batch, and only then records the session checkpoint — a failed forward
// must never advance the shared checkpoint.
func (c *logsConsumer) forwardTrackedLogs(
	ctx context.Context,
	decoderOffset int64,
	logs plog.Logs,
	session *fileSession,
	commonAttributes map[string]string,
) (int, error) {
	var checkpoint int64
	if session != nil {
		stampFrom, cp, stampable := session.markBatch(decoderOffset, logs.LogRecordCount())
		if stampable {
			stampFileLine(logs, stampFrom)
		} else {
			session.tracker.countLineAttrSkipped(ctx)
		}
		checkpoint = cp
	}
	c.recordLinesRead(session, logs.LogRecordCount())
	if statusCode, err := c.forwardLogs(ctx, logs, commonAttributes); err != nil {
		return statusCode, err
	}
	if session != nil {
		session.forwarded = true
		return session.saveCheckpoint(ctx, checkpoint)
	}
	return http.StatusOK, nil
}

// forwardLogs attaches common attributes (if any) to each resource of a
// decoded batch and sends it to the next consumer.
func (c *logsConsumer) forwardLogs(
	ctx context.Context,
	logs plog.Logs,
	commonAttributes map[string]string,
) (int, error) {
	var newest pcommon.Timestamp
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		rm := logs.ResourceLogs().At(i)
		for j := 0; j < rm.ScopeLogs().Len(); j++ {
			scopeLogs := rm.ScopeLogs().At(j)
			for k := 0; k < scopeLogs.LogRecords().Len(); k++ {
				timestamp := scopeLogs.LogRecords().At(k).Timestamp()
				if timestamp > newest {
					newest = timestamp
				}
			}
		}
		if commonAttributes != nil {
			for k, v := range commonAttributes {
				if _, found := rm.Resource().Attributes().Get(k); !found {
					rm.Resource().Attributes().PutStr(k, v)
				}
			}
		}
	}
	records := int64(logs.LogRecordCount())
	if err := c.consumer.ConsumeLogs(ctx, logs); err != nil {
		if consumererror.IsPermanent(err) {
			c.recordExport(records, "failed")
			return http.StatusBadRequest, err
		}
		c.recordExport(records, "retried")
		return http.StatusServiceUnavailable, err
	}
	c.recordExport(records, "sent")
	c.recordFreshness(newest)
	return http.StatusOK, nil
}

func (c *logsConsumer) setTelemetry(telemetry *metadata.TelemetryBuilder) {
	c.telemetry = telemetry
}

func (c *logsConsumer) recordLinesRead(session *fileSession, records int) {
	if c.telemetry == nil || records == 0 {
		return
	}
	count := int64(records)
	if session != nil && session.lastBatchLineCount > 0 {
		count = session.lastBatchLineCount
	}
	c.telemetry.HttplogsReceiverLogsLinesReadTotal.Add(context.Background(), count)
}

func (c *logsConsumer) recordUnparseable() {
	if c.telemetry != nil {
		c.telemetry.HttplogsReceiverLogsLinesUnparseableTotal.Add(context.Background(), 1)
	}
}

func (c *logsConsumer) recordExport(records int64, result string) {
	if c.telemetry == nil || records == 0 {
		return
	}
	c.telemetry.HttplogsReceiverLogsExportsTotal.Add(context.Background(), records, metric.WithAttributes(
		attribute.String("log_export_result", result),
	))
}

func (c *logsConsumer) recordFreshness(newest pcommon.Timestamp) {
	if c.telemetry == nil {
		return
	}
	if newest == 0 {
		return
	}
	newestNanos := int64(newest)
	for {
		previous := c.newest.Load()
		if newestNanos <= previous {
			return
		}
		if c.newest.CompareAndSwap(previous, newestNanos) {
			break
		}
	}
	age := time.Since(time.Unix(0, newestNanos)).Seconds()
	if age < 0 {
		age = 0
	}
	c.telemetry.HttplogsReceiverLogsFreshness.Record(context.Background(), age)
}

// Shutdown releases the file tracking resources (safe when file tracking is
// disabled).
func (c *logsConsumer) Shutdown(ctx context.Context) error {
	c.tracker.Shutdown(ctx)
	return nil
}
