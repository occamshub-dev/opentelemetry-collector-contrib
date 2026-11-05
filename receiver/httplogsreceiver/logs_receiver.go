// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package httplogsreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
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
	return &httpLogsReceiver{
		settings: set,
		config:   config,
		consumer: c,
	}, nil
}

// Start sets the consumer's log unmarshaler to either a built-in
// unmarshaler or one loaded from an encoding extension.
func (c *logsConsumer) Start(_ context.Context, host component.Host) error {
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
	if factory, ok := c.unmarshaler.(encoding.LogsDecoderFactory); ok {
		decoder, err := factory.NewLogsDecoder(body, decoderOptions(c.config)...)
		if err == nil {
			return c.consumeDecodedLogs(ctx, decoder, commonAttributes)
		}
		// The extension accepted this reader but does not stream the given
		// format (or its stream could not be started). Buffer the body and
		// unmarshal it in one piece instead.
		c.settings.Logger.Warn(
			"Streaming decode not available for encoding; falling back to whole-body unmarshalling.",
			zap.Error(err),
		)
	}
	return c.consumeWholeBody(ctx, body, commonAttributes)
}

// consumeDecodedLogs forwards each batch produced by a streaming decoder to
// the next consumer as it is decoded.
func (c *logsConsumer) consumeDecodedLogs(
	ctx context.Context,
	decoder encoding.LogsDecoder,
	commonAttributes map[string]string,
) (int, error) {
	for {
		logs, err := decoder.DecodeLogs()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return http.StatusBadRequest, fmt.Errorf("decoding logs from the request body: %w", err)
			}
			// Some decoders deliver the last partial batch together with
			// io.EOF, so it must still be forwarded.
			if logs.LogRecordCount() > 0 {
				if statusCode, err := c.forwardLogs(ctx, logs, commonAttributes); err != nil {
					return statusCode, err
				}
			}
			break
		}
		if logs.LogRecordCount() > 0 {
			if statusCode, err := c.forwardLogs(ctx, logs, commonAttributes); err != nil {
				return statusCode, err
			}
		}
	}
	return http.StatusOK, nil
}

// consumeWholeBody reads the entire request body and unmarshalls it in one
// piece with the configured unmarshaler.
func (c *logsConsumer) consumeWholeBody(
	ctx context.Context,
	body io.Reader,
	commonAttributes map[string]string,
) (int, error) {
	raw, err := io.ReadAll(body)
	if err != nil {
		return http.StatusBadRequest, err
	}
	logs, err := c.unmarshaler.UnmarshalLogs(raw)
	if err != nil {
		return http.StatusBadRequest, err
	}
	return c.forwardLogs(ctx, logs, commonAttributes)
}

// forwardLogs attaches common attributes (if any) to each resource of a
// decoded batch and sends it to the next consumer.
func (c *logsConsumer) forwardLogs(
	ctx context.Context,
	logs plog.Logs,
	commonAttributes map[string]string,
) (int, error) {
	if commonAttributes != nil {
		for i := 0; i < logs.ResourceLogs().Len(); i++ {
			rm := logs.ResourceLogs().At(i)
			for k, v := range commonAttributes {
				if _, found := rm.Resource().Attributes().Get(k); !found {
					rm.Resource().Attributes().PutStr(k, v)
				}
			}
		}
	}
	if err := c.consumer.ConsumeLogs(ctx, logs); err != nil {
		if consumererror.IsPermanent(err) {
			return http.StatusBadRequest, err
		}
		return http.StatusServiceUnavailable, err
	}
	return http.StatusOK, nil
}
