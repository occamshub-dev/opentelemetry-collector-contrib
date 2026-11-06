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
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awscloudwatchmetricstreamsencodingextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/unmarshaler/cwmetricstream"
)

const defaultMetricsEncoding = cwmetricstream.TypeStr

// The metricsConsumer implements the httpLogsConsumer
// to use a metrics consumer and unmarshaler.
type metricsConsumer struct {
	config   *Config
	settings receiver.Settings
	// consumer passes the translated metrics on to the
	// next consumer.
	consumer consumer.Metrics
	// unmarshaler is the configured pmetric.Unmarshaler
	// to use when processing the records.
	unmarshaler pmetric.Unmarshaler
	// tracker holds the per-file delivery checkpoints of this receiver
	// instance; it is nil when file tracking is disabled.
	tracker *fileTracker
}

var _ httpLogsConsumer = (*metricsConsumer)(nil)

// newMetricsReceiver creates a new instance of the receiver
// with a metricsConsumer.
func newMetricsReceiver(
	config *Config,
	set receiver.Settings,
	nextConsumer consumer.Metrics,
) (receiver.Metrics, error) {
	c := &metricsConsumer{
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

func (c *metricsConsumer) Start(ctx context.Context, host component.Host) error {
	tracker, err := newFileTracker(ctx, c.config, c.settings, host)
	if err != nil {
		return fmt.Errorf("failed to initialize file tracking: %w", err)
	}
	c.tracker = tracker

	encoding := c.config.Encoding
	if encoding == "" {
		encoding = c.config.RecordType
		if encoding == "" {
			encoding = defaultMetricsEncoding
		}
	}
	switch encoding {
	case cwmetricstream.TypeStr:
		c.settings.Logger.Warn(
			"The built-in \"cwmetrics\" encoding is deprecated and will be removed in a future version. " +
				"Use the \"awscloudwatchmetricstreams_encoding\" encoding extension with format \"json\" instead.",
		)
		c.unmarshaler = cwmetricstream.NewUnmarshaler(c.settings.Logger, c.settings.BuildInfo)
	case "otlp_v1":
		c.settings.Logger.Warn(
			"The built-in \"otlp_v1\" encoding is deprecated and will be removed in a future version. " +
				"Use the \"awscloudwatchmetricstreams_encoding\" encoding extension with format \"opentelemetry1.0\" instead.",
		)
		unmarshaler, err := c.newUnmarshalerFromEncoding(ctx, encoding, "opentelemetry1.0")
		if err != nil {
			return err
		}
		c.unmarshaler = unmarshaler
	default:
		unmarshaler, err := loadEncodingExtension[pmetric.Unmarshaler](host, encoding, "metrics")
		if err != nil {
			return fmt.Errorf("failed to load encoding extension: %w", err)
		}
		c.unmarshaler = unmarshaler
	}
	return nil
}

// newUnmarshalerFromEncoding creates a new unmarshaler from
// aws cloudwatch metric streams encoding extension.
func (c *metricsConsumer) newUnmarshalerFromEncoding(
	ctx context.Context,
	encoding string,
	format string,
) (pmetric.Unmarshaler, error) {
	f := awscloudwatchmetricstreamsencodingextension.NewFactory()
	ext, err := f.Create(ctx, extension.Settings{
		ID:                component.NewID(f.Type()),
		BuildInfo:         c.settings.BuildInfo,
		TelemetrySettings: c.settings.TelemetrySettings,
	}, &awscloudwatchmetricstreamsencodingextension.Config{
		Format: format,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create encoding extension for %q format: %w", encoding, err)
	}
	unmarshaler, ok := ext.(pmetric.Unmarshaler)
	if !ok {
		return nil, errors.New("unexpected: failed to cast aws cloudwatch metric streams encoding extension to unmarshaler")
	}
	return unmarshaler, nil
}

// Consume deserializes the (decompressed) request body and sends the
// resulting pmetric.Metrics to the next consumer.
//
// If the configured unmarshaler provides a streaming decoder, batches are
// decoded and forwarded as they are produced from the body, so the receiver
// never holds more than one decoder batch in memory. Otherwise (and when the
// unmarshaler does not implement streaming), the whole body is unmarshalled
// at once.
func (c *metricsConsumer) Consume(ctx context.Context, body io.Reader, commonAttributes map[string]string) (int, error) {
	body, session, statusCode, err := c.tracker.establish(ctx, body, commonAttributes)
	if err != nil {
		return statusCode, err
	}
	if factory, ok := c.unmarshaler.(encoding.MetricsDecoderFactory); ok {
		decoder, err := factory.NewMetricsDecoder(body, decoderOptions(c.config)...)
		if err == nil {
			return c.consumeDecodedMetrics(ctx, decoder, session, commonAttributes)
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

// consumeDecodedMetrics forwards each batch produced by a streaming decoder to
// the next consumer as it is decoded. When the request carries a file
// tracking session, each batch is anchored at the decoder's offset and its
// checkpoint is saved only after the batch was forwarded successfully.
func (c *metricsConsumer) consumeDecodedMetrics(
	ctx context.Context,
	decoder encoding.MetricsDecoder,
	session *fileSession,
	commonAttributes map[string]string,
) (int, error) {
	for {
		metrics, err := decoder.DecodeMetrics()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				if session.stale(err) {
					return session.handleStale(ctx)
				}
				return http.StatusBadRequest, fmt.Errorf("decoding metrics from the request body: %w", err)
			}
			// Some decoders deliver the last partial batch together with
			// io.EOF, so it must still be forwarded.
			if metrics.MetricCount() > 0 {
				if statusCode, ferr := c.forwardTrackedMetrics(ctx, decoder.Offset(), metrics, session, commonAttributes); ferr != nil {
					return statusCode, ferr
				}
			}
			break
		}
		if metrics.MetricCount() > 0 {
			if statusCode, ferr := c.forwardTrackedMetrics(ctx, decoder.Offset(), metrics, session, commonAttributes); ferr != nil {
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
func (c *metricsConsumer) consumeWholeBody(
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
	metrics, err := c.unmarshaler.UnmarshalMetrics(raw)
	if err != nil {
		return http.StatusBadRequest, err
	}
	var checkpoint int64
	if session != nil {
		_, checkpoint, _ = session.prepareWholeBody(metrics.MetricCount())
	}
	if statusCode, err := c.forwardMetrics(ctx, metrics, commonAttributes); err != nil {
		return statusCode, err
	}
	if session != nil {
		if sc, serr := session.saveCheckpoint(ctx, checkpoint); serr != nil {
			return sc, serr
		}
		session.delivered(ctx)
	}
	return http.StatusOK, nil
}

// forwardTrackedMetrics anchors a decoded batch at the decoder's byte offset,
// forwards the batch, and only then records the session checkpoint — a failed
// forward must never advance the shared checkpoint.
func (c *metricsConsumer) forwardTrackedMetrics(
	ctx context.Context,
	decoderOffset int64,
	metrics pmetric.Metrics,
	session *fileSession,
	commonAttributes map[string]string,
) (int, error) {
	var checkpoint int64
	if session != nil {
		_, checkpoint, _ = session.markBatch(decoderOffset, metrics.MetricCount())
	}
	if statusCode, err := c.forwardMetrics(ctx, metrics, commonAttributes); err != nil {
		return statusCode, err
	}
	if session != nil {
		return session.saveCheckpoint(ctx, checkpoint)
	}
	return http.StatusOK, nil
}

// Shutdown releases the file tracking resources (safe when file tracking is
// disabled).
func (c *metricsConsumer) Shutdown(ctx context.Context) error {
	c.tracker.Shutdown(ctx)
	return nil
}

// forwardMetrics attaches common attributes (if any) to each resource of a
// decoded batch and sends it to the next consumer.
func (c *metricsConsumer) forwardMetrics(
	ctx context.Context,
	metrics pmetric.Metrics,
	commonAttributes map[string]string,
) (int, error) {
	if commonAttributes != nil {
		for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
			rm := metrics.ResourceMetrics().At(i)
			for k, v := range commonAttributes {
				if _, found := rm.Resource().Attributes().Get(k); !found {
					rm.Resource().Attributes().PutStr(k, v)
				}
			}
		}
	}
	if err := c.consumer.ConsumeMetrics(ctx, metrics); err != nil {
		if consumererror.IsPermanent(err) {
			return http.StatusBadRequest, err
		}
		return http.StatusServiceUnavailable, err
	}
	return http.StatusOK, nil
}
