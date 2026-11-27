// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package httplogsreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/decode"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/metadata"
)

const (
	headerHttpRequestID        = "X-Request-Id"
	headerHttpAccessKey        = "X-Access-Key"
	headerHttpCommonAttributes = "X-Common-Attributes"
	headerHttpAppName          = "X-App-Name"
	headerContentType          = "Content-Type"
	headerContentLength        = "Content-Length"
	headerContentEncoding      = "Content-Encoding"
	headerRetryAfter           = "Retry-After"
)

var (
	errInvalidAccessKey           = errors.New("invalid access key")
	errUnsupportedContentEncoding = errors.New("unsupported content encoding")
	errInHeaderMissingRequestID   = errors.New("missing request id in header")
	errInBodyMissingRequestID     = errors.New("missing request id in body")
	errInBodyDiffRequestID        = errors.New("different request id in body")
	errConcurrencyLimitReached    = errors.New("max_concurrent_requests reached; try again later")
)

// The httpLogsConsumer is responsible for using the unmarshaler and the consumer.
type httpLogsConsumer interface {
	Start(context.Context, component.Host) error

	// Consume decodes and consumes all the records found in the
	// (decompressed) request body.
	Consume(ctx context.Context, body io.Reader, commonAttributes map[string]string) (int, error)
}

type telemetryAwareConsumer interface {
	setTelemetry(*metadata.TelemetryBuilder)
}

type requestOutcome struct {
	duplicate bool
}

type requestOutcomeContextKey struct{}

func requestOutcomeFromContext(ctx context.Context) *requestOutcome {
	outcome, _ := ctx.Value(requestOutcomeContextKey{}).(*requestOutcome)
	return outcome
}

// decoderOptions returns the decoder flush options derived from the receiver
// config for use with streaming decoders. Unset (0) values are intentionally
// not forwarded: 0 would disable the corresponding flush criterion, and the
// decoders apply their own defaults when no options are given.
func decoderOptions(config *Config) []encoding.DecoderOption {
	var options []encoding.DecoderOption
	if config.FlushItems > 0 {
		options = append(options, encoding.WithFlushItems(config.FlushItems))
	}
	if config.FlushBytes > 0 {
		options = append(options, encoding.WithFlushBytes(config.FlushBytes))
	}
	return options
}

// httpLogsReceiver
type httpLogsReceiver struct {
	// settings is the base receiver settings.
	settings receiver.Settings
	// config is the configuration for the receiver.
	config *Config
	// server is the HTTP/HTTPS server set up to listen
	// for requests.
	server *http.Server
	// shutdownWG is the WaitGroup that is used to wait until
	// the server shutdown has completed.
	shutdownWG sync.WaitGroup
	// consumer is the httpLogsConsumer to use to process/send
	// the records in each request.
	consumer httpLogsConsumer
	// slots bounds the number of requests processed concurrently;
	// it is nil when max_concurrent_requests is 0 (unlimited).
	slots chan struct{}
	// active is the number of requests currently holding a slot.
	active atomic.Int64
	// telemetry records the receiver's own metrics.
	telemetry *metadata.TelemetryBuilder
}

// The httpLogsResponse is the expected body for the response back to
// the delivery stream.
type httpLogsResponse struct {
	// RequestID is the same GUID that was received in
	// the request.
	RequestID string `json:"requestId"`
	// Timestamp is the milliseconds since epoch for when the
	// request finished being processed.
	Timestamp int64 `json:"timestamp"`
	// ErrorMessage is the error to report. Empty if request
	// was successfully processed.
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// The httpLogsCommonAttributes is the format for the common attributes
// found in the header of requests.
type httpLogsCommonAttributes struct {
	// CommonAttributes can be set when creating the delivery stream.
	// These will be passed to the httpLogsConsumer, which should
	// attach the attributes.
	CommonAttributes map[string]string `json:"commonAttributes"`
}

var (
	_ receiver.Metrics = (*httpLogsReceiver)(nil)
	_ http.Handler     = (*httpLogsReceiver)(nil)
)

// Start spins up the receiver's HTTP server and makes the receiver start
// its processing.
func (fmr *httpLogsReceiver) Start(ctx context.Context, host component.Host) error {
	if err := fmr.consumer.Start(ctx, host); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	var err error
	fmr.server, err = fmr.config.ToServer(ctx, host.GetExtensions(), fmr.settings.TelemetrySettings, fmr)
	if err != nil {
		return fmt.Errorf("failed to initialize HTTP server: %w", err)
	}

	var listener net.Listener
	listener, err = fmr.config.ToListener(ctx)
	if err != nil {
		return fmt.Errorf("failed to start listening for HTTP requests: %w", err)
	}
	fmr.shutdownWG.Go(func() {
		if errHTTP := fmr.server.Serve(listener); errHTTP != nil && !errors.Is(errHTTP, http.ErrServerClosed) {
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(errHTTP))
		}
	})

	return nil
}

// Shutdown tells the receiver that should stop reception,
// giving it a chance to perform any necessary clean-up (including closing
// the file tracking store) and shutting down its HTTP server.
func (fmr *httpLogsReceiver) Shutdown(ctx context.Context) error {
	if tracker, ok := fmr.consumer.(interface{ Shutdown(context.Context) error }); ok {
		if err := tracker.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shut down file tracking: %w", err)
		}
	}
	if fmr.telemetry != nil {
		fmr.telemetry.Shutdown()
	}
	if fmr.server == nil {
		return nil
	}
	err := fmr.server.Close()
	fmr.shutdownWG.Wait()
	return err
}

// ServeHTTP receives HTTP requests, unmarshalls them, and sends them along to the httpLogsConsumer,
// which is responsible for unmarshalling the records and sending them to the next consumer.
func (fmr *httpLogsReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	outcome := &requestOutcome{}
	ctx = context.WithValue(ctx, requestOutcomeContextKey{}, outcome)

	requestID := r.Header.Get(headerHttpRequestID)
	fmr.settings.Logger.Debug("Processing request", zap.String("RequestID", requestID))

	if statusCode, err := fmr.validate(r); err != nil {
		fmr.settings.Logger.Error(
			"Invalid request",
			zap.Error(err),
		)
		fmr.sendResponse(w, requestID, statusCode, err)
		fmr.recordRequest("rejected", "invalid_access_key")
		return
	}

	commonAttributes, err := fmr.getCommonAttributes(r)
	if err != nil {
		fmr.settings.Logger.Error(
			"Unable to get common attributes from request header. Will not attach attributes.",
			zap.Error(err),
		)
	}

	contentEncoding := r.Header.Get(headerContentEncoding)
	if !fmr.config.contentEncodingAllowed(contentEncoding) {
		err = fmt.Errorf("%w: %q", errUnsupportedContentEncoding, contentEncoding)
		fmr.settings.Logger.Error(
			"Request rejected due to content encoding",
			zap.Error(err),
		)
		fmr.sendResponse(w, requestID, http.StatusBadRequest, err)
		fmr.recordRequest("rejected", "unsupported_content_encoding")
		return
	}

	// Acquire a concurrency slot before the body is decompressed and
	// decoded: the cap bounds how many requests may hold decoded data at
	// once. Waiting or rejected requests never read the body, so their
	// bytes stay on the wire (TCP backpressure applies to the sender).
	if fmr.slots != nil {
		if !fmr.acquireSlot(ctx, w, requestID) {
			return
		}
		defer fmr.releaseSlot()
	}

	body, err := decode.ReaderFor(r.Body, contentEncoding)
	if err != nil {
		fmr.settings.Logger.Error(
			"Failed to set up decompression for the request body",
			zap.Error(err),
		)
		fmr.sendResponse(w, requestID, http.StatusBadRequest, err)
		fmr.recordRequest("rejected", "decompression")
		return
	}
	if closer, ok := body.(io.Closer); ok {
		defer closer.Close()
	}

	statusCode, err := fmr.consumer.Consume(ctx, body, commonAttributes)
	if err != nil {
		fmr.settings.Logger.Error(
			"Unable to consume records",
			zap.String("RequestID", requestID),
			zap.Error(err),
		)
		fmr.sendResponse(w, requestID, statusCode, err)
		if statusCode == http.StatusServiceUnavailable {
			fmr.recordRequest("rejected", "upstream_retry")
		} else if statusCode >= http.StatusInternalServerError {
			fmr.recordRequest("rejected", "processing_error")
		} else {
			fmr.recordRequest("rejected", "invalid_payload")
		}
		return
	}
	if outcome.duplicate {
		fmr.recordRequest("duplicate", "file_already_processed")
	} else {
		fmr.recordRequest("accepted", "success")
	}
	fmr.sendResponse(w, requestID, http.StatusOK, nil)
}

// newHttpLogsReceiver builds the shared HTTP receiver around a signal
// consumer, wiring the concurrency limiter and telemetry from the config.
func newHttpLogsReceiver(set receiver.Settings, config *Config, consumer httpLogsConsumer) (*httpLogsReceiver, error) {
	telemetry, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("failed to create receiver telemetry: %w", err)
	}
	r := &httpLogsReceiver{
		settings:  set,
		config:    config,
		consumer:  consumer,
		telemetry: telemetry,
	}
	if telemetryConsumer, ok := consumer.(telemetryAwareConsumer); ok {
		telemetryConsumer.setTelemetry(telemetry)
	}
	if limit := config.MaxConcurrentRequests; limit > 0 {
		r.slots = make(chan struct{}, limit)
	}
	return r, nil
}

// acquireSlot takes one concurrency slot for this request, waiting for a
// free slot when concurrency_strategy is "block" and failing fast when it is
// "fast_fail". It reports whether the slot was acquired; a failed
// acquisition has already responded to the sender.
func (fmr *httpLogsReceiver) acquireSlot(ctx context.Context, w http.ResponseWriter, requestID string) bool {
	if fmr.config.ConcurrencyStrategy == concurrencyStrategyFastFail {
		select {
		case <-ctx.Done():
			fmr.sendResponse(w, requestID, http.StatusServiceUnavailable, ctx.Err())
			fmr.recordRequest("rejected", "cancelled")
			return false
		default:
		}
		select {
		case fmr.slots <- struct{}{}:
			fmr.recordActive(+1)
			return true
		default:
			fmr.settings.Logger.Info(
				"Rejecting request: max_concurrent_requests reached (concurrency_strategy=fast_fail)",
				zap.String("RequestID", requestID),
			)
			w.Header().Set(headerRetryAfter, "1")
			fmr.sendResponse(w, requestID, http.StatusServiceUnavailable, errConcurrencyLimitReached)
			fmr.countRejected()
			fmr.recordRequest("rejected", "concurrency_limit")
			return false
		}
	}
	select {
	case fmr.slots <- struct{}{}:
	case <-ctx.Done():
		fmr.settings.Logger.Info(
			"Request cancelled while waiting for a concurrency slot",
			zap.String("RequestID", requestID),
		)
		fmr.sendResponse(w, requestID, http.StatusServiceUnavailable, ctx.Err())
		fmr.recordRequest("rejected", "cancelled")
		return false
	}
	fmr.recordActive(+1)
	return true
}

// releaseSlot frees the slot held by this request and refreshes the
// active_requests gauge.
func (fmr *httpLogsReceiver) releaseSlot() {
	<-fmr.slots
	fmr.recordActive(-1)
}

// recordActive updates the active_requests gauge after a slot transition.
// It records on context.Background(): the gauge reflects receiver state and
// must not be tied to the lifetime of a (possibly cancelled) request.
func (fmr *httpLogsReceiver) recordActive(delta int64) {
	fmr.active.Add(delta)
	if fmr.telemetry != nil {
		fmr.telemetry.HttplogsReceiverConcurrencyActiveRequests.Record(context.Background(), fmr.active.Load())
	}
}

// countRejected counts a request rejected by concurrency_strategy=fast_fail.
func (fmr *httpLogsReceiver) countRejected() {
	if fmr.telemetry == nil {
		return
	}
	fmr.telemetry.HttplogsReceiverConcurrencyRequestsRejectedTotal.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("strategy", concurrencyStrategyFastFail),
	))
}

func (fmr *httpLogsReceiver) recordRequest(state, reason string) {
	if fmr.telemetry == nil {
		return
	}
	fmr.telemetry.HttplogsReceiverRequestsTotal.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("request_state", state),
		attribute.String("request_reason", reason),
	))
}

// validate checks the access key in the header against
// the one passed into the Config
func (fmr *httpLogsReceiver) validate(r *http.Request) (int, error) {
	if string(fmr.config.AccessKey) == "" {
		// No access key is configured - accept all requests.
		return http.StatusAccepted, nil
	}
	if accessKey := r.Header.Get(headerHttpAccessKey); accessKey == string(fmr.config.AccessKey) {
		return http.StatusAccepted, nil
	}
	return http.StatusUnauthorized, errInvalidAccessKey
}

// getCommonAttributes unmarshalls the common attributes from the request header.
// A non-empty X-App-Name header is additionally injected as service.name,
// unless the X-Common-Attributes header already provides one.
func (*httpLogsReceiver) getCommonAttributes(r *http.Request) (map[string]string, error) {
	attributes := make(map[string]string)
	if commonAttributes := r.Header.Get(headerHttpCommonAttributes); commonAttributes != "" {
		if err := json.Unmarshal([]byte(commonAttributes), &attributes); err != nil {
			return nil, err
		}
	}
	if appName := r.Header.Get(headerHttpAppName); appName != "" {
		if _, ok := attributes["service.name"]; !ok {
			attributes["service.name"] = appName
		}
	}
	return attributes, nil
}

// sendResponse writes a response to in the expected format.
func (fmr *httpLogsReceiver) sendResponse(w http.ResponseWriter, requestID string, statusCode int, err error) {
	var errorMessage string
	if err != nil {
		errorMessage = err.Error()
	}
	body := httpLogsResponse{
		RequestID:    requestID,
		Timestamp:    time.Now().UnixMilli(),
		ErrorMessage: errorMessage,
	}
	payload, _ := json.Marshal(body)
	w.Header().Set(headerContentType, "application/json")
	w.Header().Set(headerContentLength, strconv.Itoa(len(payload)))
	w.WriteHeader(statusCode)
	if _, err = w.Write(payload); err != nil {
		fmr.settings.Logger.Error("Failed to send response", zap.Error(err))
	}
}

// loadEncodingExtension tries to load an available extension for the given encoding.
func loadEncodingExtension[T any](host component.Host, encoding, signalType string) (T, error) {
	var zero T
	extensionID, err := encodingToComponentID(encoding)
	if err != nil {
		return zero, err
	}
	encodingExtension, ok := host.GetExtensions()[*extensionID]
	if !ok {
		return zero, fmt.Errorf("unknown encoding extension %q", encoding)
	}
	unmarshaler, ok := encodingExtension.(T)
	if !ok {
		return zero, fmt.Errorf("extension %q is not a %s unmarshaler", encoding, signalType)
	}
	return unmarshaler, nil
}

// encodingToComponentID attempts to parse the encoding string as a component ID.
func encodingToComponentID(encoding string) (*component.ID, error) {
	var id component.ID
	if err := id.UnmarshalText([]byte(encoding)); err != nil {
		return nil, fmt.Errorf("invalid component type: %w", err)
	}
	return &id, nil
}
