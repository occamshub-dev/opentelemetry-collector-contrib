// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package httplogsreceiver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/metadatatest"
)

const (
	testHttpRequestID = "request-id"
	testHttpAccessKey = "access-key"
)

type nopHttpLogsConsumer struct {
	statusCode int
	err        error
}

var _ = (*nopHttpLogsConsumer)(nil)

func newNopHttpLogsConsumer(statusCode int, err error) *nopHttpLogsConsumer {
	return &nopHttpLogsConsumer{statusCode, err}
}

func (*nopHttpLogsConsumer) Start(context.Context, component.Host) error {
	return nil
}

func (nfc *nopHttpLogsConsumer) Consume(context.Context, io.Reader, map[string]string) (int, error) {
	return nfc.statusCode, nfc.err
}

type httpLogsConsumerFunc func(context.Context, io.Reader, map[string]string) (int, error)

func (httpLogsConsumerFunc) Start(context.Context, component.Host) error {
	return nil
}

func (f httpLogsConsumerFunc) Consume(ctx context.Context, body io.Reader, common map[string]string) (int, error) {
	return f(ctx, body, common)
}

func TestStart(t *testing.T) {
	testCases := map[string]struct {
		host    component.Host
		wantErr error
	}{
		"WithHost": {
			host: componenttest.NewNopHost(),
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			cfg := &Config{
				ServerConfig: confighttp.ServerConfig{
					NetAddr: confignet.AddrConfig{
						Transport: "tcp",
					},
				},
			}
			ctx := t.Context()
			r := testHttpReceiver(cfg, &nopHttpLogsConsumer{})
			got := r.Start(ctx, testCase.host)
			require.Equal(t, testCase.wantErr, got)
			if r.server != nil {
				require.NoError(t, r.Shutdown(ctx))
			}
		})
	}
	t.Run("WithPortTaken", func(t *testing.T) {
		listener, err := net.Listen("tcp", "localhost:")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, listener.Close())
		})
		cfg := &Config{
			ServerConfig: confighttp.ServerConfig{
				NetAddr: confignet.AddrConfig{
					Transport: "tcp",
					Endpoint:  listener.Addr().String(),
				},
			},
		}
		ctx := t.Context()
		r := testHttpReceiver(cfg, &nopHttpLogsConsumer{})
		got := r.Start(ctx, componenttest.NewNopHost())
		require.Error(t, got)
		if r.server != nil {
			require.NoError(t, r.Shutdown(ctx))
		}
	})
}

func TestRequest(t *testing.T) {
	defaultConsumer := newNopHttpLogsConsumer(http.StatusOK, nil)
	httpLogsConsumerErr := errors.New("consumer error")
	cfg := &Config{
		AccessKey: testHttpAccessKey,
	}
	noRecords := ""
	testCases := map[string]struct {
		headers          map[string]string
		commonAttributes map[string]string
		body             any
		consumer         httpLogsConsumer
		wantStatusCode   int
		wantErr          error
	}{
		"WithoutRequestId/Header": {
			headers: map[string]string{
				headerHttpRequestID: "",
			},
			body:           testHttpRequest(testHttpRequestID, noRecords),
			wantStatusCode: http.StatusOK,
		},
		"WithDifferentAccessKey": {
			headers: map[string]string{
				headerHttpAccessKey: "test",
			},
			body:           testHttpRequest(testHttpRequestID, noRecords),
			wantStatusCode: http.StatusUnauthorized,
			wantErr:        errInvalidAccessKey,
		},
		"WithNoAccessKey": {
			headers: map[string]string{
				headerHttpAccessKey: "",
			},
			body:           testHttpRequest(testHttpRequestID, noRecords),
			wantStatusCode: http.StatusUnauthorized,
			wantErr:        errInvalidAccessKey,
		},
		"WithNoRecords": {
			body:           testHttpRequest(testHttpRequestID, noRecords),
			wantStatusCode: http.StatusOK,
		},
		"WithHttpLogsConsumerError": {
			body:           testHttpRequest(testHttpRequestID, noRecords),
			consumer:       newNopHttpLogsConsumer(http.StatusInternalServerError, httpLogsConsumerErr),
			wantStatusCode: http.StatusInternalServerError,
			wantErr:        httpLogsConsumerErr,
		},
		"WithValidRecords": {
			body:           testHttpRequest(testHttpRequestID, "test"),
			wantStatusCode: http.StatusOK,
		},
		"WithValidRecords/CommonAttributes": {
			body: testHttpRequest(testHttpRequestID, "test"),
			commonAttributes: map[string]string{
				"TestAttribute": "common",
			},
			wantStatusCode: http.StatusOK,
		},
		"WithAppNameHeader": {
			headers: map[string]string{
				headerHttpAppName: "my-app",
			},
			body: testHttpRequest(testHttpRequestID, "test"),
			consumer: httpLogsConsumerFunc(func(ctx context.Context, body io.Reader, common map[string]string) (int, error) {
				require.Equal(t, "my-app", common["service.name"])
				return http.StatusOK, nil
			}),
			wantStatusCode: http.StatusOK,
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			body, err := json.Marshal(testCase.body)
			require.NoError(t, err)

			request := newTestRequest(body)
			if testCase.headers != nil {
				for k, v := range testCase.headers {
					request.Header.Set(k, v)
				}
			}
			if testCase.commonAttributes != nil {
				attrs, err := json.Marshal(httpLogsCommonAttributes{
					CommonAttributes: testCase.commonAttributes,
				})
				require.NoError(t, err)
				request.Header.Set(headerHttpCommonAttributes, string(attrs))
			}

			consumer := testCase.consumer
			if consumer == nil {
				consumer = defaultConsumer
			}
			r := testHttpReceiver(cfg, consumer)

			got := httptest.NewRecorder()
			r.ServeHTTP(got, request)

			require.Equal(t, testCase.wantStatusCode, got.Code)
			var gotResponse httpLogsResponse
			require.NoError(t, json.Unmarshal(got.Body.Bytes(), &gotResponse))
			require.Equal(t, request.Header.Get(headerHttpRequestID), gotResponse.RequestID)
			if testCase.wantErr != nil {
				require.Equal(t, testCase.wantErr.Error(), gotResponse.ErrorMessage)
			} else {
				require.Empty(t, gotResponse.ErrorMessage)
			}
		})
	}
}

// blockingConsumer returns a consumer that signals on started the first
// time it begins processing a request and blocks until release is closed.
func blockingConsumer(started, release chan struct{}) httpLogsConsumer {
	var once sync.Once
	return httpLogsConsumerFunc(func(context.Context, io.Reader, map[string]string) (int, error) {
		once.Do(func() { close(started) })
		<-release
		return http.StatusOK, nil
	})
}

// waitStarted fails the test if the consumer has not started within the
// timeout.
func waitStarted(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("consumer did not start in time")
	}
}

func TestConcurrencyLimitBlock(t *testing.T) {
	cfg := &Config{
		MaxConcurrentRequests: 1,
		ConcurrencyStrategy:   concurrencyStrategyBlock,
	}
	started := make(chan struct{})
	release := make(chan struct{})
	r := testHttpReceiver(cfg, blockingConsumer(started, release))

	firstDone := make(chan struct{})
	first := httptest.NewRecorder()
	go func() {
		r.ServeHTTP(first, newTestRequest([]byte("first")))
		close(firstDone)
	}()
	waitStarted(t, started)

	second := httptest.NewRecorder()
	secondDone := make(chan struct{})
	go func() {
		r.ServeHTTP(second, newTestRequest([]byte("second")))
		close(secondDone)
	}()
	select {
	case <-secondDone:
		t.Fatal("second request completed while the slot was still held")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first request did not complete")
	}
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second request did not complete after the slot was released")
	}
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
}

func TestConcurrencyLimitFastFail(t *testing.T) {
	cfg := &Config{
		MaxConcurrentRequests: 1,
		ConcurrencyStrategy:   concurrencyStrategyFastFail,
	}
	started := make(chan struct{})
	release := make(chan struct{})
	r := testHttpReceiver(cfg, blockingConsumer(started, release))

	firstDone := make(chan struct{})
	first := httptest.NewRecorder()
	go func() {
		r.ServeHTTP(first, newTestRequest([]byte("first")))
		close(firstDone)
	}()
	waitStarted(t, started)

	second := httptest.NewRecorder()
	r.ServeHTTP(second, newTestRequest([]byte("second")))
	require.Equal(t, http.StatusServiceUnavailable, second.Code)
	require.Equal(t, "1", second.Header().Get("Retry-After"))

	close(release)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first request did not complete")
	}
	require.Equal(t, http.StatusOK, first.Code)
}

func TestConcurrencyLimitBlockCancelled(t *testing.T) {
	cfg := &Config{
		MaxConcurrentRequests: 1,
		ConcurrencyStrategy:   concurrencyStrategyBlock,
	}
	started := make(chan struct{})
	release := make(chan struct{})
	r := testHttpReceiver(cfg, blockingConsumer(started, release))

	firstDone := make(chan struct{})
	first := httptest.NewRecorder()
	go func() {
		r.ServeHTTP(first, newTestRequest([]byte("first")))
		close(firstDone)
	}()
	waitStarted(t, started)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	second := httptest.NewRecorder()
	r.ServeHTTP(second, newTestRequest([]byte("second")).WithContext(ctx))
	require.Equal(t, http.StatusServiceUnavailable, second.Code)

	close(release)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first request did not complete")
	}
	require.Equal(t, http.StatusOK, first.Code)
}

func TestConcurrencyUnlimited(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	consumer := httpLogsConsumerFunc(func(context.Context, io.Reader, map[string]string) (int, error) {
		started <- struct{}{}
		<-release
		return http.StatusOK, nil
	})
	r := testHttpReceiver(&Config{}, consumer)

	first := httptest.NewRecorder()
	second := httptest.NewRecorder()
	firstDone := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		r.ServeHTTP(first, newTestRequest([]byte("first")))
		close(firstDone)
	}()
	go func() {
		r.ServeHTTP(second, newTestRequest([]byte("second")))
		close(secondDone)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not start")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second request did not start; the default config must be unlimited")
	}

	close(release)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first request did not complete")
	}
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second request did not complete")
	}
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
}

func TestConcurrencyTelemetry(t *testing.T) {
	tt := newTestTelemetry(t)
	set := metadatatest.NewSettings(tt)
	telemetry, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	require.NoError(t, err)

	cfg := &Config{
		MaxConcurrentRequests: 1,
		ConcurrencyStrategy:   concurrencyStrategyFastFail,
	}
	started := make(chan struct{})
	release := make(chan struct{})
	r := &httpLogsReceiver{
		settings:  set,
		config:    cfg,
		consumer:  blockingConsumer(started, release),
		telemetry: telemetry,
		slots:     make(chan struct{}, 1),
	}

	firstDone := make(chan struct{})
	first := httptest.NewRecorder()
	go func() {
		r.ServeHTTP(first, newTestRequest([]byte("first")))
		close(firstDone)
	}()
	waitStarted(t, started)
	metadatatest.AssertEqualHttplogsReceiverConcurrencyActiveRequests(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 1}}, metricdatatest.IgnoreTimestamp())

	for i := 0; i < 2; i++ {
		rejected := httptest.NewRecorder()
		r.ServeHTTP(rejected, newTestRequest([]byte("rejected")))
		require.Equal(t, http.StatusServiceUnavailable, rejected.Code)
	}
	metadatatest.AssertEqualHttplogsReceiverConcurrencyRequestsRejectedTotal(t, tt,
		[]metricdata.DataPoint[int64]{{
			Attributes: attribute.NewSet(attribute.String("strategy", "fast_fail")),
			Value:      2,
		}}, metricdatatest.IgnoreTimestamp())

	close(release)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first request did not complete")
	}
	require.Equal(t, http.StatusOK, first.Code)
	metadatatest.AssertEqualHttplogsReceiverConcurrencyActiveRequests(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 0}}, metricdatatest.IgnoreTimestamp())
	require.NoError(t, r.Shutdown(context.Background()))
}

func TestRequestTelemetry(t *testing.T) {
	tt := newTestTelemetry(t)
	set := metadatatest.NewSettings(tt)
	telemetry, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	require.NoError(t, err)
	r := &httpLogsReceiver{
		settings:  set,
		config:    &Config{AccessKey: testHttpAccessKey},
		consumer:  newNopHttpLogsConsumer(http.StatusOK, nil),
		telemetry: telemetry,
	}

	accepted := httptest.NewRecorder()
	r.ServeHTTP(accepted, newTestRequest([]byte("accepted")))
	require.Equal(t, http.StatusOK, accepted.Code)

	rejected := newTestRequest([]byte("rejected"))
	rejected.Header.Set(headerHttpAccessKey, "wrong")
	rejectedResponse := httptest.NewRecorder()
	r.ServeHTTP(rejectedResponse, rejected)
	require.Equal(t, http.StatusUnauthorized, rejectedResponse.Code)

	unsupported := newTestRequest([]byte("unsupported"))
	unsupported.Header.Set(headerContentEncoding, "br")
	unsupportedResponse := httptest.NewRecorder()
	r.ServeHTTP(unsupportedResponse, unsupported)
	require.Equal(t, http.StatusBadRequest, unsupportedResponse.Code)

	metadatatest.AssertEqualHttplogsReceiverRequestsTotal(t, tt, []metricdata.DataPoint[int64]{
		{
			Attributes: attribute.NewSet(
				attribute.String("request_reason", "invalid_access_key"),
				attribute.String("request_state", "rejected"),
			),
			Value: 1,
		},
		{
			Attributes: attribute.NewSet(
				attribute.String("request_reason", "success"),
				attribute.String("request_state", "accepted"),
			),
			Value: 1,
		},
		{
			Attributes: attribute.NewSet(
				attribute.String("request_reason", "unsupported_content_encoding"),
				attribute.String("request_state", "rejected"),
			),
			Value: 1,
		},
	}, metricdatatest.IgnoreTimestamp())
	require.NoError(t, r.Shutdown(context.Background()))
}

func TestGetCommonAttributes(t *testing.T) {
	testCases := map[string]struct {
		headers map[string]string
		want    map[string]string
		wantErr bool
	}{
		"WithoutAttributes": {
			want: map[string]string{},
		},
		"CommonAttributesOnly": {
			headers: map[string]string{
				headerHttpCommonAttributes: `{"file.path":"somelocation/somefile.gz"}`,
			},
			want: map[string]string{"file.path": "somelocation/somefile.gz"},
		},
		"AppNameOnly": {
			headers: map[string]string{
				headerHttpAppName: "my-app",
			},
			want: map[string]string{"service.name": "my-app"},
		},
		"CommonAttributesTakePrecedence": {
			headers: map[string]string{
				headerHttpCommonAttributes: `{"service.name":"from-common-attributes"}`,
				headerHttpAppName:          "my-app",
			},
			want: map[string]string{"service.name": "from-common-attributes"},
		},
		"AppNameFillsMissingServiceName": {
			headers: map[string]string{
				headerHttpCommonAttributes: `{"file.path":"somelocation/somefile.gz"}`,
				headerHttpAppName:          "my-app",
			},
			want: map[string]string{"file.path": "somelocation/somefile.gz", "service.name": "my-app"},
		},
		"EmptyAppNameIgnored": {
			headers: map[string]string{
				headerHttpCommonAttributes: `{"service.name":"from-common-attributes"}`,
				headerHttpAppName:          "",
			},
			want: map[string]string{"service.name": "from-common-attributes"},
		},
		"InvalidCommonAttributes": {
			headers: map[string]string{
				headerHttpCommonAttributes: "not-json",
			},
			wantErr: true,
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			r := newTestRequest(nil)
			for k, v := range testCase.headers {
				r.Header.Set(k, v)
			}
			fmr := testHttpReceiver(&Config{}, nil)
			got, err := fmr.getCommonAttributes(r)
			if testCase.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.want, got)
		})
	}
}

// testHttpReceiver is a convenience function for creating a test
// httpLogsReceiver. It uses the same wiring (slots, telemetry) as the
// production constructors.
func testHttpReceiver(config *Config, consumer httpLogsConsumer) *httpLogsReceiver {
	r, err := newHttpLogsReceiver(receivertest.NewNopSettings(metadata.Type), config, consumer)
	if err != nil {
		panic(err)
	}
	return r
}

func newTestRequest(requestBody []byte) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(requestBody))
	request.Header.Set(headerContentType, "application/json")
	request.Header.Set(headerContentLength, strconv.Itoa(len(requestBody)))
	request.Header.Set(headerHttpRequestID, testHttpRequestID)
	request.Header.Set(headerHttpAccessKey, testHttpAccessKey)
	return request
}

func testHttpRequest(requestID, records string) string {
	return records
}

func testHttpRecord(data string) string {
	return data
}

// requestBody concatenates the given records into the body of a request,
// mimicking newline-delimited records on the wire.
func requestBody(records ...[]byte) io.Reader {
	return bytes.NewReader(bytes.Join(records, []byte("\n")))
}

type hostWithExtensions struct {
	component.Host
	extensions map[component.ID]component.Component
}

func (h hostWithExtensions) GetExtensions() map[component.ID]component.Component {
	return h.extensions
}

type plogUnmarshalerExtension struct {
	logs plog.Logs
}

func (plogUnmarshalerExtension) Start(context.Context, component.Host) error {
	return nil
}

func (plogUnmarshalerExtension) Shutdown(context.Context) error {
	return nil
}

func (e plogUnmarshalerExtension) UnmarshalLogs([]byte) (plog.Logs, error) {
	return e.logs, nil
}

type pmetricUnmarshalerExtension struct {
	metrics pmetric.Metrics
}

func (pmetricUnmarshalerExtension) Start(context.Context, component.Host) error {
	return nil
}

func (pmetricUnmarshalerExtension) Shutdown(context.Context) error {
	return nil
}

func (e pmetricUnmarshalerExtension) UnmarshalMetrics([]byte) (pmetric.Metrics, error) {
	return e.metrics, nil
}

// fakePlogLogsDecoderFactory implements both plog.Unmarshaler and
// encoding.LogsDecoderFactory:
//
//   - NewLogsDecoder returns newDecoderError, or a decoder that replays the
//     configured batches (followed by batchError, or io.EOF by default).
//   - UnmarshalLogs delegates to fallback when NewLogsDecoder errors.
type fakePlogLogsDecoderFactory struct {
	batches         []plog.Logs
	batchError      error
	newDecoderError error
	fallback        plog.Unmarshaler
}

var (
	_ plog.Unmarshaler            = (*fakePlogLogsDecoderFactory)(nil)
	_ encoding.LogsDecoderFactory = (*fakePlogLogsDecoderFactory)(nil)
)

func (f *fakePlogLogsDecoderFactory) UnmarshalLogs(data []byte) (plog.Logs, error) {
	if f.fallback != nil {
		return f.fallback.UnmarshalLogs(data)
	}
	return plog.NewLogs(), nil
}

func (f *fakePlogLogsDecoderFactory) NewLogsDecoder(_ io.Reader, _ ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	if f.newDecoderError != nil {
		return nil, f.newDecoderError
	}
	return &fakePlogLogsDecoder{batches: f.batches, batchError: f.batchError}, nil
}

// fakePlogLogsDecoder replays its batches one per DecodeLogs call. Once the
// batches are exhausted it returns batchError (or io.EOF if nil).
type fakePlogLogsDecoder struct {
	batches    []plog.Logs
	batchError error
}

func (d *fakePlogLogsDecoder) DecodeLogs() (plog.Logs, error) {
	if len(d.batches) == 0 {
		if d.batchError != nil {
			return plog.NewLogs(), d.batchError
		}
		return plog.NewLogs(), io.EOF
	}
	logs := d.batches[0]
	d.batches = d.batches[1:]
	return logs, nil
}

func (*fakePlogLogsDecoder) Offset() int64 { return 0 }

// fakePmetricMetricsDecoderFactory mirrors fakePlogLogsDecoderFactory for
// metrics.
type fakePmetricMetricsDecoderFactory struct {
	batches         []pmetric.Metrics
	batchError      error
	newDecoderError error
	fallback        pmetric.Unmarshaler
}

var (
	_ pmetric.Unmarshaler            = (*fakePmetricMetricsDecoderFactory)(nil)
	_ encoding.MetricsDecoderFactory = (*fakePmetricMetricsDecoderFactory)(nil)
)

func (f *fakePmetricMetricsDecoderFactory) UnmarshalMetrics(data []byte) (pmetric.Metrics, error) {
	if f.fallback != nil {
		return f.fallback.UnmarshalMetrics(data)
	}
	return pmetric.NewMetrics(), nil
}

func (f *fakePmetricMetricsDecoderFactory) NewMetricsDecoder(_ io.Reader, _ ...encoding.DecoderOption) (encoding.MetricsDecoder, error) {
	if f.newDecoderError != nil {
		return nil, f.newDecoderError
	}
	return &fakePmetricMetricsDecoder{batches: f.batches, batchError: f.batchError}, nil
}

// fakePmetricMetricsDecoder replays its batches one per DecodeMetrics call.
// Once the batches are exhausted it returns batchError (or io.EOF if nil).
type fakePmetricMetricsDecoder struct {
	batches    []pmetric.Metrics
	batchError error
}

func (d *fakePmetricMetricsDecoder) DecodeMetrics() (pmetric.Metrics, error) {
	if len(d.batches) == 0 {
		if d.batchError != nil {
			return pmetric.NewMetrics(), d.batchError
		}
		return pmetric.NewMetrics(), io.EOF
	}
	metrics := d.batches[0]
	d.batches = d.batches[1:]
	return metrics, nil
}

func (*fakePmetricMetricsDecoder) Offset() int64 { return 0 }
