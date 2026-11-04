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
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/metadata"
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

func (nfc *nopHttpLogsConsumer) Consume(context.Context, nextRecordFunc, map[string]string) (int, error) {
	return nfc.statusCode, nfc.err
}

type httpLogsConsumerFunc func(context.Context, nextRecordFunc, map[string]string) (int, error)

func (httpLogsConsumerFunc) Start(context.Context, component.Host) error {
	return nil
}

func (f httpLogsConsumerFunc) Consume(ctx context.Context, next nextRecordFunc, common map[string]string) (int, error) {
	return f(ctx, next, common)
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

// testHttpReceiver is a convenience function for creating a test httpLogsReceiver
func testHttpReceiver(config *Config, consumer httpLogsConsumer) *httpLogsReceiver {
	return &httpLogsReceiver{
		settings: receivertest.NewNopSettings(metadata.Type),
		config:   config,
		consumer: consumer,
	}
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

func newNextRecordFunc(records [][]byte) nextRecordFunc {
	return func() ([]byte, error) {
		if len(records) == 0 {
			return nil, io.EOF
		}
		next := records[0]
		records = records[1:]
		return next, nil
	}
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
