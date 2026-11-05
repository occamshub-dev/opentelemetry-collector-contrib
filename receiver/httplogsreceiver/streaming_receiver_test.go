// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package httplogsreceiver

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"

	awslogs "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension"
	cwmetrics "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awscloudwatchmetricstreamsencodingextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/metadata"
)

// awsLogsEncodingType and cwMetricsEncodingType are the component types of
// the encoding extensions used by these tests. The receiver looks them up in
// the host's extension set by these ids.
const (
	awsLogsEncodingType   = "aws_logs_encoding"
	cwMetricsEncodingType = "awscloudwatchmetricstreams_encoding"
)

var (
	awsLogsID   = component.MustNewID(awsLogsEncodingType)
	cwMetricsID = component.MustNewID(cwMetricsEncodingType)
)

// albSampleLines are valid ALB access log lines (the set from the
// awslogs extension's test fixture) used as the record pool for these tests.
var albSampleLines = []string{
	`https 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.086 0.048 0.037 200 200 0 57 "GET https://www.example.com:443/ HTTP/1.1" "curl/7.46.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337281-1d84f3d73c47ec4e58577259" "www.example.com" "arn:aws:acm:us-east-2:123456789012:certificate/12345678-1234-1234-1234-123456789012" 1 2018-07-02T22:22:48.364000Z "authenticate,forward" "-" "-" "10.0.0.1:80" "200" "-" "-" TID_1234abcd5678ef90`,
	`https 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 [fe80::202:b3ff:fe1e:8329]:443 [2001:db8::1]:80 0.086 0.048 0.037 200 200 0 57 "GET https://www.example.com:443/ HTTP/1.1" "curl/7.46.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337281-1d84f3d73c47ec4e58577259" "www.example.com" "arn:aws:acm:us-east-2:123456789012:certificate/12345678-1234-1234-1234-123456789012" 1 2018-07-02T22:22:48.364000Z "authenticate,forward" "-" "-" "10.0.0.1:80" "200" "-" "-" TID_1234abcd5678ef90`,
	`https 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.086 0.048 0.037 200 200 0 57 "GET https://www.example.com:443/ HTTP/1.1" "curl/7.46.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337281-1d84f3d73c47ec4e58577259" "www.example.com" "arn:aws:acm:us-east-2:123456789012:certificate/12345678-1234-1234-1234-123456789012" 1 2018-07-02T22:22:48.364000Z "authenticate,forward" "-" "-" "10.0.0.1:80" "200" "-" "-" TID_1234abcd5678ef90 "transformed.example.com" "https://transformed.example.com/index.html" "TransformSuccess"`,
}

// elbControlLine is a valid ELB control-message line.
const elbControlLine = `Enable AccessLog for ELB: app/elb-name/abc123456 at 2025-05-30T16:47:00.781Z`

// cwSampleLines are valid CloudWatch Metric Streams JSON records, one metric
// per line (distinct metric names so each line maps to its own metric).
var cwSampleLines = []string{
	`{"metric_stream_name":"MyMetricStream","account_id":"1234567890","region":"us-east-1","namespace":"AWS/EC2","metric_name":"DiskWriteOps","dimensions":{"InstanceId":"i-123456789012"},"timestamp":1611929698000,"value":{"count":3.0,"sum":20.0,"max":18.0,"min":0.0},"unit":"Seconds"}`,
	`{"metric_stream_name":"MyMetricStream","account_id":"1234567890","region":"us-east-1","namespace":"AWS/EC2","metric_name":"DiskReadOps","dimensions":{"InstanceId":"i-123456789012"},"timestamp":1611929700000,"value":{"count":5.0,"sum":45.0,"max":25.0,"min":2.0},"unit":"Seconds"}`,
	`{"metric_stream_name":"OtherStream","account_id":"1234567890","region":"us-west-2","namespace":"AWS/EC2","metric_name":"CreditUsage","dimensions":{"InstanceId":"i-987654321098"},"timestamp":1611929710000,"value":{"count":1.0,"sum":1.0,"max":1.0,"min":1.0},"unit":"Count"}`,
}

// testCodecs are the encodings accepted by the default allow-list.
var testCodecs = []string{
	string(configcompression.TypeGzip),
	string(configcompression.TypeZstd),
	string(configcompression.TypeZlib),
	string(configcompression.TypeDeflate),
}

// newStreamTestConfig builds a minimal receiver config for the given
// encoding extension name.
func newStreamTestConfig(encoding string) *Config {
	return &Config{
		ServerConfig: confighttp.ServerConfig{
			NetAddr: confignet.AddrConfig{Transport: "tcp"},
		},
		Encoding: encoding,
	}
}

// hostWithTestExtension wraps a nop host so that exactly one extension is
// visible under the given id.
func hostWithTestExtension(id component.ID, ext component.Component) component.Host {
	return hostWithExtensions{
		Host:       componenttest.NewNopHost(),
		extensions: map[component.ID]component.Component{id: ext},
	}
}

// createAWSLogsExtension instantiates the real aws_logs_encoding extension
// configured for ELB access logs.
func createAWSLogsExtension(t *testing.T) extension.Extension {
	t.Helper()
	factory := awslogs.NewFactory()
	ext, err := factory.Create(
		t.Context(),
		extensiontest.NewNopSettings(component.MustNewType(awsLogsEncodingType)),
		&awslogs.Config{Format: "elbaccess"},
	)
	require.NoError(t, err)
	return ext
}

// createCWMetricsExtension instantiates the real CloudWatch Metric Streams
// encoding extension configured for JSON records.
func createCWMetricsExtension(t *testing.T) extension.Extension {
	t.Helper()
	factory := cwmetrics.NewFactory()
	ext, err := factory.Create(
		t.Context(),
		extensiontest.NewNopSettings(component.MustNewType(cwMetricsEncodingType)),
		&cwmetrics.Config{Format: "json"},
	)
	require.NoError(t, err)
	return ext
}

// newELBLogsConsumer builds a started logsConsumer wired to a real
// ELB-capable aws_logs_encoding extension with next as the target consumer.
func newELBLogsConsumer(t *testing.T, cfg *Config, next consumer.Logs) *logsConsumer {
	t.Helper()
	c := &logsConsumer{
		config:   cfg,
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: next,
	}
	require.NoError(t, c.Start(t.Context(), hostWithTestExtension(awsLogsID, createAWSLogsExtension(t))))
	require.NotNil(t, c.unmarshaler)
	return c
}

// newCWMetricsConsumer builds a started metricsConsumer wired to a real
// CloudWatch Metric Streams encoding extension.
func newCWMetricsConsumer(t *testing.T, cfg *Config, next consumer.Metrics) *metricsConsumer {
	t.Helper()
	c := &metricsConsumer{
		config:   cfg,
		settings: receivertest.NewNopSettings(metadata.Type),
		consumer: next,
	}
	require.NoError(t, c.Start(t.Context(), hostWithTestExtension(cwMetricsID, createCWMetricsExtension(t))))
	require.NotNil(t, c.unmarshaler)
	return c
}

// serveBody delivers body to the receiver's HTTP handler and returns the
// response. An empty contentEncoding means the header is not sent.
// All test-framework calls happen on the current goroutine.
func serveBody(t *testing.T, cfg *Config, cons httpLogsConsumer, body io.Reader, contentEncoding string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := serveBodyNoTest(cfg, cons, body, contentEncoding)
	requireBodyRecoded(t, recorder, true)
	return recorder
}

// requireBodyRecoded validates a response body: a 200 response must carry a
// well-formed httpLogsResponse with no error message.
func requireBodyRecoded(t *testing.T, rec *httptest.ResponseRecorder, wantSuccess bool) {
	t.Helper()
	var response httpLogsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "response body must be valid JSON")
	if wantSuccess {
		require.Equal(t, http.StatusOK, rec.Code)
		require.Empty(t, response.ErrorMessage, "200 response must not carry an error message")
	}
}

// serveBodyNoTest is like serveBody but performs no test-framework checks,
// so it can be run from a goroutine.
func serveBodyNoTest(cfg *Config, cons httpLogsConsumer, body io.Reader, contentEncoding string) *httptest.ResponseRecorder {
	receiver := testHttpReceiver(cfg, cons)
	request := httptest.NewRequest(http.MethodPost, "/", body)
	request.Header.Set(headerHttpRequestID, testHttpRequestID)
	request.Header.Set(headerContentType, "text/plain")
	if contentEncoding != "" {
		request.Header.Set(headerContentEncoding, contentEncoding)
	}
	recorder := httptest.NewRecorder()
	receiver.ServeHTTP(recorder, request)
	return recorder
}

// compressBody encodes payload with the named codec using the same
// libraries the receiver decompresses with.
func compressBody(t *testing.T, payload []byte, algorithm string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	switch algorithm {
	case "gzip":
		writer := gzip.NewWriter(&buffer)
		_, err := writer.Write(payload)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	case "zstd":
		writer, err := zstd.NewWriter(&buffer)
		require.NoError(t, err)
		_, err = writer.Write(payload)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	case "zlib":
		writer := zlib.NewWriter(&buffer)
		_, err := writer.Write(payload)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	case "deflate":
		writer, err := flate.NewWriter(&buffer, flate.DefaultCompression)
		require.NoError(t, err)
		_, err = writer.Write(payload)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	default:
		t.Fatalf("unknown test codec %q", algorithm)
	}
	return buffer.Bytes()
}

// logsRecorder is a consumer.Logs that captures every forwarded batch so the
// tests can assert on batch boundaries and record content.
type logsRecorder struct {
	mu      sync.Mutex
	batches []plog.Logs
}

var _ consumer.Logs = (*logsRecorder)(nil)

func (*logsRecorder) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (r *logsRecorder) ConsumeLogs(_ context.Context, logs plog.Logs) error {
	r.mu.Lock()
	r.batches = append(r.batches, logs)
	r.mu.Unlock()
	return nil
}

func (r *logsRecorder) batchRecordCounts() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	counts := make([]int, len(r.batches))
	for i, b := range r.batches {
		counts[i] = b.LogRecordCount()
	}
	return counts
}

func (r *logsRecorder) totalRecords() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, b := range r.batches {
		total += b.LogRecordCount()
	}
	return total
}

func (r *logsRecorder) recordBodies() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var bodies []string
	for _, b := range r.batches {
		for i := 0; i < b.ResourceLogs().Len(); i++ {
			rl := b.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				sl := rl.ScopeLogs().At(j)
				for k := 0; k < sl.LogRecords().Len(); k++ {
					bodies = append(bodies, sl.LogRecords().At(k).Body().AsString())
				}
			}
		}
	}
	return bodies
}

// fingerprintAll renders all captured batches into a deterministic string
// covering the resource/scope structure, every record body and attribute,
// and batch boundaries (batches are joined with newlines).
func (r *logsRecorder) fingerprintAll() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.batches))
	for i, b := range r.batches {
		out[i] = fingerprintLogsValue(b)
	}
	return strings.Join(out, "\n")
}

func fingerprintLogsValue(logs plog.Logs) string {
	var sb strings.Builder
	rls := logs.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		sb.WriteString("{res:")
		sb.WriteString(sortedAttrs(rl.Resource().Attributes()))
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sl := sls.At(j)
			sb.WriteString("|scope:")
			sb.WriteString(sl.Scope().Name())
			lrs := sl.LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				lr := lrs.At(k)
				sb.WriteString("|rec:")
				sb.WriteString(lr.Body().AsString())
				sb.WriteByte('#')
				sb.WriteString(sortedAttrs(lr.Attributes()))
			}
		}
		sb.WriteString("}")
	}
	return sb.String()
}

func sortedAttrs(attrs pcommon.Map) string {
	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		v, _ := attrs.Get(k)
		parts = append(parts, fmt.Sprintf("%s=%v", k, v.AsRaw()))
	}
	return strings.Join(parts, ",")
}

// metricsRecorder is the consumer.Metrics counterpart of logsRecorder.
type metricsRecorder struct {
	mu      sync.Mutex
	batches []pmetric.Metrics
}

var _ consumer.Metrics = (*metricsRecorder)(nil)

func (*metricsRecorder) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (r *metricsRecorder) ConsumeMetrics(_ context.Context, m pmetric.Metrics) error {
	r.mu.Lock()
	r.batches = append(r.batches, m)
	r.mu.Unlock()
	return nil
}

func (r *metricsRecorder) batchMetricCounts() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	counts := make([]int, len(r.batches))
	for i, b := range r.batches {
		counts[i] = b.MetricCount()
	}
	return counts
}

func (r *metricsRecorder) totalMetrics() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, b := range r.batches {
		total += b.MetricCount()
	}
	return total
}

// fingerprintAll renders all captured batches like logsRecorder.fingerprintAll.
func (r *metricsRecorder) fingerprintAll() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.batches))
	for i, b := range r.batches {
		out[i] = fingerprintMetricsValue(b)
	}
	return strings.Join(out, "\n")
}

// fingerprintMetricsValue renders a pmetric.Metrics into a deterministic
// string. Metric order within a scope is deliberately normalized (sorted)
// because the CloudWatch extension builds its output from Go maps, whose
// iteration order is unspecified; per-metric content still must match.
func fingerprintMetricsValue(m pmetric.Metrics) string {
	rms := m.ResourceMetrics()
	resources := make([]string, 0, rms.Len())
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		out := "{res:" + sortedAttrs(rm.Resource().Attributes())
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			out += "|scope:" + sm.Scope().Name()
			ms := sm.Metrics()
			metrics := make([]string, 0, ms.Len())
			for k := 0; k < ms.Len(); k++ {
				mtc := ms.At(k)
				metrics = append(metrics, fmt.Sprintf("%s|%s|%s|%s",
					mtc.Name(), mtc.Type(), mtc.Unit(), fingerprintDataPoints(mtc)))
			}
			// Metric order within a scope is unspecified (the extension builds
			// output from maps), so normalize it before comparing.
			sort.Strings(metrics)
			out += "|metric:" + strings.Join(metrics, "&")
		}
		out += "}"
		resources = append(resources, out)
	}
	sort.Strings(resources)
	return strings.Join(resources, "\n")
}

func fingerprintDataPoints(m pmetric.Metric) string {
	var sb strings.Builder
	doublePoint := func(dp interface {
		Attributes() pcommon.Map
		DoubleValue() float64
	}) {
		sb.WriteString("|dp:")
		sb.WriteString(sortedAttrs(dp.Attributes()))
		sb.WriteByte('=')
		sb.WriteString(strconv.FormatFloat(dp.DoubleValue(), 'g', -1, 64))
	}
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			doublePoint(dps.At(i))
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			doublePoint(dps.At(i))
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			sb.WriteString("|dp:sum=")
			sb.WriteString(strconv.FormatFloat(dps.At(i).Sum(), 'g', -1, 64))
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			sb.WriteString("|dp:")
			sb.WriteString(sortedAttrs(dp.Attributes()))
			sb.WriteString(fmt.Sprintf("#count=%d#sum=%g", dp.Count(), dp.Sum()))
		}
	case pmetric.MetricTypeExponentialHistogram:
		sb.WriteString("|expHist")
	default:
		sb.WriteString("|other")
	}
	return sb.String()
}

// TestConsumeLogs_CompressedBodyEquivalence asserts that a compressed body
// decodes to exactly the same batches and records as the equivalent plain
// body, per codec, on the logs path.
func TestConsumeLogs_CompressedBodyEquivalence(t *testing.T) {
	plainBody := []byte(strings.Join(albSampleLines, "\n") + "\n")
	cfg := newStreamTestConfig(awsLogsEncodingType)

	reference := &logsRecorder{}
	refConsumer := newELBLogsConsumer(t, cfg, reference)
	rec := serveBody(t, cfg, refConsumer, bytes.NewReader(plainBody), "")
	requireBodyRecoded(t, rec, true)
	require.Equal(t, 1, len(reference.batchRecordCounts()), "reference plain body should be one batch")
	expectedTotal := reference.totalRecords()
	require.Equal(t, len(albSampleLines), expectedTotal, "one record per input line")
	expectedFingerprint := reference.fingerprintAll()

	for _, codec := range testCodecs {
		t.Run(codec, func(t *testing.T) {
			compressed := compressBody(t, plainBody, codec)
			got := &logsRecorder{}
			cons := newELBLogsConsumer(t, cfg, got)
			rec := serveBody(t, cfg, cons, bytes.NewReader(compressed), codec)
			requireBodyRecoded(t, rec, true)

			assert.Equal(t, expectedTotal, got.totalRecords(),
				"record total for %q must match the plain body", codec)
			assert.Equal(t, reference.recordBodies(), got.recordBodies(),
				"record bodies and order for %q must match the plain body", codec)
			assert.Equal(t, expectedFingerprint, got.fingerprintAll(),
				"batches and structure for %q must match the plain body", codec)
		})
	}
}

// TestConsumeLogs_UnsupportedContentEncoding asserts unsupported encodings
// are rejected before any record is consumed, and the header comparison is
// case-insensitive.
func TestConsumeLogs_UnsupportedContentEncoding(t *testing.T) {
	cfg := newStreamTestConfig(awsLogsEncodingType)

	t.Run("snappy_default_allowlist", func(t *testing.T) {
		recorder := &logsRecorder{}
		cons := newELBLogsConsumer(t, cfg, recorder)
		rec := serveBodyNoTest(cfg, cons, strings.NewReader("anything"), "snappy")
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Zero(t, recorder.totalRecords(), "no records should be consumed on rejection")
		require.Contains(t, rec.Body.String(), "unsupported content encoding")
	})

	t.Run("multi_value_rejected", func(t *testing.T) {
		recorder := &logsRecorder{}
		cons := newELBLogsConsumer(t, cfg, recorder)
		rec := serveBodyNoTest(cfg, cons, strings.NewReader("anything"), "gzip, zstd")
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Zero(t, recorder.totalRecords())
	})

	t.Run("case_insensitive_accepted", func(t *testing.T) {
		recorder := &logsRecorder{}
		cons := newELBLogsConsumer(t, cfg, recorder)
		body := compressBody(t, []byte(elbControlLine+"\n"), "gzip")
		rec := serveBody(t, cfg, cons, bytes.NewReader(body), "GZIP")
		requireBodyRecoded(t, rec, true)
		require.Zero(t, recorder.totalRecords())
	})
}

// TestConsumeLogs_CompressionAllowList asserts compression_algorithms
// restricts which Content-Encoding values are accepted.
func TestConsumeLogs_CompressionAllowList(t *testing.T) {
	cfg := newStreamTestConfig(awsLogsEncodingType)
	cfg.CompressionAlgorithms = []configcompression.Type{configcompression.TypeGzip}

	t.Run("listed_codec_accepted", func(t *testing.T) {
		recorder := &logsRecorder{}
		cons := newELBLogsConsumer(t, cfg, recorder)
		body := compressBody(t, []byte(elbControlLine+"\n"), "gzip")
		rec := serveBody(t, cfg, cons, bytes.NewReader(body), "gzip")
		requireBodyRecoded(t, rec, true)
	})

	t.Run("unlisted_codec_rejected", func(t *testing.T) {
		recorder := &logsRecorder{}
		cons := newELBLogsConsumer(t, cfg, recorder)
		body := compressBody(t, []byte(elbControlLine+"\n"), "zstd")
		rec := serveBodyNoTest(cfg, cons, bytes.NewReader(body), "zstd")
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Zero(t, recorder.totalRecords())
		require.Contains(t, rec.Body.String(), "unsupported content encoding")
	})
}

// TestConsumeLogs_ControlMessage asserts an ELB control-message body is
// accepted and produces no ConsumeLogs call at all.
func TestConsumeLogs_ControlMessage(t *testing.T) {
	cfg := newStreamTestConfig(awsLogsEncodingType)
	recorder := &logsRecorder{}
	cons := newELBLogsConsumer(t, cfg, recorder)
	body := []byte(elbControlLine + "\n")
	rec := serveBody(t, cfg, cons, bytes.NewReader(body), "")
	requireBodyRecoded(t, rec, true)
	require.Empty(t, recorder.batchRecordCounts(), "control message must not produce a ConsumeLogs call")
	require.Zero(t, recorder.totalRecords())
}

// TestConsumeLogs_FlushItemsBatching asserts record batches honor the
// FlushItems bound and that the total record count is preserved.
func TestConsumeLogs_FlushItemsBatching(t *testing.T) {
	cfg := newStreamTestConfig(awsLogsEncodingType)
	cfg.FlushItems = 2
	records := 5
	body := make([]string, 0, records)
	for i := 0; i < records; i++ {
		body = append(body, albSampleLines[i%len(albSampleLines)])
	}
	recorder := &logsRecorder{}
	cons := newELBLogsConsumer(t, cfg, recorder)
	rec := serveBody(t, cfg, cons, strings.NewReader(strings.Join(body, "\n")+"\n"), "")
	requireBodyRecoded(t, rec, true)

	counts := recorder.batchRecordCounts()
	require.NotEmpty(t, counts)
	for _, c := range counts {
		assert.Greater(t, c, 0, "no empty batches should be forwarded")
		assert.LessOrEqual(t, c, int(cfg.FlushItems), "batch must respect the FlushItems bound")
	}
	// 5 records with a bound of 2 yield exactly ceil(5/2) batches.
	require.Equal(t, (records+1)/2, len(counts), "batch count must be ceil(N/FlushItems)")
	require.Equal(t, records, recorder.totalRecords(), "total records must be preserved")
}

// TestConsumeLogs_FlushBytesBatching asserts the byte-based flush criterion
// splits batches as configured.
func TestConsumeLogs_FlushBytesBatching(t *testing.T) {
	// Line lengths (including newline): 577, 593, 667. Running totals after
	// each line: 577, 1170, 1837. A 1100-byte bound therefore fires exactly
	// after the second line, splitting the body into batches of 2 and 1.
	cfg := newStreamTestConfig(awsLogsEncodingType)
	cfg.FlushBytes = 1100
	recorder := &logsRecorder{}
	cons := newELBLogsConsumer(t, cfg, recorder)
	body := strings.Join(albSampleLines, "\n") + "\n"
	rec := serveBody(t, cfg, cons, strings.NewReader(body), "")
	requireBodyRecoded(t, rec, true)

	counts := recorder.batchRecordCounts()
	require.Equal(t, []int{2, 1}, counts, "FlushBytes=1100 must split the 3-line body into 2+1")
	require.Equal(t, len(albSampleLines), recorder.totalRecords(), "total records must be preserved")
}

// TestConsumeLogs_TruncatedGzip asserts a truncated gzip body is rejected as
// a bad request with no partial records forwarded.
func TestConsumeLogs_TruncatedGzip(t *testing.T) {
	cfg := newStreamTestConfig(awsLogsEncodingType)
	plain := strings.Repeat(strings.Join(albSampleLines, "\n")+"\n", 40)
	full := compressBody(t, []byte(plain), "gzip")
	truncated := full[:len(full)*2/3]

	recorder := &logsRecorder{}
	cons := newELBLogsConsumer(t, cfg, recorder)
	rec := serveBodyNoTest(cfg, cons, bytes.NewReader(truncated), "gzip")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Zero(t, recorder.totalRecords(), "partial decode must not forward records")
}

// pauseableLogsConsumer counts forwarded batches and blocks on the first
// batch until the test resumes it, letting the test observe the
// receiver-side heap while a request is still in flight.
type pauseableLogsConsumer struct {
	blockOnce sync.Once
	blocked   chan struct{}
	resume    chan struct{}

	mu           sync.Mutex
	batchCount   int
	totalRecords int
}

var _ consumer.Logs = (*pauseableLogsConsumer)(nil)

func (*pauseableLogsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func newPauseableLogsConsumer() *pauseableLogsConsumer {
	return &pauseableLogsConsumer{
		blocked: make(chan struct{}),
		resume:  make(chan struct{}),
	}
}

func (p *pauseableLogsConsumer) stats() (batches, records int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.batchCount, p.totalRecords
}

func (p *pauseableLogsConsumer) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	p.mu.Lock()
	first := p.batchCount == 0
	p.batchCount++
	p.totalRecords += logs.LogRecordCount()
	p.mu.Unlock()
	if !first {
		return nil
	}
	p.blockOnce.Do(func() { close(p.blocked) })
	select {
	case <-p.resume:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// heapAlloc returns the live heap allocation in bytes after a full GC.
func heapAlloc() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// writeGzipFile writes the gzip compression of plain to dir/name in 1 MiB
// chunks and returns the output size.
func writeGzipFile(t *testing.T, dir, name, plain string) int64 {
	t.Helper()
	path := filepath.Join(dir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	writer := gzip.NewWriter(file)
	data := []byte(plain)
	for len(data) > 0 {
		n := len(data)
		if n > 1<<20 {
			n = 1 << 20
		}
		_, err := writer.Write(data[:n])
		require.NoError(t, err)
		data = data[n:]
	}
	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Size()
}

// maxInt returns the largest value in a.
func maxInt(a []int) int {
	m := 0
	for _, v := range a {
		if v > m {
			m = v
		}
	}
	return m
}

// poolLines returns the ELB line pool as one string ending in a newline.
func poolLines() string {
	return strings.Join(albSampleLines, "\n") + "\n"
}

// TestConsumeLogs_LargeCompressedBody is a scale test: a large compressed
// body must be fully consumed with bounded batch sizes. Skipped under -short.
func TestConsumeLogs_LargeCompressedBody(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test; run without -short")
	}
	const targetBytes = 40 << 20 // ~40 MiB plain
	pool := poolLines()
	repeats := targetBytes / len(pool)
	require.Greater(t, repeats, 0)
	totalLines := repeats * len(albSampleLines)
	plain := strings.Repeat(pool, repeats)
	t.Logf("plain size: %d bytes (%d lines)", len(plain), totalLines)

	dir := t.TempDir()
	gzSize := writeGzipFile(t, dir, "body.log.gz", plain)
	plain = "" // release the large plain string before the request

	cfg := newStreamTestConfig(awsLogsEncodingType)
	cfg.FlushItems = 1000

	recorder := &logsRecorder{}
	cons := newELBLogsConsumer(t, cfg, recorder)

	file, err := os.Open(filepath.Join(dir, "body.log.gz"))
	require.NoError(t, err)
	start := time.Now()
	rec := serveBody(t, cfg, cons, file, "gzip")
	require.NoError(t, file.Close())

	requireBodyRecoded(t, rec, true)
	require.Equal(t, totalLines, recorder.totalRecords(), "all records must be consumed")

	counts := recorder.batchRecordCounts()
	expectedBatches := (totalLines + int(cfg.FlushItems) - 1) / int(cfg.FlushItems)
	require.Equal(t, expectedBatches, len(counts), "batch count must match ceil(N/FlushItems)")
	for _, c := range counts {
		assert.LessOrEqual(t, c, int(cfg.FlushItems), "batch size must respect the FlushItems bound")
	}
	t.Logf("processed %d lines (gzip %d B) in %v: %d batches, max batch %d",
		totalLines, gzSize, time.Since(start), len(counts), maxInt(counts))
}

// boundedHeapLimit returns the maximum live-heap delta in bytes acceptable
// while paused mid-request: it must fit one decoded batch plus overhead yet
// remain a small fraction of the body.
func boundedHeapLimit(bodyBytes int) int {
	const oneBatchCap = 32 << 20
	limit := bodyBytes / 2
	if limit > oneBatchCap {
		limit = oneBatchCap
	}
	return limit
}

// TestConsumeLogs_BoundedMemory is the memory-regression test: while the
// consumer is paused after the first batch, the receiver's live heap must be
// bounded by the decoder batch size, not the body size. Skipped under -short.
func TestConsumeLogs_BoundedMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("memory test; run without -short")
	}
	cfg := newStreamTestConfig(awsLogsEncodingType)
	cfg.FlushItems = 1000
	pool := poolLines()

	for _, sizeBytes := range []int{40 << 20, 80 << 20, 160 << 20} {
		size := sizeBytes
		t.Run(fmt.Sprintf("%dMiB", size>>20), func(t *testing.T) {
			if t.Failed() {
				t.Skip("previous subtest failed")
			}
			repeats := size / len(pool)
			require.Greater(t, repeats, 0)
			plain := strings.Repeat(pool, repeats)
			dir := t.TempDir()
			writeGzipFile(t, dir, "body.log.gz", plain)
			plain = "" // release before the request

			// Reset the heap so the baseline is taken after this subtest's
			// fixtures are allocated and released.
			base := int64(heapAlloc())

			pc := newPauseableLogsConsumer()
			cons := newELBLogsConsumer(t, cfg, pc)

			file, err := os.Open(filepath.Join(dir, "body.log.gz"))
			require.NoError(t, err)
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				defer file.Close()
				done <- serveBodyNoTest(cfg, cons, file, "gzip")
			}()

			select {
			case <-pc.blocked:
			case <-time.After(60 * time.Second):
				close(pc.resume) // unblock the request before failing
				t.Fatal("first batch was not delivered before the whole body was consumed: decode is not streaming")
			}

			batches, records := pc.stats()
			delta := int64(heapAlloc()) - base
			limit := int64(boundedHeapLimit(size))
			t.Logf("%d MiB body: live-heap delta while paused after first batch = %d B (limit %d B; %d batches, %d records)",
				size>>20, delta, limit, batches, records)
			close(pc.resume)

			select {
			case rec := <-done:
				require.Equal(t, http.StatusOK, rec.Code)
			case <-time.After(120 * time.Second):
				t.Fatal("request did not complete within 120s after resume")
			}

			require.Less(t, delta, limit,
				"live-heap delta (%d B) while paused must be bounded, not grow with the %d B body", delta, size)
		})
	}
}

// TestConsumeMetrics_CompressedBodyEquivalence mirrors the logs codec
// equivalence test on the metrics path.
func TestConsumeMetrics_CompressedBodyEquivalence(t *testing.T) {
	plainBody := []byte(strings.Join(cwSampleLines, "\n") + "\n")
	cfg := newStreamTestConfig(cwMetricsEncodingType)

	reference := &metricsRecorder{}
	refConsumer := newCWMetricsConsumer(t, cfg, reference)
	rec := serveBody(t, cfg, refConsumer, bytes.NewReader(plainBody), "")
	requireBodyRecoded(t, rec, true)
	require.NotEmpty(t, reference.batchMetricCounts(), "plain body must be consumed")
	expectedTotal := reference.totalMetrics()
	require.Equal(t, len(cwSampleLines), expectedTotal, "one metric per input line")
	expectedFingerprint := reference.fingerprintAll()

	for _, codec := range testCodecs {
		t.Run(codec, func(t *testing.T) {
			compressed := compressBody(t, plainBody, codec)
			got := &metricsRecorder{}
			cons := newCWMetricsConsumer(t, cfg, got)
			rec := serveBody(t, cfg, cons, bytes.NewReader(compressed), codec)
			requireBodyRecoded(t, rec, true)

			assert.Equal(t, expectedTotal, got.totalMetrics(),
				"metric total for %q must match the plain body", codec)
			assert.Equal(t, expectedFingerprint, got.fingerprintAll(),
				"batches and metrics for %q must match the plain body", codec)
		})
	}
}

// TestConsumeMetrics_UnsupportedContentEncoding asserts the allow-list
// applies to the metrics path too.
func TestConsumeMetrics_UnsupportedContentEncoding(t *testing.T) {
	cfg := newStreamTestConfig(cwMetricsEncodingType)
	recorder := &metricsRecorder{}
	cons := newCWMetricsConsumer(t, cfg, recorder)
	rec := serveBodyNoTest(cfg, cons, strings.NewReader("anything"), "br")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Zero(t, recorder.totalMetrics())
	require.Contains(t, rec.Body.String(), "unsupported content encoding")
}

// TestConsumeMetrics_FlushItemsBatching mirrors the logs FlushItems test.
func TestConsumeMetrics_FlushItemsBatching(t *testing.T) {
	cfg := newStreamTestConfig(cwMetricsEncodingType)
	cfg.FlushItems = 2
	recorder := &metricsRecorder{}
	cons := newCWMetricsConsumer(t, cfg, recorder)
	body := strings.Join(cwSampleLines, "\n") + "\n"
	rec := serveBody(t, cfg, cons, strings.NewReader(body), "")
	requireBodyRecoded(t, rec, true)

	total := recorder.totalMetrics()
	require.Equal(t, len(cwSampleLines), total, "total metrics must be preserved")
	counts := recorder.batchMetricCounts()
	require.Equal(t, (total+1)/2, len(counts), "batch count must be ceil(N/FlushItems)")
	for _, c := range counts {
		assert.Greater(t, c, 0, "no empty batches should be forwarded")
		assert.LessOrEqual(t, c, int(cfg.FlushItems), "batch must respect the FlushItems bound")
	}
}
