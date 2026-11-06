// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package httplogsreceiver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	storage "go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/filetracker"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/metadatatest"
)

const (
	testFileName = "/var/log/app.log"
	// testBody holds the four lines used to drive the Consume tests.
	testBody = "line1\nline22\nline333\nline444\n"
)

// failingCheckpointer always fails, used to exercise the on_error policies.
type failingCheckpointer struct{ err error }

func (f failingCheckpointer) Load(context.Context, string) (int64, bool, error) {
	return 0, false, f.err
}
func (f failingCheckpointer) Save(context.Context, string, int64) error { return f.err }
func (f failingCheckpointer) Delete(context.Context, string) error      { return f.err }
func (f failingCheckpointer) Close(context.Context) error               { return nil }

// fakeCheckpointStore serves a fixed checkpoint and records its mutations,
// used to exercise the stale_checkpoint policies.
type fakeCheckpointStore struct {
	checkpoint  int64
	found       bool
	deleteCalls int
	saveCalls   []int64
}

func (f *fakeCheckpointStore) Load(context.Context, string) (int64, bool, error) {
	return f.checkpoint, f.found, nil
}

func (f *fakeCheckpointStore) Save(_ context.Context, _ string, v int64) error {
	f.saveCalls = append(f.saveCalls, v)
	f.checkpoint = v
	f.found = true
	return nil
}

func (f *fakeCheckpointStore) Delete(context.Context, string) error {
	f.deleteCalls++
	f.found = false
	return nil
}

func (f *fakeCheckpointStore) Close(context.Context) error { return nil }

// memStorageClient is an in-memory storage.Client for wiring the storage
// backend in tests.
type memStorageClient struct {
	entries map[string][]byte
	closed  bool
}

func newMemStorageClient() *memStorageClient {
	return &memStorageClient{entries: make(map[string][]byte)}
}

func (c *memStorageClient) Get(_ context.Context, key string) ([]byte, error) {
	v, ok := c.entries[key]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), v...), nil
}

func (c *memStorageClient) Set(_ context.Context, key string, value []byte) error {
	c.entries[key] = append([]byte(nil), value...)
	return nil
}

func (c *memStorageClient) Delete(_ context.Context, key string) error {
	delete(c.entries, key)
	return nil
}

func (c *memStorageClient) Batch(ctx context.Context, ops ...*storage.Operation) error {
	for _, op := range ops {
		switch op.Type {
		case storage.Get:
			v, err := c.Get(ctx, op.Key)
			if err != nil {
				return err
			}
			op.Value = v
		case storage.Set:
			if err := c.Set(ctx, op.Key, op.Value); err != nil {
				return err
			}
		case storage.Delete:
			if err := c.Delete(ctx, op.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *memStorageClient) Close(context.Context) error {
	c.closed = true
	return nil
}

// fakeStorageExtension is a minimal storage.Extension returning a pre-built
// client.
type fakeStorageExtension struct{ client storage.Client }

func (fakeStorageExtension) Start(context.Context, component.Host) error { return nil }
func (fakeStorageExtension) Shutdown(context.Context) error              { return nil }
func (e fakeStorageExtension) GetClient(context.Context, component.Kind, component.ID, string) (storage.Client, error) {
	return e.client, nil
}

// lineLogsDecoderFactory implements both plog.Unmarshaler and
// encoding.LogsDecoderFactory: the streaming decoder reads the (possibly
// tracking-wrapped) stream line by line and emits recordsPerLine log records
// per line, reporting the decoded byte offset. It stands in for a real
// encoding extension so that the receiver's bookkeeping can be exercised
// against a well-behaved 1-line-per-batch stream.
type lineLogsDecoderFactory struct {
	recordsPerLine int
}

var (
	_ plog.Unmarshaler            = (*lineLogsDecoderFactory)(nil)
	_ encoding.LogsDecoderFactory = (*lineLogsDecoderFactory)(nil)
)

func (f *lineLogsDecoderFactory) UnmarshalLogs(_ []byte) (plog.Logs, error) {
	return plog.NewLogs(), nil
}

func (f *lineLogsDecoderFactory) NewLogsDecoder(r io.Reader, _ ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	n := f.recordsPerLine
	if n <= 0 {
		n = 1
	}
	return &lineLogsDecoder{input: bufio.NewReader(r), recordsPerLine: n}, nil
}

type lineLogsDecoder struct {
	input          *bufio.Reader
	recordsPerLine int
	offset         int64
}

func (d *lineLogsDecoder) DecodeLogs() (plog.Logs, error) {
	line, err := d.input.ReadString('\n')
	d.offset += int64(len(line))
	logs := plog.NewLogs()
	if line != "" {
		records := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
		for i := 0; i < d.recordsPerLine; i++ {
			records.AppendEmpty().Body().SetStr(strings.TrimSpace(line))
		}
	}
	if err != nil {
		if line == "" {
			return plog.NewLogs(), err
		}
		// Deliver the final batch together with the terminating error.
		return logs, err
	}
	return logs, nil
}

func (d *lineLogsDecoder) Offset() int64 { return d.offset }

// fixedOffsetLogsDecoderFactory emits prepared batches while keeping its
// offset at a position unreachable from the stream, simulating a decoder
// whose offset is not comparable to stream byte positions (the anchorLost
// degradation path).
type fixedOffsetLogsDecoderFactory struct {
	batches []plog.Logs
}

var (
	_ plog.Unmarshaler            = (*fixedOffsetLogsDecoderFactory)(nil)
	_ encoding.LogsDecoderFactory = (*fixedOffsetLogsDecoderFactory)(nil)
)

func (f *fixedOffsetLogsDecoderFactory) UnmarshalLogs(_ []byte) (plog.Logs, error) {
	return plog.NewLogs(), nil
}

func (f *fixedOffsetLogsDecoderFactory) NewLogsDecoder(_ io.Reader, _ ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	return &fixedOffsetLogsDecoder{batches: f.batches, offset: 1 << 20}, nil
}

type fixedOffsetLogsDecoder struct {
	batches []plog.Logs
	offset  int64
}

func (d *fixedOffsetLogsDecoder) DecodeLogs() (plog.Logs, error) {
	if len(d.batches) == 0 {
		return plog.NewLogs(), io.EOF
	}
	b := d.batches[0]
	d.batches = d.batches[1:]
	return b, nil
}

func (d *fixedOffsetLogsDecoder) Offset() int64 { return d.offset }

// newTestTelemetry returns fresh test telemetry, shut down on test cleanup.
func newTestTelemetry(t *testing.T) *componenttest.Telemetry {
	t.Helper()
	tt := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) })
	return tt
}

// newTestTracker builds an enabled file tracker using the in-memory backend,
// wired to the test telemetry so emitted metrics can be asserted.
func newTestTracker(t *testing.T, cfg FileTrackingConfig, tt *componenttest.Telemetry) *fileTracker {
	t.Helper()
	cfg.Enabled = true
	if cfg.FileAttribute == "" {
		cfg.FileAttribute = defaultFileAttribute
	}
	config := createDefaultConfig().(*Config)
	config.FileTracking = cfg
	ft, err := newFileTracker(context.Background(), config, metadatatest.NewSettings(tt), componenttest.NewNopHost())
	require.NoError(t, err)
	require.NotNil(t, ft)
	t.Cleanup(func() { ft.Shutdown(context.Background()) })
	return ft
}

// trackedConsumer wires the logs consumer under test against the recording
// consumer, the fake unmarshaler, and the tracker.
func trackedConsumer(set receiver.Settings, rc *logsRecordConsumer, unmarshaler plog.Unmarshaler, ft *fileTracker) *logsConsumer {
	return &logsConsumer{
		config:      createDefaultConfig().(*Config),
		settings:    set,
		consumer:    rc,
		unmarshaler: unmarshaler,
		tracker:     ft,
	}
}

// recordFileLine returns the value of the file.line attribute of the idx-th
// record of the first resource/scope of logs, if present.
func recordFileLine(logs plog.Logs, idx int) (int64, bool) {
	rec := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(idx)
	v, ok := rec.Attributes().Get(fileLineKey)
	if !ok {
		return 0, false
	}
	return v.Int(), true
}

func requireNoFileLine(t *testing.T, logs plog.Logs) {
	t.Helper()
	recs := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	for i := 0; i < recs.Len(); i++ {
		_, ok := recs.At(i).Attributes().Get(fileLineKey)
		require.Falsef(t, ok, "record %d carries an unexpected %s attribute", i, fileLineKey)
	}
}

func TestNewFileTracker(t *testing.T) {
	t.Run("Disabled", func(t *testing.T) {
		tt := newTestTelemetry(t)
		config := createDefaultConfig().(*Config)
		ft, err := newFileTracker(context.Background(), config, metadatatest.NewSettings(tt), componenttest.NewNopHost())
		require.NoError(t, err)
		require.Nil(t, ft)
	})

	t.Run("InMemory", func(t *testing.T) {
		tt := newTestTelemetry(t)
		ft := newTestTracker(t, FileTrackingConfig{Enabled: true}, tt)
		assert.Equal(t, "inmem", ft.backend)
		assert.IsType(t, &filetracker.MemCheckpointer{}, ft.checkpointer)
	})

	t.Run("Storage", func(t *testing.T) {
		tt := newTestTelemetry(t)
		storageID := component.MustNewIDWithName("memstorage", "main")
		config := createDefaultConfig().(*Config)
		config.FileTracking.Enabled = true
		config.StorageID = &storageID
		client := newMemStorageClient()
		host := hostWithExtensions{
			extensions: map[component.ID]component.Component{
				storageID: fakeStorageExtension{client: client},
			},
		}
		ft, err := newFileTracker(context.Background(), config, metadatatest.NewSettings(tt), host)
		require.NoError(t, err)
		require.NotNil(t, ft)
		assert.Equal(t, "storage", ft.backend)
		_, isMem := ft.checkpointer.(*filetracker.MemCheckpointer)
		require.False(t, isMem)
		t.Cleanup(func() {
			ft.Shutdown(context.Background())
			assert.True(t, client.closed)
		})
	})

	t.Run("StorageNotFound", func(t *testing.T) {
		tt := newTestTelemetry(t)
		storageID := component.MustNewIDWithName("memstorage", "main")
		config := createDefaultConfig().(*Config)
		config.FileTracking.Enabled = true
		config.StorageID = &storageID
		_, err := newFileTracker(context.Background(), config, metadatatest.NewSettings(tt), componenttest.NewNopHost())
		require.ErrorContains(t, err, "not found")
	})

	t.Run("StorageWrongType", func(t *testing.T) {
		tt := newTestTelemetry(t)
		storageID := component.MustNewIDWithName("memstorage", "main")
		config := createDefaultConfig().(*Config)
		config.FileTracking.Enabled = true
		config.StorageID = &storageID
		host := hostWithExtensions{
			extensions: map[component.ID]component.Component{
				storageID: plogUnmarshalerExtension{},
			},
		}
		_, err := newFileTracker(context.Background(), config, metadatatest.NewSettings(tt), host)
		require.ErrorContains(t, err, "not a storage extension")
	})
}

func TestEstablish_StoreError(t *testing.T) {
	testErr := errors.New("store down")
	t.Run("Fail", func(t *testing.T) {
		tt := newTestTelemetry(t)
		ft := newTestTracker(t, FileTrackingConfig{OnError: onErrorPolicyFail}, tt)
		ft.checkpointer = failingCheckpointer{err: testErr}

		body := strings.NewReader("a\nb\n")
		_, session, status, err := ft.establish(context.Background(), body, map[string]string{defaultFileAttribute: testFileName})
		require.Error(t, err)
		require.Nil(t, session)
		require.Equal(t, http.StatusServiceUnavailable, status)
		metadatatest.AssertEqualHttplogsReceiverFiletrackingErrorsTotal(t, tt,
			[]metricdata.DataPoint[int64]{{
				Attributes: attribute.NewSet(
					attribute.String("backend", "inmem"),
					attribute.String("on_error", "fail"),
					attribute.String("op", "load"),
				),
				Value: 1,
			}}, metricdatatest.IgnoreTimestamp())
	})

	t.Run("Degrade", func(t *testing.T) {
		tt := newTestTelemetry(t)
		ft := newTestTracker(t, FileTrackingConfig{OnError: onErrorPolicyDegrade}, tt)
		ft.checkpointer = failingCheckpointer{err: testErr}

		body := strings.NewReader("a\nb\n")
		_, session, status, err := ft.establish(context.Background(), body, map[string]string{defaultFileAttribute: testFileName})
		require.NoError(t, err)
		require.Nil(t, session)
		require.Zero(t, status)
		metadatatest.AssertEqualHttplogsReceiverFiletrackingErrorsTotal(t, tt,
			[]metricdata.DataPoint[int64]{{
				Attributes: attribute.NewSet(
					attribute.String("backend", "inmem"),
					attribute.String("on_error", "degrade"),
					attribute.String("op", "load"),
				),
				Value: 1,
			}}, metricdatatest.IgnoreTimestamp())
	})
}

func TestEstablish_NoFileName(t *testing.T) {
	tt := newTestTelemetry(t)
	ft := newTestTracker(t, FileTrackingConfig{FileAttribute: defaultFileAttribute}, tt)

	body := strings.NewReader("a\nb\n")
	gotBody, session, status, err := ft.establish(context.Background(), body, map[string]string{"unrelated": "value"})
	require.NoError(t, err)
	require.Nil(t, session)
	require.Zero(t, status)
	assert.Same(t, body, gotBody)

	// A nil tracker (file tracking disabled) never tracks.
	var nilTracker *fileTracker
	gotBody, session, status, err = nilTracker.establish(context.Background(), body, map[string]string{defaultFileAttribute: testFileName})
	require.NoError(t, err)
	require.Nil(t, session)
	require.Zero(t, status)
	assert.Same(t, body, gotBody)
}

func TestConsume_FreshDelivery(t *testing.T) {
	ctx := context.Background()
	tt := newTestTelemetry(t)
	ft := newTestTracker(t, FileTrackingConfig{}, tt)
	rc := &logsRecordConsumer{}
	lc := trackedConsumer(metadatatest.NewSettings(tt), rc, &lineLogsDecoderFactory{}, ft)

	status, err := lc.Consume(ctx, strings.NewReader(testBody), map[string]string{defaultFileAttribute: testFileName})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)

	// Each line became one forward of one record, stamped 1..4 in order.
	require.Len(t, rc.results, 4)
	var lines []int64
	for i, l := range rc.results {
		v, ok := recordFileLine(l, 0)
		require.Truef(t, ok, "record %d carries no %s attribute", i, fileLineKey)
		lines = append(lines, v)
	}
	require.Equal(t, []int64{1, 2, 3, 4}, lines)

	// The checkpoint advanced to the last exported line.
	base, found, lerr := ft.checkpointer.Load(ctx, testFileName)
	require.NoError(t, lerr)
	require.True(t, found)
	require.Equal(t, int64(4), base)

	metadatatest.AssertEqualHttplogsReceiverFiletrackingResumedFilesTotal(t, tt,
		[]metricdata.DataPoint[int64]{{
			Attributes: attribute.NewSet(attribute.String("result", "fresh")),
			Value:      1,
		}}, metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualHttplogsReceiverFiletrackingTrackedFiles(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 1}}, metricdatatest.IgnoreTimestamp())

	// Nothing was skipped and nothing failed.
	for _, name := range []string{
		"otelcol_httplogs_receiver.filetracking.skipped_lines_total",
		"otelcol_httplogs_receiver.filetracking.line_attr_batches_skipped_total",
		"otelcol_httplogs_receiver.filetracking.errors_total",
	} {
		_, mErr := tt.GetMetric(name)
		require.ErrorContains(t, mErr, "not found")
	}
}

func TestConsume_ResumedDelivery(t *testing.T) {
	ctx := context.Background()
	tt := newTestTelemetry(t)
	ft := newTestTracker(t, FileTrackingConfig{}, tt)
	// A previous delivery already exported all four lines of the file.
	require.NoError(t, ft.checkpointer.Save(ctx, testFileName, 4))

	rc := &logsRecordConsumer{}
	lc := trackedConsumer(metadatatest.NewSettings(tt), rc, &lineLogsDecoderFactory{}, ft)

	status, err := lc.Consume(ctx, strings.NewReader(testBody), map[string]string{defaultFileAttribute: testFileName})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, rc.results, "a fully-exported file must not be re-forwarded")

	// The checkpoint is unchanged.
	base, found, lerr := ft.checkpointer.Load(ctx, testFileName)
	require.NoError(t, lerr)
	require.True(t, found)
	require.Equal(t, int64(4), base)

	metadatatest.AssertEqualHttplogsReceiverFiletrackingResumedFilesTotal(t, tt,
		[]metricdata.DataPoint[int64]{{
			Attributes: attribute.NewSet(attribute.String("result", "resumed")),
			Value:      1,
		}}, metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualHttplogsReceiverFiletrackingSkippedLinesTotal(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 4}}, metricdatatest.IgnoreTimestamp())
	// No checkpoint was saved during this request.
	metadatatest.AssertEqualHttplogsReceiverFiletrackingTrackedFiles(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 0}}, metricdatatest.IgnoreTimestamp())
}

func TestConsume_StaleCheckpoint(t *testing.T) {
	// The checkpoint claims four exported lines but the delivered file has two.
	shortBody := "one\ntwo\n"
	common := map[string]string{defaultFileAttribute: testFileName}

	t.Run("Reject", func(t *testing.T) {
		tt := newTestTelemetry(t)
		ft := newTestTracker(t, FileTrackingConfig{}, tt)
		store := &fakeCheckpointStore{checkpoint: 4, found: true}
		ft.checkpointer = store
		rc := &logsRecordConsumer{}
		lc := trackedConsumer(metadatatest.NewSettings(tt), rc, &lineLogsDecoderFactory{}, ft)

		status, err := lc.Consume(context.Background(), strings.NewReader(shortBody), common)
		require.Error(t, err)
		require.Equal(t, http.StatusBadRequest, status)
		require.Empty(t, rc.results)
		require.Zero(t, store.deleteCalls, "reject must not clear the entry")
		require.True(t, store.found)

		metadatatest.AssertEqualHttplogsReceiverFiletrackingResumedFilesTotal(t, tt,
			[]metricdata.DataPoint[int64]{{
				Attributes: attribute.NewSet(attribute.String("result", "stale")),
				Value:      1,
			}}, metricdatatest.IgnoreTimestamp())
	})

	t.Run("Reset", func(t *testing.T) {
		tt := newTestTelemetry(t)
		ft := newTestTracker(t, FileTrackingConfig{StaleCheckpoint: staleCheckpointReset}, tt)
		store := &fakeCheckpointStore{checkpoint: 4, found: true}
		ft.checkpointer = store
		rc := &logsRecordConsumer{}
		lc := trackedConsumer(metadatatest.NewSettings(tt), rc, &lineLogsDecoderFactory{}, ft)

		status, err := lc.Consume(context.Background(), strings.NewReader(shortBody), common)
		require.Error(t, err)
		require.Equal(t, http.StatusServiceUnavailable, status)
		require.Empty(t, rc.results)
		require.Equal(t, 1, store.deleteCalls, "reset must clear the entry")
		require.False(t, store.found)

		metadatatest.AssertEqualHttplogsReceiverFiletrackingResumedFilesTotal(t, tt,
			[]metricdata.DataPoint[int64]{{
				Attributes: attribute.NewSet(attribute.String("result", "stale")),
				Value:      1,
			}}, metricdatatest.IgnoreTimestamp())
	})
}

func TestConsume_GuardViolationSkipsFileLine(t *testing.T) {
	ctx := context.Background()
	tt := newTestTelemetry(t)
	ft := newTestTracker(t, FileTrackingConfig{}, tt)
	rc := &logsRecordConsumer{}
	// Two records per line breaks the 1:1 guard for every batch.
	lc := trackedConsumer(metadatatest.NewSettings(tt), rc, &lineLogsDecoderFactory{recordsPerLine: 2}, ft)

	status, err := lc.Consume(ctx, strings.NewReader("alpha\nbeta\n"), map[string]string{defaultFileAttribute: testFileName})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)

	// The batches are still forwarded, without the file.line attribute.
	require.Len(t, rc.results, 2)
	for _, l := range rc.results {
		requireNoFileLine(t, l)
	}

	// Checkpointing is line-based and still advances.
	base, found, lerr := ft.checkpointer.Load(ctx, testFileName)
	require.NoError(t, lerr)
	require.True(t, found)
	require.Equal(t, int64(2), base)

	metadatatest.AssertEqualHttplogsReceiverFiletrackingLineAttrBatchesSkippedTotal(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 2}}, metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualHttplogsReceiverFiletrackingResumedFilesTotal(t, tt,
		[]metricdata.DataPoint[int64]{{
			Attributes: attribute.NewSet(attribute.String("result", "fresh")),
			Value:      1,
		}}, metricdatatest.IgnoreTimestamp())
}

func TestConsume_AnchorLostSkipsTracking(t *testing.T) {
	ctx := context.Background()
	tt := newTestTelemetry(t)
	ft := newTestTracker(t, FileTrackingConfig{}, tt)
	batch := plog.NewLogs()
	batch.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("alpha")
	rc := &logsRecordConsumer{}
	lc := trackedConsumer(metadatatest.NewSettings(tt), rc, &fixedOffsetLogsDecoderFactory{batches: []plog.Logs{batch}}, ft)

	status, err := lc.Consume(ctx, strings.NewReader("alpha\n"), map[string]string{defaultFileAttribute: testFileName})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.Len(t, rc.results, 1)

	// Stamping is suspended: the record is forwarded without file.line.
	requireNoFileLine(t, rc.results[0])

	// The offset was not byte-based, so no checkpoint may have been written.
	_, found, lerr := ft.checkpointer.Load(ctx, testFileName)
	require.NoError(t, lerr)
	require.False(t, found)

	metadatatest.AssertEqualHttplogsReceiverFiletrackingLineAttrBatchesSkippedTotal(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 1}}, metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualHttplogsReceiverFiletrackingResumedFilesTotal(t, tt,
		[]metricdata.DataPoint[int64]{{
			Attributes: attribute.NewSet(attribute.String("result", "fresh")),
			Value:      1,
		}}, metricdatatest.IgnoreTimestamp())
	// No checkpoint was saved during this request.
	metadatatest.AssertEqualHttplogsReceiverFiletrackingTrackedFiles(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 0}}, metricdatatest.IgnoreTimestamp())
}

func TestConsume_WholeBodyTracked(t *testing.T) {
	ctx := context.Background()
	tt := newTestTelemetry(t)
	ft := newTestTracker(t, FileTrackingConfig{}, tt)
	batch := plog.NewLogs()
	records := batch.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i := 1; i <= 3; i++ {
		records.AppendEmpty().Body().SetStr(fmt.Sprintf("line%d", i))
	}
	rc := &logsRecordConsumer{}
	lc := trackedConsumer(metadatatest.NewSettings(tt), rc, plogUnmarshalerExtension{logs: batch}, ft)

	status, err := lc.Consume(ctx, strings.NewReader("line1\nline2\nline3\n"), map[string]string{defaultFileAttribute: testFileName})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)

	require.Len(t, rc.results, 1)
	for i := 0; i < 3; i++ {
		v, ok := recordFileLine(rc.results[0], i)
		require.Truef(t, ok, "record %d carries no %s attribute", i, fileLineKey)
		require.EqualValues(t, i+1, v)
	}

	base, found, lerr := ft.checkpointer.Load(ctx, testFileName)
	require.NoError(t, lerr)
	require.True(t, found)
	require.Equal(t, int64(3), base)

	metadatatest.AssertEqualHttplogsReceiverFiletrackingResumedFilesTotal(t, tt,
		[]metricdata.DataPoint[int64]{{
			Attributes: attribute.NewSet(attribute.String("result", "fresh")),
			Value:      1,
		}}, metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualHttplogsReceiverFiletrackingTrackedFiles(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 1}}, metricdatatest.IgnoreTimestamp())
}
