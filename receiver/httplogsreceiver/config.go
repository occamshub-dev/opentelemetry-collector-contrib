// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package httplogsreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver"

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/decode"
)

const (
	defaultFileAttribute   = "file.path"
	staleCheckpointReject  = "reject"
	staleCheckpointReset   = "reset"
	onErrorPolicyDegrade   = "degrade"
	onErrorPolicyFail      = "fail"
	defaultEntryTTL        = 24 * time.Hour
	defaultMaxTrackedFiles = 1000
)

var (
	errRecordTypeEncodingSet    = errors.New("record_type must not be set when encoding is set")
	errFlushItemsNegative       = errors.New("flush_items must not be negative")
	errFlushBytesNegative       = errors.New("flush_bytes must not be negative")
	errDuplicateCompressionAlgo = errors.New("compression_algorithms must not contain duplicates")
	errFileAttributeEmpty       = errors.New("file_tracking.file_attribute must not be empty when file_tracking.enabled is set")
	errStaleCheckpointPolicy    = fmt.Errorf("file_tracking.stale_checkpoint must be %q or %q", staleCheckpointReject, staleCheckpointReset)
	errOnErrorPolicy            = fmt.Errorf("file_tracking.on_error must be %q or %q", onErrorPolicyDegrade, onErrorPolicyFail)
	errEntryTTLNegative         = errors.New("file_tracking.entry_ttl must not be negative")
	errMaxTrackedFilesNegative  = errors.New("file_tracking.max_tracked_files must not be negative")
)

// defaultCompressionAlgorithms is the set of request body encodings
// accepted from the Content-Encoding header when compression_algorithms
// is unset: identity (no decompression) plus the codecs this receiver
// can decompress, excluding snappy.
var defaultCompressionAlgorithms = []configcompression.Type{
	"",
	configcompression.TypeGzip,
	configcompression.TypeZstd,
	configcompression.TypeZlib,
	configcompression.TypeDeflate,
}

type Config struct {
	// ServerConfig is used to set up the HTTP logs delivery
	// endpoint. The HTTP logs delivery stream expects an HTTPS
	// endpoint, so TLSs must be used to enable that.
	confighttp.ServerConfig `mapstructure:",squash"`
	// Encoding identifies the encoding of records received.
	//
	// Defaults to telemetry-specific encodings: "cwlog" for logs,
	// and "cwmetrics" for metrics. This default behavior is
	// deprecated as of v0.149.0, and in the future it will be
	// required to specify the encoding explicitly.
	Encoding string `mapstructure:"encoding"`
	// RecordType is an alias for Encoding for backwards compatibility.
	// It is an error to specify both encoding and record_type.
	//
	// Deprecated: [v0.121.0] use Encoding instead.
	RecordType string `mapstructure:"record_type"`
	// AccessKey is checked against the one received with each request.
	AccessKey configopaque.String `mapstructure:"access_key"`
	// CompressionAlgorithms is the allow-list of request body
	// encodings accepted from the Content-Encoding header. identity
	// (the empty value) and the encodings listed here are decompressed
	// in place of the raw body before it is handed to the decoder.
	//
	// Defaults to [none, gzip, zstd, zlib, deflate].
	CompressionAlgorithms []configcompression.Type `mapstructure:"compression_algorithms"`
	// FlushItems is the maximum number of decoded records a streaming
	// decoder holds before handing a batch downstream.
	//
	// 0 means use the decoder default (1000).
	FlushItems int64 `mapstructure:"flush_items"`
	// FlushBytes is the maximum number of decoded body bytes a streaming
	// decoder holds before handing a batch downstream.
	//
	// 0 means use the decoder default (1 MiB).
	FlushBytes int64 `mapstructure:"flush_bytes"`
	// StorageID identifies the storage extension whose client is used to
	// persist file tracking checkpoints (see FileTracking). Checkpoints are
	// then shared across all replicas of the receiver.
	//
	// When unset (and FileTracking.Enabled is true), checkpoints are kept in
	// process memory only, which is only safe for single-replica deployments.
	StorageID *component.ID `mapstructure:"storage"`
	// FileTracking configures per-file, per-line delivery tracking:
	// resending a file continues from the last successfully exported line
	// instead of reprocessing from the beginning. Disabled by default.
	FileTracking FileTrackingConfig `mapstructure:"file_tracking"`
}

// FileTrackingConfig configures per-file, per-line delivery tracking.
//
// The delivered file must be identified in the X-Common-Attributes request
// header under the FileAttribute key. When enabled, every log record is
// additionally stamped with a file.line record attribute carrying its 1-based
// line number within the delivered file (see the README).
type FileTrackingConfig struct {
	// Enabled turns on file tracking. Default: false.
	Enabled bool `mapstructure:"enabled"`

	// FileAttribute is the key in the X-Common-Attributes header that holds
	// the delivered file's name (e.g. "somelocation/somefile.gz"). It is used
	// verbatim as part of the checkpoint store key.
	//
	// Default: "file.path".
	FileAttribute string `mapstructure:"file_attribute"`

	// MaxTrackedFiles caps the number of tracked files in in-memory mode
	// (i.e. when StorageID is unset); the least recently tracked file is
	// evicted when the cap is exceeded. Ignored with a storage extension.
	//
	// 0 means unlimited. Default: 1000.
	MaxTrackedFiles int `mapstructure:"max_tracked_files"`

	// EntryTTL is the sliding time-to-live of a checkpoint entry: an entry
	// that has not been updated within this duration is treated as absent.
	// It bounds the window in which a stale checkpoint (file shrank or was
	// replaced under the same name) can cause rejected deliveries.
	//
	// 0 means entries never expire. Default: 24h.
	EntryTTL time.Duration `mapstructure:"entry_ttl"`

	// StaleCheckpoint controls the response when a resended file ends before
	// the recorded checkpoint (it was truncated, shrunk, or is a different
	// file reusing the name):
	//   - "reject" (default) returns 400; the entry clears on EntryTTL expiry,
	//     or on collector restart in in-memory mode.
	//   - "reset" clears the entry so the file is reprocessed from line 1 (the
	//     streamed request path reports 503 to trigger the sender's retry).
	StaleCheckpoint string `mapstructure:"stale_checkpoint"`

	// OnError controls the behavior when the checkpoint store is unavailable
	// mid-delivery:
	//   - "degrade" (default): continue untracked (at-least-once is preserved).
	//   - "fail": return 503 so the sender retries once the store is back.
	OnError string `mapstructure:"on_error"`
}

// contentEncodingAllowed reports whether a Content-Encoding header value is
// accepted. It compares case-insensitively, folds the "none" and "identity"
// tokens into the empty encoding, and (when compression_algorithms is unset)
// accepts the default allow-list.
func (c *Config) contentEncodingAllowed(value string) bool {
	encoding := decode.NormalizeEncoding(value)
	algorithms := c.CompressionAlgorithms
	if len(algorithms) == 0 {
		algorithms = defaultCompressionAlgorithms
	}
	for _, a := range algorithms {
		if decode.NormalizeEncoding(string(a)) == encoding {
			return true
		}
	}
	return false
}

// Validate checks that the endpoint and record type exist and
// are valid.
func (c *Config) Validate() error {
	if c.NetAddr.Endpoint == "" {
		return errors.New("must specify endpoint")
	}
	if c.RecordType != "" && c.Encoding != "" {
		return errRecordTypeEncodingSet
	}
	if c.FlushItems < 0 {
		return errFlushItemsNegative
	}
	if c.FlushBytes < 0 {
		return errFlushBytesNegative
	}
	seen := make(map[configcompression.Type]struct{}, len(c.CompressionAlgorithms))
	for _, a := range c.CompressionAlgorithms {
		if !decode.IsSupported(a) {
			return fmt.Errorf("unsupported encoding in compression_algorithms: %q", a)
		}
		if _, dup := seen[a]; dup {
			return fmt.Errorf("%w: %q", errDuplicateCompressionAlgo, a)
		}
		seen[a] = struct{}{}
	}
	if err := c.FileTracking.Validate(); err != nil {
		return err
	}
	return nil
}

// Validate checks the file tracking configuration.
func (ft *FileTrackingConfig) Validate() error {
	if ft.Enabled && ft.FileAttribute == "" {
		return errFileAttributeEmpty
	}
	switch ft.StaleCheckpoint {
	case "", staleCheckpointReject, staleCheckpointReset: // "" = unset, defaults at run time.
	default:
		return fmt.Errorf("%w: got %q", errStaleCheckpointPolicy, ft.StaleCheckpoint)
	}
	switch ft.OnError {
	case "", onErrorPolicyDegrade, onErrorPolicyFail: // "" = unset, defaults at run time.
	default:
		return fmt.Errorf("%w: got %q", errOnErrorPolicy, ft.OnError)
	}
	if ft.EntryTTL < 0 {
		return errEntryTTLNegative
	}
	if ft.MaxTrackedFiles < 0 {
		return errMaxTrackedFilesNegative
	}
	return nil
}

func handleDeprecatedConfig(cfg *Config, logger *zap.Logger) {
	if cfg.RecordType != "" {
		logger.Warn("record_type is deprecated, and will be removed in a future version. Use encoding instead.")
	}
}
