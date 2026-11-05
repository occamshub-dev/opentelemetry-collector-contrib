// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package httplogsreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver"

import (
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/decode"
)

var (
	errRecordTypeEncodingSet    = errors.New("record_type must not be set when encoding is set")
	errFlushItemsNegative       = errors.New("flush_items must not be negative")
	errFlushBytesNegative       = errors.New("flush_bytes must not be negative")
	errDuplicateCompressionAlgo = errors.New("compression_algorithms must not contain duplicates")
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
	return nil
}

func handleDeprecatedConfig(cfg *Config, logger *zap.Logger) {
	if cfg.RecordType != "" {
		logger.Warn("record_type is deprecated, and will be removed in a future version. Use encoding instead.")
	}
}
