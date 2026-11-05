// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package decode provides streaming decompression of HTTP request bodies
// for the content encodings supported by the httplogsreceiver.
package decode // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/decode"

import (
	"compress/flate"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"go.opentelemetry.io/collector/config/configcompression"
)

// ErrUnsupported is returned by ReaderFor when the requested content
// encoding is not one this receiver knows how to decompress.
var ErrUnsupported = errors.New("unsupported content encoding")

// supported lists the compressed encodings (beyond identity) that
// ReaderFor can decompress.
var supported = map[string]struct{}{
	"deflate": {},
	"gzip":    {},
	"zlib":    {},
	"zstd":    {},
}

// NormalizeEncoding lowercases and trims s and folds the identity encodings
// ("none" and "identity") into the empty string, which is compared
// case-insensitively elsewhere.
func NormalizeEncoding(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "none", "identity":
		return ""
	}
	return s
}

// IsSupported reports whether t is an encoding the receiver can decompress.
// Identity (no compression) is reported by the empty value, and by the "none"
// and "identity" tokens (compared case-insensitively).
func IsSupported(t configcompression.Type) bool {
	normalized := NormalizeEncoding(string(t))
	if normalized == "" {
		return true
	}
	_, ok := supported[normalized]
	return ok
}

// ReaderFor returns an io.Reader that transparently decompresses the
// given reader r using the content encoding named in encoding
// (compared case-insensitively). An identity encoding (the empty value,
// "none", or "identity") returns r unchanged.
//
// It returns ErrUnsupported for an encoding the receiver cannot decode, and
// a different error if the codec cannot be initialised because the stream is
// not valid compressed data.
func ReaderFor(r io.Reader, encoding string) (io.Reader, error) {
	switch NormalizeEncoding(encoding) {
	case "":
		return r, nil
	case "gzip":
		return pgzip.NewReader(r)
	case "zstd":
		return zstd.NewReader(r)
	case "zlib":
		return zlib.NewReader(r)
	case "deflate":
		return flate.NewReader(r), nil
	default:
		return nil, fmt.Errorf("%w %q", ErrUnsupported, encoding)
	}
}
