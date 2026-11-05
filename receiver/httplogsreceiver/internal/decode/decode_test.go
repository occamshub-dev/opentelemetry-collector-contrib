// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package decode

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"io"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/config/configcompression"
)

func TestNormalizeEncoding(t *testing.T) {
	testCases := map[string]string{
		"":           "",
		"gzip":       "gzip",
		"  GZIP  ":   "gzip",
		"ZSTD":       "zstd",
		"deflate":    "deflate",
		"ZLIB":       "zlib",
		"identity":   "",
		"none":       "",
		" Identity ": "",
		"br":         "br",
		"snappy":     "snappy",
	}
	for in, want := range testCases {
		t.Run(in, func(t *testing.T) {
			assert.Equal(t, want, NormalizeEncoding(in))
		})
	}
}

func TestIsSupported(t *testing.T) {
	supported := []configcompression.Type{
		"",
		configcompression.TypeGzip,
		configcompression.TypeZstd,
		configcompression.TypeZlib,
		configcompression.TypeDeflate,
		// Identity is also spelled this way, and case should not matter.
		"identity",
		"none",
		"GZIP",
		"  ZSTD ",
	}
	for _, enc := range supported {
		assert.Truef(t, IsSupported(enc), "expected %q to be supported", enc)
	}

	unsupported := []configcompression.Type{
		"snappy",
		"lz4",
		"br",
		"brotli",
	}
	for _, enc := range unsupported {
		assert.Falsef(t, IsSupported(enc), "expected %q not to be supported", enc)
	}
}

// compress returns payload encoded with the named codec, using the same
// library the receiver uses to decompress it.
func compressFor(t *testing.T, payload []byte, encoding string) []byte {
	switch NormalizeEncoding(encoding) {
	case "gzip":
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		_, requireErr := w.Write(payload)
		require.NoError(t, requireErr)
		require.NoError(t, w.Close())
		return buf.Bytes()
	case "zstd":
		var buf bytes.Buffer
		w, err := zstd.NewWriter(&buf)
		require.NoError(t, err)
		_, requireErr := w.Write(payload)
		require.NoError(t, requireErr)
		require.NoError(t, w.Close())
		return buf.Bytes()
	case "zlib":
		var buf bytes.Buffer
		w := zlib.NewWriter(&buf)
		_, requireErr := w.Write(payload)
		require.NoError(t, requireErr)
		require.NoError(t, w.Close())
		return buf.Bytes()
	case "deflate":
		var buf bytes.Buffer
		w, err := flate.NewWriter(&buf, flate.DefaultCompression)
		require.NoError(t, err)
		_, requireErr := w.Write(payload)
		require.NoError(t, requireErr)
		require.NoError(t, w.Close())
		return buf.Bytes()
	default:
		t.Fatalf("unknown test encoding %q", encoding)
		return nil
	}
}

func decodeAll(t *testing.T, payload []byte, encoding string) []byte {
	t.Helper()
	r, err := ReaderFor(bytes.NewReader(payload), encoding)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	return got
}

func TestReaderFor_RoundTrip(t *testing.T) {
	payload := []byte("line one of a streaming body\nsecond line\nthird line\n")
	for _, encoding := range []string{"gzip", "zstd", "zlib", "deflate"} {
		// Test the lowercase and a mixed-case form.
		encodings := []string{encoding, strings.ToUpper(encoding)}
		for _, enc := range encodings {
			t.Run(enc, func(t *testing.T) {
				compressed := compressFor(t, payload, encoding)
				assert.Equal(t, payload, decodeAll(t, compressed, enc))
			})
		}
	}
}

func TestReaderFor_Identity(t *testing.T) {
	source := strings.NewReader("plain text stream")
	for _, enc := range []string{"", "identity", "none", "Identity"} {
		t.Run(enc, func(t *testing.T) {
			got, err := ReaderFor(source, enc)
			require.NoError(t, err)
			assert.Same(t, source, got, "identity must return the reader unchanged")
		})
	}
}

func TestReaderFor_Unsupported(t *testing.T) {
	for _, enc := range []string{"snappy", "lz4", "br", "brotli"} {
		t.Run(enc, func(t *testing.T) {
			_, err := ReaderFor(strings.NewReader("irrelevant"), enc)
			require.ErrorIsf(t, err, ErrUnsupported, "expected %q to be unsupported", enc)
		})
	}
}

func TestReaderFor_CorruptStream(t *testing.T) {
	// A valid gzip magic header followed by garbage must surface an error when read.
	r, err := ReaderFor(bytes.NewReader([]byte("\x1f\x8bgarbage")), "gzip")
	if err != nil {
		// NewReader can fail fast on the header; that is also acceptable.
		return
	}
	_, err = io.ReadAll(r)
	require.Error(t, err)
}
