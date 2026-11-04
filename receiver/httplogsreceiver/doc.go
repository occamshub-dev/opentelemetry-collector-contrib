// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package httplogsreceiver implements a receiver that can be used to
// receive HTTP requests with a file as content (compressed) and transform them
// into formats usable by the OpenTelemetry collector. The configuration
// determines which unmarshaler to use. Each unmarshaler is responsible for
// processing a record format that can be sent through the delivery
// stream.

//go:generate make mdatagen

package httplogsreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver"
