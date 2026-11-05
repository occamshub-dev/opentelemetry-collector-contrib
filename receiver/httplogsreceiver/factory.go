// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package httplogsreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/metadata"
)

const (
	defaultEndpoint = "localhost:4433"
)

// NewFactory creates a receiver factory for httplogs. Currently, only
// available in metrics pipelines.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability))
}

// createDefaultConfig creates a default config with the endpoint set
// to port 8443 and the record type set to the CloudWatch metric stream.
func createDefaultConfig() component.Config {
	netAddr := confignet.NewDefaultAddrConfig()
	netAddr.Transport = confignet.TransportTypeTCP
	netAddr.Endpoint = defaultEndpoint
	return &Config{
		ServerConfig: confighttp.ServerConfig{
			NetAddr: netAddr,
		},
		CompressionAlgorithms: defaultCompressionAlgorithms,
	}
}

// createMetricsReceiver implements the CreateMetrics function type.
func createMetricsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (receiver.Metrics, error) {
	c := cfg.(*Config)
	handleDeprecatedConfig(c, set.Logger)
	return newMetricsReceiver(c, set, nextConsumer)
}

// createMetricsReceiver implements the CreateMetricsReceiver function type.
func createLogsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (receiver.Logs, error) {
	c := cfg.(*Config)
	handleDeprecatedConfig(c, set.Logger)
	return newLogsReceiver(c, set, nextConsumer)
}
