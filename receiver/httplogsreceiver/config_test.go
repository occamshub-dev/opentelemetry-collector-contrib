// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package httplogsreceiver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httplogsreceiver/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	for _, configType := range []string{
		"cwmetrics", "cwlogs", "otlp_v1",
	} {
		t.Run(configType, func(t *testing.T) {
			fileName := configType + "_config.yaml"
			cm, err := confmaptest.LoadConf(filepath.Join("testdata", fileName))
			require.NoError(t, err)

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			err = xconfmap.Validate(cfg)
			assert.NoError(t, err)
			require.Equal(t, &Config{
				RecordType:            configType,
				AccessKey:             "some_access_key",
				CompressionAlgorithms: defaultCompressionAlgorithms,
				ServerConfig: confighttp.ServerConfig{
					NetAddr: confignet.AddrConfig{
						Transport: "tcp",
						Endpoint:  "0.0.0.0:4433",
					},
					TLS: configoptional.Some(configtls.ServerConfig{
						Config: configtls.Config{
							CertFile: "server.crt",
							KeyFile:  "server.key",
						},
					}),
				},
				FileTracking: FileTrackingConfig{
					FileAttribute:   defaultFileAttribute,
					MaxTrackedFiles: defaultMaxTrackedFiles,
					EntryTTL:        defaultEntryTTL,
					StaleCheckpoint: staleCheckpointReject,
					OnError:         onErrorPolicyDegrade,
				},
			}, cfg)
		})
	}
}

func TestLoadConfigFileTracking(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "file_tracking_config.yaml"))
	require.NoError(t, err)

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	assert.NoError(t, xconfmap.Validate(cfg))

	config := cfg.(*Config)
	require.NotNil(t, config.StorageID)
	assert.Equal(t, "memstorage/main", config.StorageID.String())
	assert.Equal(t, FileTrackingConfig{
		Enabled:         true,
		FileAttribute:   "my.file",
		MaxTrackedFiles: 42,
		EntryTTL:        72 * time.Hour,
		StaleCheckpoint: staleCheckpointReset,
		OnError:         onErrorPolicyFail,
	}, config.FileTracking)
}

func TestLoadConfigFileTrackingInvalid(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "invalid_file_tracking_config.yaml"))
	require.NoError(t, err)

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	err = xconfmap.Validate(cfg)
	require.Error(t, err)
	assert.ErrorIs(t, err, errStaleCheckpointPolicy)
}

func TestLoadConfigInvalid(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "invalid_config.yaml"))
	require.NoError(t, err)

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	err = xconfmap.Validate(cfg)
	assert.ErrorIs(t, err, errRecordTypeEncodingSet)
}
