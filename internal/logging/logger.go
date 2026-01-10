package logging

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(env string) (*zap.SugaredLogger, func() error, error) {
	cfg := zap.NewProductionConfig()
	if env == "local" || env == "development" || env == "dev" || env == "docker" {
		cfg = zap.NewDevelopmentConfig()
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	zapLogger, err := cfg.Build()
	if err != nil {
		return nil, func() error { return nil }, fmt.Errorf("failed to build zap logger: %w", err)
	}

	return zapLogger.Sugar(), zapLogger.Sync, nil
}
