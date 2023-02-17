package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/byuoitav/db-replicator/replication"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func buildLogger(lvl string) *zap.Logger {
	var level zapcore.Level
	if err := level.Set(lvl); err != nil {
		panic(fmt.Sprintf("invalid log level: %s", err))
	}

	config := zap.Config{
		Level: zap.NewAtomicLevelAt(level),
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding: "json",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "@",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			StacktraceKey:  "trace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}

	log, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("unable to build logger: %s", err))
	}

	return log
}

func readConfig(filepath string) (*replication.ReplicationConfig, error) {
	// check if config file exists
	b, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %s", err.Error())
	}

	var config replication.ReplicationConfig
	err = json.Unmarshal(b, &config)
	if err != nil {
		return nil, fmt.Errorf("config file in an invalid format: %s", err.Error())
	}

	return &config, nil
}
