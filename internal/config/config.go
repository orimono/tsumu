package config

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
)

type Config struct {
	NatsURL     string `json:"nats_url"`
	DatabaseURL string `json:"database_url"`
	StreamName  string `json:"stream_name"`
	Subject     string `json:"subject"`
	Consumer    string `json:"consumer"`
}

func Load() (*Config, error) {
	cfgPath := os.Getenv("TSUMU_CONFIG_PATH")
	if cfgPath == "" {
		home := os.Getenv("TSUMU_HOME")
		if home == "" {
			home = "."
		}
		cfgPath = filepath.Join(home, "config.json")
	}

	absPath, err := filepath.Abs(cfgPath)
	if err != nil {
		return nil, err
	}

	content, err := os.ReadFile(absPath)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	if err := json.Unmarshal(content, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func MustLoad() *Config {
	cfg, err := Load()
	if err != nil {
		slog.Error("failed to load config", "err", err)
		panic("critical configuration error")
	}
	return cfg
}
