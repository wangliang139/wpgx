package wpgx

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/rs/zerolog/log"
)

const (
	HighQPSMaxOpenConns = 100
	DefaultEnvPrefix    = "postgres"
)

type Config struct {
	Username         string        `default:"root"`
	Password         string        `default:"my-secret"`
	Host             string        `default:"localhost"`
	Port             int           `default:"5432"`
	DBName           string        `default:"test_db"`
	MaxConns         int32         `default:"10"`
	MinConns         int32         `default:"0"`
	MaxConnLifetime  time.Duration `default:"6h"`
	MaxConnIdleTime  time.Duration `default:"30m"`
	EnablePrometheus bool          `default:"true"`
	EnableTracing    bool          `default:"true"`
	AppName          string        `default:""`
}

func (c *Config) valid() error {
	if !(c.MinConns <= c.MaxConns) {
		return fmt.Errorf("MinConns must <= MaxConns, incorrect config: %s", c)
	}
	return nil
}

func (c *Config) String() string {
	if c == nil {
		return "nil"
	}
	copy := *c
	if len(copy.Password) > 0 {
		copy.Password = "*hidden*"
	}
	return fmt.Sprintf("%+v", copy)
}

func ConfigFromEnv() *Config {
	config := &Config{}
	envconfig.MustProcess(DefaultEnvPrefix, config)
	if err := config.valid(); err != nil {
		log.Fatal().Msgf("%s", err)
	}
	return config
}

func ConfigFromEnvPrefix(prefix string) *Config {
	config := &Config{}
	envconfig.MustProcess(prefix, config)
	if err := config.valid(); err != nil {
		log.Fatal().Msgf("%s", err)
	}
	return config
}
