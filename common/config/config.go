package config

import (
	"bytes"
	"encoding/json"
	"fmt"
)

var gConfig GConfig

type GConfig struct {
	Version string `toml:"version"`
	Broker  `toml:"broker"`
	Connect `toml:"connect"`
	Auth    `toml:"auth"`
	Server  `toml:"server"`
	PProf   `toml:"pprof"`
}

func (cfg *GConfig) String() string {
	b, err := json.Marshal(*cfg)
	if err != nil {
		return fmt.Sprintf("%+v", *cfg)
	}
	var out bytes.Buffer
	err = json.Indent(&out, b, "", "    ")
	if err != nil {
		return fmt.Sprintf("%+v", *cfg)
	}
	return out.String()
}

type Connect struct {
	Keepalive      int   `toml:"keepalive"  validate:"default=100"`
	ReadTimeout    int   `toml:"readTimeout"  validate:"default=3"`
	WriteTimeout   int   `toml:"writeTimeout"  validate:"default=3"`
	ConnectTimeout int   `toml:"connectTimeout" validate:"default=1000"`
	AckTimeout     int   `toml:"ackTimeout" validate:"default=5000"`
	TimeoutRetries int   `toml:"timeOutRetries" validate:"default=2"`
	Quota          int64 `toml:"quota" validate:"default=0"`
	QuotaLimit     int   `toml:"quotaLimit" validate:"default=0"`
}

type Auth struct {
	Allows []string `toml:"allows"`
}

type Server struct {
	Redirects         []string `tome:"redirects"`
	RedirectOpen      bool     `tome:"redirectOpen"`
	RedirectIsForEver bool     `tome:"redirectIsForEver"`
}
