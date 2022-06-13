package config

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/BurntSushi/toml"
)

//go:embed config.toml
var CfgFile []byte

var gConfig *GConfig

func GetGCfg() *GConfig {
	return gConfig
}

func init() {
	if len(CfgFile) == 0 {
		panic(errors.New("not found config.toml"))
	}
	gConfig = &GConfig{}
	gConfig.Broker.RetainAvailable = true

	if err := toml.Unmarshal(CfgFile, gConfig); err != nil {
		panic(err)
	}

	if gConfig.Broker.MaxQos > 2 || gConfig.Broker.MaxQos < 1 {
		gConfig.Broker.MaxQos = 2
	}

	fmt.Println(gConfig.String())
}

type GConfig struct {
	Version string `toml:"version"`
	Broker  `toml:"broker"`
	Connect `toml:"connect"`
	Auth    `toml:"auth"`
	Server  `toml:"server"`
	PProf   `toml:"pprof"`
	Log     `toml:"log"`
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
