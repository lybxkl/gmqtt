package gcfg

import (
	"github.com/lybxkl/gmqtt/common/log"
)

type Log struct {
	Level string `toml:"level" validate:"default=INFO"`
}

func (l Log) GetLevel() log.Level {
	return log.ToLevel(l.Level)
}
