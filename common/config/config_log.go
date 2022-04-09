package config

type Log struct {
	Level string `toml:"level" validate:"default=INFO"`
}

func (l Log) GetLevel() string {
	return l.Level
}
