package gcfg

type PProf struct {
	Open bool `toml:"open" `
	Port int  `toml:"port" validate:"default=6060"`
}
