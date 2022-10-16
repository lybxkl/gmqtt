package store

import (
	"context"

	config2 "github.com/lybxkl/gmqtt/broker/gcfg"
)

type BaseStore interface {
	Start(ctx context.Context, config *config2.GConfig) error
	Stop(ctx context.Context) error
}
