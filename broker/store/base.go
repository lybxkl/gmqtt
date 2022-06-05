package store

import (
	"context"

	"github.com/lybxkl/gmqtt/common/config"
)

type BaseStore interface {
	Start(ctx context.Context, config *config.GConfig) error
	Stop(ctx context.Context) error
}

type Store interface {
	SetStore(store SessionStore, messageStore MessageStore)
}
