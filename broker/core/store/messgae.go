package store

import (
	"context"

	"github.com/lybxkl/gmqtt/broker/core/message"
)

type MessageStore interface {
	BaseStore

	StoreWillMessage(ctx context.Context, clientId string, msg *message.PublishMessage) error
	ClearWillMessage(ctx context.Context, clientId string) error
	GetWillMessage(ctx context.Context, clientId string) (*message.PublishMessage, error)

	StoreRetainMessage(ctx context.Context, topic string, msg *message.PublishMessage) error
	ClearRetainMessage(ctx context.Context, topic string) error
	GetRetainMessage(ctx context.Context, topic string) (*message.PublishMessage, error)

	GetAllRetainMsg(ctx context.Context) ([]*message.PublishMessage, error)
}
