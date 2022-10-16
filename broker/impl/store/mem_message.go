package storeimpl

import (
	"context"
	"errors"

	"github.com/lybxkl/gmqtt/broker/core/message"
	"github.com/lybxkl/gmqtt/broker/core/store"
	config2 "github.com/lybxkl/gmqtt/broker/gcfg"

	"github.com/lybxkl/gmqtt/util/collection"
)

type memMessageStore struct {
	willTable   *collection.SafeMap // map[string]*message.PublishMessage
	retainTable *collection.SafeMap // map[string]*message.PublishMessage
}

func NewMemMsgStore() store.MessageStore {
	return &memMessageStore{
		willTable:   collection.NewSafeMap(),
		retainTable: collection.NewSafeMap(),
	}
}
func (m *memMessageStore) Start(_ context.Context, config *config2.GConfig) error {
	return nil
}

func (m *memMessageStore) Stop(_ context.Context) error {
	m.willTable = nil
	m.retainTable = nil
	return nil
}

func (m *memMessageStore) StoreWillMessage(_ context.Context, clientId string, message *message.PublishMessage) error {
	m.willTable.Set(clientId, message)
	return nil
}

func (m *memMessageStore) ClearWillMessage(_ context.Context, clientId string) error {
	m.willTable.Del(clientId)
	return nil
}

func (m *memMessageStore) GetWillMessage(_ context.Context, clientId string) (*message.PublishMessage, error) {
	msg, ok := m.willTable.Get(clientId)
	if ok {
		return msg.(*message.PublishMessage), nil
	}
	return nil, errors.New("no will msg")
}

func (m *memMessageStore) StoreRetainMessage(_ context.Context, topic string, message *message.PublishMessage) error {
	m.retainTable.Set(topic, message)
	return nil
}

func (m *memMessageStore) ClearRetainMessage(_ context.Context, topic string) error {
	m.retainTable.Del(topic)
	return nil
}

func (m *memMessageStore) GetRetainMessage(ctx context.Context, topic string) (*message.PublishMessage, error) {
	msg, ok := m.retainTable.Get(topic)
	if ok {
		return msg.(*message.PublishMessage), nil
	}
	return nil, errors.New("no retain msg")
}

func (m *memMessageStore) GetAllRetainMsg(_ context.Context) ([]*message.PublishMessage, error) {
	ret := make([]*message.PublishMessage, 0, m.retainTable.Size())
	_ = m.retainTable.Range(func(k, v interface{}) error {
		ret = append(ret, v.(*message.PublishMessage))
		return nil
	})
	return ret, nil
}
