package sessimpl

import (
	"context"
	"errors"

	"github.com/lybxkl/gmqtt/broker/core/message"
	sess "github.com/lybxkl/gmqtt/broker/core/session"
	"github.com/lybxkl/gmqtt/broker/core/store"

	. "github.com/lybxkl/gmqtt/common/log"
	"github.com/lybxkl/gmqtt/util/collection"
)

var _ sess.Manager = (*memManager)(nil)

var (
	NoSessionErr = errors.New("no session")
	CMsgNotFound = errors.New("not found connect message")
)

type memManager struct {
	sessStore store.SessionStore
	st        *collection.SafeMap // map[string]sess.Session
}

func (prv *memManager) SetStore(store store.SessionStore, _ store.MessageStore) {
	prv.sessStore = store
}

func NewMemManager() sess.Manager {
	return &memManager{
		st: collection.NewSafeMap(), // map[string]sess.Session
	}
}

func (prv *memManager) BuildSess(cMsg *message.ConnectMessage) (sess.Session, error) {
	return NewMemSession(cMsg)
}

func (prv *memManager) GetOrCreate(id string, cMsg ...*message.ConnectMessage) (sess.Session, error) {
	if len(cMsg) > 0 {
		if cMsg[0] == nil {
			return nil, CMsgNotFound
		}
		return prv.creat(cMsg[0])
	}

	_sess, ok := prv.st.Get(id)
	if !ok {
		return nil, NoSessionErr
	}
	Log.Infof("session num: %d", prv.st.Size())
	return _sess.(sess.Session), nil
}

func (prv *memManager) creat(cMsg *message.ConnectMessage) (sess.Session, error) {
	newSid := string(cMsg.ClientId())
	_sess, err := NewMemSession(cMsg) // 获取离线消息，旧订阅
	if err != nil {
		return nil, err
	}

	prv.st.Set(newSid, _sess)
	return _sess, nil
}

func (prv *memManager) Save(s sess.Session) error {
	return prv.del(s, func(s sess.Session) error {
		return prv.sessStore.StoreSession(context.Background(), s.ClientId(), s)
	})
}

func (prv *memManager) Remove(s sess.Session) error {
	return prv.del(s, func(s sess.Session) error {
		return prv.sessStore.ClearSession(context.Background(), s.ClientId(), true)
	})
}

func (prv *memManager) del(s sess.Session, handle func(s sess.Session) error) error {
	err := handle(s)
	if err != nil {
		return err
	}
	prv.st.Del(s.ID())
	return nil
}

func (prv *memManager) Close() error {
	prv.st = nil
	return nil
}
