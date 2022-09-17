package sess

import (
	"context"

	"github.com/lybxkl/gmqtt/broker/core/message"
	sess "github.com/lybxkl/gmqtt/broker/core/session"
	"github.com/lybxkl/gmqtt/broker/core/store"

	. "github.com/lybxkl/gmqtt/common/log"
	"github.com/lybxkl/gmqtt/util/collection"
)

var _ sess.Manager = (*memManager)(nil)

type memManager struct {
	sessStore store.SessionStore
	st        *collection.SafeMap // map[string]sess.Session
}

func (prv *memManager) SetStore(store store.SessionStore, _ store.MessageStore) {
	prv.sessStore = store
}

func NewMemManager(sessionStore store.SessionStore) sess.Manager {
	return &memManager{
		st:        collection.NewSafeMap(), // map[string]sess.Session
		sessStore: sessionStore,
	}
}

func (prv *memManager) BuildSess(cMsg *message.ConnectMessage) (sess.Session, error) {
	return NewMemSession(cMsg)
}

func (prv *memManager) GetOrCreate(id string, cMsg ...*message.ConnectMessage) (_ sess.Session, _exist bool, _ error) {
	_sess, ok := prv.st.Get(id)
	if ok {
		return _sess.(sess.Session), true, nil
	}
	if len(cMsg) == 0 || cMsg[0] == nil {
		return nil, false, nil
	}
	newSid := string(cMsg[0].ClientId())
	_sess, err := NewMemSession(cMsg[0]) // 获取离线消息，旧订阅
	if err != nil {
		return nil, false, err
	}
	old, exist := prv.st.GetOrSet(newSid, _sess)
	Log.Infof("session num: %d", prv.st.Size())
	return old.(sess.Session), exist, nil
}

func (prv *memManager) Exist(id string) bool {
	_, exist, _ := prv.GetOrCreate(id)
	return exist
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
