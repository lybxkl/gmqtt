package sess

import (
	"context"

	"github.com/lybxkl/gmqtt/broker/core/message"
	sess "github.com/lybxkl/gmqtt/broker/core/session"
	"github.com/lybxkl/gmqtt/broker/core/store"
	"github.com/lybxkl/gmqtt/util/collection"
)

var _ sess.Manager = (*memManager)(nil)

type memManager struct {
	sessStore  store.SessionStore
	localStore *collection.SafeMap // map[string]session.Session
}

func (prv *memManager) SetStore(store store.SessionStore, _ store.MessageStore) {
	prv.sessStore = store
}

func NewMemManager(sessionStore store.SessionStore) sess.Manager {
	return &memManager{
		sessStore:  sessionStore,
		localStore: collection.NewSafeMap(),
	}
}

func (prv *memManager) BuildSess(cMsg *message.ConnectMessage) (sess.Session, error) {
	return NewMemSession(cMsg)
}

func (prv *memManager) GetOrCreate(id string, cMsg ...*message.ConnectMessage) (_ sess.Session, _exist bool, _ error) {
	ctx := context.Background()
	if _, exist := prv.localStore.GetDel(id); exist {
		return nil, false, nil
	}
	_sess, err := prv.sessStore.GetSession(ctx, id)
	if err != nil {
		return nil, false, err
	}
	if _sess != nil {
		return _sess, true, nil
	}
	if len(cMsg) == 0 || cMsg[0] == nil {
		return nil, false, nil
	}
	newSid := string(cMsg[0].ClientId())
	_sess, err = NewMemSession(cMsg[0]) // 获取离线消息，旧订阅
	if err != nil {
		return nil, false, err
	}
	if cMsg[0].CleanSession() { // 保存本地即可
		err = prv.sessStore.ClearSession(ctx, id, true)
		if err != nil {
			return nil, false, err
		}
		prv.localStore.Set(id, _sess)
		return _sess, false, nil
	}

	err = prv.sessStore.StoreSession(ctx, newSid, _sess)
	if err != nil {
		return nil, false, err
	}
	return _sess, false, nil
}

func (prv *memManager) Exist(id string) bool {
	_, exist, _ := prv.GetOrCreate(id)
	return exist
}

func (prv *memManager) Save(s sess.Session) error {
	if s.CMsg().CleanSession() {
		prv.localStore.Set(s.ClientId(), s)
		return nil
	}
	return prv.sessStore.StoreSession(context.Background(), s.ClientId(), s)
}

func (prv *memManager) Remove(s sess.Session) error {
	if s.CMsg().CleanSession() {
		prv.localStore.Del(s.ClientId())
		return nil
	}
	return prv.sessStore.ClearSession(context.Background(), s.ClientId(), true)
}

func (prv *memManager) Close() error {
	return prv.sessStore.Stop(context.Background())
}
