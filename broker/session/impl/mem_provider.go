package sessimpl

import (
	"context"
	"errors"
	"sync"

	"gmqtt/broker/message"
	sess "gmqtt/broker/session"
	"gmqtt/broker/store"
	. "gmqtt/common/log"
)

var _ sess.Manager = (*memManager)(nil)

var (
	NoSessionErr = errors.New("no session")
	CMsgNotFound = errors.New("not found connect message")
)

type memManager struct {
	sessStore store.SessionStore

	mu       sync.RWMutex
	st       map[string]sess.Session
	delReNew int // 当map删除一定次数后，对st重新创建
}

func (prv *memManager) SetStore(store store.SessionStore, _ store.MessageStore) {
	prv.sessStore = store
}

func NewMemManager() sess.Manager {
	return &memManager{
		st: make(map[string]sess.Session),
	}
}

func (prv *memManager) GetOrCreate(id string, cMsg ...*message.ConnectMessage) (sess.Session, error) {
	prv.mu.RLock()
	if len(cMsg) > 0 {
		if cMsg[0] == nil {
			return nil, CMsgNotFound
		}
		prv.mu.RUnlock()
		return prv.creat(cMsg)
	}

	defer prv.mu.RUnlock()
	_sess, ok := prv.st[id]
	if !ok {
		return nil, NoSessionErr
	}
	Log.Infof("session num: %d", len(prv.st))
	return _sess, nil
}

func (prv *memManager) creat(cMsg []*message.ConnectMessage) (sess.Session, error) {
	newSid := string(cMsg[0].ClientId())
	_sess, err := NewMemSession(cMsg[0]) // 获取离线消息，旧订阅
	if err != nil {
		return nil, err
	}
	prv.mu.Lock()
	prv.st[newSid] = _sess
	prv.mu.Unlock()
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
	prv.mu.Unlock()
	delete(prv.st, s.ID())
	prv.delReNew++
	if prv.delReNew >= 100 {
		oldSt := prv.st
		prv.st = make(map[string]sess.Session)
		for sid, _ses := range oldSt {
			ses := _ses
			prv.st[sid] = ses
		}
	}
	prv.mu.Unlock()
	return nil
}

func (prv *memManager) Close() error {
	prv.st = make(map[string]sess.Session)
	return nil
}
