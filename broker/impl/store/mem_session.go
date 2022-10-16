package storeimpl

import (
	"container/list"
	"context"
	"errors"
	"strconv"

	"github.com/lybxkl/gmqtt/broker/core/message"
	sess "github.com/lybxkl/gmqtt/broker/core/session"
	"github.com/lybxkl/gmqtt/broker/core/store"
	config2 "github.com/lybxkl/gmqtt/broker/gcfg"

	"github.com/lybxkl/gmqtt/util/collection"
)

var obj struct{}

type memSessionStore struct {
	db           *collection.SafeMap // map[string]sess.Session // 会话
	subCache     *collection.SafeMap // map[string]map[string]*message.SubscribeMessage // clientID -> topic -> sub_msg
	offlineTable *collection.SafeMap // map[string]*list.List // 离线消息表 clientID -> offline msg list
	msgMaxNum    int
	recCache     *collection.SafeMap // map[string]map[uint16]message.Message // clientID -> pkid -> msg
	sendCache    *collection.SafeMap // map[string]map[uint16]message.Message // clientID -> pkid -> msg
	secTwoCache  *collection.SafeMap // map[string]map[uint16]struct{} // clientID -> pkid -> 占位
}

func NewMemSessStore() store.SessionStore {
	return &memSessionStore{
		db:           collection.NewSafeMap(),
		subCache:     collection.NewSafeMap(),
		offlineTable: collection.NewSafeMap(),
		msgMaxNum:    1000,
		recCache:     collection.NewSafeMap(),
		sendCache:    collection.NewSafeMap(),
		secTwoCache:  collection.NewSafeMap(),
	}
}
func (m *memSessionStore) Start(_ context.Context, config *config2.GConfig) error {
	return nil
}

func (m *memSessionStore) Stop(_ context.Context) error {
	return nil
}

func (m *memSessionStore) GetSession(_ context.Context, clientId string) (sess.Session, error) {
	s, ok := m.db.Get(clientId)
	if !ok {
		return nil, nil
	}
	return s.(sess.Session), nil
}

func (m *memSessionStore) StoreSession(ctx context.Context, clientId string, session sess.Session) error {
	m.db.Set(clientId, session)
	return nil
}

func (m *memSessionStore) ClearSession(ctx context.Context, clientId string, clearOfflineMsg bool) error {
	m.db.Del(clientId)
	if clearOfflineMsg {
		return m.ClearOfflineMsgs(ctx, clientId)
	}
	return nil
}

func (m *memSessionStore) StoreSubscription(ctx context.Context, clientId string, subMsg *message.SubscribeMessage) error {
	v, _ := m.subCache.GetOrSet(clientId, collection.NewSafeMap()) // map[string]*message.SubscribeMessage
	topicS := subMsg.Topics()
	subs := subMsg.Clone()
	vm := v.(*collection.SafeMap)
	for i := 0; i < len(topicS); i++ {
		vm.Set(string(topicS[i]), subs[i])
	}
	return nil
}

func (m *memSessionStore) DelSubscription(ctx context.Context, clientId, topic string) error {
	v, _ := m.subCache.GetOrSet(clientId, collection.NewSafeMap()) // map[string]*message.SubscribeMessage
	v.(*collection.SafeMap).Del(topic)
	return nil
}

func (m *memSessionStore) ClearSubscriptions(ctx context.Context, clientId string) error {
	m.subCache.Del(clientId)
	return nil
}

func (m *memSessionStore) GetSubscriptions(ctx context.Context, clientId string) ([]*message.SubscribeMessage, error) {
	v, ok := m.subCache.Get(clientId)
	if !ok {
		return nil, nil
	}
	vm := v.(*collection.SafeMap)
	ret := make([]*message.SubscribeMessage, 0, vm.Size())
	_ = vm.Range(func(k, sub interface{}) error {
		ret = append(ret, sub.(*message.SubscribeMessage))
		return nil
	})
	return ret, nil
}

func (m *memSessionStore) CacheInflowMsg(ctx context.Context, clientId string, msg message.Message) error {
	v, _ := m.recCache.GetOrSet(clientId, collection.NewSafeMap()) // map[uint16]message.Message
	v.(*collection.SafeMap).Set(msg.PacketId(), msg)
	return nil
}

func (m *memSessionStore) ReleaseInflowMsg(ctx context.Context, clientId string, pkId uint16) (message.Message, error) {
	v, _ := m.recCache.GetOrSet(clientId, collection.NewSafeMap()) // map[uint16]message.Message
	vm := v.(*collection.SafeMap)
	msg, ok := vm.GetDel(pkId)
	if !ok {
		return nil, errors.New("no inflow msg")
	}
	return msg.(message.Message), nil
}

func (m *memSessionStore) ReleaseInflowMsgs(ctx context.Context, clientId string, pkIds []uint16) error {
	v, _ := m.recCache.GetOrSet(clientId, collection.NewSafeMap()) // map[uint16]message.Message
	vm := v.(*collection.SafeMap)
	vm.Dels(uint16ToInterface(pkIds)...)
	return nil
}
func uint16ToInterface(ids []uint16) []interface{} {
	v := make([]interface{}, 0, len(ids))
	for i := 0; i < len(ids); i++ {
		v = append(v, ids[i])
	}
	return v
}

func (m *memSessionStore) ReleaseAllInflowMsg(ctx context.Context, clientId string) error {
	m.recCache.Del(clientId)
	return nil
}

func (m *memSessionStore) GetAllInflowMsg(ctx context.Context, clientId string) ([]message.Message, error) {
	v, ok := m.recCache.GetOrSet(clientId, collection.NewSafeMap()) // map[uint16]message.Message
	if !ok {
		return nil, nil
	}
	vm := v.(*collection.SafeMap)
	ret := make([]message.Message, 0, vm.Size())
	_ = vm.Range(func(k, v interface{}) error {
		msg := v.(message.Message)
		ret = append(ret, msg)
		return nil
	})
	return ret, nil
}

func (m *memSessionStore) CacheOutflowMsg(ctx context.Context, clientId string, msg message.Message) error {
	v, _ := m.sendCache.GetOrSet(clientId, collection.NewSafeMap()) // map[uint16]message.Message
	vm := v.(*collection.SafeMap)
	vm.Set(msg.PacketId(), msg)
	return nil
}

func (m *memSessionStore) GetAllOutflowMsg(ctx context.Context, clientId string) ([]message.Message, error) {
	v, ok := m.sendCache.GetOrSet(clientId, collection.NewSafeMap()) // map[uint16]message.Message
	if !ok {
		return nil, nil
	}
	vm := v.(*collection.SafeMap)
	ret := make([]message.Message, 0, vm.Size())
	_ = vm.Range(func(k, v interface{}) error {
		msg := v.(message.Message)
		ret = append(ret, msg)
		return nil
	})
	return ret, nil
}

func (m *memSessionStore) ReleaseOutflowMsg(ctx context.Context, clientId string, pkId uint16) (message.Message, error) {
	v, ok := m.sendCache.GetOrSet(clientId, collection.NewSafeMap()) // map[uint16]message.Message
	if !ok {
		return nil, errors.New("no out inflow msg")
	}
	vm := v.(*collection.SafeMap)
	msg, ok := vm.GetDel(pkId)
	if !ok {
		return nil, errors.New("no out inflow msg")
	}
	return msg.(message.Message), nil
}

func (m *memSessionStore) ReleaseOutflowMsgs(ctx context.Context, clientId string, pkIds []uint16) error {
	v, ok := m.sendCache.GetOrSet(clientId, collection.NewSafeMap()) // map[uint16]message.Message
	if !ok {
		return nil
	}
	vm := v.(*collection.SafeMap)
	vm.Dels(uint16ToInterface(pkIds)...)
	return nil
}

func (m *memSessionStore) ReleaseAllOutflowMsg(ctx context.Context, clientId string) error {
	m.sendCache.Del(clientId)
	return nil
}

func (m *memSessionStore) CacheOutflowSecMsgId(ctx context.Context, clientId string, pkId uint16) error {
	v, _ := m.secTwoCache.GetOrSet(clientId, collection.NewSafeMap()) // map[uint16]struct{}
	vm := v.(*collection.SafeMap)
	vm.Set(pkId, obj)
	return nil
}

func (m *memSessionStore) GetAllOutflowSecMsg(ctx context.Context, clientId string) ([]uint16, error) {
	v, ok := m.secTwoCache.GetOrSet(clientId, collection.NewSafeMap()) // map[uint16]struct{}
	if !ok {
		return nil, nil
	}

	vm := v.(*collection.SafeMap)
	ret := make([]uint16, 0, vm.Size())
	_ = vm.Range(func(k, _ interface{}) error {
		pkid := k.(uint16)
		ret = append(ret, pkid)
		return nil
	})
	return ret, nil
}

func (m *memSessionStore) ReleaseOutflowSecMsgId(ctx context.Context, clientId string, pkId uint16) error {
	v, ok := m.secTwoCache.GetOrSet(clientId, collection.NewSafeMap()) // map[uint16]struct{}
	if !ok {
		return nil
	}
	vm := v.(*collection.SafeMap)
	vm.Del(pkId)
	return nil
}

func (m *memSessionStore) ReleaseOutflowSecMsgIds(ctx context.Context, clientId string, pkIds []uint16) error {
	v, ok := m.secTwoCache.GetOrSet(clientId, collection.NewSafeMap()) // map[uint16]struct{}
	if !ok {
		return nil
	}
	vm := v.(*collection.SafeMap)
	vm.Dels(uint16ToInterface(pkIds)...)
	return nil
}

func (m *memSessionStore) ReleaseAllOutflowSecMsg(ctx context.Context, clientId string) error {
	m.secTwoCache.Del(clientId)
	return nil
}

func (m *memSessionStore) StoreOfflineMsg(ctx context.Context, clientId string, msg message.Message) error {
	v, _ := m.offlineTable.GetOrSet(clientId, list.New()) // *list.List
	vl := v.(*list.List)
	if vl.Len() >= m.msgMaxNum {
		return errors.New("too many msg")
	}
	// fixme 加锁添加
	vl.PushBack(msg)
	return nil
}

func (m *memSessionStore) GetAllOfflineMsg(ctx context.Context, clientId string) ([]message.Message, []string, error) {
	v, ok := m.offlineTable.GetOrSet(clientId, list.New()) // *list.List
	vl := v.(*list.List)
	if !ok || vl.Len() == 0 {
		return []message.Message{}, []string{}, nil
	}
	// fixme 加锁操作
	ret := make([]message.Message, 0, vl.Len())
	ids := make([]string, 0, vl.Len())
	head := vl.Front()

	for head != nil {
		msg := head.Value.(message.Message)
		head = head.Next()

		ret = append(ret, msg)
		ids = append(ids, strconv.Itoa(int(msg.PacketId())))
	}
	return ret, ids, nil
}

func (m *memSessionStore) ClearOfflineMsgs(ctx context.Context, clientId string) error {
	m.offlineTable.Del(clientId)
	return nil
}

func (m *memSessionStore) ClearOfflineMsgById(ctx context.Context, clientId string, msgIds []string) error {
	v, ok := m.offlineTable.GetOrSet(clientId, list.New()) // *list.List
	vl := v.(*list.List)
	if !ok || vl.Len() == 0 {
		return nil
	}
	// fixme 加锁操作
	head := vl.Front()
	for head != nil {
		msg := head.Value.(message.Message)
		del := head
		head = head.Next()
		for i := 0; i < len(msgIds); i++ {
			if msgIds[i] != "" && strconv.Itoa(int(msg.PacketId())) == msgIds[i] {
				msgIds[i] = ""
				vl.Remove(del)
				break
			}
		}
	}
	return nil
}
