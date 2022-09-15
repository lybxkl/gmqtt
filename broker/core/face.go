package core

import (
	"sync"

	"github.com/lybxkl/gmqtt/broker/core/auth"
	sess "github.com/lybxkl/gmqtt/broker/core/session"
	"github.com/lybxkl/gmqtt/broker/core/store"
	"github.com/lybxkl/gmqtt/broker/core/topic"
)

type (
	Topic     = topic.Manager
	Session   = sess.Manager
	Auth      = auth.Manager
	MsgStore  = store.MessageStore
	SessStore = store.SessionStore
)

var (
	Core *core
	once = &sync.Once{}
)

func InitCore(p1 Auth, p2 Topic, p3 Session, p4 MsgStore, p5 SessStore) {
	once.Do(func() {
		Core = &core{
			am:  p1,
			tm:  p2,
			ssm: p3,
			mst: p4,
			sst: p5,
		}
	})
}

var (
	TopicManager     = Core.tm
	SessionManager   = Core.ssm
	AuthManager      = Core.am
	MsgStoreManager  = Core.mst
	SessStoreManager = Core.sst
)
