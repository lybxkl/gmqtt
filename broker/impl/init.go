package memimpl

import (
	"github.com/lybxkl/gmqtt/broker/core"
	authimpl "github.com/lybxkl/gmqtt/broker/impl/auth"
	sessimpl "github.com/lybxkl/gmqtt/broker/impl/session"
	storeimpl "github.com/lybxkl/gmqtt/broker/impl/store"
	topicimpl "github.com/lybxkl/gmqtt/broker/impl/topic"
)

func init() {
	authManager := authimpl.NewDefaultAuth()
	sessStore := storeimpl.NewMemSessStore()
	msgStore := storeimpl.NewMemMsgStore()
	topicManager := topicimpl.NewMemProvider(msgStore)
	sessManager := sessimpl.NewMemManager(sessStore)

	core.InitCore(authManager, topicManager, sessManager, msgStore, sessStore)
}
