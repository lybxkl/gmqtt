package store

import (
	"context"

	"github.com/lybxkl/gmqtt/broker/core/message"
	sess "github.com/lybxkl/gmqtt/broker/core/session"
)

type SessionStore interface {
	BaseStore

	GetSession(ctx context.Context, clientId string) (sess.Session, error)
	StoreSession(ctx context.Context, clientId string, session sess.Session) error
	ClearSession(ctx context.Context, clientId string, clearOfflineMsg bool) error
	StoreSubscription(ctx context.Context, clientId string, subMsg *message.SubscribeMessage) error
	DelSubscription(ctx context.Context, client, topic string) error
	ClearSubscriptions(ctx context.Context, clientId string) error
	GetSubscriptions(ctx context.Context, clientId string) ([]*message.SubscribeMessage, error)
	/**
	 * 缓存qos2 publish报文消息-入栈消息
	 * @return true:缓存成功   false:缓存失败
	 */

	CacheInflowMsg(ctx context.Context, clientId string, msg message.Message) error
	ReleaseInflowMsg(ctx context.Context, clientId string, pkId uint16) (message.Message, error)
	ReleaseInflowMsgs(ctx context.Context, clientId string, pkId []uint16) error
	ReleaseAllInflowMsg(ctx context.Context, clientId string) error
	GetAllInflowMsg(ctx context.Context, clientId string) ([]message.Message, error)

	/**
	 * 缓存出栈消息-分发给客户端的qos1,qos2消息
	 */

	CacheOutflowMsg(ctx context.Context, client string, msg message.Message) error
	GetAllOutflowMsg(ctx context.Context, clientId string) ([]message.Message, error)
	ReleaseOutflowMsg(ctx context.Context, clientId string, pkId uint16) (message.Message, error)
	ReleaseOutflowMsgs(ctx context.Context, clientId string, pkId []uint16) error
	ReleaseAllOutflowMsg(ctx context.Context, clientId string) error

	/**
	 * 出栈qos2第二阶段，缓存msgId
	 */

	CacheOutflowSecMsgId(ctx context.Context, clientId string, pkId uint16) error
	GetAllOutflowSecMsg(ctx context.Context, clientId string) ([]uint16, error)
	ReleaseOutflowSecMsgId(ctx context.Context, clientId string, pkId uint16) error
	ReleaseOutflowSecMsgIds(ctx context.Context, clientId string, pkId []uint16) error
	ReleaseAllOutflowSecMsg(ctx context.Context, clientId string) error

	// 采用离线消息方便做离线消息的限制最大数量

	StoreOfflineMsg(ctx context.Context, clientId string, msg message.Message) error
	GetAllOfflineMsg(ctx context.Context, clientId string) ([]message.Message, []string, error)
	ClearOfflineMsgs(ctx context.Context, clientId string) error
	ClearOfflineMsgById(ctx context.Context, clientId string, msgIds []string) error
}
