package v2

import (
	"github.com/lybxkl/gmqtt/broker/core/message"
	"github.com/lybxkl/gmqtt/broker/core/topic"
)

type Init interface {
	Start() error
	Stop() error
}

type EventType int

const (
	_ = iota
	CommonMsg
	ShareMsg
	CleanSess
)

type Event struct {
	cid    *string // 指定操作哪个客户端
	fromIp string
	body   []byte // 消息或者其它数据
	EventType
}

// MsgDispatcher 消息调度器，broker内部消费消息使用
// 启动协程一直循环从队列中消费，32个一组封装为一个集合
// 然后丢到协程池中执行
type MsgDispatcher interface {
	RegisterHandle(func(msg message.Message, cid *string) error) // 注册消息到达后的处理逻辑
	Init
}

// ReSendMsgService 消息重发服务
// 启动协程从队列中获取需要重发的clientId及消息，
// 客户端和服务端都必须使用原始报文标识符重新发送任何未被确认的 PUBLISH 报文(当QoS > 0)和PUBREL报文.
// 这是唯一要求客户端 或服务端重发消息的情况. 客户端和服务端不能在其他任何时间重发消息
type ReSendMsgService interface {
	RegisterHandle(func(msg message.Message) error) // 注册处理包重发服务
	ReSendOne(cid string, msg message.Message)      // 单个包重发处理，超时未收到ack的包
	ReSend(cid string)                              // 客户端重连【cleanStart为0，并且会话存在】，重发未完成的过程消息以及离线消息
	Init
}

type SessionManagerService interface {
	// QueryLastState 查询客户端最后的会话状态
	// 若为1 表示在线，需要挤掉
	// 若为2 表示从未连接过
	// 若为>2，表示上次离线10位时间戳
	QueryLastState(cid string) (int64, error)
	// GetSubscription 获取订阅数据
	GetSubscription(cid string) ([]topic.Sub, error)

	Init
}

type ClusterMsgTransfer interface {
	Init

	// Subscribe 订阅此broker需要从集群中消费的主题
	// 集群其它broker就可以清楚哪些数据需要发送给此broker
	Subscribe(tSub topic.Sub) error

	// SendMsg 发送本broker的消息到集群中订阅该主题的broker去
	SendMsg(msg message.Message) error
	// ConsumeMsg 从集群中消费消息，将收到的消息丢到 MsgDispatcher 去处理，当前消息类型为 CLUSTER_MSG
	ConsumeMsg(size uint64) ([]message.Message, error)
}

// NetServer 网络服务 接收客户端连接
type NetServer interface {
	Init
}
