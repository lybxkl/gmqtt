package sess

import (
	"github.com/lybxkl/gmqtt/broker/core/message"
	"github.com/lybxkl/gmqtt/broker/core/topic"
)

type Manager interface {
	BuildSess(cMsg *message.ConnectMessage) (Session, error) // 将cMsg构建为一个session, 不可用的
	GetOrCreate(id string, cMsg ...*message.ConnectMessage) (_sess Session, _exist bool, _e error)
	Save(s Session) error
	Remove(s Session) error
	Exist(id string) bool
	Close() error
}

type Session interface {
	ID() string

	Update(msg *message.ConnectMessage) error
	OfflineMsg() []message.Message

	AddTopic(sub topic.Sub) error
	AddTopicAlice(topic string, alice uint16)
	GetTopicAlice(topic []byte) (uint16, bool)
	GetAliceTopic(alice uint16) (string, bool)

	RemoveTopic(topic string) error
	Topics() ([]topic.Sub, error)
	SubOption(topic string) topic.Sub // 获取主题的订阅选项

	CMsg() *message.ConnectMessage
	Will() *message.PublishMessage

	Pub1ACK() Ackqueue
	Pub2in() Ackqueue
	Pub2out() Ackqueue
	SubACK() Ackqueue
	UnsubACK() Ackqueue
	PingACK() Ackqueue

	Expand
}

type Expand interface {
	SessExpiryInterval() uint32
	Status() Status
	ClientId() string
	ReceiveMaximum() uint16
	MaxPacketSize() uint32
	TopicAliasMax() uint16
	RequestRespInfo() byte
	RequestProblemInfo() byte
	UserProperty() []string

	OfflineTime() int64

	SetSessExpiryInterval(uint32)
	SetStatus(Status)
	SetClientId(string)
	SetReceiveMaximum(uint16)
	SetMaxPacketSize(uint32)
	SetTopicAliasMax(uint16)
	SetRequestRespInfo(byte)
	SetRequestProblemInfo(byte)
	SetUserProperty([]string)

	SetOfflineTime(int64)

	SetWill(*message.PublishMessage)
	SetSub(*message.SubscribeMessage)
}

type Status uint8

const (
	_       Status = iota
	NULL           // 从未连接过（之前 cleanStart为1 的也为NULL）
	ONLINE         // 在线
	OFFLINE        // cleanStart为0，且连接过mqtt集群，已离线，会返回offlineTime（离线时间）
)
