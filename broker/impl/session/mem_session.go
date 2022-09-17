package sess

import (
	"fmt"
	"sync"

	"github.com/lybxkl/gmqtt/broker/core/message"
	sess "github.com/lybxkl/gmqtt/broker/core/session"
	"github.com/lybxkl/gmqtt/broker/core/store"
	"github.com/lybxkl/gmqtt/broker/core/topic"
)

const (
	// Queue size for the ack queue
	//队列的队列大小
	defaultQueueSize = 1024 >> 2
)

type AckQueues struct {
	queueSize int

	//用于传出发布QoS 1消息的Ack队列
	pub1ack sess.Ackqueue
	//传入发布QoS 2消息的Ack队列
	pub2in sess.Ackqueue
	//用于传出发布QoS 2消息的Ack队列
	pub2out sess.Ackqueue
	//用于发送订阅消息的Ack队列
	suback sess.Ackqueue
	//发送取消订阅消息的Ack队列
	unsuback sess.Ackqueue
	//用于发送PINGREQ消息的Ack队列
	pingack sess.Ackqueue
}

// 客户端会话
type session struct {
	sid string // == 客户端id

	AckQueues
	// cMsg是连接消息
	cMsg        *message.ConnectMessage
	status      sess.Status // session状态
	offlineTime int64       // 离线时间

	// 离线消息
	offlineMsg []message.Message

	//如果连接意外关闭，遗嘱消息将发布
	will *message.PublishMessage

	//cbuf是连接消息缓冲区，用于存储所有的will内容
	cbuf []byte

	// rbuf是保留的发布消息缓冲区
	rbuf []byte

	//主题存储此会话/客户机的所有topics
	topics map[string]*topic.Sub

	topicAlice   map[uint16]string
	topicAliceRe map[string]uint16

	// Initialized?
	initted bool

	//序列化对该会话的访问锁
	mu   sync.Mutex
	stop int8 // 2 为关闭
}

func NewMemSession(cMsg *message.ConnectMessage, op ...Option) (*session, error) {
	cbuf := make([]byte, cMsg.Len())

	if _, err := cMsg.Encode(cbuf); err != nil {
		return nil, err
	}

	if _, err := cMsg.Decode(cbuf); err != nil {
		return nil, err
	}

	var will *message.PublishMessage
	if cMsg.WillFlag() {
		will = message.NewPublishMessage()
		err := will.SetQoS(cMsg.WillQos())
		if err != nil {
			return nil, err
		}
		err = will.SetTopic(cMsg.WillTopic())
		if err != nil {
			return nil, err
		}
		will.SetPayload(cMsg.WillMessage())
		will.SetRetain(cMsg.WillRetain())
		will.SetMessageExpiryInterval(cMsg.WillMsgExpiryInterval())
	}

	queueSize := defaultQueueSize
	s := &session{
		sid: string(cMsg.ClientId()),
		AckQueues: AckQueues{
			queueSize: queueSize,
			pub1ack:   newAckqueue(queueSize << 1),
			pub2in:    newAckqueue(queueSize << 1),
			pub2out:   newAckqueue(queueSize << 1),
			suback:    newAckqueue(queueSize >> 2),
			unsuback:  newAckqueue(queueSize >> 2),
			pingack:   newAckqueue(queueSize >> 3),
		},
		cMsg:         cMsg,
		status:       sess.ONLINE,
		offlineTime:  0, // 待查询后赋值
		offlineMsg:   make([]message.Message, 0),
		will:         will,
		cbuf:         cbuf,
		rbuf:         make([]byte, 0),
		topics:       make(map[string]*topic.Sub),
		topicAlice:   make(map[uint16]string),
		topicAliceRe: make(map[string]uint16),
		initted:      true,
		mu:           sync.Mutex{},
		stop:         0,
	}

	for _, option := range op {
		option(s)
	}

	return s, nil
}

func (s *session) Topics() ([]topic.Sub, error) {
	return s.topic()
}

func (s *session) OfflineMsg() []message.Message {
	return s.offlineMsg
}

func (s *session) Update(msg *message.ConnectMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cbuf = make([]byte, msg.Len())
	s.cMsg = message.NewConnectMessage()

	if _, err := msg.Encode(s.cbuf); err != nil {
		return err
	}

	if _, err := s.cMsg.Decode(s.cbuf); err != nil {
		return err
	}
	s.stop = 1

	return nil
}

func (s *session) AddTopicAlice(topic string, alice uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topicAlice[alice] = topic
	s.topicAliceRe[topic] = alice
}

func (s *session) GetTopicAlice(topic []byte) (uint16, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tp, exist := s.topicAliceRe[string(topic)]
	return tp, exist
}

func (s *session) GetAliceTopic(alice uint16) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tp, exist := s.topicAlice[alice]
	return tp, exist
}

func (s *session) AddTopic(sub topic.Sub) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initted {
		return fmt.Errorf("session not yet initialized")
	}

	s.topics[string(sub.Topic)] = &sub

	return nil
}

func (s *session) RemoveTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initted {
		return fmt.Errorf("session not yet initialized")
	}

	delete(s.topics, topic)

	return nil
}

func (s *session) SubOption(tp string) topic.Sub {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.topics[tp]
	if !ok {
		return topic.Sub{}
	}
	return *sub
}

func (s *session) topic() ([]topic.Sub, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initted {
		return nil, fmt.Errorf("session not yet initialized")
	}

	var (
		subs []topic.Sub
	)

	for _, v := range s.topics {
		subs = append(subs, *v)
	}

	return subs, nil
}

func (s *session) ID() string {
	return string(s.CMsg().ClientId())
}

func (s *session) CMsg() *message.ConnectMessage {
	return s.cMsg
}

func (s *session) Will() *message.PublishMessage {
	if s.stop == 1 {
		s.stop = 2
	} else {
		s.stop = 1
	}
	return s.will
}

func (s *session) Pub1ACK() sess.Ackqueue {
	return s.pub1ack
}

func (s *session) Pub2in() sess.Ackqueue {
	return s.pub2in
}

func (s *session) Pub2out() sess.Ackqueue {
	return s.pub2out
}

func (s *session) SubACK() sess.Ackqueue {
	return s.suback
}

func (s *session) UnsubACK() sess.Ackqueue {
	return s.unsuback
}

func (s *session) PingACK() sess.Ackqueue {
	return s.pingack
}

func (s *session) SessExpiryInterval() uint32 {
	return s.cMsg.SessionExpiryInterval()
}

func (s *session) Status() sess.Status {
	return s.status
}

func (s *session) ReceiveMaximum() uint16 {
	return s.cMsg.ReceiveMaximum()
}

func (s *session) MaxPacketSize() uint32 {
	return s.cMsg.MaxPacketSize()
}

func (s *session) TopicAliasMax() uint16 {
	return s.cMsg.TopicAliasMax()
}

func (s *session) RequestRespInfo() byte {
	return s.cMsg.RequestRespInfo()
}

func (s *session) RequestProblemInfo() byte {
	return s.cMsg.RequestProblemInfo()
}

func (s *session) UserProperty() []string {
	u := s.cMsg.UserProperty()
	up := make([]string, len(u))
	for i := 0; i < len(u); i++ {
		up[i] = string(u[i])
	}
	return up
}

func (s *session) OfflineTime() int64 {
	return s.offlineTime
}

func (s *session) ClientId() string {
	return string(s.cMsg.ClientId())
}

func (s *session) SetClientId(cid string) {
	_ = s.cMsg.SetClientId([]byte(cid))
}

func (s *session) SetSessExpiryInterval(u uint32) {
	s.cMsg.SetSessionExpiryInterval(u)
}

func (s *session) SetStatus(status sess.Status) {
	s.status = status
}

func (s *session) SetReceiveMaximum(u uint16) {
	s.cMsg.SetReceiveMaximum(u)
}

func (s *session) SetMaxPacketSize(u uint32) {
	s.cMsg.SetMaxPacketSize(u)
}

func (s *session) SetTopicAliasMax(u uint16) {
	s.cMsg.SetTopicAliasMax(u)
}

func (s *session) SetRequestRespInfo(b byte) {
	s.cMsg.SetRequestRespInfo(b)
}

func (s *session) SetRequestProblemInfo(b byte) {
	s.cMsg.SetRequestProblemInfo(b)
}

func (s *session) SetUserProperty(up []string) {
	u := make([][]byte, len(up))
	for i := 0; i < len(up); i++ {
		u[i] = []byte(up[i])
	}
	s.cMsg.AddUserPropertys(u)
}

func (s *session) SetOfflineTime(i int64) {
	s.offlineTime = i
}

func (s *session) SetWill(will *message.PublishMessage) {
	s.will = will
}

func (s *session) SetSub(sub *message.SubscribeMessage) {
	tp := sub.Topics()
	qos := sub.Qos()
	for i := 0; i < len(tp); i++ {
		_ = s.AddTopic(topic.Sub{
			Topic:             tp[i],
			Qos:               qos[i],
			NoLocal:           sub.TopicNoLocal(tp[i]),
			RetainAsPublished: sub.TopicRetainAsPublished(tp[i]),
			RetainHandling:    sub.TopicRetainHandling(tp[i]),
			SubIdentifier:     sub.SubscriptionIdentifier(),
		})
	}
}

func (s *session) SetStore(_ store.SessionStore, _ store.MessageStore) {
}
