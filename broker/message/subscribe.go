package message

import (
	"bytes"
	"fmt"
	"sync/atomic"
)

var (
	_ Message             = (*SubscribeMessage)(nil)
	_ CleanReqProblemInfo = (*SubscribeMessage)(nil)
)

// SubscribeMessage The SUBSCRIBE Packet is sent from the Client to the Server to create one or more
// Subscriptions. Each Subscription registers a Client’s interest in one or more
// Topics. The Server sends PUBLISH Packets to the Client in order to forward
// Application Messages that were published to Topics that match these Subscriptions.
// The SUBSCRIBE Packet also specifies (for each Subscription) the maximum QoS with
// which the Server can send Application Messages to the Client.
type SubscribeMessage struct {
	header
	// 可变报头
	propertiesLen          uint32 // 属性长度
	subscriptionIdentifier uint32 // 订阅标识符 变长字节整数 取值范围从1到268,435,455
	userProperty           [][]byte
	// 载荷

	topics [][]byte
	qos    []byte // 订阅选项的第0和1比特代表最大服务质量字段

	/**
	非本地（NoLocal）和发布保留（Retain As Published）订阅选项在客户端把消息发送给其他服务端的情况下，可以被用来实现桥接。

	已存在订阅的情况下不发送保留消息是很有用的，比如重连完成时客户端不确定订阅是否在之前的会话连接中被创建

	不发送保存的保留消息给新创建的订阅是很有用的，比如客户端希望接收变更通知且不需要知道最初的状态

	对于某个指示其不支持保留消息的服务端，发布保留和保留处理选项的所有有效值都将得到同样的结果：
			订阅时不发送任何保留消息，且所有消息的保留标志都会被设置为0
	**/

	// 订阅选项的第2比特表示非本地（No Local）选项。
	//	值为1，表示应用消息不能被转发给发布此消息的客户端。
	// 共享订阅时把非本地选项设为1将造成协议错误
	noLocal []byte
	// 订阅选项的第3比特表示发布保留（Retain As Published）选项。
	//   值为1，表示向此订阅转发应用消息时保持消息被发布时设置的保留（RETAIN）标志。
	//   值为0，表示向此订阅转发应用消息时把保留标志设置为0。当订阅建立之后，发送保留消息时保留标志设置为1。
	retainAsPub []byte
	// 订阅选项的第4和5比特表示保留操作（Retain Handling）选项。
	//	此选项指示当订阅建立时，是否发送保留消息。
	//	此选项不影响之后的任何保留消息的发送。
	//	如果没有匹配主题过滤器的保留消息，则此选项所有值的行为都一样。值可以设置为：
	//		0 = 订阅建立时发送保留消息
	//		1 = 订阅建立时，若该订阅当前不存在则发送保留消息
	//		2 = 订阅建立时不要发送保留消息
	//	保留操作的值设置为3将造成协议错误（Protocol Error）。
	retainHandling []byte
	// 订阅选项的第6和7比特为将来所保留。服务端必须把此保留位非0的SUBSCRIBE报文当做无效报文
}

// NewSubscribeMessage creates a new SUBSCRIBE message.
func NewSubscribeMessage() *SubscribeMessage {
	msg := &SubscribeMessage{}
	msg.SetType(SUBSCRIBE)

	return msg
}

// Clone 简单克隆部分数据
func (sub *SubscribeMessage) Clone() []*SubscribeMessage {
	ret := make([]*SubscribeMessage, 0, len(sub.topics))
	for i := 0; i < len(sub.topics); i++ {
		subClone := NewSubscribeMessage()
		subClone.userProperty = sub.userProperty
		subClone.subscriptionIdentifier = sub.subscriptionIdentifier

		subClone.topics = [][]byte{sub.topics[0]}
		subClone.qos = []byte{sub.qos[0]}
		subClone.retainHandling = []byte{sub.retainHandling[0]}
		subClone.retainAsPub = []byte{sub.retainAsPub[0]}
		subClone.noLocal = []byte{sub.noLocal[0]}

		ret = append(ret, sub)
	}
	return ret
}

func (sub *SubscribeMessage) PropertiesLen() uint32 {
	return sub.propertiesLen
}

func (sub *SubscribeMessage) SetPropertiesLen(propertiesLen uint32) {
	sub.propertiesLen = propertiesLen
	sub.dirty = true
}

func (sub *SubscribeMessage) SubscriptionIdentifier() uint32 {
	return sub.subscriptionIdentifier
}

func (sub *SubscribeMessage) SetSubscriptionIdentifier(subscriptionIdentifier uint32) {
	sub.subscriptionIdentifier = subscriptionIdentifier
	sub.dirty = true
}

func (sub *SubscribeMessage) UserProperty() [][]byte {
	return sub.userProperty
}

func (sub *SubscribeMessage) SetReasonStr(reasonStr []byte) {
}

func (sub *SubscribeMessage) SetUserProperties(userProperty [][]byte) {
	sub.userProperty = userProperty
	sub.dirty = true
}

func (sub *SubscribeMessage) AddUserPropertys(userProperty [][]byte) {
	sub.userProperty = append(sub.userProperty, userProperty...)
	sub.dirty = true
}

func (sub *SubscribeMessage) AddUserProperty(userProperty []byte) {
	sub.userProperty = append(sub.userProperty, userProperty)
	sub.dirty = true
}

func (sub SubscribeMessage) String() string {
	msgstr := fmt.Sprintf("%s, Packet ID=%d", sub.header, sub.PacketId())
	msgstr = fmt.Sprintf("%s, PropertiesLen=%v, Subscription Identifier=%v, User Properties=%v", msgstr, sub.PropertiesLen(), sub.subscriptionIdentifier, sub.UserProperty())

	for i, t := range sub.topics {
		msgstr = fmt.Sprintf("%s, Topic[%d]=%q，Qos：%d，noLocal：%d，Retain As Publish：%d，RetainHandling：%d",
			msgstr, i, string(t), sub.qos[i], sub.noLocal[i], sub.retainAsPub[i], sub.retainHandling[i])
	}

	return msgstr
}

// Topics returns a list of topics sent by the Client.
func (sub *SubscribeMessage) Topics() [][]byte {
	return sub.topics
}

// AddTopic adds a single topic to the message, along with the corresponding QoS.
// An error is returned if QoS is invalid.
func (sub *SubscribeMessage) AddTopic(topic []byte, qos byte) error {
	if !ValidQos(qos) {
		return fmt.Errorf("Invalid QoS %d", qos)
	}

	var i int
	var t []byte
	var found bool

	for i, t = range sub.topics {
		if bytes.Equal(t, topic) {
			found = true
			break
		}
	}

	if found {
		sub.qos[i] = qos
		return nil
	}

	sub.topics = append(sub.topics, topic)
	sub.qos = append(sub.qos, qos)
	sub.noLocal = append(sub.noLocal, 0)
	sub.retainAsPub = append(sub.retainAsPub, 0)
	sub.retainHandling = append(sub.retainHandling, 0)
	sub.dirty = true

	return nil
}
func (sub *SubscribeMessage) AddTopicAll(topic []byte, qos byte, noLocal, retainAsPub bool, retainHandling byte) error {
	if !ValidQos(qos) {
		return fmt.Errorf("Invalid QoS %d", qos)
	}
	if retainHandling > 2 {
		return fmt.Errorf("Invalid Sub Option RetainHandling %d", retainHandling)
	}

	var i int
	var t []byte
	var found bool

	for i, t = range sub.topics {
		if bytes.Equal(t, topic) {
			found = true
			break
		}
	}

	if found {
		sub.qos[i] = qos
		return nil
	}

	sub.topics = append(sub.topics, topic)
	sub.qos = append(sub.qos, qos)
	if noLocal {
		sub.noLocal = append(sub.noLocal, 1)
	} else {
		sub.noLocal = append(sub.noLocal, 0)
	}
	if retainAsPub {
		sub.retainAsPub = append(sub.retainAsPub, 1)
	} else {
		sub.retainAsPub = append(sub.retainAsPub, 0)
	}

	sub.retainHandling = append(sub.retainHandling, retainHandling)
	sub.dirty = true

	return nil
}

// RemoveTopic removes a single topic from the list of existing ones in the message.
// If topic does not exist it just does nothing.
func (sub *SubscribeMessage) RemoveTopic(topic []byte) {
	var i int
	var t []byte
	var found bool

	for i, t = range sub.topics {
		if bytes.Equal(t, topic) {
			found = true
			break
		}
	}

	if found {
		sub.topics = append(sub.topics[:i], sub.topics[i+1:]...)
		sub.qos = append(sub.qos[:i], sub.qos[i+1:]...)
		sub.noLocal = append(sub.noLocal[:i], sub.noLocal[i+1:]...)
		sub.retainAsPub = append(sub.retainAsPub[:i], sub.retainAsPub[i+1:]...)
		sub.retainHandling = append(sub.retainHandling[:i], sub.retainHandling[i+1:]...)
	}

	sub.dirty = true
}

// TopicExists checks to see if a topic exists in the list.
func (sub *SubscribeMessage) TopicExists(topic []byte) bool {
	for _, t := range sub.topics {
		if bytes.Equal(t, topic) {
			return true
		}
	}

	return false
}

// TopicQos returns the QoS level of a topic. If topic does not exist, QosFailure
// is returned.
func (sub *SubscribeMessage) TopicQos(topic []byte) byte {
	for i, t := range sub.topics {
		if bytes.Equal(t, topic) {
			return sub.qos[i]
		}
	}

	return QosFailure
}
func (sub *SubscribeMessage) TopicNoLocal(topic []byte) bool {
	for i, t := range sub.topics {
		if bytes.Equal(t, topic) {
			return sub.noLocal[i] > 0
		}
	}
	return false
}
func (sub *SubscribeMessage) SetTopicNoLocal(topic []byte, noLocal bool) {
	for i, t := range sub.topics {
		if bytes.Equal(t, topic) {
			if noLocal {
				sub.noLocal[i] = 1
			} else {
				sub.noLocal[i] = 0
			}
			sub.dirty = true
			return
		}
	}
}
func (sub *SubscribeMessage) TopicRetainAsPublished(topic []byte) bool {
	for i, t := range sub.topics {
		if bytes.Equal(t, topic) {
			return sub.retainAsPub[i] > 0
		}
	}
	return false
}
func (sub *SubscribeMessage) SetTopicRetainAsPublished(topic []byte, rap bool) {
	for i, t := range sub.topics {
		if bytes.Equal(t, topic) {
			if rap {
				sub.retainAsPub[i] = 1
			} else {
				sub.retainAsPub[i] = 0
			}
			sub.dirty = true
			return
		}
	}
}
func (sub *SubscribeMessage) TopicRetainHandling(topic []byte) RetainHandling {
	for i, t := range sub.topics {
		if bytes.Equal(t, topic) {
			return RetainHandling(sub.retainHandling[i])
		}
	}
	return CanSendRetain
}
func (sub *SubscribeMessage) SetTopicRetainHandling(topic []byte, hand RetainHandling) {
	for i, t := range sub.topics {
		if bytes.Equal(t, topic) {
			sub.retainHandling[i] = byte(hand)
			sub.dirty = true
			return
		}
	}
}

// Qos returns the list of QoS current in the message.
func (sub *SubscribeMessage) Qos() []byte {
	return sub.qos
}

func (sub *SubscribeMessage) Len() int {
	if !sub.dirty {
		return len(sub.dbuf)
	}

	ml := sub.msglen()

	if err := sub.SetRemainingLength(uint32(ml)); err != nil {
		return 0
	}

	return sub.header.msglen() + ml
}

func (sub *SubscribeMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := sub.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	//sub.packetId = binary.BigEndian.Uint16(src[total:])
	sub.packetId = CopyLen(src[total:total+2], 2)
	total += 2
	var n int
	sub.propertiesLen, n, err = lbDecode(src[total:])
	total += n
	if err != nil {
		return total, err
	}
	if int(sub.propertiesLen) > len(src[total:]) {
		return total, ProtocolError
	}
	if total < len(src) && src[total] == DefiningIdentifiers {
		total++
		sub.subscriptionIdentifier, n, err = lbDecode(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if sub.subscriptionIdentifier == 0 || src[total] == DefiningIdentifiers {
			return total, ProtocolError
		}
	}

	sub.userProperty, n, err = decodeUserProperty(src[total:]) // 用户属性
	total += n
	if err != nil {
		return total, err
	}

	remlen := int(sub.remlen) - (total - hn)
	var t []byte
	for remlen > 0 {
		t, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}

		sub.topics = append(sub.topics, t)

		sub.qos = append(sub.qos, src[total]&3)
		if src[total]&3 == 3 {
			return 0, ProtocolError
		}
		sub.noLocal = append(sub.noLocal, (src[total]&4)>>2)
		sub.retainAsPub = append(sub.retainAsPub, (src[total]&8)>>3)
		sub.retainHandling = append(sub.retainHandling, (src[total]&48)>>4)
		if (src[total]&48)>>4 == 3 {
			return 0, ProtocolError
		}
		if src[total]>>6 != 0 {
			return 0, ProtocolError
		}
		total++

		remlen = remlen - n - 1
	}

	if len(sub.topics) == 0 {
		return 0, ProtocolError //fmt.Errorf("subscribe/Decode: Empty topic list")
	}

	sub.dirty = false

	return total, nil
}

func (sub *SubscribeMessage) Encode(dst []byte) (int, error) {
	if !sub.dirty {
		if len(dst) < len(sub.dbuf) {
			return 0, fmt.Errorf("subscribe/Encode: Insufficient buffer size. Expecting %d, got %d.", len(sub.dbuf), len(dst))
		}

		return copy(dst, sub.dbuf), nil
	}

	ml := sub.msglen()
	hl := sub.header.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("subscribe/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
	}

	if err := sub.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := sub.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	if sub.PacketId() == 0 {
		sub.SetPacketId(uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff))
		//sub.packetId = uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff)
	}

	n = copy(dst[total:], sub.packetId)
	//binary.BigEndian.PutUint16(dst[total:], sub.packetId)
	total += n

	tb := lbEncode(sub.propertiesLen)
	copy(dst[total:], tb)
	total += len(tb)

	if sub.subscriptionIdentifier > 0 && sub.subscriptionIdentifier <= 268435455 {
		dst[total] = DefiningIdentifiers
		total++
		tb = lbEncode(sub.subscriptionIdentifier)
		copy(dst[total:], tb)
		total += len(tb)
	}

	n, err = writeUserProperty(dst[total:], sub.userProperty) // 用户属性
	total += n

	for i, t := range sub.topics {
		n, err = writeLPBytes(dst[total:], t)
		total += n
		if err != nil {
			return total, err
		}
		// 订阅选项
		subOp := byte(0)
		subOp |= sub.qos[i]
		subOp |= sub.noLocal[i] << 2
		subOp |= sub.retainAsPub[i] << 3
		subOp |= sub.retainHandling[i] << 4
		dst[total] = subOp
		total++
	}

	return total, nil
}

func (sub *SubscribeMessage) EncodeToBuf(dst *bytes.Buffer) (int, error) {
	if !sub.dirty {
		return dst.Write(sub.dbuf)
	}

	ml := sub.msglen()

	if err := sub.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	_, err := sub.header.encodeToBuf(dst)
	if err != nil {
		return dst.Len(), err
	}

	if sub.PacketId() == 0 {
		sub.SetPacketId(uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff))
		//sub.packetId = uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff)
	}

	dst.Write(sub.packetId)

	dst.Write(lbEncode(sub.propertiesLen))

	if sub.subscriptionIdentifier > 0 && sub.subscriptionIdentifier <= 268435455 {
		dst.WriteByte(DefiningIdentifiers)
		dst.Write(lbEncode(sub.subscriptionIdentifier))
	}

	_, err = writeUserPropertyByBuf(dst, sub.userProperty) // 用户属性
	if err != nil {
		return dst.Len(), err
	}

	for i, t := range sub.topics {
		_, err = writeToBufLPBytes(dst, t)
		if err != nil {
			return dst.Len(), err
		}
		// 订阅选项
		subOp := byte(0)
		subOp |= sub.qos[i]
		subOp |= sub.noLocal[i] << 2
		subOp |= sub.retainAsPub[i] << 3
		subOp |= sub.retainHandling[i] << 4
		dst.WriteByte(subOp)
	}

	return dst.Len(), nil
}

func (sub *SubscribeMessage) build() {
	// packet ID
	total := 2

	if sub.subscriptionIdentifier > 0 && sub.subscriptionIdentifier <= 268435455 {
		total++
		total += len(lbEncode(sub.subscriptionIdentifier))
	}

	n := buildUserPropertyLen(sub.userProperty) // 用户属性
	total += n

	sub.propertiesLen = uint32(total - 2)
	total += len(lbEncode(sub.propertiesLen))
	for _, t := range sub.topics {
		total += 2 + len(t) + 1
	}
	_ = sub.SetRemainingLength(uint32(total))
}
func (sub *SubscribeMessage) msglen() int {
	sub.build()
	return int(sub.remlen)
}
