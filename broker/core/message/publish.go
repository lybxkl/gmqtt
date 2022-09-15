package message

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync/atomic"
)

var _ Message = (*PublishMessage)(nil)

// PublishMessage A PUBLISH Control Packet is sent from a Client to a Server or from Server to a Client
// to transport an Application Message.
type PublishMessage struct {
	header

	// === 可变报头 ===
	topic []byte // 主题
	// qos > 0 需要要报文标识符，数据放在header里面
	// 位置还是在topic后面

	// 属性
	propertiesLen          uint32   // 属性长度，变长字节整数
	payloadFormatIndicator byte     // 载荷格式指示，默认0 ， 服务端必须把接收到的应用消息中的载荷格式指示原封不动的发给所有的订阅者
	messageExpiryInterval  uint32   // 消息过期间隔，没有过期间隔，则应用消息不会过期 如果消息过期间隔已过期，服务端还没开始向匹配的订阅者交付该消息，则服务端必须删除该订阅者的消息副本
	topicAlias             uint16   // 主题别名，可以没有传，传了就不能为0，并且不能发超过connack中主题别名最大值
	responseTopic          []byte   // 响应主题
	correlationData        []byte   // 对比数据
	userProperty           [][]byte // 用户属性 , 保证顺序
	subscriptionIdentifier uint32   // 一个变长字节整数表示的订阅标识符 从1到268,435,455。订阅标识符的值为0将造成协议错误
	contentType            []byte   // 内容类型 UTF-8编码

	// === 载荷 ===
	payload []byte // 用固定报头中的剩余长度字段的值减去可变报头的长度。包含零长度有效载荷的PUBLISH报文是合法的
}

// NewPublishMessage creates a new PUBLISH message.
func NewPublishMessage() *PublishMessage {
	msg := &PublishMessage{}
	msg.SetType(PUBLISH)

	return msg
}

func (pub PublishMessage) String() string {
	return fmt.Sprintf("%s, Topic=%q, QoS=%d, Retained=%t, Dup=%t, Payload=%s, "+
		"PropertiesLen=%d, Payload Format Indicator=%d, Message Expiry Interval=%d, Topic Alias=%v, Response Topic=%s, "+
		"Correlation Data=%s, User Property=%s, Subscription Identifier=%v, Content Type=%s",
		pub.header, pub.topic, pub.QoS(), pub.Retain(), pub.Dup(), pub.payload,
		pub.propertiesLen, pub.payloadFormatIndicator, pub.messageExpiryInterval, pub.topicAlias, pub.responseTopic,
		pub.correlationData, pub.userProperty, pub.subscriptionIdentifier, pub.contentType)
}

func (pub *PublishMessage) PropertiesLen() uint32 {
	return pub.propertiesLen
}

func (pub *PublishMessage) SetPropertiesLen(propertiesLen uint32) {
	pub.propertiesLen = propertiesLen
	pub.dirty = true
}

func (pub *PublishMessage) PayloadFormatIndicator() byte {
	return pub.payloadFormatIndicator
}

func (pub *PublishMessage) SetPayloadFormatIndicator(payloadFormatIndicator byte) {
	pub.payloadFormatIndicator = payloadFormatIndicator
	pub.dirty = true
}

func (pub *PublishMessage) MessageExpiryInterval() uint32 {
	return pub.messageExpiryInterval
}

func (pub *PublishMessage) SetMessageExpiryInterval(messageExpiryInterval uint32) {
	pub.messageExpiryInterval = messageExpiryInterval
	pub.dirty = true
}

func (pub *PublishMessage) TopicAlias() uint16 {
	return pub.topicAlias
}

func (pub *PublishMessage) SetTopicAlias(topicAlias uint16) {
	pub.topicAlias = topicAlias
	pub.dirty = true
}
func (pub *PublishMessage) SetNilTopicAndAlias(alias uint16) {
	pub.topicAlias = alias
	pub.topic = nil
	pub.dirty = true
}
func (pub *PublishMessage) ResponseTopic() []byte {
	return pub.responseTopic
}

func (pub *PublishMessage) SetResponseTopic(responseTopic []byte) {
	pub.responseTopic = responseTopic
	pub.dirty = true
}

func (pub *PublishMessage) CorrelationData() []byte {
	return pub.correlationData
}

func (pub *PublishMessage) SetCorrelationData(correlationData []byte) {
	pub.correlationData = correlationData
	pub.dirty = true
}

func (pub *PublishMessage) UserProperty() [][]byte {
	return pub.userProperty
}

func (pub *PublishMessage) AddUserPropertys(userProperty [][]byte) {
	pub.userProperty = append(pub.userProperty, userProperty...)
	pub.dirty = true
}

func (pub *PublishMessage) AddUserProperty(userProperty []byte) {
	pub.userProperty = append(pub.userProperty, userProperty)
	pub.dirty = true
}

func (pub *PublishMessage) SubscriptionIdentifier() uint32 {
	return pub.subscriptionIdentifier
}

func (pub *PublishMessage) SetSubscriptionIdentifier(subscriptionIdentifier uint32) {
	pub.subscriptionIdentifier = subscriptionIdentifier
	pub.dirty = true
}

func (pub *PublishMessage) ContentType() []byte {
	return pub.contentType
}

func (pub *PublishMessage) SetContentType(contentType []byte) {
	pub.contentType = contentType
	pub.dirty = true
}

// Dup returns the value specifying the duplicate delivery of a PUBLISH Control Packet.
// If the DUP flag is set to 0, it indicates that pub is the first occasion that the
// Client or Server has attempted to send pub MQTT PUBLISH Packet. If the DUP flag is
// set to 1, it indicates that pub might be re-delivery of an earlier attempt to send
// the Packet.
func (pub *PublishMessage) Dup() bool {
	return ((pub.Flags() >> 3) & 0x1) == 1
}

// SetDup sets the value specifying the duplicate delivery of a PUBLISH Control Packet.
func (pub *PublishMessage) SetDup(v bool) {
	if v {
		pub.mtypeflags[0] |= 0x8 // 00001000
	} else {
		pub.mtypeflags[0] &= 247 // 11110111
	}
}

// Retain returns the value of the RETAIN flag. This flag is only used on the PUBLISH
// Packet. If the RETAIN flag is set to 1, in a PUBLISH Packet sent by a Client to a
// Server, the Server MUST store the Application Message and its QoS, so that it can be
// delivered to future subscribers whose subscriptions match its topic name.
func (pub *PublishMessage) Retain() bool {
	return (pub.Flags() & 0x1) == 1
}

// SetRetain sets the value of the RETAIN flag.
func (pub *PublishMessage) SetRetain(v bool) {
	if v {
		pub.mtypeflags[0] |= 0x1 // 00000001
	} else {
		pub.mtypeflags[0] &= 254 // 11111110
	}
}

// QoS returns the field that indicates the level of assurance for delivery of an
// Application Message. The values are QosAtMostOnce, QosAtLeastOnce and QosExactlyOnce.
func (pub *PublishMessage) QoS() byte {
	return (pub.Flags() >> 1) & 0x3
}

// SetQoS sets the field that indicates the level of assurance for delivery of an
// Application Message. The values are QosAtMostOnce, QosAtLeastOnce and QosExactlyOnce.
// An error is returned if the value is not one of these.
func (pub *PublishMessage) SetQoS(v byte) error {
	if v != 0x0 && v != 0x1 && v != 0x2 {
		return fmt.Errorf("publish/SetQoS: Invalid QoS %d.", v)
	}

	pub.mtypeflags[0] = (pub.mtypeflags[0] & 249) | (v << 1) // 249 = 11111001
	pub.dirty = true
	return nil
}

// Topic returns the the topic name that identifies the information channel to which
// payload data is published.
func (pub *PublishMessage) Topic() []byte {
	return pub.topic
}

// SetTopic sets the the topic name that identifies the information channel to which
// payload data is published. An error is returned if ValidTopic() is falbase.
func (pub *PublishMessage) SetTopic(v []byte) error {
	if !ValidTopic(v) {
		return fmt.Errorf("publish/SetTopic: Invalid topic name (%s). Must not be empty or contain wildcard characters", string(v))
	}

	pub.topic = v
	pub.dirty = true

	return nil
}

// Payload returns the application message that's part of the PUBLISH message.
func (pub *PublishMessage) Payload() []byte {
	return pub.payload
}

// SetPayload sets the application message that's part of the PUBLISH message.
func (pub *PublishMessage) SetPayload(v []byte) {
	pub.payload = v
	pub.dirty = true
}

func (pub *PublishMessage) Len() int {
	if !pub.dirty {
		return len(pub.dbuf)
	}

	ml := pub.msglen()

	if err := pub.SetRemainingLength(uint32(ml)); err != nil {
		return 0
	}

	return pub.header.msglen() + ml
}

func (pub *PublishMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := pub.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	n := 0
	// === 可变报头 ===
	pub.topic, n, err = readLPBytes(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	// The packet identifier field is only present in the PUBLISH packets where the
	// QoS level is 1 or 2
	if pub.QoS() != 0 {
		//pub.packetId = binary.BigEndian.Uint16(src[total:])

		pub.packetId = CopyLen(src[total:total+2], 2) //src[total : total+2]
		total += 2
	}

	propertiesLen, n, err := lbDecode(src[total:]) // 属性长度
	if err != nil {
		return 0, err
	}
	total += n
	pub.propertiesLen = propertiesLen

	if total < len(src) && src[total] == LoadFormatDescription { // 载荷格式
		total++
		pub.payloadFormatIndicator = src[total]
		total++
		if total < len(src) && src[total] == LoadFormatDescription {
			return 0, ProtocolError
		}
	}
	if total < len(src) && src[total] == MessageExpirationTime { // 消息过期间隔
		total++
		pub.messageExpiryInterval = binary.BigEndian.Uint32(src[total:])
		total += 4
		if total < len(src) && src[total] == MessageExpirationTime {
			return 0, ProtocolError
		}
	}
	if total < len(src) && src[total] == ThemeAlias { // 主题别名
		total++
		pub.topicAlias = binary.BigEndian.Uint16(src[total:])
		total += 2
		if pub.topicAlias == 0 || (total < len(src) && src[total] == ThemeAlias) {
			return 0, ProtocolError
		}
	}
	if pub.topicAlias == 0 { // 没有主题别名才验证topic
		if !ValidTopic(pub.topic) {
			return total, fmt.Errorf("publish/Decode: Invalid topic name (%s). Must not be empty or contain wildcard characters", string(pub.topic))
		}
	}
	if total < len(src) && src[total] == ResponseTopic { // 响应主题
		total++
		pub.responseTopic, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if len(pub.responseTopic) == 0 || (total < len(src) && src[total] == ResponseTopic) {
			return 0, ProtocolError
		}
	}
	if total < len(src) && src[total] == RelatedData { // 对比数据
		total++
		pub.correlationData, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if len(pub.correlationData) == 0 || (total < len(src) && src[total] == RelatedData) {
			return 0, ProtocolError
		}
	}
	pub.userProperty, n, err = decodeUserProperty(src[total:]) // 用户属性
	total += n
	if err != nil {
		return total, err
	}
	if total < len(src) && src[total] == DefiningIdentifiers { // 订阅标识符
		total++
		pub.subscriptionIdentifier, n, err = lbDecode(src[total:])
		if err != nil {
			return 0, ProtocolError
		}
		total += n
		if pub.subscriptionIdentifier == 0 || (total < len(src) && src[total] == DefiningIdentifiers) {
			return 0, ProtocolError
		}
	}
	if total < len(src) && src[total] == ContentType { // 内容类型
		total++
		pub.contentType, n, err = readLPBytes(src[total:])
		if err != nil {
			return 0, ProtocolError
		}
		total += n
		if len(pub.contentType) == 0 || (total < len(src) && src[total] == ContentType) {
			return 0, ProtocolError
		}
	}
	// === 载荷 ===
	l := int(pub.remlen) - (total - hn)
	pub.payload = CopyLen(src[total:total+l], l)
	total += len(pub.payload)

	pub.dirty = false

	return total, nil
}

func (pub *PublishMessage) Encode(dst []byte) (int, error) {
	if !pub.dirty {
		if len(dst) < len(pub.dbuf) {
			return 0, fmt.Errorf("publish/Encode: Insufficient buffer size. Expecting %d, got %d.", len(pub.dbuf), len(dst))
		}

		return copy(dst, pub.dbuf), nil
	}
	if len(pub.topic) == 0 && pub.topicAlias == 0 {
		return 0, fmt.Errorf("publish/Encode: Topic name is empty, and topic alice <= 0.")
	}

	if len(pub.payload) == 0 {
		return 0, fmt.Errorf("publish/Encode: Payload is empty.")
	}

	ml := pub.msglen()
	hl := pub.header.msglen()

	if err := pub.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("publish/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
	}

	total := 0

	n, err := pub.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	n, err = writeLPBytes(dst[total:], pub.topic)
	total += n
	if err != nil {
		return total, err
	}

	// The packet identifier field is only present in the PUBLISH packets where the QoS level is 1 or 2
	if pub.QoS() != 0 {
		if pub.PacketId() == 0 {
			pub.SetPacketId(uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff))
			//pub.packetId = uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff)
		}

		n = copy(dst[total:], pub.packetId)
		//binary.BigEndian.PutUint16(dst[total:], pub.packetId)
		total += n
	}
	// === 属性 ===
	b := lbEncode(pub.propertiesLen)
	copy(dst[total:], b)
	total += len(b)
	if pub.payloadFormatIndicator != 0 { // 载荷格式指示
		dst[total] = LoadFormatDescription
		total++
		dst[total] = pub.payloadFormatIndicator
		total++
	}
	if pub.messageExpiryInterval > 0 { // 消息过期间隔
		dst[total] = MessageExpirationTime
		total++
		binary.BigEndian.PutUint32(dst[total:], pub.messageExpiryInterval)
		total += 4
	}
	if pub.topicAlias > 0 { // 主题别名
		dst[total] = ThemeAlias
		total++
		binary.BigEndian.PutUint16(dst[total:], pub.topicAlias)
		total += 2
	}
	if len(pub.responseTopic) > 0 { // 响应主题
		dst[total] = ResponseTopic
		total++
		n, err = writeLPBytes(dst[total:], pub.responseTopic)
		total += n
		if err != nil {
			return total, err
		}
	}
	if len(pub.correlationData) > 0 { // 对比数据
		dst[total] = RelatedData
		total++
		n, err = writeLPBytes(dst[total:], pub.correlationData)
		total += n
		if err != nil {
			return total, err
		}
	}

	n, err = writeUserProperty(dst[total:], pub.userProperty) // 用户属性
	total += n

	if pub.subscriptionIdentifier > 0 && pub.subscriptionIdentifier < 168435455 { // 订阅标识符
		dst[total] = DefiningIdentifiers
		total++
		b1 := lbEncode(pub.subscriptionIdentifier)
		copy(dst[total:], b1)
		total += len(b1)
	}
	if len(pub.contentType) > 0 { // 内容类型
		dst[total] = ContentType
		total++
		n, err = writeLPBytes(dst[total:], pub.contentType)
		total += n
		if err != nil {
			return total, err
		}
	}

	copy(dst[total:], pub.payload)
	total += len(pub.payload)

	return total, nil
}

func (pub *PublishMessage) EncodeToBuf(dst *bytes.Buffer) (int, error) {
	if !pub.dirty {
		return dst.Write(pub.dbuf)
	}

	if len(pub.topic) == 0 && pub.topicAlias == 0 {
		return 0, fmt.Errorf("publish/Encode: Topic name is empty, and topic alice <= 0.")
	}

	if len(pub.payload) == 0 {
		return 0, fmt.Errorf("publish/Encode: Payload is empty.")
	}

	ml := pub.msglen()

	if err := pub.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	_, err := pub.header.encodeToBuf(dst)
	if err != nil {
		return dst.Len(), err
	}

	_, err = writeToBufLPBytes(dst, pub.topic)
	if err != nil {
		return dst.Len(), err
	}

	// The packet identifier field is only present in the PUBLISH packets where the QoS level is 1 or 2
	if pub.QoS() != 0 {
		if pub.PacketId() == 0 {
			pub.SetPacketId(uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff))
			//pub.packetId = uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff)
		}

		dst.Write(pub.packetId)
	}
	// === 属性 ===
	dst.Write(lbEncode(pub.propertiesLen))

	if pub.payloadFormatIndicator != 0 { // 载荷格式指示
		dst.WriteByte(LoadFormatDescription)
		dst.WriteByte(pub.payloadFormatIndicator)
	}
	if pub.messageExpiryInterval > 0 { // 消息过期间隔
		dst.WriteByte(MessageExpirationTime)
		_ = BigEndianPutUint32(dst, pub.messageExpiryInterval)
	}
	if pub.topicAlias > 0 { // 主题别名
		dst.WriteByte(ThemeAlias)
		_ = BigEndianPutUint16(dst, pub.topicAlias)
	}
	if len(pub.responseTopic) > 0 { // 响应主题
		dst.WriteByte(ResponseTopic)
		_, err = writeToBufLPBytes(dst, pub.responseTopic)
		if err != nil {
			return dst.Len(), err
		}
	}
	if len(pub.correlationData) > 0 { // 对比数据
		dst.WriteByte(RelatedData)
		_, err = writeToBufLPBytes(dst, pub.correlationData)
		if err != nil {
			return dst.Len(), err
		}
	}

	_, err = writeUserPropertyByBuf(dst, pub.userProperty) // 用户属性
	if err != nil {
		return dst.Len(), err
	}

	if pub.subscriptionIdentifier > 0 && pub.subscriptionIdentifier < 168435455 { // 订阅标识符
		dst.WriteByte(DefiningIdentifiers)
		dst.Write(lbEncode(pub.subscriptionIdentifier))
	}
	if len(pub.contentType) > 0 { // 内容类型
		dst.WriteByte(ContentType)
		_, err = writeToBufLPBytes(dst, pub.contentType)
		if err != nil {
			return dst.Len(), err
		}
	}

	dst.Write(pub.payload)
	return dst.Len(), nil
}

func (pub *PublishMessage) build() {
	total := 0

	total += 2 // 主题
	total += len(pub.topic)
	if pub.QoS() != 0 {
		total += 2
	}
	tag := total
	if pub.payloadFormatIndicator != 0 { // 载荷格式指示
		total += 2
	}
	if pub.messageExpiryInterval > 0 { // 消息过期间隔
		total += 5
	}
	if pub.topicAlias > 0 { // 主题别名
		total += 3
	}
	if len(pub.responseTopic) > 0 { // 响应主题
		total++
		total += 2
		total += len(pub.responseTopic)
	}
	if len(pub.correlationData) > 0 { // 对比数据
		total++
		total += 2
		total += len(pub.correlationData)
	}

	n := buildUserPropertyLen(pub.userProperty) // 用户属性
	total += n

	if pub.subscriptionIdentifier > 0 && pub.subscriptionIdentifier < 168435455 { // 订阅标识符
		total++
		b1 := lbEncode(pub.subscriptionIdentifier)
		total += len(b1)
	}
	if len(pub.contentType) > 0 { // 内容类型
		total++
		total += 2
		total += len(pub.contentType)
	}
	propertiesLen := uint32(total - tag) // 可变的属性长度
	pub.propertiesLen = propertiesLen
	total += len(lbEncode(propertiesLen))
	total += len(pub.payload)
	_ = pub.SetRemainingLength(uint32(total))
}

func (pub *PublishMessage) msglen() int {
	pub.build()
	return int(pub.remlen)
}
