package message

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type ConnackMessage struct {
	header

	sessionPresent bool
	reasonCode     ReasonCode

	// Connack 属性
	propertiesLen                   uint32   // 属性长度
	sessionExpiryInterval           uint32   // 会话过期间隔
	receiveMaximum                  uint16   // 接收最大值
	maxQos                          byte     // 最大服务质量
	retainAvailable                 byte     // 保留可用
	maxPacketSize                   uint32   // 最大报文长度是 MQTT 控制报文的总长度
	assignedIdentifier              []byte   // 分配的客户标识符，分配是因为connect报文中，clientid长度为0，由服务器生成
	topicAliasMax                   uint16   // 主题别名最大值（Topic Alias Maximum）
	reasonStr                       []byte   // 原因字符串 不应该被客户端所解析，如果加上原因字符串之后的CONNACK报文长度超出了客户端指定的最大报文长度，则服务端不能发送此原因字符串
	userProperties                  [][]byte // 用户属性 如果加上用户属性之后的CONNACK报文长度超出了客户端指定的最大报文长度，则服务端不能发送此属性
	wildcardSubscriptionAvailable   byte     // 通配符可用
	subscriptionIdentifierAvailable byte     // 订阅标识符可用
	sharedSubscriptionAvailable     byte     // 支持共享订阅
	serverKeepAlive                 uint16   // 服务保持连接时间 如果服务端发送了服务端保持连接（Server Keep Alive）属性，客户端必须使用此值代替其在CONNECT报文中发送的保持连接时间值
	responseInformation             []byte   // 响应信息 以UTF-8编码的字符串，作为创建响应主题（Response Topic）的基本信息
	serverReference                 []byte   // 服务端参考
	authMethod                      []byte   // 认证方法，必须与connect中的一致
	authData                        []byte   // 认证数据 此数据的内容由认证方法和已交换的认证数据状态定义
	// 无载荷
}

var _ Message = (*ConnackMessage)(nil)

// NewConnackMessage creates a new CONNACK message
func NewConnackMessage() *ConnackMessage {
	msg := &ConnackMessage{}
	msg.SetType(CONNACK)
	msg.SetRetainAvailable(0x01)
	msg.SetMaxPacketSize(1024) // FIXME 更新为根据connect中的最大来取
	return msg
}

// String returns a string representation of the CONNACK message
func (connAckMsg ConnackMessage) String() string {
	return fmt.Sprintf("Header==>> \t%s\nVariable header==>> \tSession Present=%t\tReason code=%s\t"+
		"Properties\t\t"+
		"Length=%v\t\tSession Expiry Interval=%v\t\tReceive Maximum=%v\t\tMaximum QoS=%v\t\t"+
		"Retain Available=%b\t\tMax Packet Size=%v\t\tAssignedIdentifier=%v\t\t"+
		"Topic Alias Max=%v\t\tReason Str=%v\t\tUser Properties=%v\t\t"+
		"Wildcard Subscription Available=%b\t\tSubscription Identifier Available=%b\t\tShared Subscription Available=%b\t\tServer Keep Alive=%v\t\t"+
		"Response Information=%v\t\tServer Reference=%v\t\tAuth Method=%v\t\tAuth Data=%v\t",
		connAckMsg.header,

		connAckMsg.sessionPresent, connAckMsg.reasonCode,

		connAckMsg.propertiesLen,
		connAckMsg.sessionExpiryInterval,
		connAckMsg.receiveMaximum,
		connAckMsg.maxQos,
		connAckMsg.retainAvailable,
		connAckMsg.maxPacketSize,
		connAckMsg.assignedIdentifier,
		connAckMsg.topicAliasMax,
		connAckMsg.reasonStr,
		connAckMsg.userProperties,

		connAckMsg.wildcardSubscriptionAvailable,
		connAckMsg.subscriptionIdentifierAvailable,
		connAckMsg.sharedSubscriptionAvailable,
		connAckMsg.serverKeepAlive,
		connAckMsg.responseInformation,
		connAckMsg.serverReference,
		connAckMsg.authMethod,
		connAckMsg.authData,
	)
}
func (connAckMsg *ConnackMessage) build() {
	propertiesLen := 0
	// 属性
	if connAckMsg.sessionExpiryInterval > 0 { // 会话过期间隔
		propertiesLen += 5
	}
	if connAckMsg.receiveMaximum > 0 && connAckMsg.receiveMaximum < 65535 { // 接收最大值
		propertiesLen += 3
	} else {
		connAckMsg.receiveMaximum = 65535
	}
	if connAckMsg.maxQos > 0 { // 最大服务质量，正常都会编码
		propertiesLen += 2
	}
	if connAckMsg.retainAvailable != 1 { // 保留可用
		propertiesLen += 2
	}
	if connAckMsg.maxPacketSize > 0 { // 最大报文长度
		propertiesLen += 5
	} else {
		// todo connAckMsg.maxPacketSize = ?
	}
	if len(connAckMsg.assignedIdentifier) > 0 { // 分配客户标识符
		propertiesLen++
		propertiesLen += 2
		propertiesLen += len(connAckMsg.assignedIdentifier)
	}
	if connAckMsg.topicAliasMax > 0 { // 主题别名最大值
		propertiesLen += 3
	}
	if len(connAckMsg.reasonStr) > 0 { // 原因字符串
		propertiesLen++
		propertiesLen += 2
		propertiesLen += len(connAckMsg.reasonStr)
	}

	n := buildUserPropertyLen(connAckMsg.userProperties) // 用户属性
	propertiesLen += n

	if connAckMsg.wildcardSubscriptionAvailable != 1 { // 通配符订阅可用
		propertiesLen += 2
	}
	if connAckMsg.subscriptionIdentifierAvailable != 1 { // 订阅标识符可用
		propertiesLen += 2
	}
	if connAckMsg.sharedSubscriptionAvailable != 1 { // 共享订阅可用
		propertiesLen += 2
	}
	if connAckMsg.serverKeepAlive > 0 { // 服务端保持连接
		propertiesLen += 3
	}
	if len(connAckMsg.responseInformation) > 0 { // 响应信息
		propertiesLen++
		propertiesLen += 2
		propertiesLen += len(connAckMsg.responseInformation)
	}
	if len(connAckMsg.serverReference) > 0 { // 服务端参考
		propertiesLen++
		propertiesLen += 2
		propertiesLen += len(connAckMsg.serverReference)
	}
	if len(connAckMsg.authMethod) > 0 { // 认证方法
		propertiesLen++
		propertiesLen += 2
		propertiesLen += len(connAckMsg.authMethod)
	}
	if len(connAckMsg.authData) > 0 { // 认证数据
		propertiesLen++
		propertiesLen += 2
		propertiesLen += len(connAckMsg.authData)
	}
	connAckMsg.propertiesLen = uint32(propertiesLen)
	// 两个 1 分别是连接确认标志和连接原因码
	_ = connAckMsg.SetRemainingLength(uint32(1 + 1 + propertiesLen + len(lbEncode(connAckMsg.propertiesLen))))
}
func (connAckMsg *ConnackMessage) PropertiesLen() uint32 {
	return connAckMsg.propertiesLen
}

func (connAckMsg *ConnackMessage) SetPropertiesLen(propertiesLen uint32) {
	connAckMsg.propertiesLen = propertiesLen
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) SessionExpiryInterval() uint32 {
	return connAckMsg.sessionExpiryInterval
}

func (connAckMsg *ConnackMessage) SetSessionExpiryInterval(sessionExpiryInterval uint32) {
	connAckMsg.sessionExpiryInterval = sessionExpiryInterval
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) ReceiveMaximum() uint16 {
	return connAckMsg.receiveMaximum
}

func (connAckMsg *ConnackMessage) SetReceiveMaximum(receiveMaximum uint16) {
	connAckMsg.receiveMaximum = receiveMaximum
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) MaxQos() byte {
	return connAckMsg.maxQos
}

func (connAckMsg *ConnackMessage) SetMaxQos(maxQos byte) {
	connAckMsg.maxQos = maxQos
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) RetainAvailable() byte {
	return connAckMsg.retainAvailable
}

func (connAckMsg *ConnackMessage) SetRetainAvailable(retainAvailable byte) {
	connAckMsg.retainAvailable = retainAvailable
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) MaxPacketSize() uint32 {
	return connAckMsg.maxPacketSize
}

func (connAckMsg *ConnackMessage) SetMaxPacketSize(maxPacketSize uint32) {
	connAckMsg.maxPacketSize = maxPacketSize
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) AssignedIdentifier() []byte {
	return connAckMsg.assignedIdentifier
}

func (connAckMsg *ConnackMessage) SetAssignedIdentifier(assignedIdentifier []byte) {
	connAckMsg.assignedIdentifier = assignedIdentifier
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) TopicAliasMax() uint16 {
	return connAckMsg.topicAliasMax
}

func (connAckMsg *ConnackMessage) SetTopicAliasMax(topicAliasMax uint16) {
	connAckMsg.topicAliasMax = topicAliasMax
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) ReasonStr() []byte {
	return connAckMsg.reasonStr
}

func (connAckMsg *ConnackMessage) SetReasonStr(reasonStr []byte) {
	connAckMsg.reasonStr = reasonStr
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) UserProperties() [][]byte {
	return connAckMsg.userProperties
}

func (connAckMsg *ConnackMessage) SetUserProperties(userProperty [][]byte) {
	connAckMsg.userProperties = userProperty
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) AddUserProperties(userProperty [][]byte) {
	connAckMsg.userProperties = append(connAckMsg.userProperties, userProperty...)
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) AddUserProperty(userProperty []byte) {
	connAckMsg.userProperties = append(connAckMsg.userProperties, userProperty)
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) WildcardSubscriptionAvailable() byte {
	return connAckMsg.wildcardSubscriptionAvailable
}

func (connAckMsg *ConnackMessage) SetWildcardSubscriptionAvailable(wildcardSubscriptionAvailable byte) {
	connAckMsg.wildcardSubscriptionAvailable = wildcardSubscriptionAvailable
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) SubscriptionIdentifierAvailable() byte {
	return connAckMsg.subscriptionIdentifierAvailable
}

func (connAckMsg *ConnackMessage) SetSubscriptionIdentifierAvailable(subscriptionIdentifierAvailable byte) {
	connAckMsg.subscriptionIdentifierAvailable = subscriptionIdentifierAvailable
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) SharedSubscriptionAvailable() byte {
	return connAckMsg.sharedSubscriptionAvailable
}

func (connAckMsg *ConnackMessage) SetSharedSubscriptionAvailable(sharedSubscriptionAvailable byte) {
	connAckMsg.sharedSubscriptionAvailable = sharedSubscriptionAvailable
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) ServerKeepAlive() uint16 {
	return connAckMsg.serverKeepAlive
}

func (connAckMsg *ConnackMessage) SetServerKeepAlive(serverKeepAlive uint16) {
	connAckMsg.serverKeepAlive = serverKeepAlive
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) ResponseInformation() []byte {
	return connAckMsg.responseInformation
}

func (connAckMsg *ConnackMessage) SetResponseInformation(responseInformation []byte) {
	connAckMsg.responseInformation = responseInformation
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) ServerReference() []byte {
	return connAckMsg.serverReference
}

func (connAckMsg *ConnackMessage) SetServerReference(serverReference []byte) {
	connAckMsg.serverReference = serverReference
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) AuthMethod() []byte {
	return connAckMsg.authMethod
}

func (connAckMsg *ConnackMessage) SetAuthMethod(authMethod []byte) {
	connAckMsg.authMethod = authMethod
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) AuthData() []byte {
	return connAckMsg.authData
}

func (connAckMsg *ConnackMessage) SetAuthData(authData []byte) {
	connAckMsg.authData = authData
	connAckMsg.dirty = true
}

// SessionPresent returns the session present flag value
func (connAckMsg *ConnackMessage) SessionPresent() bool {
	return connAckMsg.sessionPresent
}

// SetSessionPresent sets the value of the session present flag
// SetSessionPresent设置会话present标志的值
func (connAckMsg *ConnackMessage) SetSessionPresent(v bool) {
	if v {
		connAckMsg.sessionPresent = true
	} else {
		connAckMsg.sessionPresent = false
	}

	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) ReasonCode() ReasonCode {
	return connAckMsg.reasonCode
}

func (connAckMsg *ConnackMessage) SetReasonCode(ret ReasonCode) {
	connAckMsg.reasonCode = ret
	connAckMsg.dirty = true
}

func (connAckMsg *ConnackMessage) Len() int {
	if !connAckMsg.dirty {
		return len(connAckMsg.dbuf)
	}

	ml := connAckMsg.msglen()

	if err := connAckMsg.SetRemainingLength(uint32(ml)); err != nil {
		return 0
	}

	return connAckMsg.header.msglen() + ml
}

func (connAckMsg *ConnackMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := connAckMsg.header.decode(src)
	total += n
	if err != nil {
		return total, err
	}

	b := src[total] // 连接确认标志，7-1必须设置为0

	if b&254 != 0 {
		return 0, ProtocolError // fmt.Errorf("connack/Decode: Bits 7-1 in Connack Acknowledge Flags byte (1) are not 0")
	}

	connAckMsg.sessionPresent = b&0x1 == 1 // 连接确认标志的第0位为会话存在标志
	total++

	b = src[total] // 连接原因码

	// Read reason code
	if b > UnsupportedWildcardSubscriptions.Value() {
		return 0, ProtocolError // fmt.Errorf("connack/Decode: Invalid CONNACK return code (%d)", b)
	}

	connAckMsg.reasonCode = ReasonCode(b)
	total++
	if !ValidConnAckReasonCode(connAckMsg.reasonCode) {
		return total, ProtocolError
	}

	// Connack 属性

	connAckMsg.propertiesLen, n, err = lbDecode(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	if total < len(src) && src[total] == SessionExpirationInterval { // 会话过期间隔
		total++
		connAckMsg.sessionExpiryInterval = binary.BigEndian.Uint32(src[total:])
		total += 4
		if total < len(src) && src[total] == SessionExpirationInterval {
			return 0, ProtocolError
		}
	}
	if total < len(src) && src[total] == MaximumQuantityReceived { // 接收最大值
		total++
		connAckMsg.receiveMaximum = binary.BigEndian.Uint16(src[total:])
		total += 2
		if connAckMsg.receiveMaximum == 0 || (total < len(src) && src[total] == MaximumQuantityReceived) {
			return 0, ProtocolError
		}
	} else {
		connAckMsg.receiveMaximum = 65535
	}
	if total < len(src) && src[total] == MaximumQoS { // 最大服务质量
		total++
		connAckMsg.maxQos = src[total]
		total++
		if connAckMsg.maxQos > 2 || connAckMsg.maxQos < 0 || (total < len(src) && src[total] == MaximumQoS) {
			return 0, ProtocolError
		}
	} else {
		connAckMsg.maxQos = 2 //  默认2
	}
	if total < len(src) && src[total] == PreservePropertyAvailability { // 保留可用
		total++
		connAckMsg.retainAvailable = src[total]
		total++
		if (connAckMsg.retainAvailable != 0 && connAckMsg.retainAvailable != 1) || (total < len(src) && src[total] == PreservePropertyAvailability) {
			return 0, ProtocolError
		}
	} else {
		connAckMsg.retainAvailable = 0x01
	}
	if total < len(src) && src[total] == MaximumMessageLength { // 最大报文长度
		total++
		connAckMsg.maxPacketSize = binary.BigEndian.Uint32(src[total:])
		total += 4
		if connAckMsg.maxPacketSize == 0 || (total < len(src) && src[total] == MaximumMessageLength) {
			return 0, ProtocolError
		}
	} else {
		// TODO 按照协议由固定报头中的剩余长度可编码最大值和协议报头对数据包的大小做限制
	}
	if total < len(src) && src[total] == AssignCustomerIdentifiers { // 分配的客户端标识符
		total++
		connAckMsg.assignedIdentifier, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if total < len(src) && src[total] == AssignCustomerIdentifiers {
			return total, ProtocolError
		}
	}
	if total < len(src) && src[total] == MaximumLengthOfTopicAlias { // 主题别名最大值
		total++
		connAckMsg.topicAliasMax = binary.BigEndian.Uint16(src[total:])
		total += 2
		if total < len(src) && src[total] == MaximumLengthOfTopicAlias {
			return 0, ProtocolError
		}
	}
	if total < len(src) && src[total] == ReasonString { // 分配的客户端标识符
		total++
		connAckMsg.reasonStr, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if total < len(src) && src[total] == ReasonString {
			return total, ProtocolError
		}
	}

	connAckMsg.userProperties, n, err = decodeUserProperty(src[total:]) // 用户属性
	total += n
	if err != nil {
		return total, err
	}

	if total < len(src) && src[total] == WildcardSubscriptionAvailability { // 通配符订阅可用
		total++
		connAckMsg.wildcardSubscriptionAvailable = src[total]
		total++
		if (connAckMsg.wildcardSubscriptionAvailable != 0 && connAckMsg.wildcardSubscriptionAvailable != 1) ||
			(total < len(src) && src[total] == WildcardSubscriptionAvailability) {
			return 0, ProtocolError
		}
	} else {
		connAckMsg.wildcardSubscriptionAvailable = 0x01
	}
	if total < len(src) && src[total] == AvailabilityOfSubscriptionIdentifiers { // 订阅标识符可用
		total++
		connAckMsg.subscriptionIdentifierAvailable = src[total]
		total++
		if (connAckMsg.subscriptionIdentifierAvailable != 0 && connAckMsg.subscriptionIdentifierAvailable != 1) ||
			(total < len(src) && src[total] == AvailabilityOfSubscriptionIdentifiers) {
			return 0, ProtocolError
		}
	} else {
		connAckMsg.subscriptionIdentifierAvailable = 0x01
	}
	if total < len(src) && src[total] == SharedSubscriptionAvailability { // 共享订阅标识符可用
		total++
		connAckMsg.sharedSubscriptionAvailable = src[total]
		total++
		if (connAckMsg.sharedSubscriptionAvailable != 0 && connAckMsg.sharedSubscriptionAvailable != 1) ||
			(total < len(src) && src[total] == SharedSubscriptionAvailability) {
			return 0, ProtocolError
		}
	} else {
		connAckMsg.subscriptionIdentifierAvailable = 0x01
	}
	if total < len(src) && src[total] == ServerSurvivalTime { // 服务保持连接
		total++
		connAckMsg.serverKeepAlive = binary.BigEndian.Uint16(src[total:])
		total++
		if total < len(src) && src[total] == ServerSurvivalTime {
			return 0, ProtocolError
		}
	}
	if total < len(src) && src[total] == SolicitedMessage { // 响应信息
		total++
		connAckMsg.responseInformation, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if total < len(src) && src[total] == SolicitedMessage {
			return total, ProtocolError
		}
	}
	if total < len(src) && src[total] == ServerReference { // 服务端参考
		total++
		connAckMsg.serverReference, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if total < len(src) && src[total] == ServerReference {
			return total, ProtocolError
		}
	}
	if total < len(src) && src[total] == AuthenticationMethod { // 认证方法
		total++
		connAckMsg.authMethod, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if total < len(src) && src[total] == AuthenticationMethod {
			return total, ProtocolError
		}
	}
	if total < len(src) && src[total] == AuthenticationData { // 认证数据
		total++
		connAckMsg.authData, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if total < len(src) && src[total] == AuthenticationData {
			return total, ProtocolError
		}
	}
	connAckMsg.dirty = false

	return total, nil
}

func (connAckMsg *ConnackMessage) Encode(dst []byte) (int, error) {
	if !connAckMsg.dirty {
		if len(dst) < len(connAckMsg.dbuf) {
			return 0, fmt.Errorf("connack/Encode: Insufficient buffer size. Expecting %d, got %d.", len(connAckMsg.dbuf), len(dst))
		}

		return copy(dst, connAckMsg.dbuf), nil
	}

	// CONNACK remaining length fixed at 2 bytes
	ml := connAckMsg.msglen()
	hl := connAckMsg.header.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("connack/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
	}

	if err := connAckMsg.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := connAckMsg.header.encode(dst[total:])
	total += n
	if err != nil {
		return 0, err
	}

	if connAckMsg.sessionPresent { // 连接确认标志
		dst[total] = 1
	}
	total++

	if connAckMsg.reasonCode > UnsupportedWildcardSubscriptions {
		return total, fmt.Errorf("connack/Encode: Invalid CONNACK return code (%d)", connAckMsg.reasonCode)
	}

	dst[total] = connAckMsg.reasonCode.Value() // 原因码
	total++

	n = copy(dst[total:], lbEncode(connAckMsg.propertiesLen))
	total += n

	// 属性
	if connAckMsg.sessionExpiryInterval > 0 { // 会话过期间隔
		dst[total] = SessionExpirationInterval
		total++
		binary.BigEndian.PutUint32(dst[total:], connAckMsg.sessionExpiryInterval)
		total += 4
	}
	if connAckMsg.receiveMaximum > 0 && connAckMsg.receiveMaximum < 65535 { // 接收最大值
		dst[total] = MaximumQuantityReceived
		total++
		binary.BigEndian.PutUint16(dst[total:], connAckMsg.receiveMaximum)
		total += 2
	}
	if connAckMsg.maxQos > 0 { // 最大服务质量，正常都会编码
		dst[total] = MaximumQoS
		total++
		dst[total] = connAckMsg.maxQos
		total++
	}
	if connAckMsg.retainAvailable != 1 { // 保留可用
		dst[total] = PreservePropertyAvailability
		total++
		dst[total] = connAckMsg.retainAvailable
		total++
	}
	if connAckMsg.maxPacketSize != 1 { // 最大报文长度
		dst[total] = MaximumMessageLength
		total++
		binary.BigEndian.PutUint32(dst[total:], connAckMsg.maxPacketSize)
		total += 4
	}
	if len(connAckMsg.assignedIdentifier) > 0 { // 分配客户标识符
		dst[total] = AssignCustomerIdentifiers
		total++
		n, err = writeLPBytes(dst[total:], connAckMsg.assignedIdentifier)
		total += n
		if err != nil {
			return total, err
		}
	}
	if connAckMsg.topicAliasMax > 0 { // 主题别名最大值
		dst[total] = MaximumLengthOfTopicAlias
		total++
		binary.BigEndian.PutUint16(dst[total:], connAckMsg.topicAliasMax)
		total += 2
	}
	if len(connAckMsg.reasonStr) > 0 { // 原因字符串
		dst[total] = ReasonString
		total++
		n, err = writeLPBytes(dst[total:], connAckMsg.reasonStr)
		total += n
		if err != nil {
			return total, err
		}
	}

	n, err = writeUserProperty(dst[total:], connAckMsg.userProperties) // 用户属性
	total += n

	if connAckMsg.wildcardSubscriptionAvailable != 1 { // 通配符订阅可用
		dst[total] = WildcardSubscriptionAvailability
		total++
		dst[total] = connAckMsg.wildcardSubscriptionAvailable
		total++
	}
	if connAckMsg.subscriptionIdentifierAvailable != 1 { // 订阅标识符可用
		dst[total] = AvailabilityOfSubscriptionIdentifiers
		total++
		dst[total] = connAckMsg.subscriptionIdentifierAvailable
		total++
	}
	if connAckMsg.sharedSubscriptionAvailable != 1 { // 共享订阅可用
		dst[total] = SharedSubscriptionAvailability
		total++
		dst[total] = connAckMsg.sharedSubscriptionAvailable
		total++
	}
	if connAckMsg.serverKeepAlive > 0 { // 服务端保持连接
		dst[total] = ServerSurvivalTime
		total++
		binary.BigEndian.PutUint16(dst[total:], connAckMsg.serverKeepAlive)
		total += 2
	}
	if len(connAckMsg.responseInformation) > 0 { // 响应信息
		dst[total] = SolicitedMessage
		total++
		n, err = writeLPBytes(dst[total:], connAckMsg.responseInformation)
		total += n
		if err != nil {
			return total, err
		}
	}
	if len(connAckMsg.serverReference) > 0 { // 服务端参考
		dst[total] = ServerReference
		total++
		n, err = writeLPBytes(dst[total:], connAckMsg.serverReference)
		total += n
		if err != nil {
			return total, err
		}
	}
	if len(connAckMsg.authMethod) > 0 { // 认证方法
		dst[total] = AuthenticationMethod
		total++
		n, err = writeLPBytes(dst[total:], connAckMsg.authMethod)
		total += n
		if err != nil {
			return total, err
		}
	}
	if len(connAckMsg.authData) > 0 { // 认证数据
		dst[total] = AuthenticationData
		total++
		n, err = writeLPBytes(dst[total:], connAckMsg.authData)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (connAckMsg *ConnackMessage) EncodeToBuf(dst *bytes.Buffer) (int, error) {
	if !connAckMsg.dirty {
		return dst.Write(connAckMsg.dbuf)
	}

	// CONNACK remaining length fixed at 2 bytes
	ml := connAckMsg.msglen()

	if err := connAckMsg.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	_, err := connAckMsg.header.encodeToBuf(dst)
	if err != nil {
		return 0, err
	}

	if connAckMsg.sessionPresent { // 连接确认标志
		dst.WriteByte(1)
	} else {
		dst.WriteByte(0)
	}

	if connAckMsg.reasonCode > UnsupportedWildcardSubscriptions {
		return dst.Len(), fmt.Errorf("connack/Encode: Invalid CONNACK return code (%d)", connAckMsg.reasonCode)
	}

	dst.WriteByte(connAckMsg.reasonCode.Value()) // 原因码

	dst.Write(lbEncode(connAckMsg.propertiesLen))

	// 属性
	if connAckMsg.sessionExpiryInterval > 0 { // 会话过期间隔
		dst.WriteByte(SessionExpirationInterval)
		_ = BigEndianPutUint32(dst, connAckMsg.sessionExpiryInterval)
	}
	if connAckMsg.receiveMaximum > 0 && connAckMsg.receiveMaximum < 65535 { // 接收最大值
		dst.WriteByte(MaximumQuantityReceived)
		_ = BigEndianPutUint16(dst, connAckMsg.receiveMaximum)
	}
	if connAckMsg.maxQos > 0 { // 最大服务质量，正常都会编码
		dst.WriteByte(MaximumQoS)
		dst.WriteByte(connAckMsg.maxQos)
	}
	if connAckMsg.retainAvailable != 1 { // 保留可用
		dst.WriteByte(PreservePropertyAvailability)
		dst.WriteByte(connAckMsg.retainAvailable)
	}
	if connAckMsg.maxPacketSize != 1 { // 最大报文长度
		dst.WriteByte(MaximumMessageLength)
		_ = BigEndianPutUint32(dst, connAckMsg.maxPacketSize)
	}
	if len(connAckMsg.assignedIdentifier) > 0 { // 分配客户标识符
		dst.WriteByte(AssignCustomerIdentifiers)
		_, err = writeToBufLPBytes(dst, connAckMsg.assignedIdentifier)
		if err != nil {
			return dst.Len(), err
		}
	}
	if connAckMsg.topicAliasMax > 0 { // 主题别名最大值
		dst.WriteByte(MaximumLengthOfTopicAlias)
		_ = BigEndianPutUint16(dst, connAckMsg.topicAliasMax)
	}
	if len(connAckMsg.reasonStr) > 0 { // 原因字符串
		dst.WriteByte(ReasonString)
		_, err = writeToBufLPBytes(dst, connAckMsg.reasonStr)
		if err != nil {
			return dst.Len(), err
		}
	}

	_, err = writeUserPropertyByBuf(dst, connAckMsg.userProperties) // 用户属性
	if err != nil {
		return dst.Len(), err
	}

	if connAckMsg.wildcardSubscriptionAvailable != 1 { // 通配符订阅可用
		dst.WriteByte(WildcardSubscriptionAvailability)
		dst.WriteByte(connAckMsg.wildcardSubscriptionAvailable)
	}
	if connAckMsg.subscriptionIdentifierAvailable != 1 { // 订阅标识符可用
		dst.WriteByte(AvailabilityOfSubscriptionIdentifiers)
		dst.WriteByte(connAckMsg.subscriptionIdentifierAvailable)
	}
	if connAckMsg.sharedSubscriptionAvailable != 1 { // 共享订阅可用
		dst.WriteByte(SharedSubscriptionAvailability)
		dst.WriteByte(connAckMsg.sharedSubscriptionAvailable)
	}
	if connAckMsg.serverKeepAlive > 0 { // 服务端保持连接
		dst.WriteByte(ServerSurvivalTime)
		_ = BigEndianPutUint16(dst, connAckMsg.serverKeepAlive)
	}
	if len(connAckMsg.responseInformation) > 0 { // 响应信息
		dst.WriteByte(SolicitedMessage)
		_, err = writeToBufLPBytes(dst, connAckMsg.responseInformation)
		if err != nil {
			return dst.Len(), err
		}
	}
	if len(connAckMsg.serverReference) > 0 { // 服务端参考
		dst.WriteByte(ServerReference)
		_, err = writeToBufLPBytes(dst, connAckMsg.serverReference)
		if err != nil {
			return dst.Len(), err
		}
	}
	if len(connAckMsg.authMethod) > 0 { // 认证方法
		dst.WriteByte(AuthenticationMethod)
		_, err = writeToBufLPBytes(dst, connAckMsg.authMethod)
		if err != nil {
			return dst.Len(), err
		}
	}
	if len(connAckMsg.authData) > 0 { // 认证数据
		dst.WriteByte(AuthenticationData)
		_, err = writeToBufLPBytes(dst, connAckMsg.authData)
		if err != nil {
			return dst.Len(), err
		}
	}
	return dst.Len(), nil
}

// propertiesLen
func (connAckMsg *ConnackMessage) msglen() int {
	connAckMsg.build()
	return int(connAckMsg.remlen)
}
