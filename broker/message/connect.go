package message

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"regexp"
)

var (
	clientIdRegexp *regexp.Regexp
	defaultCId     = []byte("#")
)

func init() {
	// Added space for Paho compliance test
	// Added underscore (_) for MQTT C client test
	clientIdRegexp = regexp.MustCompile("^[/\\-0-9a-zA-Z _]*$")
}

type ConnectMessage struct {
	header

	// 7: username flag
	// 6: password flag
	// 5: will retain
	// 4-3: will QoS
	// 2: will flag
	// 1: clean session
	// 0: reserved
	connectFlags byte

	version byte

	keepAlive uint16

	protoName,
	clientId,
	willTopic,
	willMessage,
	username,
	password []byte

	propertiesLen         uint32   // 属性长度
	sessionExpiryInterval uint32   // 会话过期间隔
	receiveMaximum        uint16   // 接收最大值
	maxPacketSize         uint32   // 最大报文长度是 MQTT 控制报文的总长度
	topicAliasMax         uint16   // 主题别名最大值（Topic Alias Maximum）没有设置主题别名最大值属性的情况下，主题别名最大值默认为零
	requestRespInfo       byte     // 一个字节表示的 0 或 1, 请求响应信息
	requestProblemInfo    byte     // 一个字节表示的 0 或 1 , 请求问题信息
	userProperty          [][]byte // 用户属性，可变报头的 , 每对属性都是串行放置 [k1][v1][k2][v2]
	authMethod            []byte   // 认证方法
	authData              []byte   // 认证数据

	willPropertiesLen      uint32   // 遗嘱属性长度
	willDelayInterval      uint32   // 遗嘱延时间隔
	payloadFormatIndicator byte     // 载荷格式指示
	willMsgExpiryInterval  uint32   // 遗嘱消息过期间隔
	contentType            []byte   // 遗嘱内容类型 UTF-8 格式编码
	willUserProperty       [][]byte // 用户属性，载荷遗嘱属性的 , 每对属性都是串行放置 [k1][v1][k2][v2]
	responseTopic          []byte   // 响应主题
	correlationData        []byte   // 对比数据
}

var _ Message = (*ConnectMessage)(nil)

// NewConnectMessage creates a new CONNECT message.
func NewConnectMessage() *ConnectMessage {
	msg := &ConnectMessage{}
	_ = msg.SetType(CONNECT)
	msg.SetRequestProblemInfo(1)
	msg.SetMaxPacketSize(1024)
	return msg
}

// String returns a string representation of the CONNECT message
func (connMsg ConnectMessage) String() string {
	return fmt.Sprintf("Header==>> %s, Variable header==>> \tProtocol Name=%s\tProtocol Version=%v, "+
		"Connect Flags=%08b\t"+
		"User Name Flag=%v\tPassword Flag=%v\tWill Retain=%v\tWill QoS=%d\tWill Flag=%v\tClean Start=%v\tReserved=%v, "+
		"KeepAlive=%v, "+
		"Properties\t"+
		"Length=%v\tSession Expiry Interval=%v\tReceive Maximum=%v\tMaximum Packet Size=%v\t"+
		"Topic Alias Maximum=%v\tRequest Response Information=%v\tRequest Problem Information=%v\t"+
		"User Property=%s\tAuthentication Method=%s\tAuthentication Data=%s, "+
		"载荷==>>"+
		"Client ID=%q, "+
		"Will Properties\t"+
		"Will Properties Len=%v\tWill Delay Interval=%v\tPayload Format Indicator=%v\tMessage Expiry Interval=%v\t"+
		"Content Type=%s\tResponse Topic=%s\tCorrelation Data=%v\tUser Property=%s, "+
		"Will Topic=%s\tWill Payload=%s\tUsername=%s\tPassword=%s。",
		connMsg.header,

		connMsg.protoName,
		connMsg.Version(),
		connMsg.connectFlags,
		connMsg.UsernameFlag(),
		connMsg.PasswordFlag(),
		connMsg.WillRetain(),
		connMsg.WillQos(),
		connMsg.WillFlag(),
		connMsg.CleanSession(),
		connMsg.connectFlags&0x01,

		connMsg.KeepAlive(),

		connMsg.propertiesLen,
		connMsg.sessionExpiryInterval,
		connMsg.receiveMaximum,
		connMsg.maxPacketSize,
		connMsg.topicAliasMax,
		connMsg.requestRespInfo,
		connMsg.requestProblemInfo,
		connMsg.userProperty,
		connMsg.authMethod,
		connMsg.authData,

		connMsg.ClientId(),
		connMsg.willPropertiesLen,
		connMsg.willDelayInterval,
		connMsg.payloadFormatIndicator,
		connMsg.willMsgExpiryInterval,
		connMsg.contentType,
		connMsg.responseTopic,
		connMsg.correlationData,
		connMsg.willUserProperty,

		connMsg.WillTopic(),
		connMsg.WillMessage(),
		connMsg.Username(),
		connMsg.Password(),
	)
}

// 自动设置remlen , 属性长度， 遗嘱属性长度
func (connMsg *ConnectMessage) build() {
	remlen := 0
	remlen += 1 // version
	remlen += 1 // 连接标志
	remlen += 2 // keep alive
	//  protoName,
	//	clientId,
	//	willTopic,
	//	willMessage,
	//	username,
	//	password []byte
	remlen += 2
	remlen += len(connMsg.protoName)
	// 载荷 客户端标识符
	remlen += 2
	remlen += len(connMsg.clientId)
	// 载荷 遗嘱
	if connMsg.WillFlag() && len(connMsg.willTopic) > 0 {
		remlen += 2
		remlen += len(connMsg.willTopic)
	}
	if connMsg.WillFlag() && len(connMsg.willMessage) > 0 {
		remlen += 2
		remlen += len(connMsg.willMessage)
	}
	if connMsg.UsernameFlag() {
		remlen += 2
		remlen += len(connMsg.username)
	}
	if connMsg.PasswordFlag() {
		remlen += 2
		remlen += len(connMsg.password)
	}
	// 属性长度
	//  sessionExpiryInterval uint32            // 会话过期间隔
	//	receiveMaximum        uint16            // 接收最大值
	//	maxPacketSize         uint32            // 最大报文长度是 MQTT 控制报文的总长度
	//	topicAliasMax         uint16            // 主题别名最大值（Topic Alias Maximum）
	//	requestRespInfo       byte              // 一个字节表示的 0 或 1, 请求响应信息
	//	requestProblemInfo    byte              // 一个字节表示的 0 或 1 , 请求问题信息
	//	userProperty          map[string]string // 用户属性，可变报头的
	//	authMethod            string            // 认证方法
	//	authData              []byte            // 认证数据
	propertiesLen := 0
	if connMsg.sessionExpiryInterval > 0 {
		propertiesLen += 5
	}
	if connMsg.receiveMaximum > 0 && connMsg.receiveMaximum < 65535 {
		propertiesLen += 3
	}
	if connMsg.maxPacketSize > 0 {
		propertiesLen += 5
	}
	if connMsg.topicAliasMax > 0 {
		propertiesLen += 3
	}
	if connMsg.requestRespInfo != 0 {
		propertiesLen += 2
	}
	if connMsg.requestProblemInfo != 1 {
		propertiesLen += 2
	}
	n := buildUserPropertyLen(connMsg.userProperty) // 用户属性
	propertiesLen += n

	if len(connMsg.authMethod) > 0 {
		propertiesLen += 1
		propertiesLen += 2
		propertiesLen += len(connMsg.authMethod)
	}
	if len(connMsg.authData) > 0 {
		propertiesLen += 1
		propertiesLen += 2
		propertiesLen += len(connMsg.authData)
	}
	connMsg.propertiesLen = uint32(propertiesLen)

	//	willDelayInterval      uint32            // 遗嘱延时间隔
	//	payloadFormatIndicator byte              // 载荷格式指示
	//	willMsgExpiryInterval  uint32            // 遗嘱消息过期间隔
	//	contentType            string            // 遗嘱内容类型 UTF-8 格式编码
	//	willUserProperty       map[string]string // 用户属性，载荷遗嘱属性的
	//	responseTopic          string            // 响应主题
	//	correlationData        []byte            // 对比数据
	// 载荷 遗嘱属性
	willPropertiesLen := 0
	if connMsg.willDelayInterval > 0 {
		willPropertiesLen += 5
	}
	if connMsg.payloadFormatIndicator == 1 {
		willPropertiesLen += 2
	}
	if connMsg.willMsgExpiryInterval > 0 {
		willPropertiesLen += 5
	}
	if len(connMsg.contentType) > 0 {
		willPropertiesLen += 1
		willPropertiesLen += 2
		willPropertiesLen += len(connMsg.contentType)
	}

	n = buildUserPropertyLen(connMsg.willUserProperty) // 遗嘱用户属性
	propertiesLen += n

	if len(connMsg.responseTopic) > 0 {
		willPropertiesLen += 1
		willPropertiesLen += 2
		willPropertiesLen += len(connMsg.responseTopic)
	}
	if len(connMsg.correlationData) > 0 {
		willPropertiesLen += 1
		willPropertiesLen += 2
		willPropertiesLen += len(connMsg.correlationData)
	}
	connMsg.willPropertiesLen = uint32(willPropertiesLen)
	rl := uint32(remlen + propertiesLen + len(lbEncode(connMsg.propertiesLen)))
	if connMsg.WillFlag() {
		rl += uint32(willPropertiesLen + len(lbEncode(connMsg.willPropertiesLen)))
	}
	_ = connMsg.SetRemainingLength(rl)
}

func (connMsg *ConnectMessage) PropertiesLen() uint32 {
	return connMsg.propertiesLen
}

func (connMsg *ConnectMessage) SetPropertiesLen(propertiesLen uint32) {
	connMsg.propertiesLen = propertiesLen
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) SessionExpiryInterval() uint32 {
	return connMsg.sessionExpiryInterval
}

func (connMsg *ConnectMessage) SetSessionExpiryInterval(sessionExpiryInterval uint32) {
	connMsg.sessionExpiryInterval = sessionExpiryInterval
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) ReceiveMaximum() uint16 {
	return connMsg.receiveMaximum
}

func (connMsg *ConnectMessage) SetReceiveMaximum(receiveMaximum uint16) {
	connMsg.receiveMaximum = receiveMaximum
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) MaxPacketSize() uint32 {
	return connMsg.maxPacketSize
}

func (connMsg *ConnectMessage) SetMaxPacketSize(maxPacketSize uint32) {
	connMsg.maxPacketSize = maxPacketSize
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) TopicAliasMax() uint16 {
	return connMsg.topicAliasMax
}

func (connMsg *ConnectMessage) SetTopicAliasMax(topicAliasMax uint16) {
	connMsg.topicAliasMax = topicAliasMax
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) RequestRespInfo() byte {
	return connMsg.requestRespInfo
}

func (connMsg *ConnectMessage) SetRequestRespInfo(requestRespInfo byte) {
	connMsg.requestRespInfo = requestRespInfo
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) RequestProblemInfo() byte {
	return connMsg.requestProblemInfo
}

func (connMsg *ConnectMessage) SetRequestProblemInfo(requestProblemInfo byte) {
	connMsg.requestProblemInfo = requestProblemInfo
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) UserProperty() [][]byte {
	return connMsg.userProperty
}

func (connMsg *ConnectMessage) AddUserPropertys(userProperty [][]byte) {
	connMsg.userProperty = append(connMsg.userProperty, userProperty...)
	connMsg.dirty = true
}
func (connMsg *ConnectMessage) AddUserProperty(userProperty []byte) {
	connMsg.userProperty = append(connMsg.userProperty, userProperty)
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) WillUserProperty() [][]byte {
	return connMsg.willUserProperty
}

func (connMsg *ConnectMessage) AddWillUserPropertys(willUserProperty [][]byte) {
	connMsg.willUserProperty = append(connMsg.willUserProperty, willUserProperty...)
	connMsg.dirty = true
}
func (connMsg *ConnectMessage) AddWillUserProperty(willUserProperty []byte) {
	connMsg.willUserProperty = append(connMsg.willUserProperty, willUserProperty)
	connMsg.dirty = true
}
func (connMsg *ConnectMessage) AuthMethod() []byte {
	return connMsg.authMethod
}

func (connMsg *ConnectMessage) SetAuthMethod(authMethod []byte) {
	connMsg.authMethod = authMethod
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) AuthData() []byte {
	return connMsg.authData
}

func (connMsg *ConnectMessage) SetAuthData(authData []byte) {
	connMsg.authData = authData
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) WillPropertiesLen() uint32 {
	return connMsg.willPropertiesLen
}

func (connMsg *ConnectMessage) SetWillPropertiesLen(willPropertiesLen uint32) {
	connMsg.willPropertiesLen = willPropertiesLen
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) WillDelayInterval() uint32 {
	return connMsg.willDelayInterval
}

func (connMsg *ConnectMessage) SetWillDelayInterval(willDelayInterval uint32) {
	connMsg.willDelayInterval = willDelayInterval
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) PayloadFormatIndicator() byte {
	return connMsg.payloadFormatIndicator
}

func (connMsg *ConnectMessage) SetPayloadFormatIndicator(payloadFormatIndicator byte) {
	connMsg.payloadFormatIndicator = payloadFormatIndicator
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) WillMsgExpiryInterval() uint32 {
	return connMsg.willMsgExpiryInterval
}

func (connMsg *ConnectMessage) SetWillMsgExpiryInterval(willMsgExpiryInterval uint32) {
	connMsg.willMsgExpiryInterval = willMsgExpiryInterval
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) ContentType() []byte {
	return connMsg.contentType
}

func (connMsg *ConnectMessage) SetContentType(contentType []byte) {
	connMsg.contentType = contentType
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) ResponseTopic() []byte {
	return connMsg.responseTopic
}

func (connMsg *ConnectMessage) SetResponseTopic(responseTopic []byte) {
	connMsg.responseTopic = responseTopic
	connMsg.dirty = true
}

func (connMsg *ConnectMessage) CorrelationData() []byte {
	return connMsg.correlationData
}

func (connMsg *ConnectMessage) SetCorrelationData(correlationData []byte) {
	connMsg.correlationData = correlationData
	connMsg.dirty = true
}

// Version returns the the 8 bit unsigned value that represents the revision level
// of the protocol used by the Client. The value of the Protocol Level field for
// the version 3.1.1 of the protocol is 4 (0x04).
func (connMsg *ConnectMessage) Version() byte {
	return connMsg.version
}

// SetVersion sets the version value of the CONNECT message
func (connMsg *ConnectMessage) SetVersion(v byte) error {
	if pn, ok := SupportedVersions[v]; !ok {
		return fmt.Errorf("connect/SetVersion: Invalid version number %d", v)
	} else {
		connMsg.protoName = []byte(pn)
	}

	connMsg.version = v
	connMsg.dirty = true

	return nil
}

// CleanSession returns the bit that specifies the handling of the Session state.
// The Client and Server can store Session state to enable reliable messaging to
// continue across a sequence of Network Connections. This bit is used to control
// the lifetime of the Session state.
// CleanSession返回指定会话状态处理的位。
//客户端和服务器可以存储会话状态，以实现可靠的消息传递
//继续通过网络连接序列。这个位用来控制
//会话状态的生存期。
func (connMsg *ConnectMessage) CleanSession() bool {
	return ((connMsg.connectFlags >> 1) & 0x1) == 1
}
func (connMsg *ConnectMessage) CleanStart() bool {
	return ((connMsg.connectFlags >> 1) & 0x1) == 1
}

// SetCleanSession sets the bit that specifies the handling of the Session state.
func (connMsg *ConnectMessage) SetCleanSession(v bool) {
	if v {
		connMsg.connectFlags |= 0x2 // 00000010
	} else {
		connMsg.connectFlags &= 253 // 11111101
	}

	connMsg.dirty = true
}

// WillFlag returns the bit that specifies whether a Will Message should be stored
// on the server. If the Will Flag is set to 1 connMsg indicates that, if the Connect
// request is accepted, a Will Message MUST be stored on the Server and associated
// with the Network Connection.
// WillFlag返回指定是否存储Will消息的位
//在服务器上。如果Will标志设置为1，这表示如果连接
//请求被接受，一个Will消息必须存储在服务器上并关联
//与网络连接。
func (connMsg *ConnectMessage) WillFlag() bool {
	return ((connMsg.connectFlags >> 2) & 0x1) == 1
}

// SetWillFlag sets the bit that specifies whether a Will Message should be stored
// on the server.
func (connMsg *ConnectMessage) SetWillFlag(v bool) {
	if v {
		connMsg.connectFlags |= 0x4 // 00000100
	} else {
		connMsg.connectFlags &= 251 // 11111011
	}

	connMsg.dirty = true
}

// WillQos returns the two bits that specify the QoS level to be used when publishing
// the Will Message.
func (connMsg *ConnectMessage) WillQos() byte {
	return (connMsg.connectFlags >> 3) & 0x3
}

// SetWillQos sets the two bits that specify the QoS level to be used when publishing
// the Will Message.
func (connMsg *ConnectMessage) SetWillQos(qos byte) error {
	if qos != QosAtMostOnce && qos != QosAtLeastOnce && qos != QosExactlyOnce {
		return fmt.Errorf("connect/SetWillQos: Invalid QoS level %d", qos)
	}

	connMsg.connectFlags = (connMsg.connectFlags & 231) | (qos << 3) // 231 = 11100111
	connMsg.dirty = true

	return nil
}

// WillRetain returns the bit specifies if the Will Message is to be Retained when it
// is published.
// Will retain返回指定Will消息是否被保留的位
//出版。
func (connMsg *ConnectMessage) WillRetain() bool {
	return ((connMsg.connectFlags >> 5) & 0x1) == 1
}

// SetWillRetain sets the bit specifies if the Will Message is to be Retained when it
// is published.
func (connMsg *ConnectMessage) SetWillRetain(v bool) {
	if v {
		connMsg.connectFlags |= 32 // 00100000
	} else {
		connMsg.connectFlags &= 223 // 11011111
	}

	connMsg.dirty = true
}

// UsernameFlag returns the bit that specifies whether a user name is present in the
// payload.
func (connMsg *ConnectMessage) UsernameFlag() bool {
	return ((connMsg.connectFlags >> 7) & 0x1) == 1
}

// SetUsernameFlag sets the bit that specifies whether a user name is present in the
// payload.
func (connMsg *ConnectMessage) SetUsernameFlag(v bool) {
	if v {
		connMsg.connectFlags |= 128 // 10000000
	} else {
		connMsg.connectFlags &= 127 // 01111111
	}

	connMsg.dirty = true
}

// PasswordFlag returns the bit that specifies whether a password is present in the
// payload.
func (connMsg *ConnectMessage) PasswordFlag() bool {
	return ((connMsg.connectFlags >> 6) & 0x1) == 1
}

// SetPasswordFlag sets the bit that specifies whether a password is present in the
// payload.
func (connMsg *ConnectMessage) SetPasswordFlag(v bool) {
	if v {
		connMsg.connectFlags |= 64 // 01000000
	} else {
		connMsg.connectFlags &= 191 // 10111111
	}

	connMsg.dirty = true
}

// KeepAlive returns a time interval measured in seconds. Expressed as a 16-bit word,
// it is the maximum time interval that is permitted to elapse between the point at
// which the Client finishes transmitting one Control Packet and the point it starts
// sending the next.
func (connMsg *ConnectMessage) KeepAlive() uint16 {
	return connMsg.keepAlive
}

// SetKeepAlive sets the time interval in which the server should keep the connection
// alive.
func (connMsg *ConnectMessage) SetKeepAlive(v uint16) {
	connMsg.keepAlive = v

	connMsg.dirty = true
}

// ClientId returns an ID that identifies the Client to the Server. Each Client
// connecting to the Server has a unique ClientId. The ClientId MUST be used by
// Clients and by Servers to identify state that they hold relating to connMsg MQTT
// Session between the Client and the Server
func (connMsg *ConnectMessage) ClientId() []byte {
	return connMsg.clientId
}

// SetClientId sets an ID that identifies the Client to the Server.
func (connMsg *ConnectMessage) SetClientId(v []byte) error {
	if len(v) > 0 && !connMsg.validClientId(v) {
		return InvalidTopicName
	}

	connMsg.clientId = v
	connMsg.dirty = true

	return nil
}

// WillTopic returns the topic in which the Will Message should be published to.
// If the Will Flag is set to 1, the Will Topic must be in the payload.
func (connMsg *ConnectMessage) WillTopic() []byte {
	return connMsg.willTopic
}

// SetWillTopic sets the topic in which the Will Message should be published to.
func (connMsg *ConnectMessage) SetWillTopic(v []byte) {
	connMsg.willTopic = v

	if len(v) > 0 {
		connMsg.SetWillFlag(true)
	} else if len(connMsg.willMessage) == 0 {
		connMsg.SetWillFlag(false)
	}

	connMsg.dirty = true
}

// WillMessage returns the Will Message that is to be published to the Will Topic.
func (connMsg *ConnectMessage) WillMessage() []byte {
	return connMsg.willMessage
}

// SetWillMessage sets the Will Message that is to be published to the Will Topic.
func (connMsg *ConnectMessage) SetWillMessage(v []byte) {
	connMsg.willMessage = v

	if len(v) > 0 {
		connMsg.SetWillFlag(true)
	} else if len(connMsg.willTopic) == 0 {
		connMsg.SetWillFlag(false)
	} else if len(v) == 0 {
		connMsg.SetWillFlag(false)
	}

	connMsg.dirty = true
}

// Username returns the username from the payload. If the User Name Flag is set to 1,
// connMsg must be in the payload. It can be used by the Server for authentication and
// authorization.
func (connMsg *ConnectMessage) Username() []byte {
	return connMsg.username
}

// SetUsername sets the username for authentication.
func (connMsg *ConnectMessage) SetUsername(v []byte) {
	connMsg.username = v

	if len(v) > 0 {
		connMsg.SetUsernameFlag(true)
	} else {
		connMsg.SetUsernameFlag(false)
	}

	connMsg.dirty = true
}

// Password returns the password from the payload. If the Password Flag is set to 1,
// connMsg must be in the payload. It can be used by the Server for authentication and
// authorization.
func (connMsg *ConnectMessage) Password() []byte {
	return connMsg.password
}

// SetPassword sets the username for authentication.
func (connMsg *ConnectMessage) SetPassword(v []byte) {
	connMsg.password = v

	if len(v) > 0 {
		connMsg.SetPasswordFlag(true)
	} else {
		connMsg.SetPasswordFlag(false)
	}

	connMsg.dirty = true
}

func (connMsg *ConnectMessage) Len() int {
	if !connMsg.dirty {
		return len(connMsg.dbuf)
	}

	ml := connMsg.msglen()

	if err := connMsg.SetRemainingLength(uint32(ml)); err != nil {
		return 0
	}

	return connMsg.header.msglen() + ml
}

// Decode For the CONNECT message, the error returned could be a ConnackReturnCode, so
// be sure to check that. Otherwise it's a generic error. If a generic error is
// returned, connMsg Message should be considered invalid.
//
// Caller should call ValidConnackError(err) to see if the returned error is
// a Connack error. If so, caller should send the Client back the corresponding
// CONNACK message.
func (connMsg *ConnectMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := connMsg.header.decode(src[total:])
	if err != nil {
		return total + n, err
	}
	total += n

	if n, err = connMsg.decodeMessage(src[total:]); err != nil {
		return total + n, err
	}
	total += n

	connMsg.dirty = false

	return total, nil
}

func (connMsg *ConnectMessage) Encode(dst []byte) (int, error) {
	if !connMsg.dirty {
		if len(dst) < len(connMsg.dbuf) {
			return 0, fmt.Errorf("connect/Encode: Insufficient buffer size. Expecting %d, got %d.", len(connMsg.dbuf), len(dst))
		}

		return copy(dst, connMsg.dbuf), nil
	}

	if connMsg.Type() != CONNECT {
		return 0, fmt.Errorf("connect/Encode: Invalid message type. Expecting %d, got %d", CONNECT, connMsg.Type())
	}

	_, ok := SupportedVersions[connMsg.version]
	if !ok {
		return 0, UnSupportedProtocolVersion
	}

	ml := connMsg.msglen()
	hl := connMsg.header.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("connect/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
	}

	if err := connMsg.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := connMsg.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	n, err = connMsg.encodeMessage(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	return total, nil
}

func (connMsg *ConnectMessage) EncodeToBuf(dst *bytes.Buffer) (int, error) {
	if !connMsg.dirty {
		return dst.Write(connMsg.dbuf)
	}

	if connMsg.Type() != CONNECT {
		return 0, fmt.Errorf("connect/Encode: Invalid message type. Expecting %d, got %d", CONNECT, connMsg.Type())
	}

	_, ok := SupportedVersions[connMsg.version]
	if !ok {
		return 0, UnSupportedProtocolVersion
	}

	ml := connMsg.msglen()

	if err := connMsg.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	_, err := connMsg.header.encodeToBuf(dst)
	if err != nil {
		return dst.Len(), err
	}

	_, err = connMsg.encodeMessageToBuf(dst)
	return dst.Len(), err
}

func (connMsg *ConnectMessage) encodeMessage(dst []byte) (int, error) {
	total := 0
	/**
		===可变报头===
	**/
	n, err := writeLPBytes(dst[total:], []byte(SupportedVersions[connMsg.version])) // 写入协议长度和协议名称
	total += n
	if err != nil {
		return total, err
	}

	dst[total] = connMsg.version // 写入协议版本号
	total += 1

	dst[total] = connMsg.connectFlags // 写入连接标志
	total += 1

	binary.BigEndian.PutUint16(dst[total:], connMsg.keepAlive) // 写入保持连接
	total += 2

	// 属性
	pLen := lbEncode(connMsg.propertiesLen) // 属性长度
	copy(dst[total:], pLen)
	total += len(pLen)

	if connMsg.sessionExpiryInterval > 0 {
		dst[total] = SessionExpirationInterval
		total++
		binary.BigEndian.PutUint32(dst[total:], connMsg.sessionExpiryInterval) // 会话过期间隔
		total += 4
	}

	if connMsg.receiveMaximum > 0 && connMsg.receiveMaximum < 65535 {
		dst[total] = MaximumQuantityReceived
		total++
		binary.BigEndian.PutUint16(dst[total:], connMsg.receiveMaximum) // 接收最大值
		total += 2
	}

	dst[total] = MaximumMessageLength
	total++
	binary.BigEndian.PutUint32(dst[total:], connMsg.maxPacketSize) // 最大报文长度
	total += 4

	if connMsg.topicAliasMax > 0 {
		dst[total] = MaximumLengthOfTopicAlias
		total++
		binary.BigEndian.PutUint16(dst[total:], connMsg.topicAliasMax) // 主题别名最大值
		total += 2
	}

	// TODO if connMsg.requestRespInfo == 0 ==>> 可发可不发
	if connMsg.requestRespInfo != 0 { // 默认0
		dst[total] = RequestResponseInformation
		total++
		dst[total] = connMsg.requestRespInfo // 请求响应信息
		total++
	}
	if connMsg.requestProblemInfo != 1 { // 默认1
		dst[total] = RequestProblemInformation
		total++
		dst[total] = connMsg.requestProblemInfo // 请求问题信息
		total++
	}

	n, err = writeUserProperty(dst[total:], connMsg.userProperty) // 用户属性
	total += n

	if len(connMsg.authMethod) > 0 {
		dst[total] = AuthenticationMethod // 认证方法
		total++
		n, err = writeLPBytes(dst[total:], connMsg.authMethod)
		total += n
		if err != nil {
			return total, err
		}
	}
	if len(connMsg.authData) > 0 {
		dst[total] = AuthenticationData // 认证数据
		total++
		n, err = writeLPBytes(dst[total:], connMsg.authData)
		total += n
		if err != nil {
			return total, err
		}
	}

	/**
		===载荷===
	**/
	n, err = writeLPBytes(dst[total:], connMsg.clientId) // 客户标识符
	total += n
	if err != nil {
		return total, err
	}
	if connMsg.WillFlag() {
		// 遗嘱属性
		wpLen := lbEncode(connMsg.willPropertiesLen) // 遗嘱属性长度
		copy(dst[total:], wpLen)
		total += len(wpLen)

		if connMsg.willDelayInterval > 0 {
			dst[total] = DelayWills
			total++
			binary.BigEndian.PutUint32(dst[total:], connMsg.willDelayInterval) // 遗嘱延时间隔
			total += 4
		}
		if connMsg.payloadFormatIndicator > 0 {
			dst[total] = LoadFormatDescription
			total++
			dst[total] = connMsg.payloadFormatIndicator // 遗嘱载荷指示
			total++
		}
		if connMsg.willMsgExpiryInterval > 0 {
			dst[total] = MessageExpirationTime
			total++
			binary.BigEndian.PutUint32(dst[total:], connMsg.willMsgExpiryInterval) // 遗嘱消息过期间隔
			total += 4
		}
		if len(connMsg.contentType) > 0 {
			dst[total] = ContentType // 遗嘱内容类型
			total++
			n, err = writeLPBytes(dst[total:], connMsg.contentType)
			total += n
			if err != nil {
				return total, err
			}
		}
		if len(connMsg.responseTopic) > 0 {
			dst[total] = ResponseTopic // 遗嘱响应主题
			total++
			n, err = writeLPBytes(dst[total:], connMsg.responseTopic)
			total += n
			if err != nil {
				return total, err
			}
		}
		if len(connMsg.correlationData) > 0 {
			dst[total] = RelatedData // 对比数据
			total++
			n, err = writeLPBytes(dst[total:], connMsg.correlationData)
			total += n
			if err != nil {
				return total, err
			}
		}

		n, err = writeUserProperty(dst[total:], connMsg.willUserProperty) // 用户属性
		total += n

		n, err = writeLPBytes(dst[total:], connMsg.willTopic) // 遗嘱主题
		total += n
		if err != nil {
			return total, err
		}

		n, err = writeLPBytes(dst[total:], connMsg.willMessage) // 遗嘱载荷
		total += n
		if err != nil {
			return total, err
		}
	}

	// According to the 3.1 spec, it's possible that the usernameFlag is set,
	// but the username string is missing.
	if connMsg.UsernameFlag() && len(connMsg.username) > 0 {
		n, err = writeLPBytes(dst[total:], connMsg.username) // 用户名
		total += n
		if err != nil {
			return total, err
		}
	}

	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if connMsg.PasswordFlag() && len(connMsg.password) > 0 {
		n, err = writeLPBytes(dst[total:], connMsg.password) // 密码
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (connMsg *ConnectMessage) encodeMessageToBuf(dst *bytes.Buffer) (int, error) {
	/**
		===可变报头===
	**/
	_, err := writeToBufLPBytes(dst, []byte(SupportedVersions[connMsg.version])) // 写入协议长度和协议名称
	if err != nil {
		return dst.Len(), err
	}

	dst.WriteByte(connMsg.version) // 写入协议版本号

	dst.WriteByte(connMsg.connectFlags) // 写入连接标志

	_ = BigEndianPutUint16(dst, connMsg.keepAlive) // 写入保持连接

	// 属性
	dst.Write(lbEncode(connMsg.propertiesLen)) // 属性长度

	if connMsg.sessionExpiryInterval > 0 {
		dst.WriteByte(SessionExpirationInterval)
		_ = BigEndianPutUint32(dst, connMsg.sessionExpiryInterval) // 会话过期间隔
	}

	if connMsg.receiveMaximum > 0 && connMsg.receiveMaximum < 65535 {
		dst.WriteByte(MaximumQuantityReceived)
		_ = BigEndianPutUint16(dst, connMsg.receiveMaximum) // 接收最大值
	}

	dst.WriteByte(MaximumMessageLength)
	_ = BigEndianPutUint32(dst, connMsg.maxPacketSize) // 最大报文长度

	if connMsg.topicAliasMax > 0 {
		dst.WriteByte(MaximumLengthOfTopicAlias)
		_ = BigEndianPutUint16(dst, connMsg.topicAliasMax) // 主题别名最大值
	}

	// TODO if connMsg.requestRespInfo == 0 ==>> 可发可不发
	if connMsg.requestRespInfo != 0 { // 默认0
		dst.WriteByte(RequestResponseInformation)
		dst.WriteByte(connMsg.requestRespInfo) // 请求响应信息
	}
	if connMsg.requestProblemInfo != 1 { // 默认1
		dst.WriteByte(RequestProblemInformation)
		dst.WriteByte(connMsg.requestProblemInfo) // 请求问题信息
	}

	_, err = writeUserPropertyByBuf(dst, connMsg.userProperty) // 用户属性
	if err != nil {
		return dst.Len(), err
	}

	if len(connMsg.authMethod) > 0 {
		dst.WriteByte(AuthenticationMethod) // 认证方法
		_, err = writeToBufLPBytes(dst, connMsg.authMethod)
		if err != nil {
			return dst.Len(), err
		}
	}
	if len(connMsg.authData) > 0 {
		dst.WriteByte(AuthenticationData) // 认证数据
		_, err = writeToBufLPBytes(dst, connMsg.authData)
		if err != nil {
			return dst.Len(), err
		}
	}

	/**
		===载荷===
	**/
	_, err = writeToBufLPBytes(dst, connMsg.clientId) // 客户标识符
	if err != nil {
		return dst.Len(), err
	}

	if connMsg.WillFlag() {
		// 遗嘱属性
		dst.Write(lbEncode(connMsg.willPropertiesLen)) // 遗嘱属性长度

		if connMsg.willDelayInterval > 0 {
			dst.WriteByte(DelayWills)
			_ = BigEndianPutUint32(dst, connMsg.willDelayInterval) // 遗嘱延时间隔
		}
		if connMsg.payloadFormatIndicator > 0 {
			dst.WriteByte(LoadFormatDescription)
			dst.WriteByte(connMsg.payloadFormatIndicator) // 遗嘱载荷指示
		}
		if connMsg.willMsgExpiryInterval > 0 {
			dst.WriteByte(MessageExpirationTime)
			_ = BigEndianPutUint32(dst, connMsg.willMsgExpiryInterval) // 遗嘱消息过期间隔
		}
		if len(connMsg.contentType) > 0 {
			dst.WriteByte(ContentType)
			_, err = writeToBufLPBytes(dst, connMsg.contentType) // 遗嘱内容类型
			if err != nil {
				return dst.Len(), err
			}
		}
		if len(connMsg.responseTopic) > 0 {
			dst.WriteByte(ResponseTopic)
			_, err = writeToBufLPBytes(dst, connMsg.responseTopic) // 遗嘱响应主题
			if err != nil {
				return dst.Len(), err
			}
		}
		if len(connMsg.correlationData) > 0 {
			dst.WriteByte(RelatedData)
			_, err = writeToBufLPBytes(dst, connMsg.correlationData) // 对比数据
			if err != nil {
				return dst.Len(), err
			}
		}

		_, err = writeUserPropertyByBuf(dst, connMsg.willUserProperty) // 用户属性
		if err != nil {
			return dst.Len(), err
		}

		_, err = writeToBufLPBytes(dst, connMsg.willTopic) // 遗嘱主题
		if err != nil {
			return dst.Len(), err
		}

		_, err = writeToBufLPBytes(dst, connMsg.willMessage) // 遗嘱载荷
		if err != nil {
			return dst.Len(), err
		}
	}

	// According to the 3.1 spec, it's possible that the usernameFlag is set,
	// but the username string is missing.
	if connMsg.UsernameFlag() && len(connMsg.username) > 0 {
		_, err = writeToBufLPBytes(dst, connMsg.username) // 用户名
		if err != nil {
			return dst.Len(), err
		}
	}

	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if connMsg.PasswordFlag() && len(connMsg.password) > 0 {
		_, err = writeToBufLPBytes(dst, connMsg.password) // 密码
		if err != nil {
			return dst.Len(), err
		}
	}

	return dst.Len(), nil
}

func (connMsg *ConnectMessage) decodeMessage(src []byte) (int, error) {
	var err error
	n, total := 0, 0

	// 协议名
	connMsg.protoName, n, err = readLPBytes(src[total:])
	total += n
	if err != nil {
		return total, err
	} // 如果服务端不愿意接受 CONNECT 但希望表明其 MQTT 服务端身份，可以发送包含原因码为 0x84（不支持的协议版本）的 CONNACK 报文，然后必须关闭网络连接
	// 协议级别，版本号, 1 byte
	connMsg.version = src[total]
	total++

	if verstr, ok := SupportedVersions[connMsg.version]; !ok { // todo 发送原因码0x84（不支持的协议版本）的CONNACK报文，然后必须关闭网络连接
		return total, UnSupportedProtocolVersion // 如果协议版本不是 5 且服务端不愿意接受此 CONNECT 报文，可以发送包含原因码 0x84（不支持的协议版本）的CONNACK 报文，然后必须关闭网络连接
	} else if verstr != string(connMsg.protoName) {
		return total, ProtocolError
	}

	// 连接标志
	// 7: username flag
	// 6: password flag
	// 5: will retain
	// 4-3: will QoS
	// 2: will flag
	// 1: clean session
	// 0: reserved
	connMsg.connectFlags = src[total]
	total++

	// 服务端必须验证CONNECT报文的保留标志位（第 0 位）是否为 0 [MQTT-3.1.2-3]，如果不为0则此报文为无效报文
	if connMsg.connectFlags&0x1 != 0 {
		return total, InvalidMessage // fmt.Errorf("connect/decodeMessage: Connect Flags reserved bit 0 is not 0")
	}

	if connMsg.WillQos() > QosExactlyOnce { // 校验qos
		return total, InvalidMessage // fmt.Errorf("connect/decodeMessage: Invalid QoS level (%d) for %s message", connMsg.WillQos(), connMsg.Name())
	}
	// 遗嘱标志为0，will retain 和 will QoS都必须为0
	if !connMsg.WillFlag() && (connMsg.WillRetain() || connMsg.WillQos() != QosAtMostOnce) {
		return total, InvalidMessage // fmt.Errorf("connect/decodeMessage: Protocol violation: If the Will Flag (%t) is set to 0 the Will QoS (%d) and Will Retain (%t) fields MUST be set to zero", connMsg.WillFlag(), connMsg.WillQos(), connMsg.WillRetain())
	}

	// 用户名设置了，但是密码没有设置，也是无效报文
	// todo 相比MQTT v3.1.1，v5版本协议允许在没有用户名的情况下发送密码。这表明密码除了作为口令之外还可以有其他用途。
	if connMsg.UsernameFlag() && !connMsg.PasswordFlag() {
		return total, InvalidMessage // fmt.Errorf("connect/decodeMessage: Username flag is set but Password flag is not set")
	}

	if len(src[total:]) < 2 { // 判断是否还有超过2字节，要判断keepalive
		return 0, InvalidMessage // fmt.Errorf("connect/decodeMessage: Insufficient buffer size. Expecting %d, got %d.", 2, len(src[total:]))
	}
	// 双字节整数来表示以秒为单位的时间间隔
	// 时间为：18小时12分15秒
	// 如果保持连接的值非零，并且服务端在1.5倍的保持连接时间内没有收到客户端的控制报文，
	//     它必须断开客户端的网络连接，并判定网络连接已断开
	connMsg.keepAlive = binary.BigEndian.Uint16(src[total:])
	total += 2

	// 属性

	connMsg.propertiesLen, n, err = lbDecode(src[total:]) // 属性长度
	total += n
	if err != nil {
		return total, InvalidMessage
	}
	if len(src[total:]) < int(connMsg.propertiesLen) {
		return total, InvalidMessage
	}

	if total < len(src) && src[total] == SessionExpirationInterval { //会话过期间隔（Session Expiry Interval）标识符。 四字节过期间隔，0xFFFFFFFF表示永不过期，0或者不设置，则为网络连接关闭时立即结束
		total++
		connMsg.sessionExpiryInterval = binary.BigEndian.Uint32(src[total:])
		total += 4
		if total < len(src) && src[total] == SessionExpirationInterval {
			return total, ProtocolError
		}
	}

	if total < len(src) && src[total] == MaximumQuantityReceived { // 接收最大值（Receive Maximum）标识符。
		total++
		connMsg.receiveMaximum = binary.BigEndian.Uint16(src[total:])
		total += 2
		if connMsg.receiveMaximum == 0 || (total < len(src) && src[total] == MaximumQuantityReceived) {
			return total, ProtocolError
		}
	} else {
		connMsg.receiveMaximum = 65535
	}

	if total < len(src) && src[total] == MaximumMessageLength { // 最大报文长度（Maximum Packet Size）标识符。
		total++
		connMsg.maxPacketSize = binary.BigEndian.Uint32(src[total:])
		total += 4
		if connMsg.maxPacketSize == 0 || (total < len(src) && src[total] == MaximumMessageLength) {
			return total, ProtocolError
		}
	} else {
		// TODO 如果没有 设置最大报文长度（Maximum Packet Size），则按照协议由固定报头中的剩余长度可编码最大值和协议 报头对数据包的大小做限制。
		// 服务端不能发送超过最大报文长度（Maximum Packet Size）的报文给客户端 [MQTT-3.1.2-24]。收到长度
		// 超过限制的报文将导致协议错误，客户端发送包含原因码 0x95（报文过大）的 DISCONNECT 报文给服务端
		// 当报文过大而不能发送时，服务端必须丢弃这些报文，然后当做应用消息发送已完成处理 [MQTT-3.1.2-25]。
		// 共享订阅的情况下，如果一条消息对于部分客户端来说太长而不能发送，服务端可以选择丢弃此消息或者把消息发送给剩余能够接收此消息的客户端
	}

	if total < len(src) && src[total] == MaximumLengthOfTopicAlias { // 主题别名最大值
		total++
		// 服务端在一个 PUBLISH 报文中发送的主题别名不能超过客户端设置的
		// 主题别名最大值（Topic Alias Maximum） [MQTT-3.1.2-26]。值为零表示本次连接客户端不接受任何主题
		// 别名（Topic Alias）。如果主题别名最大值（Topic Alias）没有设置，或者设置为零，则服务端不能向此客
		// 户端发送任何主题别名（Topic Alias）
		connMsg.topicAliasMax = binary.BigEndian.Uint16(src[total:])
		total += 2
		if total < len(src) && src[total] == MaximumLengthOfTopicAlias {
			return total, ProtocolError
		}
	}

	if total < len(src) && src[total] == RequestResponseInformation { // 请求响应信息
		total++
		connMsg.requestRespInfo = src[total]
		total++
		if (connMsg.requestRespInfo != 0 && connMsg.requestRespInfo != 1) ||
			(total < len(src) && src[total] == RequestResponseInformation) {
			return total, ProtocolError
		}
	}

	if total < len(src) && src[total] == RequestProblemInformation { // 请求问题信息
		total++
		// 客户端使用此值指示遇到错误时是否发送原因字符串（Reason String）或用户属性（User Properties）
		// 如果此值为 1，服务端可以在任何被允许的报文中返回原因字符串（Reason String）或用户属性（User Properties）
		connMsg.requestProblemInfo = src[total]
		total++
		if (connMsg.requestProblemInfo != 0 && connMsg.requestProblemInfo != 1) ||
			(total < len(src) && src[total] == RequestProblemInformation) {
			return total, ProtocolError
		}
	} else {
		connMsg.requestProblemInfo = 0x01
	}

	connMsg.userProperty, n, err = decodeUserProperty(src[total:]) // 用户属性
	total += n
	if err != nil {
		return total, err
	}

	if total < len(src) && src[total] == AuthenticationMethod { // 认证方法
		// 如果客户端在 CONNECT 报文中设置了认证方法，则客户端在收到 CONNACK 报文之前不能发送除AUTH 或 DISCONNECT 之外的报文 [MQTT-3.1.2-30]。
		total++

		connMsg.authMethod, n, err = readLPBytes(src[total:])
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
		connMsg.authData, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if total < len(src) && src[total] == AuthenticationData {
			return total, ProtocolError
		}
	} else if len(connMsg.authMethod) != 0 { // 有认证方法，却没有认证数据
		return total, ProtocolError
	}

	// 载荷
	// ==== 客户标识符 ====
	connMsg.clientId, n, err = readLPBytes(src[total:]) // 客户标识符
	total += n
	if err != nil {
		return total, err
	}
	// If the Client supplies a zero-byte ClientId, the Client MUST also set CleanSession to 1
	if len(connMsg.clientId) == 0 && !connMsg.CleanSession() {
		return total, CustomerIdentifierInvalid
	}
	// The ClientId must contain only characters 0-9, a-z, and A-Z,-,_,/
	// We also support ClientId longer than 23 encoded bytes
	// We do not support ClientId outside of the above characters
	if len(connMsg.clientId) > 0 && !connMsg.validClientId(connMsg.clientId) {
		return total, CustomerIdentifierInvalid
	}
	if len(connMsg.clientId) == 0 {
		// 服务端可以允许客户端提供一个零字节的客户标识符 (ClientID)如果这样做了，服务端必须将这看作特殊
		// 情况并分配唯一的客户标识符给那个客户端 [MQTT-3.1.3-6]。
		// 然后它必须假设客户端提供了那个唯一的客户标识符，正常处理这个 CONNECT 报文 [MQTT-3.1.3-7]。
		connMsg.clientId = defaultCId
	}
	// ==== 遗嘱属性 ====
	if connMsg.WillFlag() { // 遗嘱属性
		// 遗嘱属性长度
		connMsg.willPropertiesLen, n, err = lbDecode(src[total:])
		total += n
		if err != nil {
			return total, ProtocolError
		}
		if total < len(src) && src[total] == DelayWills { // 遗嘱延时间隔
			total++
			connMsg.willDelayInterval = binary.BigEndian.Uint32(src[total:])
			total += 4
			if total < len(src) && src[total] == DelayWills {
				return total, ProtocolError
			}
		}
		if total < len(src) && src[total] == LoadFormatDescription { // 载荷格式指示
			total++
			connMsg.payloadFormatIndicator = src[total]
			total++
			if connMsg.payloadFormatIndicator != 0x00 && connMsg.payloadFormatIndicator != 0x01 {
				return total, ProtocolError
			}
			if total < len(src) && src[total] == LoadFormatDescription {
				return total, ProtocolError
			}
		}
		if total < len(src) && src[total] == MessageExpirationTime { // 消息过期间隔
			total++
			connMsg.willMsgExpiryInterval = binary.BigEndian.Uint32(src[total:])
			total += 4
			if total < len(src) && src[total] == MessageExpirationTime {
				return total, ProtocolError
			}
		}
		if total < len(src) && src[total] == ContentType { // 内容类型
			total++
			connMsg.contentType, n, err = readLPBytes(src[total:])
			total += n
			if err != nil {
				return total, err
			}
			if total < len(src) && src[total] == ContentType {
				return total, ProtocolError
			}
		}

		if total < len(src) && src[total] == ResponseTopic { // 响应主题的存在将遗嘱消息（Will Message）标识为一个请求报文
			total++
			connMsg.responseTopic, n, err = readLPBytes(src[total:])
			total += n
			if err != nil {
				return total, err
			}
			if total < len(src) && src[total] == ResponseTopic {
				return total, ProtocolError
			}
		}
		if total < len(src) && src[total] == RelatedData { // 对比数据 只对请求消息（Request Message）的发送端和响应消息（Response Message）的接收端有意义。
			total++
			connMsg.correlationData, n, err = readLPBytes(src[total:])
			total += n
			if err != nil {
				return total, err
			}
			if total < len(src) && src[total] == RelatedData {
				return total, ProtocolError
			}
		}

		connMsg.willUserProperty, n, err = decodeUserProperty(src[total:]) // 用户属性
		total += n
		if err != nil {
			return total, err
		}

	}
	// ==== 遗嘱主题，遗嘱载荷 ====
	if connMsg.WillFlag() {
		connMsg.willTopic, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}

		connMsg.willMessage, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}

	// ==== 用户名 ====
	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if connMsg.UsernameFlag() && len(src[total:]) > 0 {
		connMsg.username, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}

	// ==== 密码 ====
	// According to the 3.1 spec, it's possible that the passwordFlag is set,
	// but the password string is missing.
	if connMsg.PasswordFlag() && len(src[total:]) > 0 {
		connMsg.password, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (connMsg *ConnectMessage) msglen() int {
	connMsg.build()
	return int(connMsg.remlen)
}

// validClientId checks the client ID, which is a slice of bytes, to see if it's valid.
// Client ID is valid if it meets the requirement from the MQTT spec:
// 		The Server MUST allow ClientIds which are between 1 and 23 UTF-8 encoded bytes in length,
//		and that contain only the characters
//
//		"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
// 虽然协议写了不能超过23字节，和上面那些字符。
// 但实现还是可以不用完全有那些限制
func (connMsg *ConnectMessage) validClientId(cid []byte) bool {
	// Fixed https://github.com/surgemq/surgemq/issues/4
	//if len(cid) > 23 {
	//	return false
	//}
	//if connMsg.Version() == 0x05 {
	//	return true
	//}

	return clientIdRegexp.Match(cid)
}
