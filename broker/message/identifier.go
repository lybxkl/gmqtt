package message

import "strings"

/**
属性，原因码
字节表示1字节
*/

type Code struct {
	ReasonCode
	info string
}

// 原因码
type (
	ReasonCode   uint8
	PropertyCode = byte
)

const (
	LoadFormatDescription                 PropertyCode = 0x01 // 载荷格式说明 字节 PUBLISH, Will Properties
	MessageExpirationTime                 PropertyCode = 0x02 // 消息过期时间 四字节整数 PUBLISH, Will Properties
	ContentType                           PropertyCode = 0x03 // 内容类型 UTF-8 编码字符串 PUBLISH, Will Properties
	ResponseTopic                         PropertyCode = 0x08 // 响应主题 UTF-8 编码字符串 PUBLISH, Will Properties
	RelatedData                           PropertyCode = 0x09 // 相关数据,对比数据 二进制数据 PUBLISH, Will Properties
	DefiningIdentifiers                   PropertyCode = 0x0B // 定义标识符 变长字节整数 PUBLISH, SUBSCRIBE
	SessionExpirationInterval             PropertyCode = 0x11 // 会话过期间隔 四字节整数 CONNECT, CONNACK, DISCONNECT
	AssignCustomerIdentifiers             PropertyCode = 0x12 // 分配客户标识符 UTF-8 编码字符串 CONNACK
	ServerSurvivalTime                    PropertyCode = 0x13 // 服务端保活时间 双字节整数 CONNACK
	AuthenticationMethod                  PropertyCode = 0x15 // 认证方法 UTF-8 编码字符串 CONNECT, CONNACK, AUTH
	AuthenticationData                    PropertyCode = 0x16 // 认证数据 二进制数据 CONNECT, CONNACK, AUTH
	RequestProblemInformation             PropertyCode = 0x17 // 请求问题信息 字节 CONNECT
	DelayWills                            PropertyCode = 0x18 // 遗嘱延时间隔 四字节整数 Will Propertie
	RequestResponseInformation            PropertyCode = 0x19 // 请求响应信息 字节 CONNECT
	SolicitedMessage                      PropertyCode = 0x1A // 请求信息 UTF-8 编码字符串 CONNACK
	ServerReference                       PropertyCode = 0x1C // 服务端参考 UTF-8 编码字符串 CONNACK, DISCONNECT
	ReasonString                          PropertyCode = 0x1F // 原因字符串 UTF-8 编码字符串 CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK, DISCONNECT, AUTH
	MaximumQuantityReceived               PropertyCode = 0x21 // 接收最大数量 双字节整数 CONNECT, CONNACK
	MaximumLengthOfTopicAlias             PropertyCode = 0x22 // 主题别名最大长度 双字节整数 CONNECT, CONNACK
	ThemeAlias                            PropertyCode = 0x23 // 主题别名 双字节整数 PUBLISH
	MaximumQoS                            PropertyCode = 0x24 // 最大 QoS 字节 CONNACK
	PreservePropertyAvailability          PropertyCode = 0x25 // 保留属性可用性 字节 CONNACK
	UserProperty                          PropertyCode = 0x26 // 用户属性 UTF-8 字符串对 CONNECT, CONNACK, PUBLISH, Will Properties, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, DISCONNECT, AUTH
	MaximumMessageLength                  PropertyCode = 0x27 // 最大报文长度 四字节整数 CONNECT, CONNACK
	WildcardSubscriptionAvailability      PropertyCode = 0x28 // 通配符订阅可用性 字节 CONNACK
	AvailabilityOfSubscriptionIdentifiers PropertyCode = 0x29 // 订阅标识符可用性 字节 CONNACK
	SharedSubscriptionAvailability        PropertyCode = 0x2A // 共享订阅可用性 字节 CONNACK
)

const (
	Success                            ReasonCode = 0x00    // 成功 CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK, AUTH
	NormalDisconnected                            = Success // 正常断开 DISCONNECT
	Qos0                                          = Success // 授权的 QoS 0 SUBACK
	Qos1                               ReasonCode = 0x01    // 授权的 QoS 1 SUBACK
	Qos2                               ReasonCode = 0x02    // 授权的 QoS 2 SUBACK
	DisconnectionIncludesWill          ReasonCode = 0x04    // 包含遗嘱的断开 DISCONNECT
	NoMatchSubscription                ReasonCode = 0x10    // 无匹配订阅 PUBACK, PUBREC
	NoExistSubscription                ReasonCode = 0x11    // 订阅不存在 UNSUBACK
	ContinueAuthentication             ReasonCode = 0x18    // 继续认证 AUTH
	ReAuthentication                   ReasonCode = 0x19    // 重新认证 AUTH
	UnspecifiedError                   ReasonCode = 0x80    // 未指明的错误 CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT
	InvalidMessage                     ReasonCode = 0x81    // 无效报文 CONNACK, DISCONNECT
	ProtocolError                      ReasonCode = 0x82    // 协议错误 CONNACK, DISCONNECT
	ImplementError                     ReasonCode = 0x83    // 实现错误 CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT
	UnSupportedProtocolVersion         ReasonCode = 0x84    // 协议版本不支持 CONNACK
	CustomerIdentifierInvalid          ReasonCode = 0x85    // 客户标识符无效 CONNACK
	UserNameOrPasswordIsIncorrect      ReasonCode = 0x86    // 用户名密码错误 CONNACK
	UnAuthorized                       ReasonCode = 0x87    // 未授权 CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT
	ServerUnavailable                  ReasonCode = 0x88    // 服务端不可用 CONNACK
	ServiceBusy                        ReasonCode = 0x89    // 服务端正忙 CONNACK, DISCONNECT
	Forbid                             ReasonCode = 0x8A    // 禁止 CONNACK
	ServerBeingShutDown                ReasonCode = 0x8B    // 服务端关闭中 DISCONNECT
	InvalidAuthenticationMethod        ReasonCode = 0x8C    // 无效的认证方法 CONNACK, DISCONNECT
	KeepAliveTimeout                   ReasonCode = 0x8D    // 保活超时 DISCONNECT
	SessionTakeover                    ReasonCode = 0x8E    // 会话被接管 DISCONNECT
	InvalidTopicFilter                 ReasonCode = 0x8F    // 主题过滤器无效 SUBACK, UNSUBACK, DISCONNECT
	InvalidTopicName                   ReasonCode = 0x90    // 主题名无效 CONNACK, PUBACK, PUBREC, DISCONNECT
	PacketIdentifierIsOccupied         ReasonCode = 0x91    // 报文标识符已被占用 PUBACK, PUBREC, SUBACK, UNSUBACK
	PacketIdentifierInvalid            ReasonCode = 0x92    // 报文标识符无效 PUBREL, PUBCOMP
	MaximumNumberReceivedExceeded      ReasonCode = 0x93    // 接收超出最大数量 DISCONNECT
	InvalidTopicAlias                  ReasonCode = 0x94    // 主题别名无效 DISCONNECT
	MessageTooLong                     ReasonCode = 0x95    // 报文过长 CONNACK, DISCONNECT
	TooManyMessages                    ReasonCode = 0x96    // 消息太过频繁 DISCONNECT
	BeyondQuota                        ReasonCode = 0x97    // 超出配额 CONNACK, PUBACK, PUBREC, SUBACK, DISCONNECT
	ManagementBehavior                 ReasonCode = 0x98    // 管理行为 DISCONNECT
	InvalidLoadFormat                  ReasonCode = 0x99    // 载荷格式无效 CONNACK, PUBACK, PUBREC, DISCONNECT
	UnsupportedRetention               ReasonCode = 0x9A    // 不支持保留 CONNACK, DISCONNECT
	UnsupportedQoSLevel                ReasonCode = 0x9B    // 不支持的 QoS 等级 CONNACK, DISCONNECT
	UseOtherServers                    ReasonCode = 0x9C    //（临时）使用其他服务端 CONNACK, DISCONNECT
	ServerHasMoved                     ReasonCode = 0x9D    // 服务端已（永久）移动 CONNACK, DISCONNECT
	UnsupportedSharedSubscriptions     ReasonCode = 0x9E    // 不支持共享订阅 SUBACK, DISCONNECT
	ExceededConnectionRateLimit        ReasonCode = 0x9F    // 超出连接速率限制 CONNACK, DISCONNECT
	MaximumConnectionTime              ReasonCode = 0xA0    // 最大连接时间 DISCONNECT
	UnsupportedSubscriptionIdentifiers ReasonCode = 0xA1    // 不支持订阅标识符 SUBACK, DISCONNECT
	UnsupportedWildcardSubscriptions   ReasonCode = 0xA2    // 不支持通配符订阅 SUBACK, DISCONNECT
)

// Value returns the value of the ReasonCode, which is just the byte representation
func (code ReasonCode) Value() byte {
	return byte(code)
}
func (code ReasonCode) String() string {
	return code.Desc()
}

// Desc returns the description of the ReasonCode
func (code ReasonCode) Desc() string {
	switch code {
	case 0:
		return "Success, NormalDisconnected or Qos0"
	case Qos1:
		return "Qos1 授权的 QoS 1"
	case Qos2:
		return "Qos2 授权的 QoS 2"
	case DisconnectionIncludesWill:
		return "DisconnectionIncludesWill 包含遗嘱的断开"
	case NoMatchSubscription:
		return "NoMatchSubscription 无匹配订阅"
	case NoExistSubscription:
		return "NoExistSubscription 订阅不存在"
	case ContinueAuthentication:
		return "ContinueAuthentication 继续认证"
	case ReAuthentication:
		return "ReAuthentication 重新认证"
	case UnspecifiedError:
		return "UnspecifiedError 未指明的错误"
	case InvalidMessage:
		return "InvalidMessage 无效报文"
	case ProtocolError:
		return "ProtocolError 协议错误"
	case ImplementError:
		return "ImplementError 实现错误"
	case UnSupportedProtocolVersion:
		return "UnSupportedProtocolVersion 协议版本不支持"
	case CustomerIdentifierInvalid:
		return "CustomerIdentifierInvalid 客户标识符无效"
	case UserNameOrPasswordIsIncorrect:
		return "UserNameOrPasswordIsIncorrect 用户名密码错误"
	case UnAuthorized:
		return "UnAuthorized 未授权"
	case ServerUnavailable:
		return "ServerUnavailable 服务端不可用"
	case ServiceBusy:
		return "ServiceBusy 协议错误"
	case Forbid:
		return "Forbid 禁止"
	case ServerBeingShutDown:
		return "ServerBeingShutDown 服务端关闭中"
	case InvalidAuthenticationMethod:
		return "InvalidAuthenticationMethod 无效的认证方法"
	case KeepAliveTimeout:
		return "KeepAliveTimeout 保活超时"
	case SessionTakeover:
		return "SessionTakeover 会话被接管"
	case InvalidTopicFilter:
		return "InvalidTopicFilter 主题过滤器无效"
	case InvalidTopicName:
		return "InvalidTopicName 主题名无效"
	case PacketIdentifierIsOccupied:
		return "PacketIdentifierIsOccupied 报文标识符已被占用"
	case PacketIdentifierInvalid:
		return "PacketIdentifierInvalid 报文标识符无效"
	case MaximumNumberReceivedExceeded:
		return "MaximumNumberReceivedExceeded 接收超出最大数量"
	case InvalidTopicAlias:
		return "InvalidTopicAlias 主题别名无效"
	case MessageTooLong:
		return "MessageTooLong 报文过长"
	case TooManyMessages:
		return "TooManyMessages 消息太过频繁"
	case BeyondQuota:
		return "BeyondQuota 超出配额"
	case ManagementBehavior:
		return "ManagementBehavior 管理行为"
	case InvalidLoadFormat:
		return "InvalidLoadFormat 载荷格式无效"
	case UnsupportedRetention:
		return "UnsupportedRetention 不支持保留"
	case UnsupportedQoSLevel:
		return "UnsupportedQoSLevel 不支持的 QoS 等级"
	case UseOtherServers:
		return "UseOtherServers （临时）使用其他服务端"
	case ServerHasMoved:
		return "ServerHasMoved 服务端已（永久）移动"
	case UnsupportedSharedSubscriptions:
		return "UnsupportedSharedSubscriptions 不支持共享订阅"
	case ExceededConnectionRateLimit:
		return "ExceededConnectionRateLimit 超出连接速率限制"
	case MaximumConnectionTime:
		return "MaximumConnectionTime 最大连接时间"
	case UnsupportedSubscriptionIdentifiers:
		return "UnsupportedSubscriptionIdentifiers 不支持订阅标识符"
	case UnsupportedWildcardSubscriptions:
		return "UnsupportedWildcardSubscriptions 不支持通配符订阅"
	}
	return "无效原因码"
}

// Valid checks to see if the ReasonCode is valid. Currently valid codes are <= 5
func (code ReasonCode) Valid() bool {
	return code <= 0xA2
}

// Error returns the corresonding error string for the ReasonCode
func (code ReasonCode) Error() string {
	return code.Desc()
}

func NewCodeErr(code ReasonCode, msg ...string) error {
	info := ""
	if len(msg) > 0 {
		info = strings.Join(msg, ", ")
	}
	return &Code{
		ReasonCode: code,
		info:       info,
	}
}
func (code *Code) Error() string {
	return code.Desc() + ": " + code.info
}
