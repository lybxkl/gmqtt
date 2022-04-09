package message

import (
	"bytes"
	"fmt"
)

var _ Message = (*AuthMessage)(nil)

// AuthMessage v5版本新增
type AuthMessage struct {
	header

	// 可变报头
	//如果原因码为 0x00（成功）并且没有属性字段，则可以省略原因码和属性长度。这种情况下，AUTH 报文 剩余长度为 0。

	reasonCode ReasonCode // 0x00,0x18,0x19
	//--- 属性
	propertiesLen uint32 // 属性长度
	authMethod    []byte
	authData      []byte
	reasonStr     []byte   // 如果加上原因字符串之后的 AUTH 报文长度超出了接收端所指定的最大报文长度，则发送端不能发送此属性
	userProperty  [][]byte // 如果加上用户属性之后的 AUTH 报文长度超出了接收端所指定的最大报文长度，则发送端不能发送此属性, 每对属性都是串行放置 [k1][v1][k2][v2]
	// AUTH 报文没有有效载荷
}

// NewAuthMessage creates a new AUTH message.
func NewAuthMessage() *AuthMessage {
	msg := &AuthMessage{}
	msg.SetType(AUTH)

	return msg
}

func (authMsg *AuthMessage) ReasonCode() ReasonCode {
	return authMsg.reasonCode
}

func (authMsg *AuthMessage) SetReasonCode(reasonCode ReasonCode) {
	authMsg.reasonCode = reasonCode
	authMsg.dirty = true
}

func (authMsg *AuthMessage) PropertiesLen() uint32 {
	return authMsg.propertiesLen
}

func (authMsg *AuthMessage) SetPropertiesLen(propertiesLen uint32) {
	authMsg.propertiesLen = propertiesLen
	authMsg.dirty = true
}

func (authMsg *AuthMessage) AuthMethod() []byte {
	return authMsg.authMethod
}

func (authMsg *AuthMessage) SetAuthMethod(authMethod []byte) {
	authMsg.authMethod = authMethod
	authMsg.dirty = true
}

func (authMsg *AuthMessage) AuthData() []byte {
	return authMsg.authData
}

func (authMsg *AuthMessage) SetAuthData(authData []byte) {
	authMsg.authData = authData
	authMsg.dirty = true
}

func (authMsg *AuthMessage) ReasonStr() []byte {
	return authMsg.reasonStr
}

func (authMsg *AuthMessage) SetReasonStr(reasonStr []byte) {
	authMsg.reasonStr = reasonStr
	authMsg.dirty = true
}

func (authMsg *AuthMessage) UserProperty() [][]byte {
	return authMsg.userProperty
}

func (authMsg *AuthMessage) AddUserPropertys(userProperty [][]byte) {
	authMsg.userProperty = append(authMsg.userProperty, userProperty...)
	authMsg.dirty = true
}
func (authMsg *AuthMessage) AddUserProperty(userProperty []byte) {
	authMsg.userProperty = append(authMsg.userProperty, userProperty)
	authMsg.dirty = true
}

func (authMsg AuthMessage) String() string {
	return fmt.Sprintf("%s, ReasonCode=%b, PropertiesLen=%d, AuthMethod=%s, AuthData=%q, ReasonStr=%s, UserProperty=%s",
		authMsg.header,
		authMsg.ReasonCode(),
		authMsg.PropertiesLen(),
		authMsg.AuthMethod(),
		authMsg.AuthData(),
		authMsg.ReasonStr(),
		authMsg.UserProperty(),
	)
}

func (authMsg *AuthMessage) Len() int {
	if !authMsg.dirty {
		return len(authMsg.dbuf)
	}

	ml := authMsg.msglen()

	if err := authMsg.SetRemainingLength(ml); err != nil {
		return 0
	}

	return authMsg.header.msglen() + int(ml)
}

func (authMsg *AuthMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := authMsg.header.decode(src[total:])
	total += n
	if err != nil {
		return total, err
	}
	if int(authMsg.header.remlen) > len(src[total:]) {
		return total, ProtocolError
	}

	if authMsg.header.remlen == 0 {
		authMsg.reasonCode = Success
		return total, nil
	}

	authMsg.reasonCode = ReasonCode(src[total])
	total++
	if !ValidAuthReasonCode(authMsg.reasonCode) {
		return total, ProtocolError
	}
	authMsg.propertiesLen, n, err = lbDecode(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	if total < len(src) && src[total] == AuthenticationMethod {
		total++
		authMsg.authMethod, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if total < len(src) && src[total] == AuthenticationMethod {
			return total, ProtocolError
		}
	}
	if total < len(src) && src[total] == AuthenticationData {
		total++
		authMsg.authData, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if total < len(src) && src[total] == AuthenticationData {
			return total, ProtocolError
		}
	}
	if total < len(src) && src[total] == ReasonString {
		total++
		authMsg.reasonStr, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if total < len(src) && src[total] == ReasonString {
			return total, ProtocolError
		}
	}
	authMsg.userProperty, n, err = decodeUserProperty(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	authMsg.dirty = false

	return total, nil
}

func (authMsg *AuthMessage) Encode(dst []byte) (int, error) {
	if !authMsg.dirty {
		if len(dst) < len(authMsg.dbuf) {
			return 0, fmt.Errorf("auth/Encode: Insufficient buffer size. Expecting %d, got %d.", len(authMsg.dbuf), len(dst))
		}

		return copy(dst, authMsg.dbuf), nil
	}

	if authMsg.Type() != AUTH {
		return 0, fmt.Errorf("auth/Encode: Invalid message type. Expecting %d, got %d", AUTH, authMsg.Type())
	}

	ml := authMsg.msglen()
	hl := authMsg.header.msglen()

	ln := hl + int(ml)
	if len(dst) < ln {
		return 0, fmt.Errorf("auth/Encode: Insufficient buffer size. Expecting %d, got %d.", ln, len(dst))
	}

	if err := authMsg.SetRemainingLength(ml); err != nil {
		return 0, err
	}

	total := 0

	n, err := authMsg.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	if authMsg.reasonCode == Success && len(authMsg.authMethod) == 0 && len(authMsg.authData) == 0 && len(authMsg.reasonStr) == 0 && len(authMsg.userProperty) == 0 {
		return total, nil
	} else {
		dst[total] = byte(authMsg.reasonCode)
		total++
	}
	n = copy(dst[total:], lbEncode(authMsg.propertiesLen))
	total += n

	if len(authMsg.authMethod) > 0 {
		dst[total] = AuthenticationMethod
		total++
		n, err = writeLPBytes(dst[total:], authMsg.authMethod)
		total += n
		if err != nil {
			return total, err
		}
	}
	if len(authMsg.authData) > 0 {
		dst[total] = AuthenticationData
		total++
		n, err = writeLPBytes(dst[total:], authMsg.authData)
		total += n
		if err != nil {
			return total, err
		}
	}
	if len(authMsg.reasonStr) > 0 {
		dst[total] = ReasonString
		total++
		n, err = writeLPBytes(dst[total:], authMsg.reasonStr)
		total += n
		if err != nil {
			return total, err
		}
	}
	n, err = writeUserProperty(dst[total:], authMsg.userProperty)
	total += n
	return total, err
}

func (authMsg *AuthMessage) EncodeToBuf(dst *bytes.Buffer) (int, error) {
	if !authMsg.dirty {
		return dst.Write(authMsg.dbuf)
	}

	if authMsg.Type() != AUTH {
		return 0, fmt.Errorf("auth/Encode: Invalid message type. Expecting %d, got %d", AUTH, authMsg.Type())
	}

	ml := authMsg.msglen()

	if err := authMsg.SetRemainingLength(ml); err != nil {
		return 0, err
	}

	_, err := authMsg.header.encodeToBuf(dst)
	if err != nil {
		return dst.Len(), err
	}

	if authMsg.reasonCode == Success && len(authMsg.authMethod) == 0 && len(authMsg.authData) == 0 && len(authMsg.reasonStr) == 0 && len(authMsg.userProperty) == 0 {
		return dst.Len(), nil
	} else {
		dst.WriteByte(byte(authMsg.reasonCode))
	}
	dst.Write(lbEncode(authMsg.propertiesLen))

	if len(authMsg.authMethod) > 0 {
		dst.WriteByte(AuthenticationMethod)
		_, err = writeToBufLPBytes(dst, authMsg.authMethod)
		if err != nil {
			return dst.Len(), err
		}
	}
	if len(authMsg.authData) > 0 {
		dst.WriteByte(AuthenticationData)
		_, err = writeToBufLPBytes(dst, authMsg.authData)
		if err != nil {
			return dst.Len(), err
		}
	}
	if len(authMsg.reasonStr) > 0 {
		dst.WriteByte(ReasonString)
		_, err = writeToBufLPBytes(dst, authMsg.reasonStr)
		if err != nil {
			return dst.Len(), err
		}
	}
	return writeUserPropertyByBuf(dst, authMsg.userProperty)
}

func (authMsg *AuthMessage) build() {
	// == 可变报头 ==
	total := 1 // 认证原因码
	// 属性
	if len(authMsg.authMethod) > 0 {
		total++
		total += 2
		total += len(authMsg.authMethod)
	}
	if len(authMsg.authData) > 0 {
		total++
		total += 2
		total += len(authMsg.authData)
	}
	if len(authMsg.reasonStr) > 0 { // todo 超过接收端指定的最大报文长度，不能发送
		total++
		total += 2
		total += len(authMsg.reasonStr)
	}
	n := buildUserPropertyLen(authMsg.userProperty)
	total += n

	authMsg.propertiesLen = uint32(total - 1)

	if authMsg.propertiesLen == 0 && authMsg.reasonCode == Success {
		_ = authMsg.SetRemainingLength(0)
		return
	}
	total += len(lbEncode(authMsg.propertiesLen))
	_ = authMsg.SetRemainingLength(uint32(total))
}

func (authMsg *AuthMessage) msglen() uint32 {
	authMsg.build()
	return authMsg.remlen
}

func (authMsg *AuthMessage) SetUserProperties(userProperty [][]byte) {
	authMsg.userProperty = userProperty
	authMsg.dirty = true
}
