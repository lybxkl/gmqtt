package message

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type DisconnectMessage struct {
	header
	reasonCode ReasonCode
	//  属性
	propertyLen uint32 // 如果剩余长度小于4字节，则没有属性长度
	// 会话过期间隔不能由服务端的DISCONNECT报文发送
	sessionExpiryInterval uint32   // 会话过期间隔 如果没有设置会话过期间隔，则使用CONNECT报文中的会话过期间隔
	reasonStr             []byte   // 长度超出了接收端指定的最大报文长度，则发送端不能发送此属性
	serverReference       []byte   // 客户端可以使用它来识别其他要使用的服务端
	userProperty          [][]byte // 用户属性如果加上之后的报文长度超出了接收端指定的最大报文长度（Maximum Packet Size），则发送端不能发送此用户属性
}

func (dis *DisconnectMessage) String() string {
	return fmt.Sprintf("header: %v, reasonCode=%s, propertyLen=%v, sessionExpiryInterval=%v, reasonStr=%v, serverReference=%s, userProperty=%s",
		dis.header, dis.reasonCode, dis.propertyLen, dis.sessionExpiryInterval, dis.reasonStr, dis.serverReference, dis.userProperty)
}

var _ Message = (*DisconnectMessage)(nil)

// NewDisconnectMessage creates a new DISCONNECT message.
func NewDisconnectMessage() *DisconnectMessage {
	msg := &DisconnectMessage{}
	msg.SetType(DISCONNECT)

	return msg
}

func NewDiscMessageWithCodeInfo(code ReasonCode, str []byte) *DisconnectMessage {
	msg := &DisconnectMessage{}
	msg.SetType(DISCONNECT)
	msg.SetReasonCode(code)
	msg.SetReasonStr(str)
	return msg
}

func (dis *DisconnectMessage) Decode(src []byte) (int, error) {
	total, err := dis.header.decode(src)
	if err != nil {
		return total, err
	}
	if dis.remlen == 0 {
		dis.reasonCode = Success
		return total, nil
	}
	dis.reasonCode = ReasonCode(src[total])
	total++

	if !ValidDisconnectReasonCode(dis.reasonCode) {
		return total, ProtocolError
	}

	if dis.remlen < 2 {
		dis.propertyLen = 0
		return total, nil
	}
	var n int
	dis.propertyLen, n, err = lbDecode(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	if total < len(src) && src[total] == SessionExpirationInterval {
		total++
		dis.sessionExpiryInterval = binary.BigEndian.Uint32(src)
		total += 4
		if dis.sessionExpiryInterval == 0 {
			return total, ProtocolError
		}
		if total < len(src) && src[total] == SessionExpirationInterval {
			return total, err
		}
	}

	if total < len(src) && src[total] == ReasonString {
		total++
		dis.reasonStr, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if total < len(src) && src[total] == ReasonString {
			return total, err
		}
	}

	dis.userProperty, n, err = decodeUserProperty(src[total:]) // 用户属性
	total += n
	if err != nil {
		return total, err
	}

	if total < len(src) && src[total] == ServerReference {
		total++
		dis.serverReference, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if total < len(src) && src[total] == ServerReference {
			return total, err
		}
	}
	return total, nil
}

func (dis *DisconnectMessage) Encode(dst []byte) (int, error) {
	if !dis.dirty {
		if len(dst) < len(dis.dbuf) {
			return 0, fmt.Errorf("disconnect/Encode: Insufficient buffer size. Expecting %d, got %d.", len(dis.dbuf), len(dst))
		}

		return copy(dst, dis.dbuf), nil
	}
	ml := dis.msglen()
	hl := dis.header.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("auth/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
	}

	if err := dis.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	total, err := dis.header.encode(dst)
	if err != nil {
		return total, err
	}
	if dis.reasonCode == Success && dis.remlen == 0 {
		return total, nil
	}
	dst[total] = dis.reasonCode.Value()
	total++

	n := copy(dst[total:], lbEncode(dis.propertyLen))
	total += n

	if dis.sessionExpiryInterval > 0 {
		dst[total] = SessionExpirationInterval
		total++
		binary.BigEndian.PutUint32(dst[total:], dis.sessionExpiryInterval)
		total += 4
	}
	if len(dis.reasonStr) > 0 {
		dst[total] = ReasonString
		total++
		n, err = writeLPBytes(dst[total:], dis.reasonStr)
		total += n
		if err != nil {
			return total, err
		}
	}

	n, err = writeUserProperty(dst[total:], dis.userProperty) // 用户属性
	total += n

	if len(dis.serverReference) > 0 {
		dst[total] = ServerReference
		total++
		n, err = writeLPBytes(dst[total:], dis.serverReference)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (dis *DisconnectMessage) EncodeToBuf(dst *bytes.Buffer) (int, error) {
	if !dis.dirty {
		return dst.Write(dis.dbuf)
	}

	ml := dis.msglen()
	if err := dis.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	_, err := dis.header.encodeToBuf(dst)
	if err != nil {
		return dst.Len(), err
	}

	if dis.reasonCode == Success && dis.remlen == 0 {
		return dst.Len(), nil
	}
	dst.WriteByte(dis.reasonCode.Value())

	dst.Write(lbEncode(dis.propertyLen))

	if dis.sessionExpiryInterval > 0 {
		dst.WriteByte(SessionExpirationInterval)
		_ = BigEndianPutUint32(dst, dis.sessionExpiryInterval)
	}
	if len(dis.reasonStr) > 0 {
		dst.WriteByte(ReasonString)
		_, err = writeToBufLPBytes(dst, dis.reasonStr)
		if err != nil {
			return dst.Len(), err
		}
	}

	_, err = writeUserPropertyByBuf(dst, dis.userProperty) // 用户属性
	if err != nil {
		return dst.Len(), err
	}

	if len(dis.serverReference) > 0 {
		dst.WriteByte(ServerReference)
		_, err = writeToBufLPBytes(dst, dis.serverReference)
		if err != nil {
			return dst.Len(), err
		}
	}
	return dst.Len(), nil
}

func (dis *DisconnectMessage) build() {
	total := 0
	if dis.sessionExpiryInterval > 0 {
		total += 5
	}
	if len(dis.reasonStr) > 0 { // todo 超了就不发
		total++
		total += 2
		total += len(dis.reasonStr)
	}

	n := buildUserPropertyLen(dis.userProperty) // todo 超了就不发
	total += n

	if len(dis.serverReference) > 0 {
		total++
		total += 2
		total += len(dis.serverReference)
	}
	dis.propertyLen = uint32(total)
	if dis.reasonCode == Success && dis.propertyLen == 0 {
		_ = dis.SetRemainingLength(0)
		return
	}
	// 加 1 是断开原因码
	_ = dis.SetRemainingLength(uint32(1 + int(dis.propertyLen) + len(lbEncode(dis.propertyLen))))
}

func (dis *DisconnectMessage) msglen() int {
	dis.build()
	return int(dis.remlen)
}

func (dis *DisconnectMessage) Len() int {
	if !dis.dirty {
		return len(dis.dbuf)
	}

	ml := dis.msglen()

	if err := dis.SetRemainingLength(uint32(ml)); err != nil {
		return 0
	}

	return dis.header.msglen() + ml
}
func (dis *DisconnectMessage) ReasonCode() ReasonCode {
	return dis.reasonCode
}

func (dis *DisconnectMessage) SetReasonCode(reasonCode ReasonCode) {
	dis.reasonCode = reasonCode
	dis.dirty = true
}

func (dis *DisconnectMessage) PropertyLen() uint32 {
	return dis.propertyLen
}

func (dis *DisconnectMessage) SetPropertyLen(propertyLen uint32) {
	dis.propertyLen = propertyLen
	dis.dirty = true
}

func (dis *DisconnectMessage) SessionExpiryInterval() uint32 {
	return dis.sessionExpiryInterval
}

func (dis *DisconnectMessage) SetSessionExpiryInterval(sessionExpiryInterval uint32) {
	dis.sessionExpiryInterval = sessionExpiryInterval
	dis.dirty = true
}

func (dis *DisconnectMessage) ReasonStr() []byte {
	return dis.reasonStr
}

func (dis *DisconnectMessage) SetReasonStr(reasonStr []byte) {
	dis.reasonStr = reasonStr
	dis.dirty = true
}

func (dis *DisconnectMessage) ServerReference() []byte {
	return dis.serverReference
}

func (dis *DisconnectMessage) SetServerReference(serverReference []byte) {
	dis.serverReference = serverReference
	dis.dirty = true
}

func (dis *DisconnectMessage) UserProperty() [][]byte {
	return dis.userProperty
}

func (dis *DisconnectMessage) AddUserPropertys(userProperty [][]byte) {
	dis.userProperty = append(dis.userProperty, userProperty...)
	dis.dirty = true
}
func (dis *DisconnectMessage) AddUserProperty(userProperty []byte) {
	dis.userProperty = append(dis.userProperty, userProperty)
	dis.dirty = true
}
