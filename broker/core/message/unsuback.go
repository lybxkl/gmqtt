package message

import (
	"bytes"
	"fmt"
)

var (
	_ Message             = (*UnsubackMessage)(nil)
	_ CleanReqProblemInfo = (*UnsubackMessage)(nil)
)

// UnsubackMessage The UNSUBACK Packet is sent by the Server to the Client to confirm receipt of an
// UNSUBSCRIBE Packet.
type UnsubackMessage struct {
	header
	//  属性长度
	propertyLen  uint32   // 如果剩余长度小于4字节，则没有属性长度
	reasonStr    []byte   // 原因字符串是为诊断而设计的可读字符串，不能被接收端所解析。如果加上原因字符串之后的PUBACK报文长度超出了接收端指定的最大报文长度（Maximum Packet Size），则发送端不能发送此原因字符串
	userProperty [][]byte // 用户属性如果加上原因字符串之后的PUBACK报文长度超出了接收端指定的最大报文长度（Maximum Packet Size），则发送端不能发送此用户属性
	reasonCodes  []byte
}

// NewUnsubackMessage creates a new UNSUBACK message.
func NewUnsubackMessage() *UnsubackMessage {
	msg := &UnsubackMessage{}
	msg.SetType(UNSUBACK)

	return msg
}
func (unSubAck *UnsubackMessage) PropertyLen() uint32 {
	return unSubAck.propertyLen
}

func (unSubAck *UnsubackMessage) SetPropertyLen(propertyLen uint32) {
	unSubAck.propertyLen = propertyLen
	unSubAck.dirty = true
}

func (unSubAck *UnsubackMessage) ReasonStr() []byte {
	return unSubAck.reasonStr
}

func (unSubAck *UnsubackMessage) SetReasonStr(reasonStr []byte) {
	unSubAck.reasonStr = reasonStr
	unSubAck.dirty = true
}

func (unSubAck *UnsubackMessage) UserProperty() [][]byte {
	return unSubAck.userProperty
}

func (unSubAck *UnsubackMessage) SetUserProperties(userProperty [][]byte) {
	unSubAck.userProperty = userProperty
	unSubAck.dirty = true
}

func (unSubAck *UnsubackMessage) AddUserPropertys(userProperty [][]byte) {
	unSubAck.userProperty = append(unSubAck.userProperty, userProperty...)
	unSubAck.dirty = true
}

func (unSubAck *UnsubackMessage) AddUserProperty(userProperty []byte) {
	unSubAck.userProperty = append(unSubAck.userProperty, userProperty)
	unSubAck.dirty = true
}

func (unSubAck *UnsubackMessage) ReasonCodes() []byte {
	return unSubAck.reasonCodes
}

func (unSubAck *UnsubackMessage) AddReasonCodes(reasonCodes []byte) {
	unSubAck.reasonCodes = append(unSubAck.reasonCodes, reasonCodes...)
	unSubAck.dirty = true
}

func (unSubAck *UnsubackMessage) AddReasonCode(reasonCode byte) {
	unSubAck.reasonCodes = append(unSubAck.reasonCodes, reasonCode)
	unSubAck.dirty = true
}

func (unSubAck UnsubackMessage) String() string {
	return fmt.Sprintf("%s, Packet ID=%d, PropertyLen=%v, "+
		"Reason String=%s, User Property=%s, Reason Code=%v",
		unSubAck.header, unSubAck.packetId, unSubAck.propertyLen, unSubAck.reasonStr, unSubAck.userProperty, unSubAck.reasonCodes)
}

func (unSubAck *UnsubackMessage) Decode(src []byte) (int, error) {
	total, n := 0, 0

	hn, err := unSubAck.header.decode(src[total:])
	total += hn
	n = hn
	if err != nil {
		return total, err
	}
	//unSubAck.packetId = binary.BigEndian.Uint16(src[total:])
	unSubAck.packetId = CopyLen(src[total:total+2], 2)
	total += 2

	unSubAck.propertyLen, n, err = lbDecode(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	if total < len(src) && src[total] == ReasonString {
		total++
		unSubAck.reasonStr, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if src[total] == ReasonString {
			return total, ProtocolError
		}
	}

	unSubAck.userProperty, n, err = decodeUserProperty(src[total:]) // 用户属性
	total += n
	if err != nil {
		return total, err
	}

	l := int(unSubAck.remlen) - (total - hn)
	if l == 0 {
		return total, ProtocolError
	}
	unSubAck.reasonCodes = CopyLen(src[total:total+l], l)
	total += len(unSubAck.reasonCodes)

	for _, code := range unSubAck.reasonCodes {
		if !ValidUnSubAckReasonCode(ReasonCode(code)) {
			return total, ProtocolError // fmt.Errorf("unsuback/Decode: Invalid return code %d for topic %d", code, i)
		}
	}

	unSubAck.dirty = false
	return total, nil
}

func (unSubAck *UnsubackMessage) Encode(dst []byte) (int, error) {
	if !unSubAck.dirty {
		if len(dst) < len(unSubAck.dbuf) {
			return 0, fmt.Errorf("unsuback/Encode: Insufficient buffer size. Expecting %d, got %d.", len(unSubAck.dbuf), len(dst))
		}
		return copy(dst, unSubAck.dbuf), nil
	}

	ml := unSubAck.msglen()
	hl := unSubAck.header.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("unsuback/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
	}

	if err := unSubAck.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := unSubAck.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	// 可变报头
	if copy(dst[total:total+2], unSubAck.packetId) != 2 {
		dst[total], dst[total+1] = 0, 0
	}
	total += 2

	b := lbEncode(unSubAck.propertyLen)
	copy(dst[total:], b)
	total += len(b)

	// TODO 下面两个在PUBACK报文长度超出了接收端指定的最大报文长度（Maximum Packet Size），则发送端不能发送此原因字符串
	if len(unSubAck.reasonStr) > 0 {
		dst[total] = ReasonString
		total++
		n, err = writeLPBytes(dst[total:], unSubAck.reasonStr)
		total += n
		if err != nil {
			return total, err
		}
	}

	n, err = writeUserProperty(dst[total:], unSubAck.userProperty) // 用户属性
	total += n

	copy(dst[total:], unSubAck.reasonCodes)
	total += len(unSubAck.reasonCodes)

	return total, nil
}

func (unSubAck *UnsubackMessage) EncodeToBuf(dst *bytes.Buffer) (int, error) {
	if !unSubAck.dirty {
		return dst.Write(unSubAck.dbuf)
	}

	ml := unSubAck.msglen()

	if err := unSubAck.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	_, err := unSubAck.header.encodeToBuf(dst)
	if err != nil {
		return dst.Len(), err
	}

	// 可变报头
	if len(unSubAck.packetId) != 2 {
		dst.Write([]byte{0, 0})
	} else {
		dst.Write(unSubAck.packetId)
	}

	dst.Write(lbEncode(unSubAck.propertyLen))

	// TODO 下面两个在PUBACK报文长度超出了接收端指定的最大报文长度（Maximum Packet Size），则发送端不能发送此原因字符串
	if len(unSubAck.reasonStr) > 0 {
		dst.WriteByte(ReasonString)
		_, err = writeToBufLPBytes(dst, unSubAck.reasonStr)
		if err != nil {
			return dst.Len(), err
		}
	}

	_, err = writeUserPropertyByBuf(dst, unSubAck.userProperty) // 用户属性
	if err != nil {
		return dst.Len(), err
	}

	dst.Write(unSubAck.reasonCodes)
	return dst.Len(), nil
}

func (unSubAck *UnsubackMessage) build() {
	// packet ID
	total := 2

	if len(unSubAck.reasonStr) != 0 {
		total++
		total += 2
		total += len(unSubAck.reasonStr)
	}

	n := buildUserPropertyLen(unSubAck.userProperty) // 用户属性
	total += n

	unSubAck.propertyLen = uint32(total - 2)
	total += len(lbEncode(unSubAck.propertyLen))
	total += len(unSubAck.reasonCodes)
	_ = unSubAck.SetRemainingLength(uint32(total))
}
func (unSubAck *UnsubackMessage) msglen() int {
	unSubAck.build()
	return int(unSubAck.remlen)
}

func (unSubAck *UnsubackMessage) Len() int {
	if !unSubAck.dirty {
		return len(unSubAck.dbuf)
	}

	ml := unSubAck.msglen()

	if err := unSubAck.SetRemainingLength(uint32(ml)); err != nil {
		return 0
	}

	return unSubAck.header.msglen() + ml
}
