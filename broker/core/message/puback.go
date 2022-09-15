package message

import (
	"bytes"
	"fmt"
)

var (
	_ Message             = (*PubackMessage)(nil)
	_ CleanReqProblemInfo = (*PubackMessage)(nil)
)

// PubackMessage A PUBACK Packet is the response to a PUBLISH Packet with QoS level 1.
type PubackMessage struct {
	header
	// === PUBACK可变报头 ===
	reasonCode ReasonCode // 原因码
	//  属性长度
	propertyLen  uint32   // 如果剩余长度小于4字节，则没有属性长度
	reasonStr    []byte   // 原因字符串是为诊断而设计的可读字符串，不能被接收端所解析。如果加上原因字符串之后的PUBACK报文长度超出了接收端指定的最大报文长度（Maximum Packet Size），则发送端不能发送此原因字符串
	userProperty [][]byte // 用户属性如果加上原因字符串之后的PUBACK报文长度超出了接收端指定的最大报文长度（Maximum Packet Size），则发送端不能发送此用户属性
}

// NewPubackMessage creates a new PUBACK message.
func NewPubackMessage() *PubackMessage {
	msg := &PubackMessage{}
	msg.SetType(PUBACK)

	return msg
}

func (pubAck *PubackMessage) ReasonCode() ReasonCode {
	return pubAck.reasonCode
}

func (pubAck *PubackMessage) SetReasonCode(reasonCode ReasonCode) {
	pubAck.reasonCode = reasonCode
	pubAck.dirty = true
}

func (pubAck *PubackMessage) PropertyLen() uint32 {
	return pubAck.propertyLen
}

func (pubAck *PubackMessage) SetPropertyLen(propertyLen uint32) {
	pubAck.propertyLen = propertyLen
	pubAck.dirty = true
}

func (pubAck *PubackMessage) ReasonStr() []byte {
	return pubAck.reasonStr
}

func (pubAck *PubackMessage) SetReasonStr(reasonStr []byte) {
	pubAck.reasonStr = reasonStr
	pubAck.dirty = true
}

func (pubAck *PubackMessage) UserProperty() [][]byte {
	return pubAck.userProperty
}

func (pubAck *PubackMessage) SetUserProperties(userProperty [][]byte) {
	pubAck.userProperty = userProperty
	pubAck.dirty = true
}

func (pubAck *PubackMessage) AddUserPropertys(userProperty [][]byte) {
	pubAck.userProperty = append(pubAck.userProperty, userProperty...)
	pubAck.dirty = true
}

func (pubAck *PubackMessage) AddUserProperty(userProperty []byte) {
	pubAck.userProperty = append(pubAck.userProperty, userProperty)
	pubAck.dirty = true
}

func (pubAck PubackMessage) String() string {
	return fmt.Sprintf("%s, Packet ID=%d, Reason Code=%v, PropertyLen=%v, "+
		"Reason String=%s, User Property=%s",
		pubAck.header, pubAck.packetId, pubAck.reasonCode, pubAck.propertyLen, pubAck.reasonStr, pubAck.userProperty)
}

func (pubAck *PubackMessage) Len() int {
	if !pubAck.dirty {
		return len(pubAck.dbuf)
	}

	ml := pubAck.msglen()

	if err := pubAck.SetRemainingLength(uint32(ml)); err != nil {
		return 0
	}

	return pubAck.header.msglen() + ml
}

func (pubAck *PubackMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := pubAck.header.decode(src[total:])
	total += n
	if err != nil {
		return total, err
	}
	//pubAck.packetId = binary.BigEndian.Uint16(src[total:])
	pubAck.packetId = CopyLen(src[total:total+2], 2)
	total += 2
	if pubAck.RemainingLength() == 2 {
		pubAck.reasonCode = Success
		return total, nil
	}
	return pubAck.decodeOther(src, total, n)
}

// 从可变包头中原因码开始处理
func (pubAck *PubackMessage) decodeOther(src []byte, total, n int) (int, error) {
	var err error
	pubAck.reasonCode = ReasonCode(src[total])
	total++
	if !ValidPubAckReasonCode(pubAck.reasonCode) {
		return total, ProtocolError
	}
	if total < len(src) && len(src[total:]) > 0 {
		pubAck.propertyLen, n, err = lbDecode(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	if total < len(src) && src[total] == ReasonString {
		total++
		pubAck.reasonStr, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if src[total] == ReasonString {
			return total, ProtocolError
		}
	}
	pubAck.userProperty, n, err = decodeUserProperty(src[total:]) // 用户属性
	total += n
	if err != nil {
		return total, err
	}
	pubAck.dirty = false
	return total, nil
}

func (pubAck *PubackMessage) Encode(dst []byte) (int, error) {
	if !pubAck.dirty {
		if len(dst) < len(pubAck.dbuf) {
			return 0, fmt.Errorf("pubxxx/Encode: Insufficient buffer size. Expecting %d, got %d.", len(pubAck.dbuf), len(dst))
		}
		return copy(dst, pubAck.dbuf), nil
	}

	ml := pubAck.msglen()
	hl := pubAck.header.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("pubxxx/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
	}

	if err := pubAck.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := pubAck.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	// 可变报头
	if copy(dst[total:total+2], pubAck.packetId) != 2 {
		dst[total], dst[total+1] = 0, 0
	}
	total += 2
	if pubAck.header.remlen == 2 && pubAck.reasonCode == Success {
		return total, nil
	}
	dst[total] = pubAck.reasonCode.Value()
	total++
	if pubAck.propertyLen > 0 {
		b := lbEncode(pubAck.propertyLen)
		copy(dst[total:], b)
		total += len(b)
	} else {
		return total, nil
	}
	// TODO 下面两个在PUBACK报文长度超出了接收端指定的最大报文长度（Maximum Packet Size），则发送端不能发送此原因字符串
	if len(pubAck.reasonStr) > 0 {
		dst[total] = ReasonString
		total++
		n, err = writeLPBytes(dst[total:], pubAck.reasonStr)
		total += n
		if err != nil {
			return total, err
		}
	}

	n, err = writeUserProperty(dst[total:], pubAck.userProperty) // 用户属性
	total += n
	return total, nil
}

func (pubAck *PubackMessage) EncodeToBuf(dst *bytes.Buffer) (int, error) {
	if !pubAck.dirty {
		return dst.Write(pubAck.dbuf)
	}

	ml := pubAck.msglen()

	if err := pubAck.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	_, err := pubAck.header.encodeToBuf(dst)
	if err != nil {
		return dst.Len(), err
	}

	// 可变报头
	if len(pubAck.packetId) == 2 {
		dst.Write(pubAck.packetId)
	} else {
		dst.Write([]byte{0, 0})
	}

	if pubAck.header.remlen == 2 && pubAck.reasonCode == Success {
		return dst.Len(), nil
	}
	dst.WriteByte(pubAck.reasonCode.Value())

	if pubAck.propertyLen > 0 {
		dst.Write(lbEncode(pubAck.propertyLen))
	} else {
		return dst.Len(), nil
	}
	// TODO 下面两个在PUBACK报文长度超出了接收端指定的最大报文长度（Maximum Packet Size），则发送端不能发送此原因字符串
	if len(pubAck.reasonStr) > 0 {
		dst.WriteByte(ReasonString)
		_, err = writeToBufLPBytes(dst, pubAck.reasonStr)
		if err != nil {
			return dst.Len(), err
		}
	}

	_, err = writeUserPropertyByBuf(dst, pubAck.userProperty) // 用户属性
	if err != nil {
		return dst.Len(), err
	}
	return dst.Len(), nil
}

func (pubAck *PubackMessage) build() {
	total := 0
	if len(pubAck.reasonStr) > 0 {
		total++
		total += 2
		total += len(pubAck.reasonStr)
	}

	n := buildUserPropertyLen(pubAck.userProperty)
	total += n

	pubAck.propertyLen = uint32(total)
	// 2 是报文标识符，2字节
	// 1 是原因码
	if pubAck.propertyLen == 0 && pubAck.reasonCode == Success {
		_ = pubAck.SetRemainingLength(uint32(2))
		return
	}
	if pubAck.propertyLen == 0 && pubAck.reasonCode != Success {
		_ = pubAck.SetRemainingLength(uint32(2 + 1))
		return
	}
	_ = pubAck.SetRemainingLength(uint32(2 + 1 + int(pubAck.propertyLen) + len(lbEncode(pubAck.propertyLen))))
}
func (pubAck *PubackMessage) msglen() int {
	pubAck.build()
	return int(pubAck.remlen)
}
