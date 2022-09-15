package message

import (
	"bytes"
	"fmt"
)

var (
	_ Message             = (*SubackMessage)(nil)
	_ CleanReqProblemInfo = (*SubackMessage)(nil)
)

// SubackMessage A SUBACK Packet is sent by the Server to the Client to confirm receipt and processing
// of a SUBSCRIBE Packet.
//
// A SUBACK Packet contains a list of return codes, that specify the maximum QoS level
// that was granted in each Subscription that was requested by the SUBSCRIBE.
type SubackMessage struct {
	header
	// 可变报头
	propertiesLen uint32 // 属性长度
	reasonStr     []byte // 原因字符串
	userProperty  [][]byte
	// 载荷
	reasonCodes []byte
}

// NewSubackMessage creates a new SUBACK message.
func NewSubackMessage() *SubackMessage {
	msg := &SubackMessage{}
	msg.SetType(SUBACK)

	return msg
}
func (subAck *SubackMessage) ReasonStr() []byte {
	return subAck.reasonStr
}

func (subAck *SubackMessage) SetReasonStr(reasonStr []byte) {
	subAck.reasonStr = reasonStr
	subAck.dirty = true
}

// String returns a string representation of the message.
func (subAck SubackMessage) String() string {
	return fmt.Sprintf("%s, Packet ID=%d, Return Codes=%v, ", subAck.header, subAck.PacketId(), subAck.reasonCodes) +
		fmt.Sprintf("PropertiesLen=%v, Reason Str=%v, User Properties=%v", subAck.PropertiesLen(), subAck.ReasonStr(), subAck.UserProperty()) +
		fmt.Sprintf("%v", subAck.reasonCodes)

}
func (subAck *SubackMessage) PropertiesLen() uint32 {
	return subAck.propertiesLen
}

func (subAck *SubackMessage) SetPropertiesLen(propertiesLen uint32) {
	subAck.propertiesLen = propertiesLen
	subAck.dirty = true
}

func (subAck *SubackMessage) UserProperty() [][]byte {
	return subAck.userProperty
}

func (subAck *SubackMessage) SetUserProperties(userProperty [][]byte) {
	subAck.userProperty = userProperty
	subAck.dirty = true
}

func (subAck *SubackMessage) AddUserPropertys(userProperty [][]byte) {
	subAck.userProperty = append(subAck.userProperty, userProperty...)
	subAck.dirty = true
}
func (subAck *SubackMessage) AddUserProperty(userProperty []byte) {
	subAck.userProperty = append(subAck.userProperty, userProperty)
	subAck.dirty = true
}

// ReasonCodes returns the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
func (subAck *SubackMessage) ReasonCodes() []byte {
	return subAck.reasonCodes
}

// AddreasonCodes sets the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
// An error is returned if any of the QoS values are not valid.
func (subAck *SubackMessage) AddReasonCodes(ret []byte) error {
	for _, c := range ret {
		if c != QosAtMostOnce && c != QosAtLeastOnce && c != QosExactlyOnce && c != QosFailure {
			return fmt.Errorf("suback/AddReturnCode: Invalid return code %d. Must be 0, 1, 2, 0x80.", c)
		}

		subAck.reasonCodes = append(subAck.reasonCodes, c)
	}

	subAck.dirty = true

	return nil
}

// AddReturnCode adds a single QoS return value.
func (subAck *SubackMessage) AddReasonCode(ret byte) error {
	return subAck.AddReasonCodes([]byte{ret})
}

func (subAck *SubackMessage) Len() int {
	if !subAck.dirty {
		return len(subAck.dbuf)
	}

	ml := subAck.msglen()

	if err := subAck.SetRemainingLength(uint32(ml)); err != nil {
		return 0
	}

	return subAck.header.msglen() + ml
}

func (subAck *SubackMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := subAck.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	//subAck.packetId = binary.BigEndian.Uint16(src[total:])
	subAck.packetId = CopyLen(src[total:total+2], 2)
	total += 2
	var n int
	subAck.propertiesLen, n, err = lbDecode(src[total:])
	total += n
	if err != nil {
		return total, err
	}
	if int(subAck.propertiesLen) > len(src[total:]) {
		return total, ProtocolError
	}
	if total < len(src) && src[total] == ReasonString {
		total++
		subAck.reasonStr, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if src[total] == ReasonString {
			return total, ProtocolError
		}
	}
	subAck.userProperty, n, err = decodeUserProperty(src[total:]) // 用户属性
	total += n
	if err != nil {
		return total, err
	}

	l := int(subAck.remlen) - (total - hn)
	if l == 0 {
		return total, ProtocolError
	}
	subAck.reasonCodes = CopyLen(src[total:total+l], l)
	total += len(subAck.reasonCodes)

	for _, code := range subAck.reasonCodes {
		if !ValidSubAckReasonCode(ReasonCode(code)) {
			return total, ProtocolError // fmt.Errorf("suback/Decode: Invalid return code %d for topic %d", code, i)
		}
	}

	subAck.dirty = false

	return total, nil
}

func (subAck *SubackMessage) Encode(dst []byte) (int, error) {
	if !subAck.dirty {
		if len(dst) < len(subAck.dbuf) {
			return 0, fmt.Errorf("suback/Encode: Insufficient buffer size. Expecting %d, got %d.", len(subAck.dbuf), len(dst))
		}

		return copy(dst, subAck.dbuf), nil
	}

	for i, code := range subAck.reasonCodes {
		if !ValidSubAckReasonCode(ReasonCode(code)) {
			return 0, fmt.Errorf("suback/Encode: Invalid return code %d for topic %d", code, i)
		}
	}

	ml := subAck.msglen()
	hl := subAck.header.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("suback/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
	}

	if err := subAck.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := subAck.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	if copy(dst[total:total+2], subAck.packetId) != 2 {
		dst[total], dst[total+1] = 0, 0
	}
	total += 2

	tb := lbEncode(subAck.propertiesLen)
	copy(dst[total:], tb)
	total += len(tb)

	if len(subAck.reasonStr) > 0 { // todo && 太长，超过了客户端指定的最大报文长度，不发送
		dst[total] = ReasonString
		total++
		n, err = writeLPBytes(dst[total:], subAck.reasonStr)
		total += n
		if err != nil {
			return total, err
		}
	}

	n, err = writeUserProperty(dst[total:], subAck.userProperty) // 用户属性
	total += n

	copy(dst[total:], subAck.reasonCodes)
	total += len(subAck.reasonCodes)

	return total, nil
}

func (subAck *SubackMessage) EncodeToBuf(dst *bytes.Buffer) (int, error) {
	if !subAck.dirty {
		return dst.Write(subAck.dbuf)
	}

	for i, code := range subAck.reasonCodes {
		if !ValidSubAckReasonCode(ReasonCode(code)) {
			return 0, fmt.Errorf("suback/Encode: Invalid return code %d for topic %d", code, i)
		}
	}

	ml := subAck.msglen()

	if err := subAck.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	_, err := subAck.header.encodeToBuf(dst)
	if err != nil {
		return dst.Len(), err
	}

	if len(subAck.packetId) != 2 {
		dst.Write([]byte{0, 0})
	} else {
		dst.Write(subAck.packetId)
	}

	dst.Write(lbEncode(subAck.propertiesLen))

	if len(subAck.reasonStr) > 0 { // todo && 太长，超过了客户端指定的最大报文长度，不发送
		dst.WriteByte(ReasonString)
		_, err = writeToBufLPBytes(dst, subAck.reasonStr)
		if err != nil {
			return dst.Len(), err
		}
	}

	_, err = writeUserPropertyByBuf(dst, subAck.userProperty) // 用户属性
	if err != nil {
		return dst.Len(), err
	}

	dst.Write(subAck.reasonCodes)

	return dst.Len(), nil
}

func (subAck *SubackMessage) build() {
	// packet ID
	total := 2

	if len(subAck.reasonStr) > 0 { // todo 最大长度时
		total++
		total += 2
		total += len(subAck.reasonStr)
	}

	n := buildUserPropertyLen(subAck.userProperty) // 用户属性
	total += n

	subAck.propertiesLen = uint32(total - 2)
	total += len(lbEncode(subAck.propertiesLen))
	total += len(subAck.reasonCodes)
	_ = subAck.SetRemainingLength(uint32(total))
}
func (subAck *SubackMessage) msglen() int {
	subAck.build()

	return int(subAck.remlen)
}
