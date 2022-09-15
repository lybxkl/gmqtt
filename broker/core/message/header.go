package message

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var (
	gPacketId uint64 = 0
)

// Support v5
// 固定头
// - 1字节的控制包类型(位7-4)和标志(位3-0)
// -最多4字节的剩余长度
type header struct {
	// 剩余长度 变长字节整数, 用来表示当前控制报文剩余部分的字节数, 包括可变报头和负载的数据.
	// 剩余长度不包括用于编码剩余长度字段本身的字节数.
	// MQTT控制报文总长度等于固定报头的长度加上剩余长度.
	remlen uint32

	// mtypeflags是固定报头的第一个字节，前四位4位是mtype, 4位是flags标志
	mtypeflags []byte

	// Some messages need packet ID, 2 byte uint16
	//一些消息需要数据包ID, 2字节uint16
	// 也就是报文标识符，两个字节
	// 需要这个的有以下报文类型
	// PUBLISH报文（当QoS>0时），PUBACK，PUBREC，PUBREC，PUBREL，PUBCOMP，SUBSCRIBE，SUBACK，UNSUBSCRIBE，UNSUBACK
	// 当客户端处理完这个报文对应的确认后，这个报文标识符就释放可重用
	packetId []byte

	// Points to the decoding buffer
	//指向解码缓冲区
	dbuf []byte

	// Whether the message has changed since last decode
	//自上次解码以来，消息是否发生了变化
	dirty bool
}

// String returns a string representation of the message.
func (head header) String() string {
	return fmt.Sprintf("Type=%q, Flags=%04b, Remaining Length=%d, Packet Id=%v", head.Type().Name(), head.Flags(), head.remlen, head.packetId)
}

// Name returns a string representation of the message type. Examples include
// "PUBLISH", "SUBSCRIBE", and others. This is statically defined for each of
// the message types and cannot be changed.
func (head *header) Name() string {
	return head.Type().Name()
}
func (head *header) MtypeFlags() byte {
	if head.mtypeflags == nil {
		return 0
	}
	return head.mtypeflags[0]
}
func (head *header) SetMtypeFlags(mtypeflags byte) {
	if len(head.mtypeflags) != 1 {
		head.mtypeflags = make([]byte, 1)
	}
	head.mtypeflags[0] = mtypeflags
}

// Desc returns a string description of the message type. For example, a
// CONNECT message would return "Client request to connect to Server." These
// descriptions are statically defined (copied from the MQTT spec) and cannot
// be changed.
func (head *header) Desc() string {
	return head.Type().Desc()
}

// Type returns the MessageType of the Message. The retured value should be one
// of the constants defined for MessageType.
func (head *header) Type() MessageType {
	//return head.mtype
	if len(head.mtypeflags) != 1 {
		head.mtypeflags = make([]byte, 1)
		head.dirty = true
	}

	return MessageType(head.mtypeflags[0] >> 4)
}

// SetType sets the message type of head message. It also correctly sets the
// default flags for the message type. It returns an error if the type is invalid.
func (head *header) SetType(mtype MessageType) error {
	if !mtype.Valid() {
		return fmt.Errorf("header/SetType: Invalid control packet type %d", mtype)
	}

	// Notice we don't set the message to be dirty when we are not allocating a new
	// buffer. In head case, it means the buffer is probably a sub-slice of another
	// slice. If that's the case, then during encoding we would have copied the whole
	// backing buffer anyway.
	if len(head.mtypeflags) != 1 {
		head.mtypeflags = make([]byte, 1)
		head.dirty = true
	}

	head.mtypeflags[0] = byte(mtype)<<4 | (mtype.DefaultFlags() & 0xf)

	return nil
}

// Flags returns the fixed header flags for head message.
func (head *header) Flags() byte {
	//return head.flags
	return head.mtypeflags[0] & 0x0f
}

// RemainingLength returns the length of the non-fixed-header part of the message.
func (head *header) RemainingLength() uint32 {
	return head.remlen
}

// SetRemainingLength sets the length of the non-fixed-header part of the message.
// It returns error if the length is greater than 268435455, which is the max
// message length as defined by the MQTT spec.
func (head *header) SetRemainingLength(remlen uint32) error {
	if remlen > maxRemainingLength || remlen < 0 {
		return fmt.Errorf("header/SetLength: Remaining length (%d) out of bound (max %d, min 0)", remlen, maxRemainingLength)
	}

	head.remlen = remlen
	head.dirty = true

	return nil
}

func (head *header) Len() int {
	return head.msglen()
}

// PacketId returns the ID of the packet.
func (head *header) PacketId() uint16 {
	if len(head.packetId) == 2 {
		return binary.BigEndian.Uint16(head.packetId)
	}

	return 0
}

// SetPacketId sets the ID of the packet.
func (head *header) SetPacketId(v uint16) {
	// If setting to 0, nothing to do, move on
	if v == 0 {
		return
	}

	// If packetId buffer is not 2 bytes (uint16), then we allocate a new one and
	// make dirty. Then we encode the packet ID into the buffer.
	if len(head.packetId) != 2 {
		head.packetId = make([]byte, 2)
		head.dirty = true
	}

	// Notice we don't set the message to be dirty when we are not allocating a new
	// buffer. In head case, it means the buffer is probably a sub-slice of another
	// slice. If that's the case, then during encoding we would have copied the whole
	// backing buffer anyway.
	binary.BigEndian.PutUint16(head.packetId, v)
}

func (head *header) encode(dst []byte) (int, error) {
	ml := head.msglen()

	if len(dst) < ml {
		return 0, fmt.Errorf("header/Encode: Insufficient buffer size. Expecting %d, got %d.", ml, len(dst))
	}

	total := 0

	if head.remlen > maxRemainingLength || head.remlen < 0 {
		return total, fmt.Errorf("header/Encode: Remaining length (%d) out of bound (max %d, min 0)", head.remlen, maxRemainingLength)
	}

	if !head.Type().Valid() {
		return total, fmt.Errorf("header/Encode: Invalid message type %d", head.Type())
	}

	dst[total] = head.mtypeflags[0]
	total += 1

	n := binary.PutUvarint(dst[total:], uint64(head.remlen))
	total += n

	return total, nil
}

func (head *header) encodeToBuf(dst *bytes.Buffer) (int, error) {

	if head.remlen > maxRemainingLength || head.remlen < 0 {
		return 0, fmt.Errorf("header/Encode: Remaining length (%d) out of bound (max %d, min 0)", head.remlen, maxRemainingLength)
	}

	if !head.Type().Valid() {
		return 0, fmt.Errorf("header/Encode: Invalid message type %d", head.Type())
	}

	dst.WriteByte(head.mtypeflags[0])

	BigEndianPutUvarint(dst, uint64(head.remlen))

	return dst.Len(), nil
}

// Decode reads from the io.Reader parameter until a full message is decoded, or
// when io.Reader returns EOF or error. The first return value is the number of
// bytes read from io.Reader. The second is error if Decode encounters any problems.
func (head *header) decode(src []byte) (int, error) {
	total := 0

	head.dbuf = src

	mtype := head.Type()
	//mtype := MessageType(0)
	head.mtypeflags = CopyLen(src[total:total+1], 1) //src[total : total+1]
	//mtype := MessageType(src[total] >> 4)
	if !head.Type().Valid() {
		return total, InvalidMessage //fmt.Errorf("header/Decode: Invalid message type %d.", mtype)
	}

	if mtype != head.Type() {
		return total, InvalidMessage //fmt.Errorf("header/Decode: Invalid message type %d. Expecting %d.", head.Type(), mtype)
	}
	//head.flags = src[total] & 0x0f
	if head.Type() != PUBLISH && head.Flags() != head.Type().DefaultFlags() {
		return total, InvalidMessage //fmt.Errorf("header/Decode: Invalid message (%d) flags. Expecting %d, got %d", head.Type(), head.Type().DefaultFlags(), head.Flags())
	}
	// Bit 3	Bit 2	Bit 1	 Bit 0
	// DUP	         QOS	     RETAIN
	// publish 报文，验证qos，第一个字节的第1，2位
	if head.Type() == PUBLISH && !ValidQos((head.Flags()>>1)&0x3) {
		return total, InvalidMessage //fmt.Errorf("header/Decode: Invalid QoS (%d) for PUBLISH message.", (head.Flags()>>1)&0x3)
	}

	total++ // 第一个字节处理完毕

	// 剩余长度，传进来的就是只有固定报头数据，所以剩下的就是剩余长度变长解码的数据
	remlen, m := binary.Uvarint(src[total:])
	total += m
	head.remlen = uint32(remlen)

	if head.remlen > maxRemainingLength ||
		(head.Type() != PINGREQ && head.Type() != PINGRESP &&
			head.Type() != AUTH && head.Type() != DISCONNECT && remlen == 0) {
		return total, InvalidMessage //fmt.Errorf("header/Decode: Remaining length (%d) out of bound (max %d, min 0)", head.remlen, maxRemainingLength)
	}

	if int(head.remlen) > len(src[total:]) {
		return total, InvalidMessage //fmt.Errorf("header/Decode: Remaining length (%d) is greater than remaining buffer (%d)", head.remlen, len(src[total:]))
	}

	return total, nil
}

func (head *header) msglen() int {
	// message type and flag byte
	total := 1

	if head.remlen <= 127 {
		total += 1
	} else if head.remlen <= 16383 {
		total += 2
	} else if head.remlen <= 2097151 {
		total += 3
	} else {
		total += 4
	}

	return total
}
