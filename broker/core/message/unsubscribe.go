package message

import (
	"bytes"
	"fmt"
	"sync/atomic"
)

var (
	_ Message             = (*UnsubscribeMessage)(nil)
	_ CleanReqProblemInfo = (*UnsubscribeMessage)(nil)
)

// UnsubscribeMessage An UNSUBSCRIBE Packet is sent by the Client to the Server, to unsubscribe from topics.
type UnsubscribeMessage struct {
	header
	// === 可变报头 ===
	//  属性长度
	propertyLen  uint32
	userProperty [][]byte
	// 载荷
	topics [][]byte
}

// NewUnsubscribeMessage creates a new UNSUBSCRIBE message.
func NewUnsubscribeMessage() *UnsubscribeMessage {
	msg := &UnsubscribeMessage{}
	msg.SetType(UNSUBSCRIBE)

	return msg
}

func (unSub *UnsubscribeMessage) PropertyLen() uint32 {
	return unSub.propertyLen
}

func (unSub *UnsubscribeMessage) SetPropertyLen(propertyLen uint32) {
	unSub.propertyLen = propertyLen
	unSub.dirty = true
}

func (unSub *UnsubscribeMessage) UserProperty() [][]byte {
	return unSub.userProperty
}

func (unSub *UnsubscribeMessage) AddUserPropertys(userProperty [][]byte) {
	unSub.userProperty = append(unSub.userProperty, userProperty...)
	unSub.dirty = true
}

func (unSub *UnsubscribeMessage) AddUserProperty(userProperty []byte) {
	unSub.userProperty = append(unSub.userProperty, userProperty)
	unSub.dirty = true
}

func (unSub *UnsubscribeMessage) SetUserProperties(userProperty [][]byte) {
	unSub.userProperty = userProperty
	unSub.dirty = true
}

func (unSub *UnsubscribeMessage) SetReasonStr(reasonStr []byte) {

}

func (unSub UnsubscribeMessage) String() string {
	msgstr := fmt.Sprintf("%s", unSub.header)
	msgstr = fmt.Sprintf("%s, PropertiesLen=%v,  User Properties=%v", msgstr, unSub.propertyLen, unSub.UserProperty())

	for i, t := range unSub.topics {
		msgstr = fmt.Sprintf("%s, Topic%d=%s", msgstr, i, string(t))
	}

	return msgstr
}

// Topics returns a list of topics sent by the Client.
func (unSub *UnsubscribeMessage) Topics() [][]byte {
	return unSub.topics
}

// AddTopic adds a single topic to the message.
func (unSub *UnsubscribeMessage) AddTopic(topic []byte) {
	if unSub.TopicExists(topic) {
		return
	}

	unSub.topics = append(unSub.topics, topic)
	unSub.dirty = true
}

// RemoveTopic removes a single topic from the list of existing ones in the message.
// If topic does not exist it just does nothing.
func (unSub *UnsubscribeMessage) RemoveTopic(topic []byte) {
	var i int
	var t []byte
	var found bool

	for i, t = range unSub.topics {
		if bytes.Equal(t, topic) {
			found = true
			break
		}
	}

	if found {
		unSub.topics = append(unSub.topics[:i], unSub.topics[i+1:]...)
	}

	unSub.dirty = true
}

// TopicExists checks to see if a topic exists in the list.
func (unSub *UnsubscribeMessage) TopicExists(topic []byte) bool {
	for _, t := range unSub.topics {
		if bytes.Equal(t, topic) {
			return true
		}
	}

	return false
}

func (unSub *UnsubscribeMessage) Len() int {
	if !unSub.dirty {
		return len(unSub.dbuf)
	}

	ml := unSub.msglen()

	if err := unSub.SetRemainingLength(uint32(ml)); err != nil {
		return 0
	}

	return unSub.header.msglen() + ml
}

// Decode reads from the io.Reader parameter until a full message is decoded, or
// when io.Reader returns EOF or error. The first return value is the number of
// bytes read from io.Reader. The second is error if Decode encounters any problems.
func (unSub *UnsubscribeMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := unSub.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	//unSub.packetId = binary.BigEndian.Uint16(src[total:])
	unSub.packetId = CopyLen(src[total:total+2], 2)
	total += 2
	var n int
	if total < len(src) && len(src[total:]) >= 4 {
		unSub.propertyLen, n, err = lbDecode(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	unSub.userProperty, n, err = decodeUserProperty(src[total:]) // 用户属性
	total += n
	if err != nil {
		return total, err
	}
	remlen := int(unSub.remlen) - (total - hn)
	var t []byte
	for remlen > 0 {
		t, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}

		unSub.topics = append(unSub.topics, t)
		remlen = remlen - n - 1
	}

	if len(unSub.topics) == 0 {
		return 0, ProtocolError // fmt.Errorf("unsubscribe/Decode: Empty topic list")
	}

	unSub.dirty = false

	return total, nil
}

// Encode returns an io.Reader in which the encoded bytes can be read. The second
// return value is the number of bytes encoded, so the caller knows how many bytes
// there will be. If Encode returns an error, then the first two return values
// should be considered invalid.
// Any changes to the message after Encode() is called will invalidate the io.Reader.
func (unSub *UnsubscribeMessage) Encode(dst []byte) (int, error) {
	if !unSub.dirty {
		if len(dst) < len(unSub.dbuf) {
			return 0, fmt.Errorf("unsubscribe/Encode: Insufficient buffer size. Expecting %d, got %d.", len(unSub.dbuf), len(dst))
		}

		return copy(dst, unSub.dbuf), nil
	}

	ml := unSub.msglen()
	hl := unSub.header.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("unsubscribe/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
	}

	if err := unSub.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := unSub.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	if unSub.PacketId() == 0 {
		unSub.SetPacketId(uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff))
		//unSub.packetId = uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff)
	}

	n = copy(dst[total:], unSub.packetId)
	//binary.BigEndian.PutUint16(dst[total:], unSub.packetId)
	total += n

	b := lbEncode(unSub.propertyLen)
	copy(dst[total:], b)
	total += len(b)

	n, err = writeUserProperty(dst[total:], unSub.userProperty) // 用户属性
	total += n

	for _, t := range unSub.topics {
		n, err = writeLPBytes(dst[total:], t)
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (unSub *UnsubscribeMessage) EncodeToBuf(dst *bytes.Buffer) (int, error) {
	if !unSub.dirty {
		return dst.Write(unSub.dbuf)
	}

	ml := unSub.msglen()

	if err := unSub.SetRemainingLength(uint32(ml)); err != nil {
		return 0, err
	}

	_, err := unSub.header.encodeToBuf(dst)
	if err != nil {
		return dst.Len(), err
	}

	if unSub.PacketId() == 0 {
		unSub.SetPacketId(uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff))
		//unSub.packetId = uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff)
	}

	dst.Write(unSub.packetId)

	dst.Write(lbEncode(unSub.propertyLen))

	_, err = writeUserPropertyByBuf(dst, unSub.userProperty) // 用户属性
	if err != nil {
		return dst.Len(), err
	}

	for _, t := range unSub.topics {
		_, err = writeToBufLPBytes(dst, t)
		if err != nil {
			return dst.Len(), err
		}
	}

	return dst.Len(), nil
}

func (unSub *UnsubscribeMessage) build() {
	total := 0

	n := buildUserPropertyLen(unSub.userProperty) // 用户属性
	total += n

	unSub.propertyLen = uint32(total)

	total += 2 // packet ID
	for _, t := range unSub.topics {
		total += 2 + len(t)
	}
	total += len(lbEncode(unSub.propertyLen))
	_ = unSub.SetRemainingLength(uint32(total))
}
func (unSub *UnsubscribeMessage) msglen() int {
	unSub.build()
	return int(unSub.remlen)
}
