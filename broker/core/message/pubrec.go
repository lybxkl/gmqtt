package message

var (
	_ Message             = (*PubrecMessage)(nil)
	_ CleanReqProblemInfo = (*PubrecMessage)(nil)
)

// PubrecMessage A PUBREC Packet is the response to a PUBLISH Packet with QoS 2. It is the second
// packet of the QoS 2 protocol exchange.
type PubrecMessage struct {
	PubackMessage
}

// NewPubrecMessage creates a new PUBREC message.
func NewPubrecMessage() *PubrecMessage {
	msg := &PubrecMessage{}
	msg.SetType(PUBREC)

	return msg
}

func (pubRec *PubrecMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := pubRec.header.decode(src[total:])
	total += n
	if err != nil {
		return total, err
	}
	//pubRec.packetId = binary.BigEndian.Uint16(src[total:])
	pubRec.packetId = CopyLen(src[total:total+2], 2)
	total += 2
	if pubRec.RemainingLength() == 2 {
		pubRec.reasonCode = Success
		return total, nil
	}
	return pubRec.decodeOther(src, total, n)
}

// 从可变包头中原因码开始处理
func (pubRec *PubrecMessage) decodeOther(src []byte, total, n int) (int, error) {
	var err error
	pubRec.reasonCode = ReasonCode(src[total])
	total++
	if !ValidPubRecReasonCode(pubRec.reasonCode) {
		return total, ProtocolError
	}
	if total < len(src) && len(src[total:]) > 0 {
		pubRec.propertyLen, n, err = lbDecode(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	if total < len(src) && src[total] == ReasonString {
		total++
		pubRec.reasonStr, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if src[total] == ReasonString {
			return total, ProtocolError
		}
	}
	pubRec.userProperty, n, err = decodeUserProperty(src[total:]) // 用户属性
	total += n
	if err != nil {
		return total, err
	}
	pubRec.dirty = false
	return total, nil
}
