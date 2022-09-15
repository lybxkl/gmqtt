package message

var (
	_ Message             = (*PubrelMessage)(nil)
	_ CleanReqProblemInfo = (*PubrelMessage)(nil)
)

// PubrelMessage A PUBREL Packet is the response to a PUBREC Packet. It is the third packet of the
// QoS 2 protocol exchange.
type PubrelMessage struct {
	PubackMessage
}

// NewPubrelMessage creates a new PUBREL message.
func NewPubrelMessage() *PubrelMessage {
	msg := &PubrelMessage{}
	msg.SetType(PUBREL)

	return msg
}

func (pubRel *PubrelMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := pubRel.header.decode(src[total:])
	total += n
	if err != nil {
		return total, err
	}
	//pubRel.packetId = binary.BigEndian.Uint16(src[total:])
	pubRel.packetId = CopyLen(src[total:total+2], 2)
	total += 2
	if pubRel.RemainingLength() == 2 {
		pubRel.reasonCode = Success
		return total, nil
	}
	return pubRel.decodeOther(src, total, n)
}

// 从可变包头中原因码开始处理
func (pubRel *PubrelMessage) decodeOther(src []byte, total, n int) (int, error) {
	var err error
	pubRel.reasonCode = ReasonCode(src[total])
	total++
	if !ValidPubRelReasonCode(pubRel.reasonCode) {
		return total, ProtocolError
	}
	if total < len(src) && len(src[total:]) > 0 {
		pubRel.propertyLen, n, err = lbDecode(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	if total < len(src) && src[total] == ReasonString {
		total++
		pubRel.reasonStr, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if src[total] == ReasonString {
			return total, ProtocolError
		}
	}
	pubRel.userProperty, n, err = decodeUserProperty(src[total:]) // 用户属性
	total += n
	if err != nil {
		return total, err
	}
	pubRel.dirty = false
	return total, nil
}
