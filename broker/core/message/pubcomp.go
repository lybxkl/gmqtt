package message

var (
	_ Message             = (*PubcompMessage)(nil)
	_ CleanReqProblemInfo = (*PubcompMessage)(nil)
)

// PubcompMessage The PUBCOMP Packet is the response to a PUBREL Packet. It is the fourth and
// final packet of the QoS 2 protocol exchange.
type PubcompMessage struct {
	PubackMessage
}

// NewPubcompMessage creates a new PUBCOMP message.
func NewPubcompMessage() *PubcompMessage {
	msg := &PubcompMessage{}
	msg.SetType(PUBCOMP)

	return msg
}

func (pubComp *PubcompMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := pubComp.header.decode(src[total:])
	total += n
	if err != nil {
		return total, err
	}
	//pubComp.packetId = binary.BigEndian.Uint16(src[total:])
	pubComp.packetId = CopyLen(src[total:total+2], 2)
	total += 2
	if pubComp.header.remlen == 2 {
		pubComp.reasonCode = Success
		return total, nil
	}
	return pubComp.decodeOther(src, total, n)
}

// 从可变包头中原因码开始处理
func (pubComp *PubcompMessage) decodeOther(src []byte, total, n int) (int, error) {
	var err error
	pubComp.reasonCode = ReasonCode(src[total])
	total++
	if !ValidPubCompReasonCode(pubComp.reasonCode) {
		return total, ProtocolError
	}
	if total < len(src) && len(src[total:]) > 0 {
		pubComp.propertyLen, n, err = lbDecode(src[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	if total < len(src) && src[total] == ReasonString {
		total++
		pubComp.reasonStr, n, err = readLPBytes(src[total:])
		total += n
		if err != nil {
			return total, err
		}
		if src[total] == ReasonString {
			return total, ProtocolError
		}
	}
	pubComp.userProperty, n, err = decodeUserProperty(src[total:]) // 用户属性
	total += n
	if err != nil {
		return total, err
	}
	pubComp.dirty = false
	return total, nil
}
