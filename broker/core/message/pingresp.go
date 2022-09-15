package message

import (
	"bytes"
	"fmt"
)

// PingrespMessage A PINGRESP Packet is sent by the Server to the Client in response to a PINGREQ
// Packet. It indicates that the Server is alive.
type PingrespMessage struct {
	header
}

var _ Message = (*PingrespMessage)(nil)

// NewPingrespMessage creates a new PINGRESP message.
func NewPingrespMessage() *PingrespMessage {
	msg := &PingrespMessage{}
	msg.SetType(PINGRESP)

	return msg
}

func (pingResp *PingrespMessage) Decode(src []byte) (int, error) {
	return pingResp.header.decode(src)
}

func (pingResp *PingrespMessage) Encode(dst []byte) (int, error) {
	if !pingResp.dirty {
		if len(dst) < len(pingResp.dbuf) {
			return 0, fmt.Errorf("pingrespMessage/Encode: Insufficient buffer size. Expecting %d, got %d.", len(pingResp.dbuf), len(dst))
		}

		return copy(dst, pingResp.dbuf), nil
	}

	return pingResp.header.encode(dst)
}

func (pingResp *PingrespMessage) EncodeToBuf(dst *bytes.Buffer) (int, error) {
	if !pingResp.dirty {
		return dst.Write(pingResp.dbuf)
	}

	return pingResp.header.encodeToBuf(dst)
}
