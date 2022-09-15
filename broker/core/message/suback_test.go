package message

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeSubAck(t *testing.T) {
	sub := NewSubackMessage()

	sub.SetPacketId(100)
	require.Equal(t, 100, int(sub.PacketId()), "Error setting packet ID.")

	sub.AddUserPropertys([][]byte{[]byte("asd"), []byte("ccc:sa")})
	require.Equal(t, [][]byte{[]byte("asd"), []byte("ccc:sa")}, sub.UserProperty(), "Error adding User Property.")

	sub.SetReasonStr([]byte("aaa"))
	require.Equal(t, []byte("aaa"), sub.ReasonStr(), "Error adding Subscription Identifier.")

	sub.AddReasonCode(0x00)
	sub.AddReasonCode(0x01)

	b := make([]byte, 100)
	n, err := sub.Encode(b)
	require.NoError(t, err)
	fmt.Println(sub)
	fmt.Println(b[:n])
	sub1 := NewSubackMessage()
	_, err = sub1.Decode(b[:n])
	require.NoError(t, err)
	fmt.Println(sub1)
	sub1.dirty = true
	sub1.dbuf = nil
	require.Equal(t, true, reflect.DeepEqual(sub, sub1))
}
func TestSubackMessageFields(t *testing.T) {
	msg := NewSubackMessage()

	msg.SetPacketId(100)
	require.Equal(t, 100, int(msg.PacketId()), "Error setting packet ID.")

	msg.AddReasonCode(1)
	require.Equal(t, 1, len(msg.ReasonCodes()), "Error adding return code.")

	err := msg.AddReasonCode(0x90)
	require.Error(t, err)
}

func TestSubackMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(SUBACK << 4),
		6,
		0,    // packet ID MSB (0)
		7,    // packet ID LSB (7)
		0,    // return code 1
		1,    // return code 2
		2,    // return code 3
		0x80, // return code 4
	}

	msg := NewSubackMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, SUBACK, msg.Type(), "Error decoding message.")
	require.Equal(t, 4, len(msg.ReasonCodes()), "Error adding return code.")
}

// test with wrong return code
func TestSubackMessageDecode2(t *testing.T) {
	msgBytes := []byte{
		byte(SUBACK << 4),
		6,
		0,    // packet ID MSB (0)
		7,    // packet ID LSB (7)
		0,    // return code 1
		1,    // return code 2
		2,    // return code 3
		0x81, // return code 4
	}

	msg := NewSubackMessage()
	_, err := msg.Decode(msgBytes)

	require.Error(t, err)
}

func TestSubackMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(SUBACK << 4),
		6,
		0,    // packet ID MSB (0)
		7,    // packet ID LSB (7)
		0,    // return code 1
		1,    // return code 2
		2,    // return code 3
		0x80, // return code 4
	}

	msg := NewSubackMessage()
	msg.SetPacketId(7)
	msg.AddReasonCode(0)
	msg.AddReasonCode(1)
	msg.AddReasonCode(2)
	msg.AddReasonCode(0x80)

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestSubackDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(SUBACK << 4),
		6,
		0,    // packet ID MSB (0)
		7,    // packet ID LSB (7)
		0,    // return code 1
		1,    // return code 2
		2,    // return code 3
		0x80, // return code 4
	}

	msg := NewSubackMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")

	dst := make([]byte, 100)
	n2, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n2, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n2], "Error decoding message.")

	n3, err := msg.Decode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n3, "Error decoding message.")
}
