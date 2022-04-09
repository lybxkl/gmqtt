package message

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeUnSubAck(t *testing.T) {
	sub := NewUnsubackMessage()

	sub.SetPacketId(100)
	require.Equal(t, 100, int(sub.PacketId()), "Error setting packet ID.")

	sub.AddUserPropertys([][]byte{[]byte("asd"), []byte("ccc:sa")})
	require.Equal(t, [][]byte{[]byte("asd"), []byte("ccc:sa")}, sub.UserProperty(), "Error adding User Property.")

	sub.AddReasonCode(0x00)
	sub.AddReasonCode(0x11)

	sub.SetReasonStr([]byte("asd123"))
	b := make([]byte, 100)
	n, err := sub.Encode(b)
	require.NoError(t, err)
	fmt.Println(sub)
	fmt.Println(b[:n])
	sub1 := NewUnsubackMessage()
	_, err = sub1.Decode(b[:n])
	require.NoError(t, err)
	fmt.Println(sub1)
	sub1.dirty = true
	sub1.dbuf = nil
	require.Equal(t, true, reflect.DeepEqual(sub, sub1))
}
func TestUnsubackMessageFields(t *testing.T) {
	msg := NewUnsubackMessage()

	msg.SetPacketId(100)

	require.Equal(t, 100, int(msg.PacketId()))
}

func TestUnsubackMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(UNSUBACK << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewUnsubackMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, UNSUBACK, msg.Type(), "Error decoding message.")
	require.Equal(t, 7, int(msg.PacketId()), "Error decoding message.")
}

// test insufficient bytes
func TestUnsubackMessageDecode2(t *testing.T) {
	msgBytes := []byte{
		byte(UNSUBACK << 4),
		2,
		7, // packet ID LSB (7)
	}

	msg := NewUnsubackMessage()
	_, err := msg.Decode(msgBytes)

	require.Error(t, err)
}

func TestUnsubackMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(UNSUBACK << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewUnsubackMessage()
	msg.SetPacketId(7)

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestUnsubackDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(UNSUBACK << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewUnsubackMessage()
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
