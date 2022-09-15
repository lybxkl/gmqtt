package message

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPubAckDecodeEncode(t *testing.T) {
	puback := NewPubackMessage()
	puback.SetPacketId(123)
	puback.SetReasonCode(Success)
	//puback.SetReasonStr([]byte("aaa"))
	//puback.AddUserPropertys([][]byte{[]byte("aaa:oo"), []byte("bbb:pp")})
	//puback.build()
	b := make([]byte, 100)
	n, err := puback.Encode(b)
	if err != nil {
		panic(err)
	}
	fmt.Println(puback)
	fmt.Println(b[:n])
	puback1 := NewPubackMessage()
	puback1.Decode(b[:n])
	puback1.dirty = true
	puback1.dbuf = nil
	fmt.Println(puback1)
	fmt.Println(reflect.DeepEqual(puback1, puback))
}
func TestPubAckDecodeErr(t *testing.T) {
	puback := NewPubackMessage()
	puback.SetPacketId(123)
	puback.SetReasonCode(DisconnectionIncludesWill)
	//puback.SetReasonStr([]byte("aaa"))
	//puback.AddUserPropertys([][]byte{[]byte("aaa:oo"), []byte("bbb:pp")})
	//puback.build()
	b := make([]byte, 100)
	n, err := puback.Encode(b)
	if err != nil {
		panic(err)
	}
	fmt.Println(puback)
	fmt.Println(b[:n])
	puback1 := NewPubackMessage()
	n, err = puback1.Decode(b[:n])
	require.Error(t, err)
}
func TestPubackMessageFields(t *testing.T) {
	msg := NewPubackMessage()

	msg.SetPacketId(100)

	require.Equal(t, 100, int(msg.PacketId()))
}

func TestPubackMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(PUBACK << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewPubackMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, PUBACK, msg.Type(), "Error decoding message.")
	require.Equal(t, 7, int(msg.PacketId()), "Error decoding message.")
}

// test insufficient bytes
func TestPubackMessageDecode2(t *testing.T) {
	msgBytes := []byte{
		byte(PUBACK << 4),
		2,
		7, // packet ID LSB (7)
	}

	msg := NewPubackMessage()
	_, err := msg.Decode(msgBytes)

	require.Error(t, err)
}

func TestPubackMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(PUBACK << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewPubackMessage()
	msg.SetPacketId(7)

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestPubackDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(PUBACK << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewPubackMessage()
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
