package message

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPubRelDecodeEncode(t *testing.T) {
	pubrel := NewPubrelMessage()
	pubrel.SetPacketId(123)
	pubrel.SetReasonCode(Success)
	pubrel.SetReasonStr([]byte("aaa"))
	pubrel.AddUserPropertys([][]byte{[]byte("aaa:oo"), []byte("bbb:pp")})
	//pubrel.build()
	b := make([]byte, 100)
	n, err := pubrel.Encode(b)
	if err != nil {
		panic(err)
	}
	fmt.Println(pubrel)
	fmt.Println(b[:n])
	pubrel1 := NewPubrelMessage()
	pubrel1.Decode(b[:n])
	pubrel1.dirty = true
	pubrel1.dbuf = nil
	fmt.Println(pubrel1)
	fmt.Println(reflect.DeepEqual(pubrel1, pubrel))
}
func TestDecodePubRel(t *testing.T) {
	var b = []byte{98, 2, 0, 123}
	pubrel := NewPubrelMessage()
	pubrel.Decode(b)
	fmt.Println(pubrel)
	p := make([]byte, 100)
	n, err := pubrel.Encode(p)
	require.NoError(t, err)
	fmt.Println(p[:n])
	require.Equal(t, b, p[:n])
}
func TestPubrelMessageFields(t *testing.T) {
	msg := NewPubrelMessage()

	msg.SetPacketId(100)

	require.Equal(t, 100, int(msg.PacketId()))
}

func TestPubrelMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(PUBREL<<4) | 2,
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewPubrelMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, PUBREL, msg.Type(), "Error decoding message.")
	require.Equal(t, 7, int(msg.PacketId()), "Error decoding message.")
}

// test insufficient bytes
func TestPubrelMessageDecode2(t *testing.T) {
	msgBytes := []byte{
		byte(PUBREL<<4) | 2,
		2,
		7, // packet ID LSB (7)
	}

	msg := NewPubrelMessage()
	_, err := msg.Decode(msgBytes)

	require.Error(t, err)
}

func TestPubrelMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(PUBREL<<4) | 2,
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewPubrelMessage()
	msg.SetPacketId(7)

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestPubrelDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(PUBREL<<4) | 2,
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewPubrelMessage()
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
