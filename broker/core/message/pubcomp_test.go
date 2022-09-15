package message

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPubCompDecodeEncode(t *testing.T) {
	pubcomp := NewPubcompMessage()
	pubcomp.SetPacketId(123)
	pubcomp.SetReasonCode(Success) // SUCCESS
	//pubcomp.SetReasonStr([]byte("aaa"))
	//pubcomp.AddUserPropertys([][]byte{[]byte("aaa:oo"), []byte("bbb:pp")})

	b := make([]byte, 100)
	n, err := pubcomp.Encode(b)
	if err != nil {
		panic(err)
	}
	fmt.Println(pubcomp)
	fmt.Println(b[:n])
	pubcomp1 := NewPubcompMessage()
	_, err = pubcomp1.Decode(b[:n])
	require.NoError(t, err)
	pubcomp1.dirty = true
	pubcomp1.dbuf = nil
	fmt.Println(pubcomp1)
	fmt.Println(reflect.DeepEqual(pubcomp1, pubcomp))
}
func TestPubCompDecodeErr(t *testing.T) {
	pubcomp := NewPubcompMessage()
	pubcomp.SetPacketId(123)
	pubcomp.SetReasonCode(DisconnectionIncludesWill) // SUCCESS
	//pubcomp.SetReasonStr([]byte("aaa"))
	//pubcomp.AddUserPropertys([][]byte{[]byte("aaa:oo"), []byte("bbb:pp")})

	b := make([]byte, 100)
	n, err := pubcomp.Encode(b)
	if err != nil {
		panic(err)
	}
	fmt.Println(pubcomp)
	fmt.Println(b[:n])
	pubcomp1 := NewPubcompMessage()
	_, err = pubcomp1.Decode(b[:n])
	require.Error(t, err)
}
func TestDecodePubComp(t *testing.T) {
	var b = []byte{112, 2, 0, 123}
	pubcomp := NewPubcompMessage()
	pubcomp.Decode(b)
	fmt.Println(pubcomp)
	p := make([]byte, 100)
	n, err := pubcomp.Encode(p)
	require.NoError(t, err)
	fmt.Println(p[:n])
	require.Equal(t, b, p[:n])
}
func TestPubcompMessageFields(t *testing.T) {
	msg := NewPubcompMessage()

	msg.SetPacketId(100)

	require.Equal(t, 100, int(msg.PacketId()))
}

func TestPubcompMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(PUBCOMP << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewPubcompMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, PUBCOMP, msg.Type(), "Error decoding message.")
	require.Equal(t, 7, int(msg.PacketId()), "Error decoding message.")
}

// test insufficient bytes
func TestPubcompMessageDecode2(t *testing.T) {
	msgBytes := []byte{
		byte(PUBCOMP << 4),
		2,
		7, // packet ID LSB (7)
	}

	msg := NewPubcompMessage()
	_, err := msg.Decode(msgBytes)

	require.Error(t, err)
}

func TestPubcompMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(PUBCOMP << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewPubcompMessage()
	msg.SetPacketId(7)

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestPubcompDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(PUBCOMP << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewPubcompMessage()
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
