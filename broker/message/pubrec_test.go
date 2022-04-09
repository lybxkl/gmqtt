package message

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPubRecDecodeEncode(t *testing.T) {
	pubrec := NewPubrecMessage()
	pubrec.SetPacketId(123)
	pubrec.SetReasonCode(Success)
	pubrec.SetReasonStr([]byte("aaa"))
	pubrec.AddUserPropertys([][]byte{[]byte("aaa:oo"), []byte("bbb:pp")})
	//pubrec.build()
	b := make([]byte, 100)
	n, err := pubrec.Encode(b)
	if err != nil {
		panic(err)
	}
	fmt.Println(pubrec)
	fmt.Println(b[:n])
	pubrec1 := NewPubrecMessage()
	pubrec1.Decode(b[:n])
	pubrec1.dirty = true
	pubrec1.dbuf = nil
	fmt.Println(pubrec1)
	fmt.Println(reflect.DeepEqual(pubrec1, pubrec))
}
func TestDecodePubRec(t *testing.T) {
	var b = []byte{80, 2, 0, 123}
	pubrec := NewPubrecMessage()
	pubrec.Decode(b)
	fmt.Println(pubrec)
	p := make([]byte, 100)
	n, err := pubrec.Encode(p)
	require.NoError(t, err)
	fmt.Println(p[:n])
	require.Equal(t, b, p[:n])
}
func TestPubrecMessageFields(t *testing.T) {
	msg := NewPubrecMessage()

	msg.SetPacketId(100)

	require.Equal(t, 100, int(msg.PacketId()))
}

func TestPubrecMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(PUBREC << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewPubrecMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, PUBREC, msg.Type(), "Error decoding message.")
	require.Equal(t, 7, int(msg.PacketId()), "Error decoding message.")
}

// test insufficient bytes
func TestPubrecMessageDecode2(t *testing.T) {
	msgBytes := []byte{
		byte(PUBREC << 4),
		2,
		7, // packet ID LSB (7)
	}

	msg := NewPubrecMessage()
	_, err := msg.Decode(msgBytes)

	require.Error(t, err)
}

func TestPubrecMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(PUBREC << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewPubrecMessage()
	msg.SetPacketId(7)

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestPubrecDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(PUBREC << 4),
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewPubrecMessage()
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
