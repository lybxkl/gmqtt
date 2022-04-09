package message

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPingreqMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(PINGREQ << 4),
		0,
	}

	msg := NewPingreqMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, PINGREQ, msg.Type(), "Error decoding message.")
}

func TestPingreqMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(PINGREQ << 4),
		0,
	}

	msg := NewPingreqMessage()

	dst := make([]byte, msg.Len())
	n, err := msg.Encode(dst)
	fmt.Println(n)
	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst, "Error decoding message.")

	msg2 := NewPingreqMessage()
	n, err = msg2.Decode(dst)
	fmt.Println(n)
	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, PINGREQ, msg.Type(), "Error decoding message.")
}

func TestPingrespMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(PINGRESP << 4),
		0,
	}

	msg := NewPingrespMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, PINGRESP, msg.Type(), "Error decoding message.")
}

func TestPingrespMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(PINGRESP << 4),
		0,
	}

	msg := NewPingrespMessage()

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestPingreqDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(PINGREQ << 4),
		0,
	}

	msg := NewPingreqMessage()
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

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestPingrespDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(PINGRESP << 4),
		0,
	}

	msg := NewPingrespMessage()
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
