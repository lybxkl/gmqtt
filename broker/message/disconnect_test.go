package message

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDisconnectMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(DISCONNECT << 4),
		0,
	}

	msg := NewDisconnectMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, DISCONNECT, msg.Type(), "Error decoding message.")
}

func TestDisconnectMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(DISCONNECT << 4),
		0,
	}

	msg := NewDisconnectMessage()

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}
func TestDisconnectMessageEncode2(t *testing.T) {
	msgBytes := []byte{
		byte(DISCONNECT << 4),
		12, 0, 10, 31, 0, 3, 97, 97, 97, 38, 0, 3, 97, 115, 100,
	}

	msg := NewDisconnectMessage()
	msg.SetReasonStr([]byte("aaa"))
	msg.SetReasonCode(Success)
	msg.AddUserProperty([]byte("asd"))
	dst := make([]byte, 100)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	fmt.Println(dst[:n])
	fmt.Println(msg)
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestDisconnectDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(DISCONNECT << 4),
		0,
	}

	msg := NewDisconnectMessage()
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
