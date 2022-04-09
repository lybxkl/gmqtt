package message

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeV5(t *testing.T) {
	conack := NewConnackMessage()
	conack.SetReasonCode(Success)
	conack.SetMaxPacketSize(300)
	conack.SetAuthData([]byte("abc"))
	conack.SetAuthMethod([]byte("auth"))
	conack.SetSessionExpiryInterval(30)
	conack.SetMaxQos(0x02)
	conack.SetReasonStr([]byte("测试消息"))
	conack.SetSessionPresent(true)
	b := make([]byte, 150)
	n, err := conack.Encode(b)
	if err != nil {
		panic(err)
	}
	fmt.Println(b[:n])
	fmt.Println(conack)
	conack2 := NewConnackMessage()
	n, err = conack2.Decode(b[:n])
	if err != nil {
		panic(err)
	}
	fmt.Println(conack2)
	conack2.header.dbuf = nil
	conack2.header.dirty = true
	fmt.Println(reflect.DeepEqual(conack, conack2)) // 最大报文长度，编码前可以为0
}

func TestConnackMessageFields(t *testing.T) {
	msg := NewConnackMessage()

	msg.SetSessionPresent(true)
	require.True(t, msg.SessionPresent(), "Error setting session present flag.")

	msg.SetSessionPresent(false)
	require.False(t, msg.SessionPresent(), "Error setting session present flag.")

	msg.SetReasonCode(Success)
	require.Equal(t, Success, msg.ReasonCode(), "Error setting return code.")
}

func TestConnackMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(CONNACK << 4),
		2,
		0, // session not present
		0, // connection accepted
	}

	msg := NewConnackMessage()

	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.False(t, msg.SessionPresent(), "Error decoding session present flag.")
	require.Equal(t, Success, msg.ReasonCode(), "Error decoding return code.")
}

// testing wrong message length
func TestConnackMessageDecode2(t *testing.T) {
	msgBytes := []byte{
		byte(CONNACK << 4),
		3,
		0, // session not present
		0, // connection accepted
	}

	msg := NewConnackMessage()

	_, err := msg.Decode(msgBytes)
	require.Error(t, err, "Error decoding message.")
}

// testing wrong message size
func TestConnackMessageDecode3(t *testing.T) {
	msgBytes := []byte{
		byte(CONNACK << 4),
		2,
		0, // session not present
	}

	msg := NewConnackMessage()

	_, err := msg.Decode(msgBytes)
	require.Error(t, err, "Error decoding message.")
}

// testing wrong reserve bits
func TestConnackMessageDecode4(t *testing.T) {
	msgBytes := []byte{
		byte(CONNACK << 4),
		2,
		64, // <- wrong size
		0,  // connection accepted
	}

	msg := NewConnackMessage()

	_, err := msg.Decode(msgBytes)
	require.Error(t, err, "Error decoding message.")
}

// testing invalid return code
func TestConnackMessageDecode5(t *testing.T) {
	msgBytes := []byte{
		byte(CONNACK << 4),
		2,
		0,
		60, // <- wrong code
	}

	msg := NewConnackMessage()

	_, err := msg.Decode(msgBytes)
	require.Error(t, err, "Error decoding message.")
}

func TestConnackMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(CONNACK << 4),
		2,
		1, // session present
		0, // connection accepted
	}

	msg := NewConnackMessage()
	msg.SetReasonCode(Success)
	msg.SetSessionPresent(true)

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error encoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error encoding connack message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestConnackDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(CONNACK << 4),
		2,
		0, // session not present
		0, // connection accepted
	}

	msg := NewConnackMessage()
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
