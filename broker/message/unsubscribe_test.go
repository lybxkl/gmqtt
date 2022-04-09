package message

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeUnSub(t *testing.T) {
	msg := NewUnsubscribeMessage()

	msg.SetPacketId(100)
	require.Equal(t, 100, int(msg.PacketId()), "Error setting packet ID.")

	msg.AddUserPropertys([][]byte{[]byte("aaa"), []byte("asd:asd")})
	msg.AddTopic([]byte("aaa"))
	msg.AddTopic([]byte("bb/bb"))

	b := make([]byte, 100)
	n, err := msg.Encode(b)
	fmt.Println(msg)
	require.NoError(t, err)
	fmt.Println(b[:n])
	msg1 := NewUnsubscribeMessage()
	_, err = msg1.Decode(b[:n])
	require.NoError(t, err)
	msg1.dirty = true
	msg1.dbuf = nil
	fmt.Println(msg1)
	require.Equal(t, true, reflect.DeepEqual(msg, msg1))
}
func TestUnsubscribeMessageFields(t *testing.T) {
	msg := NewUnsubscribeMessage()

	msg.SetPacketId(100)
	require.Equal(t, 100, int(msg.PacketId()), "Error setting packet ID.")

	msg.AddTopic([]byte("/a/b/#/c"))
	require.Equal(t, 1, len(msg.Topics()), "Error adding topic.")

	msg.AddTopic([]byte("/a/b/#/c"))
	require.Equal(t, 1, len(msg.Topics()), "Error adding duplicate topic.")

	msg.RemoveTopic([]byte("/a/b/#/c"))
	require.False(t, msg.TopicExists([]byte("/a/b/#/c")), "Topic should not exist.")

	require.False(t, msg.TopicExists([]byte("a/b")), "Topic should not exist.")

	msg.RemoveTopic([]byte("/a/b/#/c"))
	require.False(t, msg.TopicExists([]byte("/a/b/#/c")), "Topic should not exist.")
}

func TestUnsubscribeMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(UNSUBSCRIBE<<4) | 2,
		33,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // topic name MSB (0)
		8, // topic name LSB (8)
		'/', 'a', '/', 'b', '/', '#', '/', 'c',
		0,  // topic name MSB (0)
		10, // topic name LSB (10)
		'/', 'a', '/', 'b', '/', '#', '/', 'c', 'd', 'd',
	}

	msg := NewUnsubscribeMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, UNSUBSCRIBE, msg.Type(), "Error decoding message.")
	require.Equal(t, 3, len(msg.Topics()), "Error decoding topics.")
	require.True(t, msg.TopicExists([]byte("surgemq")), "Topic 'surgemq' should exist.")
	require.True(t, msg.TopicExists([]byte("/a/b/#/c")), "Topic '/a/b/#/c' should exist.")
	require.True(t, msg.TopicExists([]byte("/a/b/#/cdd")), "Topic '/a/b/#/c' should exist.")
}

// test empty topic list
func TestUnsubscribeMessageDecode2(t *testing.T) {
	msgBytes := []byte{
		byte(UNSUBSCRIBE<<4) | 2,
		2,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
	}

	msg := NewUnsubscribeMessage()
	_, err := msg.Decode(msgBytes)

	require.Error(t, err)
}

func TestUnsubscribeMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(UNSUBSCRIBE<<4) | 2,
		33,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // topic name MSB (0)
		8, // topic name LSB (8)
		'/', 'a', '/', 'b', '/', '#', '/', 'c',
		0,  // topic name MSB (0)
		10, // topic name LSB (10)
		'/', 'a', '/', 'b', '/', '#', '/', 'c', 'd', 'd',
	}

	msg := NewUnsubscribeMessage()
	msg.SetPacketId(7)
	msg.AddTopic([]byte("surgemq"))
	msg.AddTopic([]byte("/a/b/#/c"))
	msg.AddTopic([]byte("/a/b/#/cdd"))

	dst := make([]byte, 100)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestUnsubscribeDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(UNSUBSCRIBE<<4) | 2,
		33,
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // topic name MSB (0)
		8, // topic name LSB (8)
		'/', 'a', '/', 'b', '/', '#', '/', 'c',
		0,  // topic name MSB (0)
		10, // topic name LSB (10)
		'/', 'a', '/', 'b', '/', '#', '/', 'c', 'd', 'd',
	}

	msg := NewUnsubscribeMessage()
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
