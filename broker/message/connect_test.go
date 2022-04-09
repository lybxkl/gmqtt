package message

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFillDecode(t *testing.T) {
	msg := NewConnectMessage()

	msg.SetCleanSession(true)
	msg.SetWillFlag(true)
	msg.SetWillQos(0x00)
	msg.SetWillRetain(false)
	msg.SetUsernameFlag(true)
	msg.SetPasswordFlag(true)

	msg.SetVersion(0x05)
	msg.SetKeepAlive(100)
	msg.protoName = []byte("MQTT")
	msg.SetClientId([]byte("aaaaaasssss"))
	msg.SetWillTopic([]byte("willtp1"))
	msg.SetWillMessage([]byte("will ploay"))
	msg.SetUsername([]byte("name"))
	msg.SetPassword([]byte("pwd"))
	msg.SetSessionExpiryInterval(132)
	msg.SetReceiveMaximum(900)
	msg.SetMaxPacketSize(100)
	msg.SetTopicAliasMax(20)
	msg.SetRequestRespInfo(0)
	msg.SetRequestProblemInfo(1)
	msg.AddUserPropertys([][]byte{[]byte("aaaddd"), []byte("ssss")})
	msg.AddWillUserPropertys([][]byte{[]byte("cccc"), []byte("saaa")})
	msg.SetAuthMethod([]byte("abcd"))
	msg.SetAuthData([]byte("abcd"))
	msg.SetWillDelayInterval(30)
	msg.SetPayloadFormatIndicator(1)
	msg.SetWillMsgExpiryInterval(20)
	msg.SetContentType([]byte("type"))
	msg.SetResponseTopic([]byte("/a/p"))
	msg.SetCorrelationData([]byte("db"))
	//msg.build()
	b := make([]byte, 180)
	n, err := msg.Encode(b)
	if err != nil {
		panic(err)
	}
	fmt.Println(msg)
	fmt.Printf("%v", b[:n])
	msg1 := NewConnectMessage()
	msg1.Decode(b[:n])
	fmt.Println(msg1)
	fmt.Println(reflect.DeepEqual(msg1, msg1))
}

func TestDecode(t *testing.T) {
	// 此数据并非全部按协议来，比如contextType后面居然是payloadFormatIndicator
	var testData = []byte{
		0x10, 0x4f, 0x00, 0x04, 0x4d, 0x51, 0x54, 0x54, 0x05, 0xc6, 0x00, 0x3c, 0x0b, 0x11, 0x00, 0x00,
		0x00, 0x03, 0x21, 0x00, 0x03, 0x22, 0x00, 0x04, 0x00, 0x0e, 0x6d, 0x71, 0x74, 0x74, 0x78, 0x5f,
		0x66, 0x65, 0x30, 0x38, 0x61, 0x61, 0x31, 0x37, 0x13, 0x18, 0x00, 0x00, 0x00, 0x0b, 0x02, 0x00,
		0x00, 0x00, 0x16, 0x03, 0x00, 0x04, 0x6a, 0x73, 0x6f, 0x6e, 0x01, 0x01, 0x00, 0x03, 0x31, 0x32,
		0x33, 0x00, 0x04, 0x61, 0x61, 0x61, 0x61, 0x00, 0x03, 0x31, 0x32, 0x33, 0x00, 0x03, 0x31, 0x32,
		0x33,
	}
	msg := NewConnectMessage()
	msg.Decode(testData)
	fmt.Printf("%v\n", msg)
	dst := make([]byte, 81)
	msg.Encode(dst)
	fmt.Println(reflect.DeepEqual(testData, dst))
	fmt.Println(testData)
	fmt.Println(dst)

	msg2 := NewConnectMessage()
	msg2.Decode(dst)
	fmt.Printf("%v\n", msg2)
}

func TestNil(t *testing.T) {
	// fmt.Println(nil == nil) // panic : invalid operation: nil == nil (operator == not defined on nil)
}
func TestConnectMessageFields(t *testing.T) {
	msg := NewConnectMessage()

	err := msg.SetVersion(0x3)
	require.NoError(t, err, "Error setting message version.")

	require.Equal(t, 0x3, int(msg.Version()), "Incorrect version number")

	err = msg.SetVersion(0x5)
	require.Error(t, err)

	msg.SetCleanSession(true)
	require.True(t, msg.CleanSession(), "Error setting clean session flag.")

	msg.SetCleanSession(false)
	require.False(t, msg.CleanSession(), "Error setting clean session flag.")

	msg.SetWillFlag(true)
	require.True(t, msg.WillFlag(), "Error setting will flag.")

	msg.SetWillFlag(false)
	require.False(t, msg.WillFlag(), "Error setting will flag.")

	msg.SetWillRetain(true)
	require.True(t, msg.WillRetain(), "Error setting will retain.")

	msg.SetWillRetain(false)
	require.False(t, msg.WillRetain(), "Error setting will retain.")

	msg.SetPasswordFlag(true)
	require.True(t, msg.PasswordFlag(), "Error setting password flag.")

	msg.SetPasswordFlag(false)
	require.False(t, msg.PasswordFlag(), "Error setting password flag.")

	msg.SetUsernameFlag(true)
	require.True(t, msg.UsernameFlag(), "Error setting username flag.")

	msg.SetUsernameFlag(false)
	require.False(t, msg.UsernameFlag(), "Error setting username flag.")

	msg.SetWillQos(1)
	require.Equal(t, 1, int(msg.WillQos()), "Error setting will QoS.")

	err = msg.SetWillQos(4)
	require.Error(t, err)

	err = msg.SetClientId([]byte("j0j0jfajf02j0asdjf"))
	require.NoError(t, err, "Error setting client ID")

	require.Equal(t, "j0j0jfajf02j0asdjf", string(msg.ClientId()), "Error setting client ID.")

	err = msg.SetClientId([]byte("this is good for v3"))
	require.NoError(t, err)

	msg.SetVersion(0x4)

	err = msg.SetClientId([]byte("this is no good for v4!"))
	require.Error(t, err)

	msg.SetVersion(0x3)

	msg.SetWillTopic([]byte("willtopic"))
	require.Equal(t, "willtopic", string(msg.WillTopic()), "Error setting will topic.")

	require.True(t, msg.WillFlag(), "Error setting will flag.")

	msg.SetWillTopic([]byte(""))
	require.Equal(t, "", string(msg.WillTopic()), "Error setting will topic.")

	require.False(t, msg.WillFlag(), "Error setting will flag.")

	msg.SetWillMessage([]byte("this is a will message"))
	require.Equal(t, "this is a will message", string(msg.WillMessage()), "Error setting will message.")

	require.True(t, msg.WillFlag(), "Error setting will flag.")

	msg.SetWillMessage([]byte(""))
	require.Equal(t, "", string(msg.WillMessage()), "Error setting will topic.")

	require.False(t, msg.WillFlag(), "Error setting will flag.")

	msg.SetWillTopic([]byte("willtopic"))
	msg.SetWillMessage([]byte("this is a will message"))
	msg.SetWillTopic([]byte(""))
	require.True(t, msg.WillFlag(), "Error setting will topic.")

	msg.SetUsername([]byte("myname"))
	require.Equal(t, "myname", string(msg.Username()), "Error setting will message.")

	require.True(t, msg.UsernameFlag(), "Error setting will flag.")

	msg.SetUsername([]byte(""))
	require.Equal(t, "", string(msg.Username()), "Error setting will message.")

	require.False(t, msg.UsernameFlag(), "Error setting will flag.")

	msg.SetPassword([]byte("myname"))
	require.Equal(t, "myname", string(msg.Password()), "Error setting will message.")

	require.True(t, msg.PasswordFlag(), "Error setting will flag.")

	msg.SetPassword([]byte(""))
	require.Equal(t, "", string(msg.Password()), "Error setting will message.")

	require.False(t, msg.PasswordFlag(), "Error setting will flag.")
}

func TestConnectMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(CONNECT << 4),
		60,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		7,   // Client ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
	}

	msg := NewConnectMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, 206, int(msg.connectFlags), "Incorrect flag value.")
	require.Equal(t, 10, int(msg.KeepAlive()), "Incorrect KeepAlive value.")
	require.Equal(t, "surgemq", string(msg.ClientId()), "Incorrect client ID value.")
	require.Equal(t, "will", string(msg.WillTopic()), "Incorrect will topic value.")
	require.Equal(t, "send me home", string(msg.WillMessage()), "Incorrect will message value.")
	require.Equal(t, "surgemq", string(msg.Username()), "Incorrect username value.")
	require.Equal(t, "verysecret", string(msg.Password()), "Incorrect password value.")
}

func TestConnectMessageDecode2(t *testing.T) {
	// missing last byte 't'
	msgBytes := []byte{
		byte(CONNECT << 4),
		60,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		7,   // Client ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e',
	}

	msg := NewConnectMessage()
	_, err := msg.Decode(msgBytes)

	require.Error(t, err)
}

func TestConnectMessageDecode3(t *testing.T) {
	// extra bytes
	msgBytes := []byte{
		byte(CONNECT << 4),
		60,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		7,   // Client ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
		'e', 'x', 't', 'r', 'a',
	}

	msg := NewConnectMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err)
	require.Equal(t, 62, n)
}

func TestConnectMessageDecode4(t *testing.T) {
	// missing client Id, clean session == 0
	msgBytes := []byte{
		byte(CONNECT << 4),
		53,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		204, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		0,   // Client ID LSB (0)
		0,   // Will Topic MSB (0)
		4,   // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
	}

	msg := NewConnectMessage()
	_, err := msg.Decode(msgBytes)

	require.Error(t, err)
}

func TestConnectMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(CONNECT << 4),
		60,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		7,   // Client ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
	}

	msg := NewConnectMessage()
	msg.SetWillQos(1)
	msg.SetVersion(4)
	msg.SetCleanSession(true)
	msg.SetClientId([]byte("surgemq"))
	msg.SetKeepAlive(10)
	msg.SetWillTopic([]byte("will"))
	msg.SetWillMessage([]byte("send me home"))
	msg.SetUsername([]byte("surgemq"))
	msg.SetPassword([]byte("verysecret"))

	dst := make([]byte, 100)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestConnectDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(CONNECT << 4),
		60,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		7,   // Client ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
	}

	msg := NewConnectMessage()
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
