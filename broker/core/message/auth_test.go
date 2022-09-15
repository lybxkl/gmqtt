package message

import (
	"fmt"
	"reflect"
	"testing"
)

func TestDecodeAuth(t *testing.T) {
	auth := NewAuthMessage()
	auth.SetReasonCode(Success)
	auth.SetAuthData([]byte("abc"))
	auth.SetAuthMethod([]byte("auth"))
	auth.SetReasonStr([]byte("测试消息"))
	auth.AddUserPropertys([][]byte{[]byte("asd"), []byte("dd:aa")})
	b := make([]byte, 150)
	n, err := auth.Encode(b)
	if err != nil {
		panic(err)
	}
	fmt.Println(b[:n])
	fmt.Println(auth)
	auth2 := NewAuthMessage()
	n, err = auth2.Decode(b[:n])
	if err != nil {
		panic(err)
	}
	fmt.Println(auth2)
	auth2.header.dbuf = nil
	auth2.header.dirty = true
	fmt.Println(reflect.DeepEqual(auth, auth2)) // 最大报文长度，编码前可以为0
}
func TestDecodeAuthZero(t *testing.T) {
	auth := NewAuthMessage()
	auth.SetReasonCode(Success)
	b := make([]byte, 150)
	n, err := auth.Encode(b)
	if err != nil {
		panic(err)
	}
	fmt.Println(b[:n])
	fmt.Println(auth)
	auth2 := NewAuthMessage()
	n, err = auth2.Decode(b[:n])
	if err != nil {
		panic(err)
	}
	fmt.Println(auth2)
	auth2.header.dbuf = nil
	auth2.header.dirty = true
	fmt.Println(reflect.DeepEqual(auth, auth2)) // 最大报文长度，编码前可以为0
}
