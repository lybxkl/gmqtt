package util

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestObjectId(t *testing.T) {
	fmt.Println(Generate())
	fmt.Println(time.Now().UnixNano())
	fmt.Println(time.Now().Unix())
	b1 := []byte("asd,asd,as")
	s := hex.EncodeToString(b1)
	fmt.Println(s)
	b, _ := hex.DecodeString(s)
	fmt.Println(b, string(b))
	fmt.Println(hex.EncodeToString(b))
}

func TestFmt(t *testing.T) {
	pkId := []int64{1, 1, 2, 3}
	clientId := "123"
	var ins strings.Builder
	ins.WriteString("client_id = ")
	ins.WriteString(clientId)
	if len(pkId) > 0 {
		ins.WriteString(" and pk_id in (")
	}
	pks := make([]interface{}, len(pkId))
	for i := 0; i < len(pkId); i++ {
		if i == len(pkId)-1 {
			ins.WriteString("%v)")
		} else {
			ins.WriteString("%v,")
		}
		pks[i] = pkId[i]
	}
	f := fmt.Sprintf(ins.String(), pks...)
	fmt.Println(f)
}
