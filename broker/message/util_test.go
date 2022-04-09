package message

import (
	"fmt"
	"testing"
)

func TestCopyL(t *testing.T) {
	b := []byte{0, 1, 2, 3, 4, 5}
	fmt.Println(CopyLen(b[1:2], 1))
}
