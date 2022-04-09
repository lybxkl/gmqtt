package bufpool

import (
	"bytes"
	"sync"
)

var bufPool sync.Pool

func init() {
	bufPool.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

func BufferPoolGet() *bytes.Buffer {
	return bufPool.Get().(*bytes.Buffer)
}

func BufferPoolPut(b *bytes.Buffer) {
	b.Reset()
	bufPool.Put(b)
}
