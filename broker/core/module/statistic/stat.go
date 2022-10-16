package statistic

import (
	"fmt"
	"sync/atomic"
)

type Stat struct {
	bytes    uint64 // bytes数量
	msgTotal uint64 // 消息数量
}

func (svc *Stat) Incr(n uint64) {
	atomic.AddUint64(&svc.bytes, n)
	atomic.AddUint64(&svc.msgTotal, 1)
}

func (svc *Stat) MsgTotal() uint64 {
	return svc.msgTotal
}

func (svc Stat) String() string {
	return fmt.Sprintf("%d bytes in %d messages.", svc.bytes, svc.msgTotal)
}
