package pkid

import (
	"sync"
)

//PacketID is the type of packet identifier
type PacketID = uint16

//Max & min packet ID
const (
	MaxPacketID PacketID = 65535
	MinPacketID PacketID = 1
)

// Limiter 限流器，作流控使用
type Limiter interface {
	PollPacketID() PacketID
	PollPacketIDs(max uint16) (id []PacketID)
	Release(id PacketID) // 释放

	MarkUsedLocked(id PacketID) // 标记使用，用于重连，对过程中的消息id设置
}

func NewPacketIDLimiter(limit ...uint16) Limiter {
	if len(limit) == 0 {
		return newPacketIDLimiter(MaxPacketID)
	}
	return newPacketIDLimiter(limit[0])
}

func newPacketIDLimiter(limit uint16) *packetIDLimiter {
	return &packetIDLimiter{
		cond:      sync.NewCond(&sync.Mutex{}),
		used:      0,
		limit:     limit,
		exit:      false,
		freePid:   MinPacketID,
		lockedPid: NewBitmap(limit + 1),
	}
}

// packetIDLimiter limit the generation of packet id to keep the number of inflight messages
// always less or equal than receive maximum setting of the client.
type packetIDLimiter struct {
	cond      *sync.Cond
	used      uint16 // 当前使用的数量
	limit     uint16 // 限制同时使用的
	exit      bool
	lockedPid *Bitmap  // packet id in-use
	freePid   PacketID // next available id
}

func (p *packetIDLimiter) Close() error {
	p.cond.L.Lock()
	p.exit = true
	p.cond.L.Unlock()
	p.cond.Signal()
	return nil
}

func (p *packetIDLimiter) PollPacketID() (id PacketID) {
	ids := p.PollPacketIDs(1)
	if len(ids) == 0 {
		return
	}
	return ids[0]
}

// PollPacketIDs returns at most max number of unused packetID and marks them as used for a client.
// If there is no available id, the call will be blocked until at least one packet id is available or the limiter has been closed.
// return 0 means the limiter is closed.
// the return number = min(max, i.used).
func (p *packetIDLimiter) PollPacketIDs(max uint16) (id []PacketID) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	for p.used >= p.limit && !p.exit {
		p.cond.Wait()
	}
	if p.exit {
		return nil
	}
	n := max
	if remain := p.limit - p.used; remain < max {
		n = remain
	}
	for j := uint16(0); j < n; j++ {
		for p.lockedPid.Get(p.freePid) == 1 {
			if p.freePid == p.limit {
				p.freePid = MinPacketID
			} else {
				p.freePid++
			}
		}
		id = append(id, p.freePid)
		p.used++
		p.lockedPid.Set(p.freePid, 1)
		if p.freePid == p.limit {
			p.freePid = MinPacketID
		} else {
			p.freePid++
		}
	}
	return id
}

// Release marks the given id list as unused
func (p *packetIDLimiter) Release(id PacketID) {
	p.cond.L.Lock()
	p.releaseLocked(id)
	p.cond.L.Unlock()
	p.cond.Signal()

}

func (p *packetIDLimiter) releaseLocked(id PacketID) {
	if p.lockedPid.Get(id) == 1 {
		p.lockedPid.Set(id, 0)
		p.used--
	}
}

func (p *packetIDLimiter) BatchRelease(id []PacketID) {
	p.cond.L.Lock()
	for _, v := range id {
		p.releaseLocked(v)
	}
	p.cond.L.Unlock()
	p.cond.Signal()

}

// MarkUsedLocked marks the given id as used.
func (p *packetIDLimiter) MarkUsedLocked(id PacketID) {
	p.used++
	p.lockedPid.Set(id, 1)
}

func (p *packetIDLimiter) lock() {
	p.cond.L.Lock()
}

func (p *packetIDLimiter) unlock() {
	p.cond.L.Unlock()
}

func (p *packetIDLimiter) unlockAndSignal() {
	p.cond.L.Unlock()
	p.cond.Signal()
}
