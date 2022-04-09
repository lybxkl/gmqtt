package impl

import (
	"errors"
	"fmt"
	"math"
	"sync"

	"gmqtt/broker/message"
	sess "gmqtt/broker/session"
)

var (
	errQueueFull   error = errors.New("queue full")
	errQueueEmpty  error = errors.New("queue empty")
	errWaitMessage error = errors.New("Invalid messagev5 to wait for ack")
	errAckMessage  error = errors.New("Invalid messagev5 for acking")
)

// ackqueue is a growing queue implemented based on a ring buffer. As the buffer
// gets full, it will auto-grow.
//
// ackqueue is used to store messages that are waiting for acks to come back. There
// are a few scenarios in which acks are required.
//   1. Client sends SUBSCRIBE messagev5 to server, waits for SUBACK.
//   2. Client sends UNSUBSCRIBE messagev5 to server, waits for UNSUBACK.
//   3. Client sends PUBLISH QoS 1 messagev5 to server, waits for PUBACK.
//   4. Server sends PUBLISH QoS 1 messagev5 to client, waits for PUBACK.
//   5. Client sends PUBLISH QoS 2 messagev5 to server, waits for PUBREC.
//   6. Server sends PUBREC messagev5 to client, waits for PUBREL.
//   7. Client sends PUBREL messagev5 to server, waits for PUBCOMP.
//   8. Server sends PUBLISH QoS 2 messagev5 to client, waits for PUBREC.
//   9. Client sends PUBREC messagev5 to server, waits for PUBREL.
//   10. Server sends PUBREL messagev5 to client, waits for PUBCOMP.
//   11. Client sends PINGREQ messagev5 to server, waits for PINGRESP.
//Ackqueue是一个正在增长的队列，它是在一个环形缓冲区的基础上实现的。
//作为缓冲
//如果满了，它会自动增长。
//
// Ackqueue用于存储正在等待ack返回的消息。
//在那里
//是几个需要ack的场景。
// 1。 客户端发送订阅消息到服务器，等待SUBACK。
// 2。 客户端发送取消订阅消息到服务器，等待UNSUBACK。
// 3。 客户端向服务器发送PUBLISH QoS 1消息，等待PUBACK。
// 4。 服务器向客户端发送PUBLISH QoS 1消息，等待PUBACK。
// 5。 客户端向服务器发送PUBLISH QoS 2消息，等待PUBREC。
// 6。 服务器向客户端发送PUBREC消息，等待PUBREL。
// 7。 客户端发送PUBREL消息到服务器，等待PUBCOMP。
// 8。 服务器向客户端发送PUBLISH QoS 2消息，等待PUBREC。
// 9。 客户端发送PUBREC消息到服务器，等待PUBREL。
// 10。 服务器向客户端发送PUBREL消息，等待PUBCOMP。
// 11。 客户端发送PINGREQ消息到服务器，等待PINGRESP。
type ackqueue struct {
	size  int64
	mask  int64
	count int64
	head  int64
	tail  int64

	ping sess.Ackmsg
	ring []sess.Ackmsg
	// 查看是否有相同的数据包ID在队列中
	emap map[uint16]int64

	ackdone []sess.Ackmsg

	mu sync.Mutex
}

func newMemAckQueue(n int) *ackqueue {
	m := int64(n)
	if !powerOfTwo64(m) {
		m = roundUpPowerOfTwo64(m)
	}

	return &ackqueue{
		size:    m,
		mask:    m - 1,
		count:   0,
		head:    0,
		tail:    0,
		ring:    make([]sess.Ackmsg, m),
		emap:    make(map[uint16]int64, m),
		ackdone: make([]sess.Ackmsg, 0),
	}
}
func newAckqueue(n int) sess.Ackqueue {
	m := int64(n)
	if !powerOfTwo64(m) {
		m = roundUpPowerOfTwo64(m)
	}

	return &ackqueue{
		size:    m,
		mask:    m - 1,
		count:   0,
		head:    0,
		tail:    0,
		ring:    make([]sess.Ackmsg, m),
		emap:    make(map[uint16]int64, m),
		ackdone: make([]sess.Ackmsg, 0),
	}
}
func (d *ackqueue) Size() int64 {
	return d.size
}

// Wait 将消息复制到一个等待队列中，并等待相应的消息
// ack消息被接收。
func (this *ackqueue) Wait(msg message.Message, onComplete interface{}) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	switch msg := msg.(type) {
	case *message.PublishMessage:
		if msg.QoS() == message.QosAtMostOnce {
			return errWaitMessage
		}

		this.insert(msg.PacketId(), msg, onComplete)

	case *message.SubscribeMessage:
		this.insert(msg.PacketId(), msg, onComplete)

	case *message.UnsubscribeMessage:
		this.insert(msg.PacketId(), msg, onComplete)

	case *message.PingreqMessage:
		this.ping = sess.Ackmsg{
			Mtype:      message.PINGREQ,
			State:      message.RESERVED,
			OnComplete: onComplete,
		}

	default:
		return errWaitMessage
	}

	return nil
}

// Ack 获取提供的Ack消息并更新消息等待的状态。
func (this *ackqueue) Ack(msg message.Message) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	switch msg.Type() {
	case message.PUBACK, message.PUBREC, message.PUBREL, message.PUBCOMP, message.SUBACK, message.UNSUBACK:
		// Check to see if the messagev5 w/ the same packet ID is in the queue
		//查看是否有相同的数据包ID在队列中
		i, ok := this.emap[msg.PacketId()]
		if ok {
			//如果消息w/报文ID存在，更新消息状态并复制
			// ack消息
			this.ring[i].State = msg.Type()

			ml := msg.Len()
			this.ring[i].Ackbuf = make([]byte, ml)

			_, err := msg.Encode(this.ring[i].Ackbuf)
			if err != nil {
				return err
			}
		}

	case message.PINGRESP:
		if this.ping.Mtype == message.PINGREQ {
			this.ping.State = message.PINGRESP
		}

	default:
		return errAckMessage
	}

	return nil
}

// Acked  returns the list of messages that have completed the ack cycle.
//返回已完成ack循环的消息列表。
func (this *ackqueue) Acked() []sess.Ackmsg {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.ackdone = this.ackdone[0:0]

	if this.ping.State == message.PINGRESP {
		this.ackdone = append(this.ackdone, this.ping)
		this.ping = sess.Ackmsg{}
	}

FORNOTEMPTY:
	for !this.empty() {
		switch this.ring[this.head].State {
		case message.PUBACK, message.PUBREL, message.PUBCOMP, message.SUBACK, message.UNSUBACK:
			this.ackdone = append(this.ackdone, this.ring[this.head])
			this.removeHead()
		// TODO 之所以没有 messagev5.PUBREC，是因为在收到PUBCOMP后依旧会替换掉this.ring中那个位置的PUBCRC，到头来最终是执行的PUBCOMP
		default:
			break FORNOTEMPTY
		}
	}

	return this.ackdone
}

func (this *ackqueue) insert(pktid uint16, msg message.Message, onComplete interface{}) error {
	if this.full() {
		this.grow()
	}

	if _, ok := this.emap[pktid]; !ok {
		// messagev5 length
		ml := msg.Len()

		// sess.Ackmsg
		am := sess.Ackmsg{
			Mtype:      msg.Type(),
			State:      message.RESERVED,
			Pktid:      msg.PacketId(),
			Msgbuf:     make([]byte, ml),
			OnComplete: onComplete,
		}

		if _, err := msg.Encode(am.Msgbuf); err != nil {
			return err
		}

		this.ring[this.tail] = am
		this.emap[pktid] = this.tail
		this.tail = this.increment(this.tail)
		this.count++
	} else {
		// If packet w/ pktid already exist, then this must be a PUBLISH messagev5
		// Other messagev5 types should never send with the same packet ID
		pm, ok := msg.(*message.PublishMessage)
		if !ok {
			return fmt.Errorf("ack/insert: duplicate packet ID for %s messagev5", msg.Name())
		}

		// If this is a publish messagev5, then the DUP flag must be set. This is the
		// only scenario in which we will receive duplicate messages.
		// 重复交付，将dup位置1
		if pm.Dup() {
			return fmt.Errorf("ack/insert: duplicate packet ID for PUBLISH messagev5, but DUP flag is not set")
		}

		// Since it's a dup, there's really nothing we need to do. Moving on...
	}

	return nil
}

func (this *ackqueue) removeHead() error {
	if this.empty() {
		return errQueueEmpty
	}

	it := this.ring[this.head]
	// set this to empty sess.Ackmsg{} to ensure GC will collect the buffer
	//将此设置为empty sess.Ackmsg{}，以确保GC将收集缓冲区
	this.ring[this.head] = sess.Ackmsg{}
	this.head = this.increment(this.head)
	this.count--
	delete(this.emap, it.Pktid)

	return nil
}

func (this *ackqueue) grow() {
	if math.MaxInt64/2 < this.size {
		panic("new size will overflow int64")
	}

	newsize := this.size << 1
	newmask := newsize - 1
	newring := make([]sess.Ackmsg, newsize)

	if this.tail > this.head {
		copy(newring, this.ring[this.head:this.tail])
	} else {
		copy(newring, this.ring[this.head:])
		copy(newring[this.size-this.head:], this.ring[:this.tail])
	}

	this.size = newsize
	this.mask = newmask
	this.ring = newring
	this.head = 0
	this.tail = this.count

	this.emap = make(map[uint16]int64, this.size)

	for i := int64(0); i < this.tail; i++ {
		this.emap[this.ring[i].Pktid] = i
	}
}
func (this *ackqueue) Len() int {
	return this.len()
}
func (this *ackqueue) len() int {
	return int(this.count)
}

func (this *ackqueue) cap() int {
	return int(this.size)
}

func (this *ackqueue) index(n int64) int64 {
	return n & this.mask
}

func (this *ackqueue) full() bool {
	return this.count == this.size
}

func (this *ackqueue) empty() bool {
	return this.count == 0
}

func (this *ackqueue) increment(n int64) int64 {
	return this.index(n + 1)
}

// 验证是否是2的幂
func powerOfTwo64(n int64) bool {
	return n != 0 && (n&(n-1)) == 0
}

// 找到比n大的最小二的次幂
func roundUpPowerOfTwo64(n int64) int64 {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++

	return n
}
