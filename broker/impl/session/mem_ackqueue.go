package sess

import (
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/lybxkl/gmqtt/broker/core/message"
	sess "github.com/lybxkl/gmqtt/broker/core/session"
)

var (
	errQueueFull   error = errors.New("queue full")
	errQueueEmpty  error = errors.New("queue empty")
	errWaitMessage error = errors.New("Invalid messagev5 to wait for ack")
	errAckMessage  error = errors.New("Invalid messagev5 for acking")
)

//Ackqueue是一个正在增长的队列，它是在一个环形缓冲区的基础上实现的。
//作为缓冲 如果满了，它会自动增长。
//
// Ackqueue用于存储正在等待ack返回的消息。
// 几个需要ack的场景。
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
func (aq *ackqueue) Size() int64 {
	return aq.size
}

// Wait 将消息复制到一个等待队列中，并等待相应的消息
// ack消息被接收。
func (aq *ackqueue) Wait(msg message.Message, onComplete interface{}) error {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	switch msg := msg.(type) {
	case *message.PublishMessage:
		if msg.QoS() == message.QosAtMostOnce {
			return errWaitMessage
		}

		aq.insert(msg.PacketId(), msg, onComplete)

	case *message.SubscribeMessage:
		aq.insert(msg.PacketId(), msg, onComplete)

	case *message.UnsubscribeMessage:
		aq.insert(msg.PacketId(), msg, onComplete)

	case *message.PingreqMessage:
		aq.ping = sess.Ackmsg{
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
func (aq *ackqueue) Ack(msg message.Message) error {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	switch msg.Type() {
	case message.PUBACK, message.PUBREC, message.PUBREL, message.PUBCOMP, message.SUBACK, message.UNSUBACK:
		// Check to see if the messagev5 w/ the same packet ID is in the queue
		//查看是否有相同的数据包ID在队列中
		i, ok := aq.emap[msg.PacketId()]
		if ok {
			//如果消息w/报文ID存在，更新消息状态并复制
			// ack消息
			aq.ring[i].State = msg.Type()

			ml := msg.Len()
			aq.ring[i].Ackbuf = make([]byte, ml)

			_, err := msg.Encode(aq.ring[i].Ackbuf)
			if err != nil {
				return err
			}
		}

	case message.PINGRESP:
		if aq.ping.Mtype == message.PINGREQ {
			aq.ping.State = message.PINGRESP
		}

	default:
		return errAckMessage
	}

	return nil
}

// Acked  returns the list of messages that have completed the ack cycle.
//返回已完成ack循环的消息列表。
func (aq *ackqueue) Acked() []sess.Ackmsg {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	aq.ackdone = aq.ackdone[0:0]

	if aq.ping.State == message.PINGRESP {
		aq.ackdone = append(aq.ackdone, aq.ping)
		aq.ping = sess.Ackmsg{}
	}

FORNOTEMPTY:
	for !aq.empty() {
		switch aq.ring[aq.head].State {
		case message.PUBACK, message.PUBREL, message.PUBCOMP, message.SUBACK, message.UNSUBACK:
			aq.ackdone = append(aq.ackdone, aq.ring[aq.head])
			aq.removeHead()
		// TODO 之所以没有 messagev5.PUBREC，是因为在收到PUBCOMP后依旧会替换掉this.ring中那个位置的PUBCRC，到头来最终是执行的PUBCOMP
		default:
			break FORNOTEMPTY
		}
	}

	return aq.ackdone
}

func (aq *ackqueue) insert(pktid uint16, msg message.Message, onComplete interface{}) error {
	if aq.full() {
		aq.grow()
	}

	if _, ok := aq.emap[pktid]; !ok {
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

		aq.ring[aq.tail] = am
		aq.emap[pktid] = aq.tail
		aq.tail = aq.increment(aq.tail)
		aq.count++
	} else {
		// If packet w/ pktid already exist, then aq must be a PUBLISH messagev5
		// Other messagev5 types should never send with the same packet ID
		pm, ok := msg.(*message.PublishMessage)
		if !ok {
			return fmt.Errorf("ack/insert: duplicate packet ID for %s messagev5", msg.Name())
		}

		// If aq is a publish messagev5, then the DUP flag must be set. aq is the
		// only scenario in which we will receive duplicate messages.
		// 重复交付，将dup位置1
		if pm.Dup() {
			return fmt.Errorf("ack/insert: duplicate packet ID for PUBLISH messagev5, but DUP flag is not set")
		}

		// Since it's a dup, there's really nothing we need to do. Moving on...
	}

	return nil
}

func (aq *ackqueue) removeHead() error {
	if aq.empty() {
		return errQueueEmpty
	}

	it := aq.ring[aq.head]
	// set aq to empty sess.Ackmsg{} to ensure GC will collect the buffer
	//将此设置为empty sess.Ackmsg{}，以确保GC将收集缓冲区
	aq.ring[aq.head] = sess.Ackmsg{}
	aq.head = aq.increment(aq.head)
	aq.count--
	delete(aq.emap, it.Pktid)

	return nil
}

func (aq *ackqueue) grow() {
	if math.MaxInt64/2 < aq.size {
		panic("new size will overflow int64")
	}

	newsize := aq.size << 1
	newmask := newsize - 1
	newring := make([]sess.Ackmsg, newsize)

	if aq.tail > aq.head {
		copy(newring, aq.ring[aq.head:aq.tail])
	} else {
		copy(newring, aq.ring[aq.head:])
		copy(newring[aq.size-aq.head:], aq.ring[:aq.tail])
	}

	aq.size = newsize
	aq.mask = newmask
	aq.ring = newring
	aq.head = 0
	aq.tail = aq.count

	aq.emap = make(map[uint16]int64, aq.size)

	for i := int64(0); i < aq.tail; i++ {
		aq.emap[aq.ring[i].Pktid] = i
	}
}
func (aq *ackqueue) Len() int {
	return aq.len()
}
func (aq *ackqueue) len() int {
	return int(aq.count)
}

func (aq *ackqueue) cap() int {
	return int(aq.size)
}

func (aq *ackqueue) index(n int64) int64 {
	return n & aq.mask
}

func (aq *ackqueue) full() bool {
	return aq.count == aq.size
}

func (aq *ackqueue) empty() bool {
	return aq.count == 0
}

func (aq *ackqueue) increment(n int64) int64 {
	return aq.index(n + 1)
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
