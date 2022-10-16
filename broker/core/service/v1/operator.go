package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/lybxkl/gmqtt/broker/core/message"
	"github.com/lybxkl/gmqtt/broker/core/module/statistic"
	"github.com/lybxkl/gmqtt/broker/gcfg"
	. "github.com/lybxkl/gmqtt/common/log"
	timeoutio "github.com/lybxkl/gmqtt/util/timeout_io"
)

type GCore struct {
	net.Conn

	// connMsg 连接消息
	connMsg *message.ConnectMessage

	//输入数据缓冲区。从连接中读取字节并放在这里。
	in *buffer
	// writeMessage互斥锁——序列化输出缓冲区的写操作。
	wmu sync.Mutex
	//输出数据缓冲区。这里写入的字节依次写入连接。
	out *buffer

	inTmp  []byte
	outTmp []byte

	inStat  statistic.Stat // 输入的记录
	outStat statistic.Stat // 输出的记录

	//等待各种goroutines完成启动和停止
	wgStarted sync.WaitGroup
	wgStopped sync.WaitGroup

	stop chan error
	done int
	err  error
}

// NewGCore 创建核心
func NewGCore(conn net.Conn, conMsg *message.ConnectMessage) *GCore {
	in, _ := newBuffer(defaultBufferSize)
	out, _ := newBuffer(defaultBufferSize)
	gCore := &GCore{
		Conn:      conn,
		connMsg:   conMsg,
		in:        in,
		wmu:       sync.Mutex{},
		out:       out,
		inTmp:     make([]byte, 0),
		outTmp:    make([]byte, 0),
		inStat:    statistic.Stat{},
		outStat:   statistic.Stat{},
		wgStarted: sync.WaitGroup{},
		wgStopped: sync.WaitGroup{},
		stop:      make(chan error),
		done:      0,
	}

	gCore.wgStarted.Add(2)
	gCore.wgStopped.Add(2)
	go gCore.receiver()
	go gCore.sender()

	gCore.wgStarted.Wait()
	Log.Debugf("<<(%s)>> GCore run", gCore.ClientId())
	return gCore
}

func (core *GCore) ClientId() string {
	if core.connMsg == nil {
		return ""
	}
	return string(core.connMsg.ClientId())
}

func (core *GCore) IsDone() bool {
	return core.done == 1
}

func (core *GCore) Close() error {
	if core.done == 1 {
		return nil
	}
	core.done = 1
	close(core.stop)

	if core.in != nil {
		_ = core.in.Close()
	}
	if core.out != nil {
		_ = core.out.Close()
	}

	// 等待优雅关闭处理
	core.wgStopped.Wait()

	if core.Conn != nil {
		return core.Conn.Close()
	}
	Log.Debugf("<<(%s)>> GCore closed ", core.ClientId())
	return nil
}

// receiver() reads data from the network, and writes the data into the incoming buffer
func (core *GCore) receiver() {
	defer func() {
		if r := recover(); r != nil {
			core.err = fmt.Errorf("<<(%s)>> recovering from panic: %v", core.ClientId(), r)
		}

		core.wgStopped.Done()
		Log.Debugf("<<(%s)>> stopping receiver", core.ClientId())
	}()

	Log.Debugf("<<(%s)>> starting receiver", core.ClientId())

	core.wgStarted.Done()

	switch conn := core.Conn.(type) {
	// 普通tcp连接
	case net.Conn:
		// 如果保持连接的值非零，并且服务端在1.5倍的保持连接时间内没有收到客户端的控制报文，
		// 它必须断开客户端的网络连接，并判定网络连接已断开
		// 保持连接的实际值是由应用指定的，一般是几分钟。允许的最大值是18小时12分15秒(两个字节)
		// 保持连接（Keep Alive）值为零的结果是关闭保持连接（Keep Alive）机制。
		// 如果保持连接（Keep Alive）612 值为零，客户端不必按照任何特定的时间发送MQTT控制报文。
		keepAlive := time.Second * time.Duration(gcfg.GetGCfg().Keepalive)

		r := timeoutio.NewReader(conn, keepAlive+(keepAlive/2))
		for {
			_, err := core.in.ReadFrom(r)
			// 检查done是否关闭，如果关闭，退出
			if err != nil {
				if er, ok := err.(*net.OpError); ok && er.Err.Error() == "i/o timeout" {
					core.err = fmt.Errorf("<<(%s)>> 读超时关闭：%v", core.ClientId(), er)
					return
				}
				if core.IsDone() && strings.Contains(err.Error(), "use of closed network connection") {
					return
				}
				if err != io.EOF {
					//连接异常或者断线啥的
					core.err = fmt.Errorf("<<(%s)>> 连接异常关闭：%v", core.ClientId(), err.Error())
					return
				}
				return
			}
		}
	//添加websocket，启动cl里有对websocket转tcp，这里就不用处理
	//case *websocket.Conn:
	//	Log.Errorf("(%s) Websocket: %v",core.ClientId(), ErrInvalidConnectionType)

	default:
		core.err = fmt.Errorf("<<(%s)>> 未知异常： %v", core.ClientId(), ErrInvalidConnectionType)
	}
}

// sender() writes data from the outgoing buffer to the network
func (core *GCore) sender() {
	defer func() {
		if r := recover(); r != nil {
			core.err = fmt.Errorf("<<(%s)>> recovering from panic: %v", core.ClientId(), r)
		}

		core.wgStopped.Done()
		Log.Debugf("<<(%s)>> stopping sender", core.ClientId())
	}()

	Log.Debugf("<<(%s)>> starting sender", core.ClientId())

	core.wgStarted.Done()
	switch conn := core.Conn.(type) {
	case net.Conn:
		writeTimeout := time.Second * time.Duration(gcfg.GetGCfg().WriteTimeout)
		r := timeoutio.NewWriter(conn, writeTimeout+(writeTimeout/2))
		for {
			_, err := core.out.WriteTo(r)
			if err != nil {
				if er, ok := err.(*net.OpError); ok && er.Err.Error() == "i/o timeout" {
					core.err = fmt.Errorf("<<(%s)>> 写超时关闭：%v", core.ClientId(), er)
					return
				}
				if core.IsDone() && (strings.Contains(err.Error(), "use of closed network connection")) {
					// TODO 怎么处理这些未发送的？
					// 无需保存会话的直接断开即可，需要的也无需，因为会重发
					return
				}
				if err != io.EOF {
					core.err = fmt.Errorf("<<(%s)>> error writing data: %v", core.ClientId(), err)
					return
				}
				return
			}
		}

	//case *websocket.Conn:
	//	Log.Errorf("(%s) Websocket not supported",core.ClientId())

	default:
		core.err = fmt.Errorf("<<(%s)>> Invalid connection type", core.ClientId())
	}
}

// peekMessageSize()读取，但不提交，足够的字节来确定大小
// @return: 下一条消息，并返回类型和大小。
func (core *GCore) peekMessageSize() (message.MessageType, int, error) {
	var (
		b   []byte
		err error
		cnt int = 2
	)

	if core.in == nil {
		err = ErrBufferNotReady
		return 0, 0, err
	}

	//让我们读取足够的字节来获取消息头(msg类型，剩余长度)
	for {
		//如果我们已经读取了5个字节，但是仍然没有完成，那么就有一个问题。
		if cnt > 5 {
			// 剩余长度的第4个字节设置了延续位
			return 0, 0, fmt.Errorf("sendrecv/peekMessageSize: 4th byte of remaining length has continuation bit set")
		}

		//从输入缓冲区中读取cnt字节。
		b, err = core.in.ReadWait(cnt)
		if err != nil {
			return 0, 0, err
		}

		//如果没有返回足够的字节，则继续，直到有足够的字节。
		if len(b) < cnt {
			continue
		}

		//如果获得了足够的字节，则检查最后一个字节，看看是否延续
		// 如果是，则增加cnt并继续窥视
		if b[cnt-1] >= 0x80 {
			cnt++
		} else {
			break
		}
	}

	//获取消息的剩余长度
	remlen, m := binary.Uvarint(b[1:])

	//消息的总长度是remlen + 1 (msg类型)+ m (remlen字节)
	total := int(remlen) + 1 + m

	mtype := message.MessageType(b[0] >> 4)

	return mtype, total, err
}

// peekMessage()从缓冲区读取消息，但是没有提交字节。
// 这意味着缓冲区仍然认为字节还没有被读取。
func (core *GCore) peekMessage(mtype message.MessageType, total int) (message.Message, int, error) {
	var (
		b    []byte
		err  error
		i, n int
		msg  message.Message
	)

	if core.in == nil {
		return nil, 0, ErrBufferNotReady
	}

	//Peek，直到我们得到总字节数
	for i = 0; ; i++ {
		//从输入缓冲区Peekremlen字节数。
		b, err = core.in.ReadWait(total)
		if err != nil && err != ErrBufferInsufficientData {
			return nil, 0, err
		}

		//如果没有返回足够的字节，则继续，直到有足够的字节。
		if len(b) >= total {
			break
		}
	}

	msg, err = mtype.New()
	if err != nil {
		return nil, 0, err
	}

	n, err = msg.Decode(b)
	if err != nil {
		return nil, n, err
	}
	// 提交缓冲区
	_, err = core.in.ReadCommit(n)
	return msg, n, err
}

// readMessage() reads and copies a message from the buffer. The buffer bytes are
// committed as a result of the read.
func (core *GCore) readMessage(mtype message.MessageType, total int) (message.Message, int, error) {
	var (
		b   []byte
		err error
		n   int
		msg message.Message
	)

	if core.in == nil {
		err = ErrBufferNotReady
		return nil, 0, err
	}

	if len(core.inTmp) < total {
		core.inTmp = make([]byte, total)
	}

	// Read until we get total bytes
	l := 0
	for l < total {
		n, err = core.in.Read(core.inTmp[l:])
		l += n
		Log.Debugf("read %d bytes, total %d", n, l)
		if err != nil {
			return nil, 0, err
		}
	}

	b = core.inTmp[:total]

	msg, err = mtype.New()
	if err != nil {
		return msg, 0, err
	}

	n, err = msg.Decode(b)
	return msg, n, err
}

// writeMessage()将消息写入传出缓冲区，
// 客户端限制的最大可接收包大小，由客户端执行处理，因为超过限制的报文将导致协议错误，客户端发送包含原因码0x95（报文过大）的DISCONNECT报文给broker
func (core *GCore) writeMessage(msg message.Message) (int, error) {
	// 当连接消息中请求问题信息为0，则需要去除部分数据再发送
	core.delRequestRespInfo(msg)

	var (
		l    int = msg.Len()
		m, n int
		err  error
		buf  []byte
		wrap bool
	)

	if core.out == nil {
		return 0, ErrBufferNotReady
	}

	//这是串行写入到底层缓冲区。 多了goroutine可能
	//可能是因为调用了Publish()或Subscribe()或其他方法而到达这里
	//发送消息的函数。 例如，如果接收到一条消息
	//另一个连接，消息需要被发布到这个客户端
	//调用Publish()函数，同时调用另一个客户机
	//做完全一样的事情。
	//
	//但这并不是一个理想的解决方案。
	//如果可能的话，我们应该移除互斥锁，并且是无锁的。
	//主要是因为有大量的goroutines想要发表文章
	//对于这个客户端，它们将全部阻塞。
	//但是，现在可以这样做。
	// FIXME: Try to find a better way than a mutex...if possible.
	core.wmu.Lock()
	defer core.wmu.Unlock()

	buf, wrap, err = core.out.WriteWait(l)
	if err != nil {
		return 0, err
	}

	if wrap {
		if len(core.outTmp) < l {
			core.outTmp = make([]byte, l)
		}

		n, err = msg.Encode(core.outTmp[0:])
		if err != nil {
			return 0, err
		}

		m, err = core.out.Write(core.outTmp[0:n])
		if err != nil {
			return m, err
		}
	} else {
		n, err = msg.Encode(buf[0:])
		if err != nil {
			return 0, err
		}

		m, err = core.out.WriteCommit(n)
		if err != nil {
			return 0, err
		}
	}

	core.outStat.Incr(uint64(m))

	return m, nil
}

// 当连接消息中请求问题信息为0，则需要去除部分数据再发送
// connectAck 和 disconnect中可不去除
func (core *GCore) delRequestRespInfo(msg message.Message) {
	if core.connMsg.RequestProblemInfo() == 0 {
		if cl, ok := msg.(message.CleanReqProblemInfo); ok {
			switch msg.Type() {
			case message.CONNACK, message.DISCONNECT, message.PUBLISH:
			default:
				cl.SetReasonStr(nil)
				cl.SetUserProperties(nil)
				Log.Debugf("<<(%s)>> 去除请求问题信息: %s", core.connMsg.ClientId(), msg.Type())
			}
		}
	}
}
