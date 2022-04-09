package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"gmqtt/broker/message"
	. "gmqtt/common/log"
)

type netReader interface {
	io.Reader
	SetReadDeadline(t time.Time) error
}
type netWriter interface {
	io.Writer
	SetWriteDeadline(t time.Time) error
}
type timeoutWriter struct {
	d    time.Duration
	conn netWriter
}

func (r timeoutWriter) Write(b []byte) (int, error) {
	if err := r.conn.SetWriteDeadline(time.Now().Add(r.d)); err != nil {
		return 0, err
	}
	return r.conn.Write(b)
}

type timeoutReader struct {
	d    time.Duration
	conn netReader
}

func (r timeoutReader) Read(b []byte) (int, error) {
	if err := r.conn.SetReadDeadline(time.Now().Add(r.d)); err != nil {
		return 0, err
	}
	return r.conn.Read(b)
}

// receiver() reads data from the network, and writes the data into the incoming buffer
func (svc *service) receiver() {
	defer func() {
		// Let's recover from panic
		if r := recover(); r != nil {
			Log.Errorf("(%s) Recovering from panic: %v", svc.cid(), r)
		}

		svc.wgStopped.Done()
		if svc.sign.TooManyMessages() {
			Log.Debugf("TooManyMessages stop in buf, client : %v", svc.cid())
		}
		Log.Debugf("(%s) Stopping receiver", svc.cid())
	}()

	Log.Debugf("(%s) Starting receiver", svc.cid())

	svc.wgStarted.Done()

	switch conn := svc.conn.(type) {
	// 普通tcp连接
	case net.Conn:
		// 如果保持连接的值非零，并且服务端在1.5倍的保持连接时间内没有收到客户端的控制报文，
		// 它必须断开客户端的网络连接，并判定网络连接已断开
		// 保持连接的实际值是由应用指定的，一般是几分钟。允许的最大值是18小时12分15秒(两个字节)
		// 保持连接（Keep Alive）值为零的结果是关闭保持连接（Keep Alive）机制。
		// 如果保持连接（Keep Alive）612 值为零，客户端不必按照任何特定的时间发送MQTT控制报文。
		keepAlive := time.Second * time.Duration(svc.server.cfg.Keepalive)
		r := timeoutReader{
			d:    keepAlive + (keepAlive / 2),
			conn: conn,
		}

		for {
			_, err := svc.in.ReadFrom(r)
			// 检查done是否关闭，如果关闭，退出
			if err != nil {
				if er, ok := err.(*net.OpError); ok && er.Err.Error() == "i/o timeout" {
					// TODO 更新session状态
					Log.Warnf("<<(%s)>> 读超时关闭：%v", svc.cid(), er)
					return
				}
				if svc.isDone() && strings.Contains(err.Error(), "use of closed network connection") {
					return
				}
				if err != io.EOF {
					//连接异常或者断线啥的
					// TODO 更新session状态
					Log.Errorf("<<(%s)>> 连接异常关闭：%v", svc.cid(), err.Error())
				}
				return
			}
		}
	//添加websocket，启动cl里有对websocket转tcp，这里就不用处理
	//case *websocket.Conn:
	//	glog.Errorf("(%s) Websocket: %v", svc.cid(), ErrInvalidConnectionType)

	default:
		Log.Errorf("未知异常 (%s) %v", svc.cid(), ErrInvalidConnectionType)
	}
}

// sender() writes data from the outgoing buffer to the network
func (svc *service) sender() {
	defer func() {
		// Let's recover from panic
		if r := recover(); r != nil {
			Log.Errorf("(%s) Recovering from panic: %v", svc.cid(), r)
		}

		svc.wgStopped.Done()
		if svc.sign.TooManyMessages() && svc.out.Len() == 0 {
			Log.Debugf("BeyondQuota or TooManyMessages stop out buf, client : %v", svc.cid())
		}
		Log.Debugf("(%s) Stopping sender", svc.cid())
	}()

	Log.Debugf("(%s) Starting sender", svc.cid())

	svc.wgStarted.Done()
	switch conn := svc.conn.(type) {
	case net.Conn:
		writeTimeout := time.Second * time.Duration(svc.server.cfg.WriteTimeout)
		r := timeoutWriter{
			d:    writeTimeout + (writeTimeout / 2),
			conn: conn,
		}
		for {
			_, err := svc.out.WriteTo(r)
			if err != nil {
				if er, ok := err.(*net.OpError); ok && er.Err.Error() == "i/o timeout" {
					Log.Warnf("<<(%s)>> 写超时关闭：%v", svc.cid(), er)
					return
				}
				if svc.isDone() && (strings.Contains(err.Error(), "use of closed network connection")) {
					// TODO 怎么处理这些未发送的
					//
					return
				}
				if err != io.EOF {
					Log.Errorf("(%s) error writing data: %v", svc.cid(), err)
				}
				return
			}
		}

	//case *websocket.Conn:
	//	glog.Errorf("(%s) Websocket not supported", svc.cid())

	default:
		Log.Infof("(%s) Invalid connection type", svc.cid())
	}
}

// peekMessageSize() reads, but not commits, enough bytes to determine the size of
// the next messagev5 and returns the type and size.
// peekMessageSize()读取，但不提交，足够的字节来确定大小
//下一条消息，并返回类型和大小。
func (svc *service) peekMessageSize() (message.MessageType, int, error) {
	var (
		b   []byte
		err error
		cnt int = 2
	)

	if svc.in == nil {
		err = ErrBufferNotReady
		return 0, 0, err
	}

	// Let's read enough bytes to get the messagev5 header (msg type, remaining length)
	//让我们读取足够的字节来获取消息头(msg类型，剩余长度)
	for {
		// If we have read 5 bytes and still not done, then there's a problem.
		//如果我们已经读取了5个字节，但是仍然没有完成，那么就有一个问题。
		if cnt > 5 {
			// 剩余长度的第4个字节设置了延续位
			return 0, 0, fmt.Errorf("sendrecv/peekMessageSize: 4th byte of remaining length has continuation bit set")
		}

		// Peek cnt bytes from the input buffer.
		//从输入缓冲区中读取cnt字节。
		b, err = svc.in.ReadWait(cnt)
		if err != nil {
			return 0, 0, err
		}

		// If not enough bytes are returned, then continue until there's enough.
		//如果没有返回足够的字节，则继续，直到有足够的字节。
		if len(b) < cnt {
			continue
		}

		// If we got enough bytes, then check the last byte to see if the continuation
		// bit is set. If so, increment cnt and continue peeking
		//如果获得了足够的字节，则检查最后一个字节，看看是否延续
		// 如果是，则增加cnt并继续窥视
		if b[cnt-1] >= 0x80 {
			cnt++
		} else {
			break
		}
	}

	// Get the remaining length of the messagev5
	//获取消息的剩余长度
	remlen, m := binary.Uvarint(b[1:])

	// Total messagev5 length is remlen + 1 (msg type) + m (remlen bytes)
	//消息的总长度是remlen + 1 (msg类型)+ m (remlen字节)
	total := int(remlen) + 1 + m

	mtype := message.MessageType(b[0] >> 4)

	return mtype, total, err
}

// peekMessage() reads a messagev5 from the buffer, but the bytes are NOT committed.
// This means the buffer still thinks the bytes are not read yet.
// peekMessage()从缓冲区读取消息，但是没有提交字节。
//这意味着缓冲区仍然认为字节还没有被读取。
func (svc *service) peekMessage(mtype message.MessageType, total int) (message.Message, int, error) {
	var (
		b    []byte
		err  error
		i, n int
		msg  message.Message
	)

	if svc.in == nil {
		return nil, 0, ErrBufferNotReady
	}

	// Peek until we get total bytes
	//Peek，直到我们得到总字节数
	for i = 0; ; i++ {
		// Peek remlen bytes from the input buffer.
		//从输入缓冲区Peekremlen字节数。
		b, err = svc.in.ReadWait(total)
		if err != nil && err != ErrBufferInsufficientData {
			return nil, 0, err
		}

		// If not enough bytes are returned, then continue until there's enough.
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
	return msg, n, err
}

// readMessage() reads and copies a messagev5 from the buffer. The buffer bytes are
// committed as a result of the read.
func (svc *service) readMessage(mtype message.MessageType, total int) (message.Message, int, error) {
	var (
		b   []byte
		err error
		n   int
		msg message.Message
	)

	if svc.in == nil {
		err = ErrBufferNotReady
		return nil, 0, err
	}

	if len(svc.intmp) < total {
		svc.intmp = make([]byte, total)
	}

	// Read until we get total bytes
	l := 0
	for l < total {
		n, err = svc.in.Read(svc.intmp[l:])
		l += n
		Log.Debugf("read %d bytes, total %d", n, l)
		if err != nil {
			return nil, 0, err
		}
	}

	b = svc.intmp[:total]

	msg, err = mtype.New()
	if err != nil {
		return msg, 0, err
	}

	n, err = msg.Decode(b)
	return msg, n, err
}

//writeMessage()将消息写入传出缓冲区，
// 客户端限制的最大可接收包大小，由客户端执行处理，因为超过限制的报文将导致协议错误，客户端发送包含原因码0x95（报文过大）的DISCONNECT报文给broker
func (svc *service) writeMessage(msg message.Message) (int, error) {
	// 当连接消息中请求问题信息为0，则需要去除部分数据再发送
	svc.delRequestRespInfo(msg)

	var (
		l    int = msg.Len()
		m, n int
		err  error
		buf  []byte
		wrap bool
	)

	if svc.out == nil {
		return 0, ErrBufferNotReady
	}

	// This is to serialize writes to the underlying buffer. Multiple goroutines could
	// potentially get here because of calling Publish() or Subscribe() or other
	// functions that will send messages. For example, if a messagev5 is received in
	// another connetion, and the messagev5 needs to be published to svc client, then
	// the Publish() function is called, and at the same time, another client could
	// do exactly the same thing.
	//
	// Not an ideal fix though. If possible we should remove mutex and be lockfree.
	// Mainly because when there's a large number of goroutines that want to publish
	// to svc client, then they will all block. However, svc will do for now.
	//
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
	svc.wmu.Lock()
	defer svc.wmu.Unlock()

	buf, wrap, err = svc.out.WriteWait(l)
	if err != nil {
		return 0, err
	}

	if wrap {
		if len(svc.outtmp) < l {
			svc.outtmp = make([]byte, l)
		}

		n, err = msg.Encode(svc.outtmp[0:])
		if err != nil {
			return 0, err
		}

		m, err = svc.out.Write(svc.outtmp[0:n])
		if err != nil {
			return m, err
		}
	} else {
		n, err = msg.Encode(buf[0:])
		if err != nil {
			return 0, err
		}

		m, err = svc.out.WriteCommit(n)
		if err != nil {
			return 0, err
		}
	}

	svc.outStat.increment(int64(m))

	return m, nil
}
