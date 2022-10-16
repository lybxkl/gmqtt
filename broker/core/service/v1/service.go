package service

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lybxkl/gmqtt/broker/core"
	"github.com/lybxkl/gmqtt/broker/core/message"
	sess "github.com/lybxkl/gmqtt/broker/core/session"
	"github.com/lybxkl/gmqtt/broker/core/topic"
	"github.com/lybxkl/gmqtt/broker/gcfg"
	. "github.com/lybxkl/gmqtt/common/log"
	"github.com/lybxkl/gmqtt/util"
	"github.com/lybxkl/gmqtt/util/gopool"
	"github.com/lybxkl/gmqtt/util/pkid"
)

type (
	// OnCompleteFunc 完成的回调方法
	OnCompleteFunc func(msg, ack message.Message, err error) error
	// OnPublishFunc 发布的func类型 sender表示当前发送消息的客户端是哪个，shareMsg=true表示是共享消息，不能被Local 操作
	OnPublishFunc func(msg *message.PublishMessage, sub topic.Sub, sender string, shareMsg bool) error
)

var (
	gsvcid uint64 = 0
)

type service struct {
	clusterBelong bool // 集群特殊使用的标记
	clusterOpen   bool // 是否开启了集群

	id   uint64 //这个服务的ID，它与客户ID无关，只是一个数字而已
	ccid string // 客户端id

	// 这是客户端还是服务器?它是由Connect (client)或 HandleConnection(服务器)。
	// 用来表示该是服务端的还是客户端的
	client bool

	//客户端最大可接收包大小，在gCoreect包内，但broker不处理，因为超过限制的报文将导致协议错误，客户端发送包含原因码0x95（报文过大）的DISCONNECT报文给broker
	// 共享订阅的情况下，如果一条消息对于部分客户端来说太长而不能发送，服务端可以选择丢弃此消息或者把消息发送给剩余能够接收此消息的客户端。
	// 非规范：服务端可以把那些没有发送就被丢弃的报文放在死信队列 上，或者执行其他诊断操作。具体的操作超出了5.0规范的范围。
	// maxPackageSize int

	gCore *GCore

	// sess是这个MQTT会话的会话对象。它跟踪会话变量
	//比如ClientId, KeepAlive，用户名等
	sess        sess.Session
	hasSendWill *atomic.Value // 防止重复发送遗嘱使用

	//等待各种goroutines完成启动和停止
	wgStarted sync.WaitGroup
	wgStopped sync.WaitGroup

	// writeMessage互斥锁——序列化输出缓冲区的写操作。
	wmu sync.Mutex

	//这个服务是否关闭。
	closed int64

	//退出信号，用于确定此服务何时结束。如果通道关闭，则退出。
	done chan struct{}

	//onPubFn 将其添加到主题订阅方列表
	// processSubscribe()方法。当服务器完成一个发布消息的ack循环时 它将调用订阅者，也就是这个方法。
	//对于服务器，当这个方法被调用时，它意味着有一个消息应该发布到连接另一端的客户端。所以我们 将调用publish()发送消息。
	onPubFn OnPublishFunc
	sign    *Sign // 信号

	rmsgs []*message.PublishMessage // 用于待发送的保留消息

	server *Server

	pkIDLimiter pkid.Limiter
}

// 运行接入的连接，会产生三个协程异步逻辑处理，当前不会阻塞
func (svc *service) start(resp *message.ConnackMessage) error {
	var err error
	svc.ccid = fmt.Sprintf("%s%d/%s", "gmqtt-", svc.id, svc.sess.ID())

	svc.sign = NewSign(gcfg.GetGCfg().Quota, gcfg.GetGCfg().QuotaLimit)
	svc.hasSendWill = &atomic.Value{}
	svc.hasSendWill.Store(false)
	svc.pkIDLimiter = pkid.NewPacketIDLimiter(svc.sess.ReceiveMaximum()) // 可作流控

	// If svc is a server
	if !svc.client {
		// 这个是发送给订阅者的，是每个订阅者都有一份的方法
		svc.onPubFn = svc.onPub

		// 恢复订阅, 只需要内存中的
		if err = svc.recoverSub(); err != nil {
			return err
		}
	}

	if resp != nil { // resp != nil 则需要发送ack
		if err = writeMessage(svc.gCore, resp); err != nil {
			return err
		}
		svc.gCore.outStat.Incr(uint64(resp.Len()))
	}

	svc.runProcessor() // run 处理逻辑

	if !svc.client {
		// 处理离线消息
		svc.dealOfflineMsg()
	}
	// Wait for all the goroutines to start before returning
	svc.wgStarted.Wait()

	return nil
}

// runProcessor 启动三个协程处理客户端数据
func (svc *service) runProcessor() {
	//处理器负责从缓冲区读取消息并进行处理
	svc.wgStarted.Add(1)
	svc.wgStopped.Add(1)
	gopool.GoSafe(svc.processor)
}

// recoverSub 重新连接时 重新订阅主题
func (svc *service) recoverSub() error {
	//如果这是一个恢复的会话，那么添加它之前订阅的任何主题
	tpc, err := svc.sess.Topics()
	if err != nil {
		return err
	}
	for _, t := range tpc {
		if gcfg.GetGCfg().CloseShareSub && util.IsShareSub(t.Topic) {
			err = core.TopicManager().Unsubscribe(t.Topic, &svc.onPubFn)
			Log.Errorf("recover sub 2 unsubscribe share topic err %+v", err)
			continue
		}
		_, _ = core.TopicManager().Subscribe(topic.Sub{
			Topic:             t.Topic,
			Qos:               t.Qos,
			NoLocal:           t.NoLocal,
			RetainAsPublished: t.RetainAsPublished,
			RetainHandling:    t.RetainHandling,
			SubIdentifier:     t.SubIdentifier,
		}, &svc.onPubFn)
	}
	return nil
}

func (svc *service) dealOfflineMsg() {
	offline := svc.sess.OfflineMsg()    //  发送获取到的离线消息
	for i := 0; i < len(offline); i++ { // 依次处理离线消息
		pub := offline[i].(*message.PublishMessage)
		// topic.Sub 获取
		var (
			subs   []interface{}
			subOpt []topic.Sub
		)
		_ = core.TopicManager().Subscribers(pub.Topic(), pub.QoS(), &subs, &subOpt, false, "", false)
		tag := false
		for j := 0; j < len(subs); j++ {
			if util.Equal(subs[i], &svc.onPubFn) {
				tag = true
				_ = svc.onPubFn(pub, subOpt[j], "", false)
				break
			}
		}
		if !tag {
			_ = svc.onPubFn(pub, topic.Sub{}, "", false)
		}
	}
	// FIXME 是否主动发送未完成确认的过程消息，还是等客户端操作
}

// onPub 当有消息来此客户端时的处理逻辑
func (svc *service) onPub(msg *message.PublishMessage, sub topic.Sub, sender string, isShareMsg bool) error {
	// 判断是否超过最大报文大小，超过客户端要求的最大值，直接当作已完成丢弃
	// 共享订阅的情况下，如果一条消息对于部分客户端来说太长而不能发送，服务端可以选择丢弃此消息或者把消息发送给剩余能够接收此消息的客户端。
	// 目前是直接丢弃
	maxPkSize := svc.sess.MaxPacketSize()
	if maxPkSize > 0 && int(maxPkSize) < msg.Len() {
		// 服务端可以把那些没有发送就被丢弃的报文放在死信队列 上，或者执行其他诊断操作
		Log.Warnf("the packet length exceeded the max packet size: sender: %s, isShareMsg: %v topic: %+v, message: %+v", sender, isShareMsg, sub, msg)
		return nil
	}

	if msg.QoS() > 0 && !svc.sign.ReqQuota() {
		// 超过配额
		return nil
	}
	if !isShareMsg && sub.NoLocal && svc.cid() == sender {
		Log.Debugf("no send noLocal option msg")
		return nil
	}
	if !sub.RetainAsPublished { //为true，表示向此订阅转发应用消息时保持消息被发布时设置的保留（RETAIN）标志
		msg.SetRetain(false)
	}
	if msg.QoS() > 0 { // qos = 0 的限流控制，需要其它控制， qos = 1 or 2则可以跳过此限流器控制
		msg.SetPacketId(svc.pkIDLimiter.PollPacketID())
	}
	if sub.SubIdentifier > 0 {
		msg.SetSubscriptionIdentifier(sub.SubIdentifier) // 订阅标识符
	}
	if alice, exist := svc.sess.GetTopicAlice(msg.Topic()); exist {
		msg.SetNilTopicAndAlias(alice) // 直接替换主题为空了，用主题别名来表示
	}

	// 发送消息
	if err := svc.publish(msg, func(msg, ack message.Message, err error) error {
		Log.Debugf("发送成功：%v,%v,%v", msg, ack, err)
		return nil
	}); err != nil {
		Log.Errorf("service/onPublish: Error publishing message: %v", err)
		return err
	}

	return nil
}

// 发布消息给客户端
func (svc *service) publish(msg *message.PublishMessage, onComplete OnCompleteFunc) (err error) {

	switch msg.QoS() {
	case message.QosAtMostOnce:
		_, err = svc.gCore.writeMessage(msg)
		if err != nil {
			err = message.NewCodeErr(message.ServiceBusy, fmt.Sprintf("(%s) Error sending %s message: %v", svc.cid(), msg.Name(), err))
		}
		if onComplete != nil {
			err = onComplete(msg, nil, nil)
			if err != nil {
				return message.NewCodeErr(message.ServerUnavailable, err.Error())
			}
			return nil
		}
	case message.QosAtLeastOnce:
		err = svc.sess.Pub1ACK().Wait(msg, onComplete)
		if err != nil {
			return message.NewCodeErr(message.ServerUnavailable, err.Error())
		}
		_, err = svc.gCore.writeMessage(msg)
		if err != nil {
			err = message.NewCodeErr(message.ServiceBusy, fmt.Sprintf("(%s) Error sending %s message: %v", svc.cid(), msg.Name(), err))
		}
	case message.QosExactlyOnce:
		err = svc.sess.Pub2out().Wait(msg, onComplete)
		if err != nil {
			return message.NewCodeErr(message.ServerUnavailable, err.Error())
		}
		_, err = svc.gCore.writeMessage(msg)
		if err != nil {
			err = message.NewCodeErr(message.ServiceBusy, fmt.Sprintf("(%s) Error sending %s message: %v", svc.cid(), msg.Name(), err))
		}
	}
	return nil
}

// subscribe 订阅主题
func (svc *service) subscribe(msg *message.SubscribeMessage, onComplete OnCompleteFunc, onPublish OnPublishFunc) error {
	if onPublish == nil {
		return fmt.Errorf("onPublish function is nil. No need to subscribe")
	}

	_, err := svc.gCore.writeMessage(msg)
	if err != nil {
		return message.NewCodeErr(message.ServiceBusy, fmt.Sprintf("(%s) Error sending %s messagev5: %v", svc.cid(), msg.Name(), err))
	}

	var onc OnCompleteFunc = func(msg, ack message.Message, err error) error {
		onComplete := onComplete
		onPublish := onPublish

		if err != nil {
			if onComplete != nil {
				return onComplete(msg, ack, err)
			}
			return err
		}

		sub, ok := msg.(*message.SubscribeMessage)
		if !ok {
			if onComplete != nil {
				return onComplete(msg, ack, fmt.Errorf("Invalid SubscribeMessage received"))
			}
			return nil
		}

		subAck, ok := ack.(*message.SubackMessage)
		if !ok {
			if onComplete != nil {
				return onComplete(msg, ack, fmt.Errorf("Invalid SubackMessage received"))
			}
			return nil
		}

		if sub.PacketId() != subAck.PacketId() {
			if onComplete != nil {
				return onComplete(msg, ack, fmt.Errorf("sub and suback packet ID not the same. %d != %d", sub.PacketId(), subAck.PacketId()))
			}
			return nil
		}

		retCodes := subAck.ReasonCodes()
		tps := sub.Topics()

		if len(tps) != len(retCodes) {
			if onComplete != nil {
				return onComplete(msg, ack, fmt.Errorf("incorrect number of return codes received. Expecting %d, got %d", len(tps), len(retCodes)))
			}
			return nil
		}

		var err2 error = nil

		for i := range tps {
			t := tps[i]
			c := retCodes[i]

			if c == message.QosFailure {
				err2 = fmt.Errorf("failed to subscribe to '%s'\n%v", string(t), err2)
			} else {
				err = svc.sess.AddTopic(topic.Sub{
					Topic:             t,
					Qos:               c,
					NoLocal:           sub.TopicNoLocal(t),
					RetainAsPublished: sub.TopicRetainAsPublished(t),
					RetainHandling:    sub.TopicRetainHandling(t),
					SubIdentifier:     sub.SubscriptionIdentifier(),
				})
				if err != nil {
					err2 = fmt.Errorf("Failed to subscribe to '%s' (%v)\n%v", string(t), err, err2)
				}
				_, err = core.TopicManager().Subscribe(topic.Sub{
					Topic:             t,
					Qos:               c,
					NoLocal:           sub.TopicNoLocal(t),
					RetainAsPublished: sub.TopicRetainAsPublished(t),
					RetainHandling:    sub.TopicRetainHandling(t),
					SubIdentifier:     sub.SubscriptionIdentifier(),
				}, &onPublish)
				if err != nil {
					err2 = fmt.Errorf("Failed to subscribe to '%s' (%v)\n%v", string(t), err, err2)
				}
			}
		}

		if onComplete != nil {
			return onComplete(msg, ack, err2)
		}

		return err2
	}

	return svc.sess.SubACK().Wait(msg, onc)
}

// unsubscribe 取消订阅主题
func (svc *service) unsubscribe(msg *message.UnsubscribeMessage, onComplete OnCompleteFunc) error {
	_, err := svc.gCore.writeMessage(msg)
	if err != nil {
		return fmt.Errorf("(%s) Error sending %s messagev5: %v", svc.cid(), msg.Name(), err)
	}

	var onc OnCompleteFunc = func(msg, ack message.Message, err error) error {
		onComplete := onComplete

		if err != nil {
			if onComplete != nil {
				return onComplete(msg, ack, err)
			}
			return err
		}

		unsub, ok := msg.(*message.UnsubscribeMessage)
		if !ok {
			if onComplete != nil {
				return onComplete(msg, ack, fmt.Errorf("Invalid UnsubscribeMessage received"))
			}
			return nil
		}

		unsuback, ok := ack.(*message.UnsubackMessage)
		if !ok {
			if onComplete != nil {
				return onComplete(msg, ack, fmt.Errorf("Invalid UnsubackMessage received"))
			}
			return nil
		}

		if unsub.PacketId() != unsuback.PacketId() {
			if onComplete != nil {
				return onComplete(msg, ack, fmt.Errorf("unsub and Unsuback packet ID not the same. %d != %d", unsub.PacketId(), unsuback.PacketId()))
			}
			return nil
		}

		var err2 error = nil

		for _, tb := range unsub.Topics() {
			// Remove all subscribers, which basically it's just svc client, since
			// each client has it's own topic tree.
			err := core.TopicManager().Unsubscribe(tb, nil)
			if err != nil {
				err2 = fmt.Errorf("%v\n%v", err2, err)
			}

			svc.sess.RemoveTopic(string(tb))
		}

		if onComplete != nil {
			return onComplete(msg, ack, err2)
		}

		return err2
	}

	return svc.sess.UnsubACK().Wait(msg, onc)
}

// ping ...
func (svc *service) ping(onComplete OnCompleteFunc) error {
	msg := message.NewPingreqMessage()

	_, err := svc.gCore.writeMessage(msg)
	if err != nil {
		return fmt.Errorf("(%s) Error sending %s messagev5: %v", svc.cid(), msg.Name(), err)
	}

	return svc.sess.PingACK().Wait(msg, onComplete)
}

// serverStopHandle broker服务关闭时，此客户端的行为，即客户端的优雅关闭
func (svc *service) serverStopHandle() {
	svc.stop(true)
}

// Stop calls svc, and closes the buffers, somehow it causes buffer.go:476 to panid.
func (svc *service) stop(isServerStop ...bool) {
	defer func() {
		if e := recover(); e != nil {
			Log.Errorf("(%s) Recovering from panic: %v", svc.cid(), e)
		}
		if len(isServerStop) > 0 && isServerStop[0] {
			svc.server.svcs.Del(svc.id)
		}
	}()

	doit := atomic.CompareAndSwapInt64(&svc.closed, 0, 1)
	if !doit {
		return
	}

	svc.sess.SetStatus(sess.OFFLINE)

	// Close quit channel, effectively telling all the goroutines it's time to quit
	if svc.done != nil {
		Log.Debugf("(%s) closing svc.done", svc.cid())
		close(svc.done)
	}

	// Close the network gCore
	if svc.gCore != nil {
		Log.Debugf("(%s) closing svc.gCore", svc.cid())
		svc.gCore.Close()
	}

	//打印该客户端生命周期内的接收字节与消息条数、发送字节与消息条数
	Log.Debugf("(%s) receive %s.", svc.cid(), svc.gCore.inStat)
	Log.Debugf("(%s) send %s.", svc.cid(), svc.gCore.outStat)

	// 如果客户端是无需保留会话的，则取消订阅该客户端的所有主题
	cleanSess := svc.sess.CMsg().CleanSession()
	sessExpiry := svc.sess.SessExpiryInterval()
	if cleanSess {
		svc.unSubAll()
	} else if !cleanSess && sessExpiry > 0 {
		// TODO 加个任务，如果客户端在会话保留期内没有重新在线，则清理订阅
		// 需要注意重新登陆时，需要取消未执行的任务
	}

	//如果设置了遗嘱消息，则发送遗嘱消息，当是收到正常DisConnect消息产生的发送遗嘱消息行为，会在收到消息处处理
	if svc.sess.CMsg().WillFlag() {
		Log.Infof("(%s) service/stop: gCoreection unexpectedly closed. Sending Will：.", svc.cid())
		svc.sendWillMsg()
	}

	if svc.sess.CMsg().CleanSession() {
		core.SessionManager().Remove(svc.sess)
	} else {
		svc.sess.SetOfflineTime(time.Now().Unix())
		core.SessionManager().Save(svc.sess)
	}

	svc.gCore = nil

	Log.Debugf("(%s) service closed.", svc.cid())
}

// unSubAll 取消订阅所有主题，客户端断线使用
func (svc *service) unSubAll() {
	if svc.sess == nil {
		return
	}
	tpc, err := svc.sess.Topics()
	if err != nil {
		Log.Errorf("(%s/%d) unSub topic err: %v", svc.cid(), svc.id, err)
		return
	}
	for _, t := range tpc {
		if err = core.TopicManager().Unsubscribe(t.Topic, &svc.onPubFn); err != nil {
			Log.Errorf("(%s): Error unsubscribing topic %q: %v", svc.cid(), t, err)
		}
	}
}

func (svc *service) isDone() bool {
	select {
	case <-svc.done:
		return true

	default:
		if svc.closed > 0 {
			return true
		}
	}

	return false
}

func (svc *service) cid() string {
	return svc.ccid
}
