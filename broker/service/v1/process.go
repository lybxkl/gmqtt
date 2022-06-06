package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"time"

	"github.com/lybxkl/gmqtt/broker/message"
	sess "github.com/lybxkl/gmqtt/broker/session"
	"github.com/lybxkl/gmqtt/broker/topic"
	. "github.com/lybxkl/gmqtt/common/log"
	"github.com/lybxkl/gmqtt/util"
	"github.com/lybxkl/gmqtt/util/bufpool"
	"github.com/lybxkl/gmqtt/util/cron"
)

var (
	errDisconnect = errors.New("disconnect") // 直接close的错误信号
)

// processor()从传入缓冲区读取消息并处理它们
func (svc *service) processor() {
	defer func() {
		if r := recover(); r != nil {
			Log.Errorf("(%s) Recovering from panic: %v", svc.cid(), r)
		}

		svc.wgStopped.Done()
		svc.stop()
		Log.Debugf("(%s) Stopping processor", svc.cid())
	}()

	Log.Debugf("(%s) Starting processor", svc.cid())

	svc.wgStarted.Done()

	for {
		// 检查done是否关闭，如果关闭，退出
		if svc.isDone() {
			return
		}
		// 流控处理
		if e := svc.streamController(); e != nil {
			Log.Warn(e)
			return
		}

		//了解接下来是什么消息以及消息的大小
		mtype, total, err := svc.peekMessageSize()
		if err != nil {
			if err != io.EOF {
				Log.Errorf("(%s) Error peeking next messagev5 size", svc.cid())
			}
			return
		}
		if total > int(svc.server.cfg.MaxPacketSize) {
			writeMessage(svc.conn, message.NewDiscMessageWithCodeInfo(message.MessageTooLong, nil))
			Log.Errorf("(%s): message sent is too large", svc.cid())
			return
		}

		msg, n, err := svc.peekMessage(mtype, total)
		if err != nil {
			if err != io.EOF {
				Log.Errorf("(%s) Error peeking next message: %v", svc.cid(), err)
			}
			return
		}

		Log.Debugf("(%s) Received: %s", svc.cid(), msg)

		svc.inStat.increment(int64(n))

		//处理读消息
		err = svc.processIncoming(msg)
		if err != nil {
			if err != errDisconnect {
				Log.Errorf("(%s) Error processing %s: %v", svc.cid(), msg, err)

				var dis = message.NewDiscMessageWithCodeInfo(message.UnspecifiedError, []byte(err.Error()))
				if reasonErr, ok := err.(*message.Code); ok {
					dis = message.NewDiscMessageWithCodeInfo(reasonErr.ReasonCode, []byte(reasonErr.Error()))
				}

				writeMessage(svc.conn, dis)
			}
			return
		}

		// 我们应该提交缓冲区中的字节，这样我们才能继续
		_, err = svc.in.ReadCommit(total)
		if err != nil {
			if err != io.EOF {
				Log.Errorf("(%s) Error committing %d read bytes: %v", svc.cid(), total, err)
			}
			return
		}

		// 中间件处理消息，可桥接
		svc.middlewareHandle(msg)
	}
}

func (svc *service) middlewareHandle(message message.Message) {
	// 中间件处理消息，可桥接
	for _, opt := range svc.server.middleware {
		canSkipErr, middleWareErr := opt.Apply(message)
		if middleWareErr != nil {
			if canSkipErr {
				Log.Errorf("(%s) middlewrae deal msg %s error [skip]: %v", svc.cid(), message, middleWareErr)
			} else {
				Log.Errorf("(%s) middlewrae deal msg %s error [no_skip]: %v", svc.cid(), message, middleWareErr)
				break
			}
		}
	}
}

// 当连接消息中请求问题信息为0，则需要去除部分数据再发送
// connectAck 和 disconnect中可不去除
func (svc *service) delRequestRespInfo(msg message.Message) {
	if svc.sess.CMsg().RequestProblemInfo() == 0 {
		if cl, ok := msg.(message.CleanReqProblemInfo); ok {
			switch msg.Type() {
			case message.CONNACK, message.DISCONNECT, message.PUBLISH:
			default:
				cl.SetReasonStr(nil)
				cl.SetUserProperties(nil)
				Log.Debugf("(%s)去除请求问题信息: %s", svc.cid(), msg.Type())
			}
		}
	}
}

// 流控
func (svc *service) streamController() error {
	// 监控流量
	if svc.inStat.msgs%100000 == 0 {
		Log.Warn(fmt.Sprintf("(%s) Going to process messagev5 %d", svc.cid(), svc.inStat.msgs))
	}
	if svc.sign.BeyondQuota() {
		_, _ = svc.sendBeyondQuota()
		return fmt.Errorf("(%s) Beyond quota", svc.cid())
	}
	if svc.sign.Limit() {
		_, _ = svc.sendTooManyMessages()
		return fmt.Errorf("(%s) limit req", svc.cid())
	}
	return nil
}

func (svc *service) sendBeyondQuota() (int, error) {
	dis := message.NewDisconnectMessage()
	dis.SetReasonCode(message.BeyondQuota)
	return svc.sendByConn(dis)
}

func (svc *service) sendTooManyMessages() (int, error) {
	dis := message.NewDisconnectMessage()
	dis.SetReasonCode(message.TooManyMessages)
	return svc.sendByConn(dis)
}

// sendByConn 直接从conn连接发送数据
func (svc *service) sendByConn(msg message.Message) (int, error) {
	b := make([]byte, msg.Len())
	_, err := msg.Encode(b)
	if err != nil {
		return 0, err
	}
	return svc.conn.(net.Conn).Write(b)
}

func (svc *service) processIncoming(msg message.Message) error {
	var err error = nil

	switch msg := msg.(type) {
	case *message.PublishMessage:
		// For PUBLISH message, we should figure out what QoS it is and process accordingly
		// If QoS == 0, we should just take the next step, no ack required
		// If QoS == 1, we should send back PUBACK, then take the next step
		// If QoS == 2, we need to put it in the ack queue, send back PUBREC
		err = svc.processPublish(msg)

	case *message.PubackMessage:
		svc.sign.AddQuota() // 增加配额
		// For PUBACK message, it means QoS 1, we should send to ack queue
		if err = svc.sess.Pub1ACK().Ack(msg); err != nil {
			err = message.NewCodeErr(message.UnspecifiedError, err.Error())
			break
		}
		svc.processAcked(svc.sess.Pub1ACK())
	case *message.PubrecMessage:
		if msg.ReasonCode() > message.QosFailure {
			svc.sign.AddQuota()
		}

		// For PUBREC message, it means QoS 2, we should send to ack queue, and send back PUBREL
		if err = svc.sess.Pub2out().Ack(msg); err != nil {
			err = message.NewCodeErr(message.UnspecifiedError, err.Error())
			break
		}

		resp := message.NewPubrelMessage()
		resp.SetPacketId(msg.PacketId())
		_, err = svc.writeMessage(resp)
		if err != nil {
			err = message.NewCodeErr(message.UnspecifiedError, err.Error())
		}
	case *message.PubrelMessage:
		// For PUBREL messagev5, it means QoS 2, we should send to ack queue, and send back PUBCOMP
		if err = svc.sess.Pub2in().Ack(msg); err != nil {
			break
		}

		svc.processAcked(svc.sess.Pub2in())

		resp := message.NewPubcompMessage()
		resp.SetPacketId(msg.PacketId())
		_, err = svc.writeMessage(resp)
		if err != nil {
			err = message.NewCodeErr(message.UnspecifiedError, err.Error())
		}
	case *message.PubcompMessage:
		svc.sign.AddQuota() // 增加配额

		// For PUBCOMP messagev5, it means QoS 2, we should send to ack queue
		if err = svc.sess.Pub2out().Ack(msg); err != nil {
			err = message.NewCodeErr(message.UnspecifiedError, err.Error())
			break
		}

		svc.processAcked(svc.sess.Pub2out())
	case *message.SubscribeMessage:
		// For SUBSCRIBE messagev5, we should add subscriber, then send back SUBACK
		//对于订阅消息，我们应该添加订阅者，然后发送回SUBACK
		return svc.processSubscribe(msg)

	case *message.SubackMessage:
		// For SUBACK messagev5, we should send to ack queue
		svc.sess.SubACK().Ack(msg)
		svc.processAcked(svc.sess.SubACK())

	case *message.UnsubscribeMessage:
		// For UNSUBSCRIBE messagev5, we should remove subscriber, then send back UNSUBACK
		return svc.processUnsubscribe(msg)

	case *message.UnsubackMessage:
		// For UNSUBACK messagev5, we should send to ack queue
		svc.sess.UnsubACK().Ack(msg)
		svc.processAcked(svc.sess.UnsubACK())

	case *message.PingreqMessage:
		// For PINGREQ messagev5, we should send back PINGRESP
		resp := message.NewPingrespMessage()
		_, err = svc.writeMessage(resp)
		if err != nil {
			err = message.NewCodeErr(message.UnspecifiedError, err.Error())
		}
	case *message.PingrespMessage:
		svc.sess.PingACK().Ack(msg)
		svc.processAcked(svc.sess.PingACK())

	case *message.DisconnectMessage:
		Log.Debugf("client %v disconnect reason code: %s", svc.cid(), msg.ReasonCode())
		// 0x04 包含遗嘱消息的断开  客户端希望断开但也需要服务端发布它的遗嘱消息。
		switch msg.ReasonCode() {
		case message.DisconnectionIncludesWill:
			// 发布遗嘱消息。或者延迟发布
			svc.sendWillMsg()
		case message.Success:
			// 服务端收到包含原因码为0x00（正常关闭） 的DISCONNECT报文之后删除了遗嘱消息
			svc.sess.CMsg().SetWillMessage(nil)
			svc.sess.SetWill(nil)
		default:
			svc.sess.CMsg().SetWillMessage(nil)
			svc.sess.SetWill(nil)
		}

		// 判断是否需要重新设置session过期时间
		if msg.SessionExpiryInterval() > 0 {
			if svc.sess.CMsg().SessionExpiryInterval() == 0 {
				// 如果CONNECT报文中的会话过期间隔为0，则客户端在DISCONNECT报文中设置非0会话过期间隔将造成协议错误（Protocol Error）
				// 如果服务端收到这种非0会话过期间隔，则不会将其视为有效的DISCONNECT报文。
				// TODO  服务端使用包含原因码为0x82（协议错误）的DISCONNECT报文
				return message.NewCodeErr(message.ProtocolError, fmt.Sprintf("(%s) message protocol error: session_ %d", svc.cid(), msg.SessionExpiryInterval()))
			}
			// TODO 需要更新过期间隔
			svc.sess.CMsg().SetSessionExpiryInterval(msg.SessionExpiryInterval())
			err = svc.server.SessionStore.StoreSession(context.Background(), svc.cid(), svc.sess)
			if err != nil {
				return message.NewCodeErr(message.UnspecifiedError, err.Error())
			}
		}
		return errDisconnect

	default:
		return message.NewCodeErr(message.ProtocolError, fmt.Sprintf("(%s) invalid message type %s", svc.cid(), msg.Name()))
	}

	if err != nil {
		Log.Debugf("(%s) Error processing acked message: %v", svc.cid(), err)
	}

	return err
}

// 发送遗嘱消息
func (svc *service) sendWillMsg() {
	willMsg := svc.sess.Will()
	if willMsg != nil && !svc.hasSendWill.Load().(bool) {
		willMsgExpiry := svc.sess.CMsg().WillMsgExpiryInterval()
		if willMsgExpiry > 0 {
			willMsg.SetMessageExpiryInterval(willMsgExpiry)
		}
		willMsgDelaySendTime := svc.sess.CMsg().WillDelayInterval()
		if willMsgDelaySendTime == 0 {
			// 直接发送
		} else {
			// 延迟发送，在延迟发送前重新登录，需要取消发送
		}
		cron.DelayTaskManager.Run(&cron.DelayTask{
			ID:       cron.ID(svc.sess.ClientId()),
			DealTime: time.Duration(willMsgDelaySendTime), // 为0还是由DelayTaskManager处理
			Data:     willMsg,
			Fn: func(data interface{}) {
				Log.Debugf("%s send will message", svc.cid())
				// 发送，只需要发送本地，集群的化，内部实现处理获取消息
				svc.pubFn(data.(*message.PublishMessage), "", false) // TODO 发送遗嘱嘱消息时是否需要处理共享订阅
			},
			CancelCallback: func() {
				svc.hasSendWill.Store(false)
			},
		})
		svc.hasSendWill.Store(true)
	}
}

func (svc *service) processAcked(ackq sess.Ackqueue) {
	for _, ackmsg := range ackq.Acked() {
		// Let's get the messages from the saved messagev5 byte slices.
		//让我们从保存的消息字节片获取消息。
		msg, err := ackmsg.Mtype.New()
		if err != nil {
			Log.Errorf("process/processAcked: Unable to creating new %s messagev5: %v", ackmsg.Mtype, err)
			continue
		}

		if msg.Type() != message.PINGREQ {
			if _, err := msg.Decode(ackmsg.Msgbuf); err != nil {
				Log.Errorf("process/processAcked: Unable to decode %s messagev5: %v", ackmsg.Mtype, err)
				continue
			}
		}

		ack, err := ackmsg.State.New()
		if err != nil {
			Log.Errorf("process/processAcked: Unable to creating new %s messagev5: %v", ackmsg.State, err)
			continue
		}

		if ack.Type() == message.PINGRESP {
			Log.Debug("process/processAcked: PINGRESP")
			continue
		}

		if _, err := ack.Decode(ackmsg.Ackbuf); err != nil {
			Log.Errorf("process/processAcked: Unable to decode %s messagev5: %v", ackmsg.State, err)
			continue
		}

		//glog.Debugf("(%s) Processing acked messagev5: %v", svc.cid(), ack)

		// - PUBACK if it's QoS 1 messagev5. This is on the client side.
		// - PUBREL if it's QoS 2 messagev5. This is on the server side.
		// - PUBCOMP if it's QoS 2 messagev5. This is on the client side.
		// - SUBACK if it's a subscribe messagev5. This is on the client side.
		// - UNSUBACK if it's a unsubscribe messagev5. This is on the client side.
		//如果是QoS 1消息，则返回。这是在客户端。
		//- PUBREL，如果它是QoS 2消息。这是在服务器端。
		//如果是QoS 2消息，则为PUBCOMP。这是在客户端。
		//- SUBACK如果它是一个订阅消息。这是在客户端。
		//- UNSUBACK如果它是一个取消订阅的消息。这是在客户端。
		switch ackmsg.State {
		case message.PUBREL:
			// If ack is PUBREL, that means the QoS 2 messagev5 sent by a remote client is
			// releassed, so let's publish it to other subscribers.
			//如果ack为PUBREL，则表示远程客户端发送的QoS 2消息为
			//发布了，让我们把它发布给其他订阅者吧。
			if err = svc.onPublish(msg.(*message.PublishMessage)); err != nil {
				Log.Errorf("(%s) Error processing ack'ed %s messagev5: %v", svc.cid(), ackmsg.Mtype, err)
			}

		case message.PUBACK, message.PUBCOMP, message.SUBACK, message.UNSUBACK:
			Log.Debugf("process/processAcked: %s", ack)
			// If ack is PUBACK, that means the QoS 1 messagev5 sent by svc service got
			// ack'ed. There's nothing to do other than calling onComplete() below.
			//如果ack是PUBACK，则表示此服务发送的QoS 1消息已获取
			// ack。除了调用下面的onComplete()之外，没有什么可以做的。

			// If ack is PUBCOMP, that means the QoS 2 messagev5 sent by svc service got
			// ack'ed. There's nothing to do other than calling onComplete() below.
			//如果ack为PUBCOMP，则表示此服务发送的QoS 2消息已获得
			// ack。除了调用下面的onComplete()之外，没有什么可以做的。

			// If ack is SUBACK, that means the SUBSCRIBE messagev5 sent by svc service
			// got ack'ed. There's nothing to do other than calling onComplete() below.
			//如果ack是SUBACK，则表示此服务发送的订阅消息
			//得到“消”。除了调用下面的onComplete()之外，没有什么可以做的。

			// If ack is UNSUBACK, that means the SUBSCRIBE messagev5 sent by svc service
			// got ack'ed. There's nothing to do other than calling onComplete() below.
			//如果ack是UNSUBACK，则表示此服务发送的订阅消息
			//得到“消”。除了调用下面的onComplete()之外，没有什么可以做的。

			// If ack is PINGRESP, that means the PINGREQ messagev5 sent by svc service
			// got ack'ed. There's nothing to do other than calling onComplete() below.
			// PINGRESP 直接跳过了，因为发送ping时，我们选择了不保存ping数据，所以这里无法验证
			err = nil

		default:
			Log.Errorf("(%s) Invalid ack messagev5 type %s.", svc.cid(), ackmsg.State)
			continue
		}

		// Call the registered onComplete function
		if ackmsg.OnComplete != nil {
			onComplete, ok := ackmsg.OnComplete.(OnCompleteFunc)
			if !ok {
				Log.Errorf("process/processAcked: Error type asserting onComplete function: %v", reflect.TypeOf(ackmsg.OnComplete))
			} else if onComplete != nil {
				if err := onComplete(msg, ack, nil); err != nil {
					Log.Errorf("process/processAcked: Error running onComplete(): %v", err)
				}
			}
		}
	}
}

// 入消息的主题别名处理 第一阶段 验证
func (svc *service) topicAliceIn(msg *message.PublishMessage) error {
	if msg.TopicAlias() > 0 && len(msg.Topic()) == 0 {
		if tp, ok := svc.sess.GetAliceTopic(msg.TopicAlias()); ok {
			_ = msg.SetTopic([]byte(tp)) // 转换为正确的主题
			msg.SetTopicAlias(0)         // 并将此主题别名置0
			Log.Debugf("%v set topic by alice ==> topic：%v alice：%v", svc.cid(), tp, msg.TopicAlias())
			return nil
		}
		return message.NewCodeErr(message.InvalidTopicAlias, fmt.Sprintf("no found topic alias: %d", msg.TopicAlias()))
	}
	if msg.TopicAlias() > 0 && len(msg.Topic()) > 0 {
		// 需要保存主题别名，只会与当前连接保存生命一致
		if msg.TopicAlias() > svc.sess.TopicAliasMax() {
			return message.NewCodeErr(message.InvalidTopicAlias, "topic alias is not allowed or too large")
		}
		svc.sess.AddTopicAlice(string(msg.Topic()), msg.TopicAlias())
		Log.Debugf("%v save topic alice ==> topic：%v alice：%v", svc.cid(), msg.Topic(), msg.TopicAlias())
		msg.SetTopicAlias(0)
		return nil
	}
	if msg.TopicAlias() > svc.sess.TopicAliasMax() {
		return message.NewCodeErr(message.InvalidTopicAlias, "beyond the max of topic alias")
	}
	return nil
}

// For PUBLISH messagev5, we should figure out what QoS it is and process accordingly
// If QoS == 0, we should just take the next step, no ack required
// If QoS == 1, we should send back PUBACK, then take the next step
// If QoS == 2, we need to put it in the ack queue, send back PUBREC
//对于发布消息，我们应该弄清楚它的QoS是什么，并相应地进行处理
//如果QoS == 0，我们应该采取下一步，不需要ack
//如果QoS == 1，我们应该返回PUBACK，然后进行下一步
//如果QoS == 2，我们需要将其放入ack队列中，发送回PUBREC
func (svc *service) processPublish(msg *message.PublishMessage) error {
	if svc.server.cfg.MaxQos < int(msg.QoS()) { // 判断是否是支持的qos等级，只会验证本broker内收到的，集群转发来的不会验证，前提是因为集群来的已经是验证过的
		return message.NewCodeErr(message.UnsupportedQoSLevel, fmt.Sprintf("a maximum of qos %d is supported", svc.server.cfg.MaxQos))
	}
	if msg.Retain() && !svc.server.cfg.RetainAvailable {
		return message.NewCodeErr(message.UnsupportedRetention, "unSupport retain message")
	}

	if err := svc.topicAliceIn(msg); err != nil {
		return err
	}
	switch msg.QoS() {
	case message.QosExactlyOnce:
		svc.sess.Pub2in().Wait(msg, nil)

		resp := message.NewPubrecMessage()
		resp.SetPacketId(msg.PacketId())

		_, err := svc.writeMessage(resp)
		if err != nil {
			return message.NewCodeErr(message.UnspecifiedError, err.Error())
		}
		return nil

	case message.QosAtLeastOnce:
		resp := message.NewPubackMessage()
		resp.SetPacketId(msg.PacketId())

		if _, err := svc.writeMessage(resp); err != nil {
			return message.NewCodeErr(message.UnspecifiedError, err.Error())
		}

		return svc.onPublish(msg)

	case message.QosAtMostOnce:
		return svc.onPublish(msg)
	}

	return message.NewCodeErr(message.UnsupportedQoSLevel, fmt.Sprintf("(%s) invalid message QoS %d.", svc.cid(), msg.QoS()))
}

// For SUBSCRIBE messagev5, we should add subscriber, then send back SUBACK
func (svc *service) processSubscribe(msg *message.SubscribeMessage) error {

	// 在编解码处已经限制了至少有一个主题过滤器/订阅选项对

	resp := message.NewSubackMessage()
	resp.SetPacketId(msg.PacketId())

	//订阅不同的主题
	var (
		retcodes []byte
		tps      = msg.Topics()
		qos      = msg.Qos()
	)

	svc.rmsgs = svc.rmsgs[0:0]

	for _, t := range tps {
		tpc := string(t)
		// 简单处理，直接断开连接，返回原因码
		if svc.server.cfg.CloseShareSub && util.IsShareSub(tpc) {
			return message.NewCodeErr(message.UnsupportedSharedSubscriptions)
		} else if !svc.server.cfg.CloseShareSub && util.IsShareSub(tpc) && msg.TopicNoLocal(t) {
			// 共享订阅时把非本地选项设为1将造成协议错误（Protocol Error）
			return message.NewCodeErr(message.ProtocolError)
		}
	}

	for i, t := range tps {
		var (
			noLocal           = msg.TopicNoLocal(t)
			retainAsPublished = msg.TopicRetainAsPublished(t)
			retainHandling    = msg.TopicRetainHandling(t)
		)

		sub := topic.Sub{
			Topic:             string(t),
			Qos:               qos[i],
			NoLocal:           noLocal,
			RetainAsPublished: retainAsPublished,
			RetainHandling:    retainHandling,
			SubIdentifier:     msg.SubscriptionIdentifier(),
		}

		rqos, err := svc.server.topicsMgr.Subscribe(sub, &svc.onpub)
		if err != nil {
			return message.NewCodeErr(message.ServiceBusy, err.Error())
		}

		err = svc.sess.AddTopic(sub)
		if err != nil {
			return message.NewCodeErr(message.ServiceBusy, err.Error())
		}
		retcodes = append(retcodes, rqos)

		if !util.IsShareSub(string(t)) { // 共享订阅不发送任何保留消息。
			//没有检查错误。如果有错误，我们不想订阅要停止，就return。
			switch retainHandling {
			case message.NoSendRetain:
				break
			case message.CanSendRetain:
				err = svc.server.topicsMgr.Retained(t, &svc.rmsgs)
				if err != nil {
					return message.NewCodeErr(message.ServiceBusy, err.Error())
				}
				Log.Debugf("(%s) topic = %s, retained count = %d", svc.cid(), t, len(svc.rmsgs))
			case message.NoExistSubSendRetain:
				// 已存在订阅的情况下不发送保留消息是很有用的，比如重连完成时客户端不确定订阅是否在之前的会话连接中被创建。
				oldTp, _ := svc.sess.Topics()

				existThisTopic := false
				for jk := 0; jk < len(oldTp); jk++ {
					if len(oldTp[jk].Topic) != len(t) {
						continue
					}
					otp := oldTp[jk].Topic
					for jj := 0; jj < len(otp); jj++ {
						if otp[jj] != t[jj] {
							goto END
						}
					}
					// 存在就不发送了
					existThisTopic = true
					break
				END:
				}
				if !existThisTopic {
					err = svc.server.topicsMgr.Retained(t, &svc.rmsgs)
					if err != nil {
						return message.NewCodeErr(message.ServiceBusy, err.Error())
					}
					Log.Debugf("(%s) topic = %s, retained count = %d", svc.cid(), t, len(svc.rmsgs))
				}
			}
		}
	}

	Log.Infof("客户端：%s，订阅主题：%s，qos：%d，retained count = %d", svc.cid(), tps, qos, len(svc.rmsgs))
	if err := resp.AddReasonCodes(retcodes); err != nil {
		return message.NewCodeErr(message.UnspecifiedError, err.Error())
	}

	if _, err := svc.writeMessage(resp); err != nil {
		return message.NewCodeErr(message.UnspecifiedError, err.Error())
	}

	svc.processToCluster(tps, msg)

	for _, rm := range svc.rmsgs {
		// 下面不用担心因为又重新设置为old qos而有问题，因为在内部ackqueue都已经encode了
		old := rm.QoS()
		// 这里也做了调整
		if rm.QoS() > qos[0] {
			rm.SetQoS(qos[0])
		}
		if err := svc.publish(rm, nil); err != nil {
			Log.Errorf("service/processSubscribe: Error publishing retained messagev5: %v", err)
			//rm.SetQoS(old)
			//return err
		}
		rm.SetQoS(old)
	}

	return nil
}

// For UNSUBSCRIBE message, we should remove the subscriber, and send back UNSUBACK
func (svc *service) processUnsubscribe(msg *message.UnsubscribeMessage) error {
	tps := msg.Topics()

	for _, t := range tps {
		svc.server.topicsMgr.Unsubscribe(t, &svc.onpub)
		svc.sess.RemoveTopic(string(t))
	}

	resp := message.NewUnsubackMessage()
	resp.SetPacketId(msg.PacketId())
	resp.AddReasonCode(message.Success.Value())

	_, err := svc.writeMessage(resp)
	if err != nil {
		return message.NewCodeErr(message.UnspecifiedError, err.Error())
	}

	svc.processToCluster(tps, msg)
	Log.Infof("客户端：%s 取消订阅主题：%s", svc.cid(), tps)
	return nil
}

var (
	shareByte = []byte("$share/")
	// reflect.DeepEqual(topic[:len(shareByte)],shareByte)
	// 这样可以减少反射操作
	deepEqual = func(topic []byte) bool {
		for i, i2 := range shareByte {
			if topic[i] != i2 {
				return false
			}
		}
		return true
	}
)

// processToCluster 分主题发送到其它节点发送
func (svc *service) processToCluster(topic [][]byte, msg message.Message) {
	if !svc.openCluster() {
		return
	}
}

// onPublish()在服务器接收到发布消息并完成时调用 ack循环。
//此方法将根据发布获取订阅服务器列表主题，并将消息发布到订阅方列表。
// 消息已经处理完过程消息了，准备进行广播发送
func (svc *service) onPublish(msg *message.PublishMessage) error {
	if msg.Retain() {
		if err := svc.server.topicsMgr.Retain(msg); err != nil { // 为这个主题保存最后一条保留消息
			Log.Errorf("(%s) Error retaining messagev5: %v", svc.cid(), err)
		}
	}

	var buf *bytes.Buffer

	if svc.openCluster() || svc.openShare() {
		buf = bufpool.BufferPoolGet()
		defer bufpool.BufferPoolPut(buf)
		msg.EncodeToBuf(buf) // 这里没有选择直接拿msg内的buf
	}

	if svc.openCluster() && buf != nil {
		tmpMsg2 := message.NewPublishMessage() // 必须重新弄一个，防止被下面改动qos引起bug
		tmpMsg2.Decode(buf.Bytes())
		svc.sendCluster(tmpMsg2)
	}
	if svc.openShare() && buf != nil {
		tmpMsg := message.NewPublishMessage() // 必须重新弄一个，防止被下面改动qos引起bug
		tmpMsg.Decode(buf.Bytes())
		svc.sendShareToCluster(tmpMsg)
	}

	// 发送非共享订阅主题, 这里面会改动qos
	return svc.pubFn(msg, "", false)
}

// 发送共享主题到集群其它节点去，以共享组的方式发送
func (svc *service) sendShareToCluster(msg *message.PublishMessage) {
	if !svc.openCluster() {
		// 没开集群，就只要发送到当前节点下就行
		// 发送当前主题的所有共享组
		err := svc.pubFnPlus(msg)
		if err != nil {
			Log.Errorf("%v 发送共享：%v 主题错误：%+v", svc.id, msg.Topic(), *msg)
		}
		return
	}
}

// 发送到集群其它节点去
func (svc *service) sendCluster(message message.Message) {
	if !svc.openCluster() {
		return
	}
}

// ClusterInToPub 集群节点发来的普通消息
func (svc *service) ClusterInToPub(msg *message.PublishMessage) error {
	if !svc.isCluster() {
		return nil
	}
	if msg.Retain() {
		if err := svc.server.topicsMgr.Retain(msg); err != nil { // 为这个主题保存最后一条保留消息
			Log.Errorf("(%s) Error retaining messagev5: %v", svc.cid(), err)
		}
	}
	// 发送非共享订阅主题
	return svc.pubFn(msg, "", false)
}

// ClusterInToPubShare 集群节点发来的共享主题消息，需要发送到特定的共享组 onlyShare = true
// 和 普通节点 onlyShare = false
func (svc *service) ClusterInToPubShare(msg *message.PublishMessage, shareName string, onlyShare bool) error {
	if !svc.isCluster() {
		return nil
	}
	if msg.Retain() {
		if err := svc.server.topicsMgr.Retain(msg); err != nil { // 为这个主题保存最后一条保留消息
			Log.Errorf("(%s) Error retaining messagev5: %v", svc.cid(), err)
		}
	}
	// 根据onlyShare 确定是否只发送共享订阅主题
	return svc.pubFn(msg, shareName, onlyShare)
}

// ClusterInToPubSys 集群节点发来的系统主题消息
func (svc *service) ClusterInToPubSys(msg *message.PublishMessage) error {
	if !svc.isCluster() {
		return nil
	}
	if msg.Retain() {
		if err := svc.server.topicsMgr.Retain(msg); err != nil { // 为这个主题保存最后一条保留消息
			Log.Errorf("(%s) Error retaining messagev5: %v", svc.cid(), err)
		}
	}
	// 发送
	return svc.pubFnSys(msg)
}

func (svc *service) pubFn(msg *message.PublishMessage, shareName string, onlyShare bool) error {
	var (
		subs []interface{}
		qoss []topic.Sub
	)

	err := svc.server.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &subs, &qoss, false, shareName, onlyShare)
	if err != nil {
		//Log.Error(err, "(%s) Error retrieving subscribers list: %v", svc.cid(), err)
		return message.NewCodeErr(message.ServiceBusy, err.Error())
	}

	msg.SetRetain(false)
	Log.Debugf("(%s) publishing to topic %s and %s subscribers：%v", svc.cid(), msg.Topic(), shareName, len(subs))

	return svc.lookSend(msg, subs, qoss, onlyShare)
}

// 发送当前主题所有共享组
func (svc *service) pubFnPlus(msg *message.PublishMessage) error {
	var (
		subs []interface{}
		qoss []topic.Sub
	)
	err := svc.server.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &subs, &qoss, false, "", true)
	if err != nil {
		//Log.Error(err, "(%s) Error retrieving subscribers list: %v", svc.cid(), err)
		return message.NewCodeErr(message.ServiceBusy, err.Error())
	}

	msg.SetRetain(false)
	Log.Debugf("(%s) Publishing to all shareName topic in %s to subscribers：%v", svc.cid(), msg.Topic(), subs)

	return svc.lookSend(msg, subs, qoss, true)
}

// 发送系统消息
func (svc *service) pubFnSys(msg *message.PublishMessage) error {
	var (
		subs []interface{}
		qoss []topic.Sub
	)
	err := svc.server.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &subs, &qoss, true, "", false)
	if err != nil {
		//Log.Error(err, "(%s) Error retrieving subscribers list: %v", svc.cid(), err)
		return message.NewCodeErr(message.ServiceBusy, err.Error())
	}

	msg.SetRetain(false)
	Log.Debugf("(%s) publishing sys topic %s to subscribers：%v", svc.cid(), msg.Topic(), subs)

	return svc.lookSend(msg, subs, qoss, false)
}

// 循环发送， todo 可丢到协程池处理
func (svc *service) lookSend(msg *message.PublishMessage, subs []interface{}, qoss []topic.Sub, onlyShare bool) error {
	for i, s := range subs {
		if s != nil {
			fn, ok := s.(*OnPublishFunc)
			if !ok {
				return fmt.Errorf("invalid onPublish Function")
			} else {
				err := (*fn)(copyMsg(msg, qoss[i].Qos), qoss[i], svc.cid(), onlyShare)
				if err == io.EOF {
					// TODO 断线了，是否对于qos=1和2的保存至离线消息
				}
			}
		}
	}
	return nil
}

// 是否是集群server
func (svc *service) isCluster() bool {
	return svc.clusterBelong
}

// 是否开启集群
func (svc *service) openCluster() bool {
	return svc.clusterOpen
}

// 是否开启共享
func (svc *service) openShare() bool {
	return true
}

func copyMsg(msg *message.PublishMessage, newQos byte) *message.PublishMessage {
	buf := bufpool.BufferPoolGet()
	msg.EncodeToBuf(buf)

	tmpMsg := message.NewPublishMessage() // 必须重新弄一个，防止被下面改动qos引起bug
	tmpMsg.Decode(buf.Bytes())
	tmpMsg.SetQoS(newQos) // 设置为该发的qos级别

	bufpool.BufferPoolPut(buf) // 归还
	return tmpMsg
}
