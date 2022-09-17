package topic

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/lybxkl/gmqtt/broker/core/message"
	"github.com/lybxkl/gmqtt/broker/core/store"
	"github.com/lybxkl/gmqtt/broker/core/topic"

	share3 "github.com/lybxkl/gmqtt/broker/impl/topic/share"
	sys3 "github.com/lybxkl/gmqtt/broker/impl/topic/sys"
	"github.com/lybxkl/gmqtt/common/constant"
	"github.com/lybxkl/gmqtt/util"
)

var _ topic.Manager = (*memtopic)(nil)

type memtopic struct {
	// Sub/unsub mutex
	smu sync.RWMutex
	// Subscription tree
	sroot *snode

	// Retained message mutex
	rmu sync.RWMutex
	// Retained messages topic tree
	rroot        *rnode
	sys          sys3.TopicProvider
	share        share3.TopicProvider // 共享订阅处理器
	messageStore store.MessageStore
}

// NewMemProvider 返回memtopic的一个新实例，该实例实现了
// topic.Manager 接口。memProvider是存储主题的隐藏结构
// 订阅并保留内存中的消息。内容不是这样持久化的 当服务器关闭时，所有东西都将消失。小心使用。
func NewMemProvider(messageStore store.MessageStore) *memtopic {
	return &memtopic{
		sroot:        newSNode(),
		rroot:        newRNode(),
		sys:          sys3.NewMemProvider(),
		share:        share3.NewMemProvider(),
		messageStore: messageStore,
	}
}

func (t *memtopic) SetStore(_ store.SessionStore, messageStore store.MessageStore) {
	t.messageStore = messageStore
}

// Subscribe 订阅主题
func (t *memtopic) Subscribe(subs topic.Sub, sub interface{}) (byte, error) {
	if !message.ValidQos(subs.Qos) {
		return message.QosFailure, fmt.Errorf("invalid QoS %d", subs.Qos)
	}

	if sub == nil {
		return message.QosFailure, fmt.Errorf("subscriber cannot be nil")
	}

	subs.Qos = util.Qos(subs.Qos)

	// 系统主题订阅
	if util.IsSysSub(subs.Topic) {
		subs.Topic = subs.Topic[util.SysTopicPrefixLen():]
		return t.sys.Subscribe(subs, sub)
	}
	// 共享主题订阅
	if util.IsShareSub(subs.Topic) {
		var index = util.ShareTopicPrefixLen()
		// 找到共享主题名称
		for i, b := range subs.Topic[util.ShareTopicPrefixLen():] {
			if b == '/' {
				index += i
				break
			} else if b == '+' || b == '#' {
				// {ShareName} 是一个不包含 "/", "+" 以及 "#" 的字符串。
				return message.QosFailure, fmt.Errorf("not allowed '+' or '#' in the {share_name}")
			}
		}
		if index == util.ShareTopicPrefixLen() {
			return message.QosFailure, fmt.Errorf("topic format error")
		}
		// 必须share name 和topic 都不为空
		if len(subs.Topic) >= 2+index && subs.Topic[index] == '/' {
			//shareName := string(topic[len(shareByte) : index])
			// TODO 注册共享订阅到redis
			//redis.SubShare(string(topic[index+1:]), string(topic[len(shareByte):index]), nodeName)
			shareName := subs.Topic[util.ShareTopicPrefixLen():index]
			subs.Topic = subs.Topic[index+1:]
			return t.share.Subscribe(shareName, subs, sub)
		} else {
			return message.QosFailure, fmt.Errorf("topic format error")
		}
	}

	t.smu.Lock()
	defer t.smu.Unlock()
	if err := t.sroot.sinsert(subs, sub); err != nil {
		return message.QosFailure, err
	}
	return subs.Qos, nil
}

// Unsubscribe 取消订阅
func (t *memtopic) Unsubscribe(topic []byte, sub interface{}) error {
	// 系统主题的取消
	if util.IsSysSub(topic) {
		return t.sys.Unsubscribe(topic[util.SysTopicPrefixLen():], sub)
	}
	// 共享主题的取消
	if util.IsShareSub(topic) {
		var index = util.ShareTopicPrefixLen()
		// 找到共享主题名称
		for i, b := range topic[util.ShareTopicPrefixLen():] {
			if b == '/' {
				index += i
				break
			} else if b == '+' || b == '#' {
				return fmt.Errorf("not allowed '+' or '#' in the {share_name}")
			}
		}
		if index == util.ShareTopicPrefixLen() {
			return fmt.Errorf("topic format error")
		}
		if len(topic) >= 2+index && topic[index] == '/' {
			//shareName := string(topic[len(shareByte) : index+len(shareByte)])
			// TODO 取消注册共享订阅到redis
			//redis.UnSubShare(string(topic[index+1:]), string(topic[len(shareByte):index]), nodeName)
			return t.share.Unsubscribe(topic[index+1:], topic[util.ShareTopicPrefixLen():index], sub)
		}
	}

	t.smu.Lock()
	defer t.smu.Unlock()

	return t.sroot.sremove(topic, sub)
}

// Subscribers 返回的值将在下一次订阅者调用时失效
// svc == true表示这是当前系统或者其它集群节点的系统消息，svc==false表示是客户端或者集群其它节点发来的普通共享、非共享消息
// needShare != ""表示是否需要获取当前服务节点下共享组名为shareName的一个共享订阅节点
func (t *memtopic) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]topic.Sub, svc bool, shareName string, onlyShare bool) error {
	if !message.ValidQos(qos) {
		return fmt.Errorf("invalid QoS %d", qos)
	}
	if !svc {
		if len(topic) > 0 && topic[0] == '$' {
			return fmt.Errorf("memtopic/Subscribers: Cannot publish to $ topic")
		}
	}

	*subs = (*subs)[0:0]
	*qoss = (*qoss)[0:0]
	if svc { // 服务发来的 系统主题 消息
		if util.IsSysSub(topic) {
			return t.sys.Subscribers(topic[util.SysTopicPrefixLen():], qos, subs, qoss)
		}
		return fmt.Errorf("memtopic/Subscribers: Publish error message to $sys/ topic")
	}

	if shareName != "" {
		err := t.share.Subscribers(topic, []byte(shareName), qos, subs, qoss)
		if err != nil {
			return err
		}
		if onlyShare { // 是否需要非共享
			return nil
		}
	} else if shareName == "" && onlyShare == true { // 获取所有shareName的每个的订阅者之一
		err := t.share.Subscribers(topic, nil, qos, subs, qoss)
		return err
	}

	t.smu.RLock()
	defer t.smu.RUnlock()
	return t.sroot.smatch(topic, qos, subs, qoss)
}

func (t *memtopic) AllSubInfo() (map[string][]string, error) {
	return t.share.AllSubInfo()
}

func (t *memtopic) Retain(msg *message.PublishMessage) error {
	t.rmu.Lock()
	defer t.rmu.Unlock()

	//很明显，至少根据MQTT一致性/互操作性
	//测试，有效载荷为0表示删除retain消息。
	// https://eclipse.org/paho/clients/testing/
	if len(msg.Payload()) == 0 {
		// 删除 DB Retain
		err := t.messageStore.ClearRetainMessage(context.Background(), string(msg.Topic()))
		if err != nil {
			return err
		}
		return t.rroot.rremove(msg.Topic())
	}
	// 新增 DB Retain
	err := t.messageStore.StoreRetainMessage(context.Background(), string(msg.Topic()), msg)
	if err != nil {
		return err
	}
	return t.rroot.rinsert(msg.Topic(), msg)
}

func (t *memtopic) Retained(topic []byte, msgs *[]*message.PublishMessage) error {
	t.rmu.RLock()
	defer t.rmu.RUnlock()

	return t.rroot.rmatch(topic, msgs)
}

func (t *memtopic) Close() error {
	t.sroot = nil
	t.rroot = nil
	return nil
}

//subscrition节点
type snode struct {
	// If t is the end of the topic string, then add subscribers here
	//如果这是主题字符串的结尾，那么在这里添加订阅者
	subs []interface{}
	qos  []topic.Sub

	// Otherwise add the next topic level here
	snodes map[string]*snode
}

func newSNode() *snode {
	return &snode{
		snodes: make(map[string]*snode),
	}
}

func (t *snode) sinsert(subs topic.Sub, sub interface{}) error {
	//如果没有更多的主题级别，这意味着我们在匹配的snode
	//插入订阅。让我们看看是否有这样的订阅者，
	//如果有，更新它。否则插入它。
	if len(subs.Topic) == 0 {
		//让我们看看用户是否已经在名单上了。如果是的,更新QoS，然后返回。
		for i := range t.subs { // 重复订阅，可更新qos
			if equal(t.subs[i], sub) {
				t.qos[i] = subs
				return nil
			}
		}

		//否则添加。
		t.subs = append(t.subs, sub)
		t.qos = append(t.qos, subs)

		return nil
	}

	//不是最后一层，让我们查找或创建下一层snode，和递归调用它的insert()。

	// ntl =下一个主题级别
	ntl, rem, err := nextTopicLevel(subs.Topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Add snode if it doesn't already exist
	n, ok := t.snodes[level]
	if !ok {
		n = newSNode()
		t.snodes[level] = n
	}
	subs.Topic = rem
	return n.sinsert(subs, sub)
}

// This remove implementation ignores the QoS, as long as the subscriber
// matches then it's removed
func (t *snode) sremove(topic []byte, sub interface{}) error {
	//如果主题是空的，这意味着我们到达了最后一个匹配的snode。 如果是这样,
	//让我们找到匹配的订阅服务器并删除它们。
	if len(topic) == 0 {
		//如果订阅者== nil，则发出删除所有订阅者的信号
		if sub == nil {
			t.subs = t.subs[0:0]
			t.qos = t.qos[0:0]
			return nil
		}

		//如果我们找到了用户，就把它从列表中删除。从技术上讲
		//我们只是把所有其他项目向上移动1，从而覆盖了这个插槽。
		for i := range t.subs {
			if equal(t.subs[i], sub) {
				t.subs = append(t.subs[:i], t.subs[i+1:]...)
				t.qos = append(t.qos[:i], t.qos[i+1:]...)
				return nil
			}
		}

		return fmt.Errorf("memtopic/remove: No topic found for subscriber")
	}

	// Not the last level, so let's find the next level snode, and recursively
	// call it's remove().
	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Find the snode that matches the topic level
	n, ok := t.snodes[level]
	if !ok {
		return fmt.Errorf("memtopic/remove: No topic found")
	}

	// Remove the subscriber from the next level snode
	if err := n.sremove(rem, sub); err != nil {
		return err
	}

	// If there are no more subscribers and snodes to the next level we just visited
	// let's remove it
	if len(n.subs) == 0 && len(n.snodes) == 0 {
		delete(t.snodes, level)
	}

	return nil
}

// smatch() returns all the subscribers that are subscribed to the topic. Given a topic
// with no wildcards (publish topic), it returns a list of subscribers that subscribes
// to the topic. For each of the level names, it's a match
// - if there are subscribers to '#', then all the subscribers are added to result set
// smatch()返回订阅该主题的所有订阅方。给定一个主题
//没有通配符(发布主题)，它返回订阅的订阅方列表
//回到主题。对于每个级别名称，它都是匹配的
// -如果“#”中有订阅者，那么所有的订阅者都将被添加到结果集中

//非规范评注
//例如, 如果客户端订阅主题 “sport/tennis/player1/#”, 它会收到使用下列主题名发布的消息:
//• “sport/tennis/player1”
//• “sport/tennis/player1/ranking
//• “sport/tennis/player1/score/wimbledon”
//
//非规范评注
//• “sport/#”也匹配单独的“sport”主题名, 因为#包括它的父级.
//• “#”是有效的, 会收到所有的应用消息.
//• “sport/tennis/#”也是有效的.
//• “sport/tennis#”是无效的.
//• “sport/tennis/#/ranking”是无效的.

//非规范评注
//例如, “sport/tennis/+”匹配“sport/tennis/player1”和“sport/tennis/player2”,
//但是不匹配“sport/tennis/player1/ranking”.同时, 由于单层通配符只能匹配一个层级,
//“sport/+”不匹配“sport”但是却匹配“sport/”.
//• “+”是有效的.
//• “+/tennis/#”是有效的.
//• “sport+”是无效的.
//• “sport/+/player1”是有效的.
//• “/finance”匹配“+/+”和“/+”, 但是不匹配“+”.

func (t *snode) smatch(topic []byte, qos byte, subs *[]interface{}, qoss *[]topic.Sub) error {
	// If the topic is empty, it means we are at the final matching snode. If so,
	// let's find the subscribers that match the qos and append them to the list.
	//如果主题是空的，这意味着我们到达了最后一个匹配的snode。如果是这样,
	//让我们找到与qos匹配的订阅服务器并将它们附加到列表中。
	if len(topic) == 0 {
		t.matchQos(qos, subs, qoss)
		if v, ok := t.snodes["#"]; ok {
			v.matchQos(qos, subs, qoss)
		}
		if v, ok := t.snodes["+"]; ok {
			v.matchQos(qos, subs, qoss)
		}
		return nil
	}

	// ntl = next topic level
	//rem和err都等于nil，意味着是 sss/sss这种后面没有/结尾的
	//len(rem)==0和err等于nil，意味着是 sss/sss/这种后面有/结尾的
	// rem用来做 #和+匹配时有用
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)
	for k, n := range t.snodes {
		// If the key is "#", then these subscribers are added to the result set
		if k == constant.MWC {
			n.matchQos(qos, subs, qoss)
		} else if k == constant.SWC || k == level {
			if rem != nil {
				if err := n.smatch(rem, qos, subs, qoss); err != nil {
					return err
				}
			} else { // 这个不需要匹配最后还有+的订阅者
				n.matchQos(qos, subs, qoss)
				if v, ok := n.snodes["#"]; ok {
					v.matchQos(qos, subs, qoss)
				}
			}
		}
	}

	return nil
}

// retained message nodes
//保留信息节点
type rnode struct {
	// If t is the end of the topic string, then add retained messages here
	//如果这是主题字符串的结尾，那么在这里添加保留的消息
	msg *message.PublishMessage
	buf []byte

	// Otherwise add the next topic level here
	rnodes map[string]*rnode
}

func newRNode() *rnode {
	return &rnode{
		rnodes: make(map[string]*rnode),
	}
}

func (t *rnode) rinsert(topic []byte, msg *message.PublishMessage) error {
	// If there's no more topic levels, that means we are at the matching rnode.
	if len(topic) == 0 {
		l := msg.Len()

		// Let's reuse the buffer if there's enough space
		if l > cap(t.buf) {
			t.buf = make([]byte, l)
		} else {
			t.buf = t.buf[0:l]
		}

		if _, err := msg.Encode(t.buf); err != nil {
			return err
		}

		// Reuse the message if possible
		if t.msg == nil {
			t.msg = message.NewPublishMessage()
		}

		if _, err := t.msg.Decode(t.buf); err != nil {
			return err
		}

		return nil
	}

	// Not the last level, so let's find or create the next level snode, and
	// recursively call it's insert().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Add snode if it doesn't already exist
	n, ok := t.rnodes[level]
	if !ok {
		n = newRNode()
		t.rnodes[level] = n
	}

	return n.rinsert(rem, msg)
}

// Remove the retained messagev for the supplied topic
func (t *rnode) rremove(topic []byte) error {
	// If the topic is empty, it means we are at the final matching rnode. If so,
	// let's remove the buffer and message.
	if len(topic) == 0 {
		t.buf = nil
		t.msg = nil
		return nil
	}

	// Not the last level, so let's find the next level rnode, and recursively
	// call it's remove().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Find the rnode that matches the topic level
	n, ok := t.rnodes[level]
	if !ok {
		return fmt.Errorf("memtopic/rremove: No topic found")
	}

	// Remove the subscriber from the next level rnode
	if err := n.rremove(rem); err != nil {
		return err
	}

	// If there are no more rnodes to the next level we just visited let's remove it
	if len(n.rnodes) == 0 {
		delete(t.rnodes, level)
	}

	return nil
}

// rmatch() finds the retained messages for the topic and qos provided. It's somewhat
// of a reverse match compare to match() since the supplied topic can contain
// wildcards, whereas the retained message topic is a full (no wildcard) topic.
func (t *rnode) rmatch(topic []byte, msgs *[]*message.PublishMessage) error {
	// If the topic is empty, it means we are at the final matching rnode. If so,
	// add the retained msg to the list.
	if len(topic) == 0 {
		if t.msg != nil {
			*msgs = append(*msgs, t.msg)
		}
		return nil
	}

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	if level == constant.MWC {
		// If '#', add all retained messages starting t node
		t.allRetained(msgs)
	} else if level == constant.SWC {
		// If '+', check all nodes at t level. Next levels must be matched.
		for _, n := range t.rnodes {
			if err := n.rmatch(rem, msgs); err != nil {
				return err
			}
		}
	} else {
		// Otherwise, find the matching node, go to the next level
		if n, ok := t.rnodes[level]; ok {
			if err := n.rmatch(rem, msgs); err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *rnode) allRetained(msgs *[]*message.PublishMessage) {
	if t.msg != nil {
		*msgs = append(*msgs, t.msg)
	}

	for _, n := range t.rnodes {
		n.allRetained(msgs)
	}
}

// Returns topic level, remaining topic levels and any errors
//返回主题级别、剩余的主题级别和任何错误
func nextTopicLevel(topic []byte) ([]byte, []byte, error) {
	s := constant.StateCHR

	//遍历topic，判断是何种类型的主题
	for i, c := range topic {
		switch c {
		case '/':
			if s == constant.StateMWC {
				//多层次通配符发现的主题，它不是在最后一层
				return nil, nil, fmt.Errorf("memtopic/nextTopicLevel: Multi-level wildcard found in topic and it's not at the last level")
			}

			if i == 0 {
				return []byte(constant.SWC), topic[i+1:], nil
			}

			return topic[:i], topic[i+1:], nil

		case '#':
			if i != 0 {
				//通配符“#”必须占据整个主题级别
				return nil, nil, fmt.Errorf("memtopic/nextTopicLevel: Wildcard character '#' must occupy entire topic level")
			}

			s = constant.StateMWC

		case '+':
			if i != 0 {
				//通配符“+”必须占据整个主题级别
				return nil, nil, fmt.Errorf("memtopic/nextTopicLevel: Wildcard character '+' must occupy entire topic level")
			}

			s = constant.StateSWC

		case '$':
			if i == 0 {
				//不能发布到$ topicv5
				return nil, nil, fmt.Errorf("memtopic/nextTopicLevel: Cannot publish to $ topicv5")
			}

			s = constant.StateSYS

		default:
			if s == constant.StateMWC || s == constant.StateSWC {
				//通配符“#”和“+”必须占据整个主题级别
				return nil, nil, fmt.Errorf("memtopic/nextTopicLevel: Wildcard characters '#' and '+' must occupy entire topic level")
			}

			s = constant.StateCHR
		}
	}

	//如果我们到了这里，那就意味着我们没有中途到达分隔符，所以
	//主题要么为空，要么不包含分隔符。不管怎样，我们都会返回完整的主题
	return topic, nil, nil
}

//响应订阅发送的有效负载消息的QoS必须为
//原始发布消息的QoS的最小值(在本例中为
// qos参数)和服务器授予的最大qos(在本例中为主题树中的QoS)。
//也有可能，即使主题匹配，订阅服务器也不包括在内
//由于授予的QoS低于发布消息的QoS。例如,
//如果只授予客户端QoS 0，且发布消息为QoS 1，则为
//客户端不能发送已发布的消息。
func (t *snode) matchQos(qos byte, subs *[]interface{}, qoss *[]topic.Sub) {
	for i, sub := range t.subs {
		//如果发布的QoS高于订阅者的QoS，则跳过
		//用户。否则，添加到列表中。
		//if qos <= t.qos[i] {
		//	*subs = append(*subs, sub)
		//	*qoss = append(*qoss, qos)
		//}
		// TODO 修改为取二者最小值qos
		if qos <= t.qos[i].Qos {
			*qoss = append(*qoss, topic.Sub{
				Topic:             t.qos[i].Topic,
				Qos:               qos,
				NoLocal:           t.qos[i].NoLocal,
				RetainAsPublished: t.qos[i].RetainAsPublished,
				RetainHandling:    t.qos[i].RetainHandling,
				SubIdentifier:     t.qos[i].SubIdentifier,
			})
		} else {
			*qoss = append(*qoss, t.qos[i])
		}
		*subs = append(*subs, sub)
	}
}

func equal(k1, k2 interface{}) bool {
	if reflect.TypeOf(k1) != reflect.TypeOf(k2) {
		return false
	}

	if reflect.ValueOf(k1).Kind() == reflect.Func {
		return &k1 == &k2
	}

	if k1 == k2 {
		return true
	}

	switch k1 := k1.(type) {
	case string:
		return k1 == k2.(string)

	case int64:
		return k1 == k2.(int64)

	case int32:
		return k1 == k2.(int32)

	case int16:
		return k1 == k2.(int16)

	case int8:
		return k1 == k2.(int8)

	case int:
		return k1 == k2.(int)

	case float32:
		return k1 == k2.(float32)

	case float64:
		return k1 == k2.(float64)

	case uint:
		return k1 == k2.(uint)

	case uint8:
		return k1 == k2.(uint8)

	case uint16:
		return k1 == k2.(uint16)

	case uint32:
		return k1 == k2.(uint32)

	case uint64:
		return k1 == k2.(uint64)

	case uintptr:
		return k1 == k2.(uintptr)
	}

	return false
}
