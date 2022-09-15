// 共享订阅
package share

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/lybxkl/gmqtt/broker/core/message"
	"github.com/lybxkl/gmqtt/broker/core/topic"

	consts "github.com/lybxkl/gmqtt/common/constant"
	"github.com/lybxkl/gmqtt/util"
)

var _ TopicProvider = (*memTopics)(nil)

type memTopics struct {
	// Sub/unsub mutex
	smu sync.RWMutex
	// Subscription tree
	sroot *snode

	// Retained message mutex
	rmu sync.RWMutex
	// Retained messages topic tree
	rroot *rnode
}

// NewMemProvider 返回memTopics的一个新实例，该实例实现了
// TopicsProvider接口。memProvider是存储主题的隐藏结构
//订阅并保留内存中的消息。内容不是这样持久化的
//当服务器关闭时，所有东西都将消失。小心使用。
func NewMemProvider() *memTopics {
	return &memTopics{
		sroot: newSNode(),
		rroot: newRNode(),
	}
}

// Subscribe 订阅主题
func (t *memTopics) Subscribe(shareName []byte, subs topic.Sub, sub interface{}) (byte, error) {
	if !message.ValidQos(subs.Qos) {
		return message.QosFailure, fmt.Errorf("invalid QoS %d", subs.Qos)
	}

	if sub == nil {
		return message.QosFailure, fmt.Errorf("subscriber cannot be nil")
	}

	t.smu.Lock()
	defer t.smu.Unlock()

	subs.Qos = util.Qos(subs.Qos)

	if err := t.sroot.sinsert(shareName, subs, sub); err != nil {
		return message.QosFailure, err
	}

	return subs.Qos, nil
}

func (t *memTopics) Unsubscribe(topic, shareName []byte, sub interface{}) error {
	t.smu.Lock()
	defer t.smu.Unlock()

	return t.sroot.sremove(topic, shareName, sub)
}

// Subscribers returned values will be invalidated by the next Subscribers call
//返回的值将在下一次订阅者调用时失效
func (t *memTopics) Subscribers(topic, shareName []byte, qos byte, subs *[]interface{}, qoss *[]topic.Sub) error {
	if !message.ValidQos(qos) {
		return fmt.Errorf("invalid QoS %d", qos)
	}

	t.smu.RLock()
	defer t.smu.RUnlock()

	*subs = (*subs)[0:0]
	*qoss = (*qoss)[0:0]

	return t.sroot.smatch(topic, shareName, qos, subs, qoss)
}

// AllSubInfo 获取所有的共享订阅，k: 主题，v: 该主题的所有共享组
// 这个会在服务停止时调用，但是再某些情况会调用失败，比如与redis也断开了连接
func (t *memTopics) AllSubInfo() (map[string][]string, error) {
	v := make(map[string][]string)
	t.smu.RLock()
	defer t.smu.RUnlock()
	err := t.sroot.smatchAll(v)
	return v, err
}

func (t *memTopics) Retain(msg *message.PublishMessage, shareName []byte) error {
	t.rmu.Lock()
	defer t.rmu.Unlock()

	//很明显，至少根据MQTT一致性/互操作性
	//测试，有效载荷为0表示删除retain消息。
	// https://eclipse.org/paho/clients/testing/
	if len(msg.Payload()) == 0 {
		return t.rroot.rremove(msg.Topic(), shareName)
	}

	return t.rroot.rinsert(msg.Topic(), shareName, msg)
}

func (t *memTopics) Retained(topic, shareName []byte, msgs *[]*message.PublishMessage) error {
	t.rmu.RLock()
	defer t.rmu.RUnlock()

	return t.rroot.rmatch(topic, shareName, msgs)
}

func (t *memTopics) Close() error {
	t.sroot = nil
	t.rroot = nil
	return nil
}

type sins struct {
	subs []interface{}
	qos  []byte
}

//subscrition节点
type snode struct {
	//如果这是主题字符串的结尾，在这里添加订阅者
	shares map[string]*sins // shareName : *sin

	snodes map[string]*snode // topic: nextNode
}

func newSNode() *snode {
	return &snode{
		shares: make(map[string]*sins),
		snodes: make(map[string]*snode),
	}
}

func (t *snode) sinsert(shareName []byte, subs topic.Sub, sub interface{}) error {
	//如果没有更多的主题级别，这意味着我们在匹配的snode
	//插入订阅。看是否有这样的订阅者， 如果有，更新它。否则插入它。
	if len(subs.Topic) == 0 {
		sn := string(shareName)
		if v, ok := t.shares[sn]; ok {
			//看用户是否已经在名单上了。如果是的,更新QoS，然后返回。
			for i := range v.subs { // 重复订阅，可更新qos
				if equal(v.subs[i], sub) {
					v.qos[i] = subs.Qos
					return nil
				}
			}
			v.subs = append(v.subs, sub)
			v.qos = append(v.qos, subs.Qos)
			return nil
		}
		// Otherwise add.
		sin := &sins{
			subs: make([]interface{}, 0),
			qos:  make([]byte, 0),
		}
		sin.subs = append(sin.subs, sub)
		sin.qos = append(sin.qos, subs.Qos)
		t.shares[sn] = sin

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
	return n.sinsert(shareName, subs, sub)
}

// This remove implementation ignores the QoS, as long as the subscriber
// matches then it's removed
func (t *snode) sremove(topic, shareName []byte, sub interface{}) error {
	//如果主题是空的，这意味着我们到达了最后一个匹配的snode。 如果是这样,
	//找到匹配的订阅服务并删除它们。
	if len(topic) == 0 {
		//如果订阅者== nil，则发出删除当前shareName的所有订阅者的信号
		// 如果shareName为空，则标识删除当前主题下的所有共享主题订阅者们
		if sub == nil {
			if len(shareName) == 0 {
				t.shares = make(map[string]*sins)
			} else {
				sn := string(shareName)
				// 共享订阅如果没有订阅者了，不需要再维护这个共享主题了
				delete(t.shares, sn)
				//if v, ok := t.shares[sn];ok{
				//	v.subs = v.subs[0:0]
				//	v.qos = v.qos[0:0]
				//}
			}
			return nil
		}
		sn := string(shareName)
		if v, ok := t.shares[sn]; ok {
			//如果我们找到了用户，就把它从列表中删除
			for i := range v.subs {
				if equal(v.subs[i], sub) {
					v.subs = append(v.subs[:i], v.subs[i+1:]...)
					v.qos = append(v.qos[:i], v.qos[i+1:]...)
					return nil
				}
			}
		}
		return fmt.Errorf("memtopics/remove: No topic found for subscriber")
	}

	//不是最后一层，递归地找到下一层snode 调用它的remove()。
	// ntl =下一个主题级别
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Find the snode that matches the topic level
	n, ok := t.snodes[level]
	if !ok {
		return fmt.Errorf("memtopics/remove: No topic found")
	}

	// Remove the subscriber from the next level snode
	if err := n.sremove(rem, shareName, sub); err != nil {
		return err
	}

	// If there are no more subscribers and snodes to the next level we just visited
	// let's remove it
	var tag = true
	for _, v := range n.shares {
		if len(v.subs) != 0 {
			tag = false
			break
		}
	}
	if tag && len(n.snodes) == 0 {
		delete(t.snodes, level)
	}

	return nil
}

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

func (t *snode) smatch(topic, shareName []byte, qos byte, subs *[]interface{}, qoss *[]topic.Sub) error {
	//如果主题是空的，这意味着我们到达了最后一个匹配的snode。如果是这样,
	//让我们找到与qos匹配的订阅服务器并将它们附加到列表中。
	if len(topic) == 0 {
		t.matchQos(qos, subs, qoss, shareName)
		if v, ok := t.snodes["#"]; ok {
			v.matchQos(qos, subs, qoss, shareName)
		}
		if v, ok := t.snodes["+"]; ok {
			v.matchQos(qos, subs, qoss, shareName)
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
		if k == consts.MWC {
			n.matchQos(qos, subs, qoss, shareName)
		} else if k == consts.SWC || k == level {
			if rem != nil {
				if err := n.smatch(rem, shareName, qos, subs, qoss); err != nil {
					return err
				}
			} else { // 这个不需要匹配最后还有+的订阅者
				n.matchQos(qos, subs, qoss, shareName)
				if v, ok := n.snodes["#"]; ok {
					v.matchQos(qos, subs, qoss, shareName)
				}
			}
		}
	}

	return nil
}

// 匹配所有的
// DPS or BPS
func (t *snode) smatchAll(v map[string][]string) error {
	for k, n := range t.snodes {
		n.matchAll(v, []byte(k))
	}
	return nil
}

//保留信息节点
type rnode struct {
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

func (t *rnode) rinsert(topic, shareName []byte, msg *message.PublishMessage) error {
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

		// Reuse the messagev5 if possible
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

	return n.rinsert(rem, shareName, msg)
}

// Remove the retained messagev5 for the supplied topic
func (t *rnode) rremove(topic, shareName []byte) error {
	// If the topic is empty, it means we are at the final matching rnode. If so,
	// let's remove the buffer and messagev5.
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
		return fmt.Errorf("memtopics/rremove: No topic found")
	}

	// Remove the subscriber from the next level rnode
	if err := n.rremove(rem, shareName); err != nil {
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
// wildcards, whereas the retained messagev5 topic is a full (no wildcard) topic.
func (t *rnode) rmatch(topic, shareName []byte, msgs *[]*message.PublishMessage) error {
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

	if level == consts.MWC {
		// If '#', add all retained messages starting t node
		t.allRetained(msgs)
	} else if level == consts.SWC {
		// If '+', check all nodes at t level. Next levels must be matched.
		for _, n := range t.rnodes {
			if err := n.rmatch(rem, shareName, msgs); err != nil {
				return err
			}
		}
	} else {
		// Otherwise, find the matching node, go to the next level
		if n, ok := t.rnodes[level]; ok {
			if err := n.rmatch(rem, shareName, msgs); err != nil {
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

const (
	stateCHR byte = iota // Regular character 普通字符
	stateMWC             // Multi-level wildcard 多层次的通配符
	stateSWC             // Single-level wildcard 单层通配符
	stateSEP             // Topic level separator 主题水平分隔符
	stateSYS             // System level topic ($) 系统级主题($)
)

// Returns topic level, remaining topic levels and any errors
//返回主题级别、剩余的主题级别和任何错误
func nextTopicLevel(topic []byte) ([]byte, []byte, error) {
	s := stateCHR

	//遍历topic，判断是何种类型的主题
	for i, c := range topic {
		switch c {
		case '/':
			if s == stateMWC {
				//多层次通配符发现的主题，它不是在最后一层
				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Multi-level wildcard found in topic and it's not at the last level")
			}

			if i == 0 {
				return []byte(consts.SWC), topic[i+1:], nil
			}

			return topic[:i], topic[i+1:], nil

		case '#':
			if i != 0 {
				//通配符“#”必须占据整个主题级别
				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Wildcard character '#' must occupy entire topic level")
			}

			s = stateMWC

		case '+':
			if i != 0 {
				//通配符“+”必须占据整个主题级别
				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Wildcard character '+' must occupy entire topic level")
			}

			s = stateSWC

		case '$':
			if i == 0 {
				//不能发布到$ topics
				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Cannot publish to $ topics")
			}

			s = stateSYS

		default:
			if s == stateMWC || s == stateSWC {
				//通配符“#”和“+”必须占据整个主题级别
				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Wildcard characters '#' and '+' must occupy entire topic level")
			}

			s = stateCHR
		}
	}

	// If we got here that means we didn't hit the separator along the way, so the
	// topic is either empty, or does not contain a separator. Either way, we return
	// the full topic
	//如果我们到了这里，那就意味着我们没有中途到达分隔符，所以
	//主题要么为空，要么不包含分隔符。不管怎样，我们都会回来
	//完整的主题
	return topic, nil, nil
}

// The QoS of the payload messages sent in response to a subscription must be the
// minimum of the QoS of the originally published messagev5 (in t case, it's the
// qos parameter) and the maximum QoS granted by the server (in t case, it's
// the QoS in the topic tree).
//
// It's also possible that even if the topic matches, the subscriber is not included
// due to the QoS granted is lower than the published messagev5 QoS. For example,
// if the client is granted only QoS 0, and the publish messagev5 is QoS 1, then t
// client is not to be send the published messagev5.
//响应订阅发送的有效负载消息的QoS必须为
//原始发布消息的QoS的最小值(在本例中为
// qos参数)和服务器授予的最大qos(在本例中为
//主题树中的QoS)。
//也有可能，即使主题匹配，订阅服务器也不包括在内
//由于授予的QoS低于发布消息的QoS。例如,
//如果只授予客户端QoS 0，且发布消息为QoS 1，则为
//客户端不能发送已发布的消息。
func (t *snode) matchQos(qos byte, subs *[]interface{}, qoss *[]topic.Sub, shareName []byte) {
	if len(shareName) == 0 {
		for _, sub := range t.shares {
			if len(sub.subs) == 0 {
				continue
			}
			// 采用随机匹配
			i := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(sub.subs))
			// TODO 修改为取二者最小值qos
			if qos <= sub.qos[i] {
				*qoss = append(*qoss, topic.Sub{Qos: qos})
			} else {
				*qoss = append(*qoss, topic.Sub{Qos: sub.qos[i]})
			}
			*subs = append(*subs, sub.subs[i])
		}
		return
	}
	if sub, ok := t.shares[string(shareName)]; ok {
		if len(sub.subs) == 0 {
			return
		}
		// 采用随机匹配
		i := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(sub.subs))
		// TODO 修改为取二者最小值qos
		if qos <= sub.qos[i] {
			*qoss = append(*qoss, topic.Sub{Qos: qos})
		} else {
			*qoss = append(*qoss, topic.Sub{Qos: sub.qos[i]})
		}
		*subs = append(*subs, sub.subs[i])
	}
}

// 获取所有的
func (t *snode) matchAll(v map[string][]string, topic []byte) {
	tp := string(topic)
	for i, _ := range t.shares {
		if _, ok := v[tp]; !ok {
			v[tp] = make([]string, 0)
		}
		v[tp] = append(v[tp], i)
	}
	for p, sn := range t.snodes {
		temp := topic
		temp = append(temp, '/')
		temp = append(temp, p...)
		sn.matchAll(v, temp)
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
