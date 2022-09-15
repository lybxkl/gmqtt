// 系统主题
package sys

import (
	"fmt"
	"reflect"
	"sync"

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
	sroot *rSnode

	// Retained message mutex
	rmu sync.RWMutex
}

func NewMemProvider() *memTopics {
	return &memTopics{
		sroot: newRSNode(),
	}
}

func (node *memTopics) Subscribe(subs topic.Sub, sub interface{}) (byte, error) {
	if !message.ValidQos(subs.Qos) {
		return message.QosFailure, fmt.Errorf("invalid QoS %d", subs.Qos)
	}

	if sub == nil {
		return message.QosFailure, fmt.Errorf("subscriber cannot be nil")
	}

	node.smu.Lock()
	defer node.smu.Unlock()

	subs.Qos = util.Qos(subs.Qos)

	if err := node.sroot.sinsert(subs, sub); err != nil {
		return message.QosFailure, err
	}

	return subs.Qos, nil
}

func (node *memTopics) Unsubscribe(topic []byte, sub interface{}) error {
	node.smu.Lock()
	defer node.smu.Unlock()

	return node.sroot.sremove(topic, sub)
}

func (node *memTopics) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]topic.Sub) error {
	if !message.ValidQos(qos) {
		return fmt.Errorf("invalid QoS %d", qos)
	}

	node.smu.RLock()
	defer node.smu.RUnlock()

	*subs = (*subs)[0:0]
	*qoss = (*qoss)[0:0]

	return node.sroot.smatch(topic, qos, subs, qoss)
}

func (node *memTopics) Close() error {
	node.sroot = nil
	return nil
}

// subscrition nodes
type rSnode struct {
	// If node is the end of the topic string, then add subscribers here
	subs []interface{}
	qos  []byte

	// Otherwise add the next topic level here
	rsnodes map[string]*rSnode
}

func newRSNode() *rSnode {
	return &rSnode{
		rsnodes: make(map[string]*rSnode),
	}
}

func (node *rSnode) sinsert(subs topic.Sub, sub interface{}) error {
	// If there's no more topic levels, that means we are at the matching snode
	// to insert the subscriber. So let's see if there's such subscriber,
	// if so, update it. Otherwise insert it.
	if len(subs.Topic) == 0 {
		// Let's see if the subscriber is already on the list. If yes, update
		// QoS and then return.
		for i := range node.subs {
			if equal(node.subs[i], sub) {
				node.qos[i] = subs.Qos
				return nil
			}
		}

		// Otherwise add.
		node.subs = append(node.subs, sub)
		node.qos = append(node.qos, subs.Qos)

		return nil
	}

	// Not the last level, so let's find or create the next level snode, and
	// recursively call it's insert().

	// ntl = next topic level
	ntl, rem, err := nextRTopicLevel(subs.Topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Add snode if it doesn't already exist
	n, ok := node.rsnodes[level]
	if !ok {
		n = newRSNode()
		node.rsnodes[level] = n
	}
	subs.Topic = rem
	return n.sinsert(subs, sub)
}

// This remove implementation ignores the QoS, as long as the subscriber
// matches then it's removed
func (node *rSnode) sremove(topic []byte, sub interface{}) error {
	// If the topic is empty, it means we are at the final matching snode. If so,
	// let's find the matching subscribers and remove them.
	if len(topic) == 0 {
		// If subscriber == nil, then it's signal to remove ALL subscribers
		if sub == nil {
			node.subs = node.subs[0:0]
			node.qos = node.qos[0:0]
			return nil
		}

		// If we find the subscriber then remove it from the list. Technically
		// we just overwrite the slot by shifting all other items up by one.
		for i := range node.subs {
			if equal(node.subs[i], sub) {
				node.subs = append(node.subs[:i], node.subs[i+1:]...)
				node.qos = append(node.qos[:i], node.qos[i+1:]...)
				return nil
			}
		}

		return fmt.Errorf("memtopics/remove: No topic found for subscriber")
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
	n, ok := node.rsnodes[level]
	if !ok {
		return fmt.Errorf("memtopics/remove: No topic found")
	}

	// Remove the subscriber from the next level snode
	if err := n.sremove(rem, sub); err != nil {
		return err
	}

	// If there are no more subscribers and snodes to the next level we just visited
	// let's remove it
	if len(n.subs) == 0 && len(n.rsnodes) == 0 {
		delete(node.rsnodes, level)
	}

	return nil
}

// smatch() returns all the subscribers that are subscribed to the topic. Given a topic
// with no wildcards (publish topic), it returns a list of subscribers that subscribes
// to the topic. For each of the level names, it's a match
// - if there are subscribers to '#', then all the subscribers are added to result set
func (node *rSnode) smatch(topic []byte, qos byte, subs *[]interface{}, qoss *[]topic.Sub) error {
	// If the topic is empty, it means we are at the final matching snode. If so,
	// let's find the subscribers that match the qos and append them to the list.
	if len(topic) == 0 {
		node.matchQos(qos, subs, qoss)
		return nil
	}

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	for k, n := range node.rsnodes {
		// If the key is "#", then these subscribers are added to the result set
		if k == consts.MWC {
			n.matchQos(qos, subs, qoss)
		} else if k == consts.SWC || k == level {
			if err := n.smatch(rem, qos, subs, qoss); err != nil {
				return err
			}
		}
	}

	return nil
}

// retained messagev5 nodes
type rRnode struct {
	// If node is the end of the topic string, then add retained messages here
	msg *message.PublishMessage
	buf []byte

	// Otherwise add the next topic level here
	rrnodes map[string]*rRnode
}

func newRRNode() *rRnode {
	return &rRnode{
		rrnodes: make(map[string]*rRnode),
	}
}

func (node *rRnode) rinsert(topic []byte, msg *message.PublishMessage) error {
	// If there's no more topic levels, that means we are at the matching rnode.
	if len(topic) == 0 {
		l := msg.Len()

		// Let's reuse the buffer if there's enough space
		if l > cap(node.buf) {
			node.buf = make([]byte, l)
		} else {
			node.buf = node.buf[0:l]
		}

		if _, err := msg.Encode(node.buf); err != nil {
			return err
		}

		// Reuse the messagev5 if possible
		if node.msg == nil {
			node.msg = message.NewPublishMessage()
		}

		if _, err := node.msg.Decode(node.buf); err != nil {
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
	n, ok := node.rrnodes[level]
	if !ok {
		n = newRRNode()
		node.rrnodes[level] = n
	}

	return n.rinsert(rem, msg)
}

// Remove the retained messagev5 for the supplied topic
func (node *rRnode) rremove(topic []byte) error {
	// If the topic is empty, it means we are at the final matching rnode. If so,
	// let's remove the buffer and messagev5.
	if len(topic) == 0 {
		node.buf = nil
		node.msg = nil
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
	n, ok := node.rrnodes[level]
	if !ok {
		return fmt.Errorf("memtopics/rremove: No topic found")
	}

	// Remove the subscriber from the next level rnode
	if err := n.rremove(rem); err != nil {
		return err
	}

	// If there are no more rnodes to the next level we just visited let's remove it
	if len(n.rrnodes) == 0 {
		delete(node.rrnodes, level)
	}

	return nil
}

// rmatch() finds the retained messages for the topic and qos provided. It's somewhat
// of a reverse match compare to match() since the supplied topic can contain
// wildcards, whereas the retained messagev5 topic is a full (no wildcard) topic.
func (node *rRnode) rmatch(topic []byte, msgs *[]*message.PublishMessage) error {
	// If the topic is empty, it means we are at the final matching rnode. If so,
	// add the retained msg to the list.
	if len(topic) == 0 {
		if node.msg != nil {
			*msgs = append(*msgs, node.msg)
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
		// If '#', add all retained messages starting node node
		node.allRetained(msgs)
	} else if level == consts.SWC {
		// If '+', check all nodes at node level. Next levels must be matched.
		for _, n := range node.rrnodes {
			if err := n.rmatch(rem, msgs); err != nil {
				return err
			}
		}
	} else {
		// Otherwise, find the matching node, go to the next level
		if n, ok := node.rrnodes[level]; ok {
			if err := n.rmatch(rem, msgs); err != nil {
				return err
			}
		}
	}

	return nil
}

func (node *rRnode) allRetained(msgs *[]*message.PublishMessage) {
	if node.msg != nil {
		*msgs = append(*msgs, node.msg)
	}

	for _, n := range node.rrnodes {
		n.allRetained(msgs)
	}
}

// Returns topic level, remaining topic levels and any errors
func nextRTopicLevel(topic []byte) ([]byte, []byte, error) {
	s := consts.StateCHR

	for i, c := range topic {
		switch c {
		case '/':
			if s == consts.StateMWC {
				return nil, nil, fmt.Errorf("redistopics/nextTopicLevel: Multi-level wildcard found in topic and it's not at the last level")
			}

			if i == 0 {
				return []byte(consts.SWC), topic[i+1:], nil
			}

			return topic[:i], topic[i+1:], nil

		case '#':
			if i != 0 {
				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Wildcard character '#' must occupy entire topic level")
			}

			s = consts.StateMWC

		case '+':
			if i != 0 {
				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Wildcard character '+' must occupy entire topic level")
			}

			s = consts.StateSWC

		case '$':
			if i == 0 {
				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Cannot publish to $ topics")
			}

			s = consts.StateSYS

		default:
			if s == consts.StateMWC || s == consts.StateSWC {
				return nil, nil, fmt.Errorf("redistopics/nextTopicLevel: Wildcard characters '#' and '+' must occupy entire topic level")
			}

			s = consts.StateCHR
		}
	}

	// If we got here that means we didn't hit the separator along the way, so the
	// topic is either empty, or does not contain a separator. Either way, we return
	// the full topic
	return topic, nil, nil
}

// The QoS of the payload messages sent in response to a subscription must be the
// minimum of the QoS of the originally published messagev5 (in node case, it's the
// qos parameter) and the maximum QoS granted by the server (in node case, it's
// the QoS in the topic tree).
//
// It's also possible that even if the topic matches, the subscriber is not included
// due to the QoS granted is lower than the published messagev5 QoS. For example,
// if the client is granted only QoS 0, and the publish messagev5 is QoS 1, then node
// client is not to be send the published messagev5.
func (node *rSnode) matchQos(qos byte, subs *[]interface{}, qoss *[]topic.Sub) {
	for i, sub := range node.subs {
		// If the published QoS is higher than the subscriber QoS, then we skip the
		// subscriber. Otherwise, add to the list.
		if qos <= node.qos[i] {
			*subs = append(*subs, sub)
			*qoss = append(*qoss, topic.Sub{Qos: qos})
		}
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

// Returns topic level, remaining topic levels and any errors
//返回主题级别、剩余的主题级别和任何错误
func nextTopicLevel(topic []byte) ([]byte, []byte, error) {
	s := consts.StateCHR

	//遍历topic，判断是何种类型的主题
	for i, c := range topic {
		switch c {
		case '/':
			if s == consts.StateMWC {
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

			s = consts.StateMWC

		case '+':
			if i != 0 {
				//通配符“+”必须占据整个主题级别
				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Wildcard character '+' must occupy entire topic level")
			}

			s = consts.StateSWC

		case '$':
			if i == 0 {
				//不能发布到$ topics
				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Cannot publish to $ topics")
			}

			s = consts.StateSYS

		default:
			if s == consts.StateMWC || s == consts.StateSWC {
				//通配符“#”和“+”必须占据整个主题级别
				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Wildcard characters '#' and '+' must occupy entire topic level")
			}

			s = consts.StateCHR
		}
	}

	//如果我们到了这里，那就意味着我们没有中途到达分隔符，所以
	//主题要么为空，要么不包含分隔符。不管怎样，我们都会返回完整的主题
	return topic, nil, nil
}
