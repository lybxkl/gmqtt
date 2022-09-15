package share

import (
	"github.com/lybxkl/gmqtt/broker/core/message"
	"github.com/lybxkl/gmqtt/broker/core/topic"
)

// TopicProvider 共享订阅 没有 保留消息
type TopicProvider interface {
	Subscribe(shareName []byte, sub topic.Sub, subscriber interface{}) (byte, error)
	Unsubscribe(topic, shareName []byte, subscriber interface{}) error
	Subscribers(topic, shareName []byte, qos byte, subs *[]interface{}, qoss *[]topic.Sub) error
	AllSubInfo() (map[string][]string, error) // 获取所有的共享订阅，k: 主题，v: 该主题的所有共享组
	Retain(msg *message.PublishMessage, shareName []byte) error
	Retained(topic, shareName []byte, msgs *[]*message.PublishMessage) error
	Close() error
}
