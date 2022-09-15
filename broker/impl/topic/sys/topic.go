package sys

import (
	"errors"

	"github.com/lybxkl/gmqtt/broker/core/topic"
)

var UnSupportTopic = errors.New("the system topic is not supported")

// TopicProvider 系统主题
type TopicProvider interface {
	Subscribe(subs topic.Sub, subscriber interface{}) (byte, error)
	Unsubscribe(topic []byte, subscriber interface{}) error
	Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]topic.Sub) error
	Close() error
}
