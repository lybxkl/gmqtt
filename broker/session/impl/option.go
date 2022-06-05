package sessimpl

import (
	"github.com/lybxkl/gmqtt/broker/message"
	"github.com/lybxkl/gmqtt/broker/topic"
)

type Option func(s *session)

func WithOfflineMsgToSess(s *session, offlineMsg []message.Message) {
	WithOfflineMsg(offlineMsg)(s)
}

func WithOfflineMsg(offlineMsg []message.Message) Option {
	return func(s *session) {
		s.offlineMsg = append(s.offlineMsg, offlineMsg...)
	}
}

func WithOldTopicToSess(s *session, sub []topic.Sub) {
	WithOldTopic(sub)(s)
}

func WithOldTopic(subs []topic.Sub) Option {
	return func(s *session) {
		for _, sub := range subs {
			b := sub
			s.topics[sub.Topic] = &b
		}
	}
}
