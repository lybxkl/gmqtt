package sessimpl

import (
	"gmqtt/broker/message"
	"gmqtt/broker/topic"
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

func WithAckQueueSize(size int) Option {
	return func(s *session) {
		if size < defaultQueueSize {
			size = defaultQueueSize
		}
		s.queueSize = size
	}
}

func WithOldTopicToSess(s *session, sub []topic.Sub) {
	WithOldTopic(sub)(s)
}
func WithOldTopic(sub []topic.Sub) Option {
	return func(s *session) {
		for i := 0; i < len(sub); i++ {
			sb := sub[i]
			s.topics[sub[i].Topic] = &sb
		}
	}
}
