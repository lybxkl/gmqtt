package topic_metrics

import (
	"github.com/lybxkl/gmqtt/broker/core/message"
	"github.com/lybxkl/gmqtt/broker/core/module"
)

type topicMetrics struct {
	*module.BaseMod
}

// Hand ... https://www.emqx.io/docs/zh/v4.3/advanced/metrics-and-stats.html#metrics-stats
func (t *topicMetrics) Hand(msg message.Message, opt ...module.Option) error {
	if !t.IsOpen() {
		return nil
	}
	// todo
	return nil
}
