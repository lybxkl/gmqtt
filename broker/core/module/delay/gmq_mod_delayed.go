package delay

import (
	"github.com/lybxkl/gmqtt/broker/core/message"
	"github.com/lybxkl/gmqtt/broker/core/module"
)

// 延迟模块
const (
	// $delayed/{DelayInterval}/{TopicName}
	//$delayed: 使用 $delay 作为主题前缀的消息都将被视为需要延迟发布的消息。延迟间隔由下一主题层级中的内容决定。
	//{DelayInterval}: 指定该 MQTT 消息延迟发布的时间间隔，单位是秒，允许的最大间隔是 4294967 秒。
	//      如果 {DelayInterval} 无法被解析为一个整型数字，broker 将丢弃该消息，客户端不会收到任何信息。
	//{TopicName}: MQTT 消息的主题名称。
	prefix = "$delayed/"
)

type delayMod struct {
	*module.BaseMod
}

// Hand ... https://www.emqx.io/docs/zh/v4.3/advanced/delay-publish.html
func (d *delayMod) Hand(msg message.Message, opt ...module.Option) error {
	if msg.Type() != message.PUBLISH || d.IsOpen() {
		return nil
	}
	// TODO
	return nil
}
