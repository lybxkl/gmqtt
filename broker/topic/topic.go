package topic

import (
	"gmqtt/broker/message"
)

type Sub struct {
	Topic             string
	Qos               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    message.RetainHandling
	SubIdentifier     uint32 // 订阅标识符
}

// Manager 主题管理者
type Manager interface {
	Subscribe(sub Sub, subscriber interface{}) (byte, error)
	Unsubscribe(topic []byte, subscriber interface{}) error
	// svc 表示是服务端下发的数据，系统主题消息
	// shareName 为空表示不需要发送任何共享消息，不为空表示只需要发送当前shareName下的订阅者
	// 系统主题消息和共享主题消息，不能同时获取，系统主题优先于共享主题

	// if shareName == "" && onlyShare == false ===>> 表示不需要获取任何共享主题订阅者，只需要所有非共享组的订阅者们
	// if shareName == "" && onlyShare == true  ===>> 表示获取当前主题shareName的所有共享组每个的组的一个订阅者，不需要所有非共享组的订阅者们
	// if onlyShare == false && shareName != "" ===>> 获取当前主题的共享组名为shareName的订阅者一个与所有非共享组订阅者们
	// if onlyShare == true && shareName != ""  ===>> 仅仅获取主题的共享组名为shareName的订阅者一个

	Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]Sub, svc bool, shareName string, onlyShare bool) error
	AllSubInfo() (map[string][]string, error) // 获取所有的共享订阅，k: 主题，v: 该主题的所有共享组
	Retain(msg *message.PublishMessage) error
	Retained(topic []byte, msgs *[]*message.PublishMessage) error
	Close() error
}
