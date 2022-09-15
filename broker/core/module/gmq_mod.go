package module

import (
	"go.uber.org/atomic"

	"github.com/lybxkl/gmqtt/broker/core/message"
)

type Option func()

// Mod 模块化
type Mod interface {
	Hand(msg message.Message, opt ...Option) error
	Open() error
	Stop() error
}

type BaseMod struct {
	open atomic.Bool
}

func (b *BaseMod) Hand(msg message.Message, opt ...Option) error {
	return nil
}

func (b *BaseMod) Open() error {
	b.open.Store(true)
	return nil
}

func (b *BaseMod) Stop() error {
	b.open.Store(false)
	return nil
}

func (b *BaseMod) IsOpen() bool {
	return b.open.Load()
}
