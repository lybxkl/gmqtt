package middleware

import (
	. "github.com/lybxkl/gmqtt/common/log"
)

type Options []Option

func (ops *Options) Apply(option Option) {
	*ops = append(*ops, option)
}

type Option interface {
	// Apply 返回bool true: 表示后面的非空error是可忽略的错误，不影响后面的中间件执行
	Apply(message interface{}) (canSkipErr bool, err error)
}

type console struct {
}

func (c *console) Apply(msg interface{}) (bool, error) {
	Log.Infof("[middleware]==> %+v\n", msg)
	return true, nil
}

func WithConsole() Option {
	return &console{}
}
