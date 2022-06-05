package gopool

import (
	"errors"
	"io"

	"github.com/panjf2000/ants/v2"

	. "github.com/lybxkl/gmqtt/common/log"
	"github.com/lybxkl/gmqtt/util"
)

var (
	taskGPool     *ants.Pool
	taskGPoolSize int
)

func InitServiceTaskPool(poolSize int) (close io.Closer) {
	var err error
	taskGPool, err = ants.NewPool(poolSize, ants.WithPanicHandler(func(i interface{}) {
		Log.Errorf("协程池处理错误：%v", i)
	}), ants.WithMaxBlockingTasks(poolSize*2))
	util.MustPanic(err)

	taskGPoolSize = poolSize
	return &closer{}
}

type closer struct {
}

func (closer closer) Close() error {
	taskGPool.Release()
	return nil
}

func Submit(f func()) {
	dealAntsErr(taskGPool.Submit(f))
}

func dealAntsErr(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, ants.ErrPoolClosed) {
		Log.Errorf("协程池错误：%v", err.Error())
		taskGPool.Reboot()
	}
	if errors.Is(err, ants.ErrPoolOverload) {
		Log.Errorf("协程池超载：%v", err.Error())
		taskGPool.Tune(int(float64(taskGPoolSize) * 1.25))
	}
	Log.Errorf("线程池处理异常：%v", err)
}
