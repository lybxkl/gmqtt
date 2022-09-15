package core

import (
	"context"
	"time"

	"github.com/lybxkl/gmqtt/broker/core/auth"
	sess "github.com/lybxkl/gmqtt/broker/core/session"
	"github.com/lybxkl/gmqtt/broker/core/store"
	"github.com/lybxkl/gmqtt/broker/core/topic"
	"github.com/lybxkl/gmqtt/common/log"
	"github.com/lybxkl/gmqtt/util/waitgroup"
)

type core struct {
	am  auth.Manager
	tm  topic.Manager
	ssm sess.Manager

	mst store.MessageStore
	sst store.SessionStore
}

func (c *core) Close() error {
	fn := make([]func() error, 4)
	fn = append(fn, c.ssm.Close, c.tm.Close, func() error {
		return c.mst.Stop(context.Background())
	}, func() error {
		return c.sst.Stop(context.Background())
	})

	wg := waitgroup.WithTimeout(context.Background(), time.Second*30)

	for _, f := range fn {
		f1 := f
		wg.Go(func(ctx context.Context) error {
			return f1()
		})
	}
	if e := wg.Wait(); e != nil {
		log.Log.Errorf("core close err %+v", e)
	}
	return nil
}
