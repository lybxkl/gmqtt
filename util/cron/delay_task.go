package cron

import (
	"fmt"
	"time"

	. "github.com/lybxkl/gmqtt/common/log"
)

var DelayTaskManager = NewMemDelayTaskManage()

type ID = string

type DelayTask struct {
	ID             ID
	DealTime       time.Duration // 处理时间
	Data           interface{}
	Fn             func(data interface{})
	CancelCallback func()
	icron          Icron
}

func (g *DelayTask) Run() {
	defer func() {
		if err := recover(); err != nil {
			Log.Error(err)
		}
		g.icron.Remove(g.ID)
	}()
	g.Fn(g.Data)
}

type DelayTaskManage interface {
	Run(*DelayTask) error
	Cancel(ID)
}

type memDelayTaskManage struct {
	icron Icron
}

func NewMemDelayTaskManage() DelayTaskManage {
	return &memDelayTaskManage{icron: Get()}
}

func (d *memDelayTaskManage) Run(task *DelayTask) error {
	Log.Debugf("添加%s的延迟发送任务, 延迟时间：%ds", task.ID, task.DealTime)
	if task.DealTime <= 0 {
		//task.DealTime = 1
		go func() {
			task.Fn(task.Data)
		}()
		return nil
	}
	task.icron = d.icron

	err := d.icron.AddJob(fmt.Sprintf("@every %ds", task.DealTime), task.ID, task)
	return err
}

func (d *memDelayTaskManage) Cancel(id ID) {
	Log.Debugf("取消%s的延迟发送任务", id)
	job, exist := d.icron.GetJob(id)
	if !exist {
		return
	}
	d.icron.Remove(id)

	if task, ok := job.(*DelayTask); ok {
		task.CancelCallback() // 执行取消任务回调方法
	}
}
