package cron

import "github.com/robfig/cron/v3"

type Icron interface {
	AddJob(spec string, id string, job cron.Job) error
	Remove(id string)
	Start()
	Stop()
}
