package cron

import "github.com/robfig/cron/v3"

type Icron interface {
	AddJob(spec string, id string, job cron.Job) error
	GetJob(id string) (cron.Job, bool)
	Remove(id string)
	Start()
	Stop()
}
