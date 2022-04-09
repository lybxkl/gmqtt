package cron

import (
	"github.com/robfig/cron/v3"

	"sync"
)

type ScheduleCron struct {
	schedule *cron.Cron

	ids sync.Map
}

func (s *ScheduleCron) initCron() {
	if s.schedule == nil {
		s.schedule = cron.New(cron.WithSeconds(), cron.WithChain(cron.Recover(cron.DefaultLogger)))
	}
}

func (s *ScheduleCron) AddJob(spec, id string, job cron.Job) error {
	entryID, err := s.schedule.AddJob(spec, job)
	s.ids.Store(id, entryID)
	return err
}

func (s *ScheduleCron) Remove(id string) {
	v, exist := s.ids.LoadAndDelete(id)
	if !exist {
		return
	}
	s.schedule.Remove(v.(cron.EntryID))
}

func (s *ScheduleCron) Start() {
	s.schedule.Start()
}

func (s *ScheduleCron) Stop() {
	s.schedule.Stop()
}
