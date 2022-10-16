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

func (s *ScheduleCron) GetJob(id string) (cron.Job, bool) {
	v, exist := s.ids.Load(id)
	if !exist {
		return nil, false
	}
	e := s.schedule.Entry(v.(cron.EntryID))
	if e.Job == nil {
		return nil, false
	}
	return e.Job, true
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
