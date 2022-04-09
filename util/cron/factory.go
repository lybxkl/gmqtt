package cron

import (
	"sync"
)

var (
	scheduleCron = new(ScheduleCron)

	once sync.Once
)

func Get() Icron {
	once.Do(func() {
		scheduleCron.initCron()
		scheduleCron.Start()
	})
	return scheduleCron
}
