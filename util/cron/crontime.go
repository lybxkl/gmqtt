package cron

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// GetDisplayCycleTime 获取cronTab转化周期时间
func GetDisplayCycleTime(cycleTime string) []string {
	result := make([]string, 0)
	specParser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := specParser.Parse(cycleTime)
	if err != nil {
		fmt.Println(err)
		return result
	}
	return printCycleTime(sched, time.Now(), 5, result)
}

func printCycleTime(sched cron.Schedule, time time.Time, printCount int, result []string) []string {
	if printCount > 0 {
		printCount--
		time = sched.Next(time)
		result = append(result, time.Format("2006-01-02 15:04:05"))
		return printCycleTime(sched, time, printCount, result)
	}
	return result
}
