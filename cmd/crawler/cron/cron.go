package cron

import (
	"github.com/go-co-op/gocron/v2"
)

type Scheduler struct {
	s gocron.Scheduler
}

func NewScheduler(scheduler gocron.Scheduler) *Scheduler {
	return &Scheduler{
		s: scheduler,
	}
}
