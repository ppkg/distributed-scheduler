package dto

import "distributed-scheduler/model"

type JobInfo struct {
	Job      *model.Job
	TaskList []*model.Task
}
