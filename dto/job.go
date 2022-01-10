package dto

import (
	"distributed-scheduler/enum"
	"distributed-scheduler/model"
	"strings"
	"sync"
)

type JobInfo struct {
	Done     chan int
	Job      *model.Job
	TaskList []*model.Task
	lock sync.Mutex
}

// 初始化已完成task channel
func (s *JobInfo) InitDoneChannel() {
	finishTask := s.FilterFinishEndTask()
	cacheSize := int(s.Job.Size) - len(finishTask)
	s.Done = make(chan int, cacheSize)
	if cacheSize == 0 {
		close(s.Done)
	}
}

// 过滤出已完成最后task
func (s *JobInfo) FilterFinishEndTask() []*model.Task {
	pluginList := strings.Split(s.Job.PluginSet, ",")
	targetPlugin := pluginList[len(pluginList)-1]
	var taskList []*model.Task
	for _, item := range s.TaskList {
		if item.Status == enum.FinishTaskStatus && item.Plugin == targetPlugin {
			taskList = append(taskList, item)
		}
	}
	return taskList
}

func (s *JobInfo) AppendSafeTask(task *model.Task)  {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.TaskList=append(s.TaskList, task)
}
