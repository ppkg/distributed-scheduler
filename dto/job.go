package dto

import (
	"distributed-scheduler/enum"
	"distributed-scheduler/model"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

type JobInfo struct {
	Done     chan int
	Job      *model.Job
	TaskList []*model.Task
	lock     sync.Mutex
	// 通知次数
	NotifyCount int32
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

func (s *JobInfo) AppendSafeTask(task *model.Task) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.TaskList = append(s.TaskList, task)
}

// 合并数据
func (s *JobInfo) Reduce() (string, error) {
	taskList := s.FilterFinishEndTask()
	var result []interface{}
	var err error
	for _, task := range taskList {
		// 如果输出结果为空则直接跳过
		if task.Output == "" {
			continue
		}
		var output interface{}
		err = json.Unmarshal([]byte(task.Output), &output)
		if err != nil {
			return "", fmt.Errorf("反序列化异常,data:%s,err:%+v", task.Output, err)
		}
		switch rs := output.(type) {
		case []interface{}:
			result = append(result, rs...)
		default:
			result = append(result, rs)
		}
	}

	if len(result) == 0 {
		return "", nil
	}

	data, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("序列化异常,data:%+v,err:%+v", result, err)
	}
	return string(data), nil
}
