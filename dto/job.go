package dto

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/ppkg/distributed-scheduler/enum"
	"github.com/ppkg/distributed-scheduler/model"
)

type JobInfo struct {
	DoneLatch *CountDownLatch
	Job       *model.Job
	TaskList  *concurrentTask
	// 已执行失败task
	ExceptionTask *concurrentTask
}

// 初始化已完成task channel
func (s *JobInfo) InitDoneChannel() {
	finishTask := s.FilterFinishEndTask()
	cacheSize := int(s.Job.Size) - len(finishTask)
	s.DoneLatch = NewCountDownLatch(cacheSize)
	s.ExceptionTask = NewConcurrentTask()
}

// 过滤出已完成最后task
func (s *JobInfo) FilterFinishEndTask() []*model.Task {
	pluginList := strings.Split(s.Job.PluginSet, ",")
	targetPlugin := pluginList[len(pluginList)-1]
	var taskList []*model.Task
	for _, item := range s.TaskList.GetAll() {
		if enum.TaskStatus(item.Status) == enum.FinishTaskStatus && item.Plugin == targetPlugin {
			taskList = append(taskList, item)
		}
	}
	return taskList
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
			return "", fmt.Errorf("JobInfo/Reduce 反序列化异常,data:%s,err:%+v", task.Output, err)
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

type concurrentTask struct {
	data []*model.Task
	lock sync.RWMutex
}

func (s *concurrentTask) Append(task *model.Task) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.data = append(s.data, task)
}

func (s *concurrentTask) GetAll() []*model.Task {
	s.lock.RLock()
	defer s.lock.RUnlock()
	list := make([]*model.Task, 0, len(s.data))
	list = append(list, s.data...)
	return list
}

func (s *concurrentTask) Size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.data)
}

func NewConcurrentTask(list ...*model.Task) *concurrentTask {
	return &concurrentTask{
		data: list,
	}
}
