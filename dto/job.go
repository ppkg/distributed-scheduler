package dto

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/ppkg/distributed-scheduler/enum"
	"github.com/ppkg/distributed-scheduler/model"
	"github.com/ppkg/glog"
)

type JobInfo struct {
	DoneLatch *CountDownLatch
	Job       *model.Job
	TaskList  *concurrentTask
	// 已执行失败task
	ExceptionTask   *concurrentTask
	lock            sync.Mutex
	lockParallelMap map[string]struct{}
}

func NewJobInfo(job *model.Job) *JobInfo {
	return &JobInfo{
		Job:             job,
		TaskList:        NewConcurrentTask(),
		ExceptionTask:   NewConcurrentTask(),
		lockParallelMap: make(map[string]struct{}),
	}
}

// 初始化已完成task channel
func (s *JobInfo) InitDoneChannel() {
	finishTask := s.FilterFinishEndTask()
	cacheSize := int(s.Job.Size) - len(finishTask)
	s.DoneLatch = NewCountDownLatch(cacheSize)
}

// 尝试并行任务锁
func (s *JobInfo) TryLockParallelTask(task *model.Task) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	key := fmt.Sprintf("%s_%d", task.Plugin, task.Sharding)
	if _, ok := s.lockParallelMap[key]; ok {
		return false
	}
	s.lockParallelMap[key] = struct{}{}
	return true
}

// 过滤出已完成最后task
func (s *JobInfo) FilterFinishEndTask() []*model.Task {
	pluginList := strings.Split(s.Job.PluginSet, ",")
	targetPlugin := pluginList[len(pluginList)-1]
	var taskList []*model.Task
	parallelTaskMap := make(map[string]struct{})
	for _, item := range s.TaskList.GetAll() {
		if enum.TaskStatus(item.Status) == enum.FinishTaskStatus && item.Plugin == targetPlugin {
			// 如果是并发task而且所有并发task未完成状态则跳过
			if IsParallelPlugin(targetPlugin) {
				key := fmt.Sprintf("%s_%d", item.Plugin, item.Sharding)
				if _, ok := parallelTaskMap[key]; ok {
					continue
				}
				result, err := s.ReduceParallel(item.Plugin, item.Sharding)
				if err != nil {
					glog.Errorf("JobInfo/FilterFinishEndTask 合并并行task结果异常,plugin:%s,sharding:%d,err:%+v", item.Plugin, item.Sharding, err)
					continue
				}
				parallelTaskMap[key] = struct{}{}
				taskList = append(taskList, &model.Task{
					Id:         item.Id,
					JobId:      item.JobId,
					Sharding:   item.Sharding,
					Name:       item.Name,
					Plugin:     item.Plugin,
					SubPlugin:  "",
					Status:     item.Status,
					Input:      item.Input,
					Output:     result,
					Message:    "",
					NodeId:     item.NodeId,
					CreateTime: item.CreateTime,
					UpdateTime: item.UpdateTime,
					FinishTime: item.FinishTime,
				})
				continue
			}
			// 普通task直接append
			taskList = append(taskList, item)
		}
	}
	return taskList
}

// 过滤出已完成的并行task
func (s *JobInfo) FilterFinishParallelTask(plugin string, sharding int32) []*model.Task {
	var list []*model.Task
	for _, item := range s.TaskList.GetAll() {
		if enum.TaskStatus(item.Status) == enum.FinishTaskStatus && item.Plugin == plugin && item.Sharding == sharding {
			list = append(list, item)
		}
	}
	return list
}

// 判断并行task是否都完成了
func (s *JobInfo) IsFinishParallelTask(plugin string, sharding int32) bool {
	list := s.FilterFinishParallelTask(plugin, sharding)
	return len(SplitParallelPlugin(plugin)) == len(list)
}

// 合并数据
func (s *JobInfo) Reduce() (string, error) {
	taskList := s.FilterFinishEndTask()
	return s.reduce(taskList)
}

func (s *JobInfo) reduce(taskList []*model.Task) (string, error) {
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
			return "", fmt.Errorf("合并数据时反序列化异常,data:%s,err:%+v", task.Output, err)
		}
		switch rs := output.(type) {
		case []interface{}:
			result = append(result, rs...)
		default:
			result = append(result, rs)
		}
	}

	if len(result) == 0 {
		return "[]", nil
	}

	data, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("合并数据时序列化异常,data:%+v,err:%+v", result, err)
	}
	return string(data), nil
}

// 合并并行task数据
func (s *JobInfo) ReduceParallel(plugin string, sharding int32) (string, error) {
	taskList := s.FilterFinishParallelTask(plugin, sharding)
	return s.reduce(taskList)
}

type concurrentTask struct {
	data []*model.Task
	lock sync.RWMutex
}

func (s *concurrentTask) Append(tasks ...*model.Task) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.data = append(s.data, tasks...)
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

// 是否为并行任务插件
func IsParallelPlugin(name string) bool {
	return strings.Contains(name, "|")
}

// 拆分出并行处理的插件名称
func SplitParallelPlugin(name string) []string {
	return strings.Split(name, "|")
}
