package core

import (
	"distributed-scheduler/enum"
	"distributed-scheduler/errCode"
	"distributed-scheduler/model"
	"sync"

	"github.com/ppkg/glog"
)

// 调度引擎
type ScheduleEngine struct {
	// 调度器线程数
	threadCount int
	// worker节点缓存器
	WorkerIndexer *WorkerIndexer
	lock          sync.RWMutex
	roundRobinMap map[string]*safeUint32
	taskChannel   chan InputTask
}

type safeUint32 struct {
	num  uint32
	lock sync.Mutex
}

func (s *safeUint32) GetAndIncr() uint32 {
	s.lock.Lock()
	defer s.lock.Unlock()
	cur := s.num
	s.num++
	return cur
}

// 返回并自增+1
func (s *ScheduleEngine) getAndIncr(key string) uint32 {
	s.lock.RLock()
	val, ok := s.roundRobinMap[key]
	s.lock.RUnlock()
	if ok {
		return val.GetAndIncr()
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	val = &safeUint32{}
	rs := val.GetAndIncr()
	s.roundRobinMap[key] = val
	return rs
}

func (s *ScheduleEngine) Put(tasks ...InputTask) {
	if len(tasks) == 0 {
		return
	}
	for _, item := range tasks {
		s.taskChannel <- item
	}
}

// 初始化引擎
func (s *ScheduleEngine) init() {
	for i := 0; i < s.threadCount; i++ {
		go s.runScheduleThead()
	}
}

// 运行调度线程
func (s *ScheduleEngine) runScheduleThead() {
	for {
		select {
		case task := <-s.taskChannel:
			err := s.processTask(task.Task)
			if err != nil {
				// 推送失败
				task.Task.Status = enum.ExceptionTaskStatus
				task.Task.Output = err.Error()
			} else {
				task.Task.Status = enum.FinishTaskStatus
			}
			task.Callback()
		}
	}
}

// 推送任务
func (s *ScheduleEngine) processTask(task *model.Task) error {
	workers, err := s.predicateWorker(task.Pipeline)
	if err != nil {
		glog.Errorf("ScheduleEngine/processTask 预选worker节点异常,err:%+v", err)
		return err
	}

	myWorker:=s.preferWorker(task.Pipeline,workers)

	// 推送任务
	s.pushTask(myWorker,task)


	return nil
}

func (s *ScheduleEngine) pushTask(worker WorkerNode,task *model.Task) error {
	task.NodeId=worker.NodeId
	task.Endpoint=worker.Endpoint
	return nil
}

// 优选worker工作节点
func (s *ScheduleEngine) preferWorker(pipeline string,list []WorkerNode) WorkerNode  {
	pos := s.getAndIncr(pipeline)
	i:=pos%uint32(len(list))
	return list[i]
}

// 预选worker工作节点
func (s *ScheduleEngine) predicateWorker(pipeline string) ([]WorkerNode, error) {
	workers := s.WorkerIndexer.GetPipelineWorker(pipeline)
	if len(workers) == 0 {
		return nil, errCode.ToGrpcErr(errCode.ErrPipelineUnsupport, pipeline)
	}
	return workers, nil
}

type InputTask struct {
	Task     *model.Task
	Callback func()
}

func NewScheduler(thread int) *ScheduleEngine {
	engine := &ScheduleEngine{
		threadCount:   thread,
		WorkerIndexer: NewWorkerIndexer(),
		taskChannel:   make(chan InputTask),
	}
	engine.init()
	return engine
}
