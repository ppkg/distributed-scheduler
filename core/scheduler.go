package core

import (
	"context"
	"distributed-scheduler/enum"
	"distributed-scheduler/errCode"
	"distributed-scheduler/model"
	"distributed-scheduler/proto/task"
	"fmt"
	"sync"

	"github.com/ppkg/glog"
	"github.com/ppkg/kit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	workerConns workerConnMap
}

func NewScheduler(thread int) *ScheduleEngine {
	engine := &ScheduleEngine{
		threadCount:   thread,
		WorkerIndexer: NewWorkerIndexer(),
		taskChannel:   make(chan InputTask),
		workerConns: workerConnMap{
			cache: make(map[string]*grpc.ClientConn),
		},
	}
	engine.init()
	return engine
}

// 初始化引擎
func (s *ScheduleEngine) init() {
	for i := 0; i < s.threadCount; i++ {
		go s.runScheduleThead()
	}
}

// 运行调度线程
func (s *ScheduleEngine) runScheduleThead() {
	for task := range s.taskChannel {
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

// 推送任务
func (s *ScheduleEngine) processTask(task *model.Task) error {
	workers, err := s.predicateWorker(task.Plugin)
	if err != nil {
		glog.Errorf("ScheduleEngine/processTask 预选worker节点异常,taskId:%d,err:%+v", task.Id, err)
		return err
	}

	// 推送任务,如果推送失败则重推
	for i := 0; i < 3; i++ {
		myWorker := s.preferWorker(task.Plugin, workers)
		err = s.pushTask(myWorker, task)
		if err == nil {
			return nil
		}
		glog.Errorf("ScheduleEngine/processTask 推送任务异常,worker:%s,taskId:%d,err:%+v", kit.JsonEncode(myWorker), task.Id, err)
	}

	return err
}

// 推送任务给worker执行
func (s *ScheduleEngine) pushTask(worker WorkerNode, t *model.Task) error {
	t.NodeId = worker.NodeId
	t.Endpoint = worker.Endpoint
	t.Status = enum.DoingTaskStatus

	conn, err := s.workerConns.Get(worker)
	if err != nil {
		return err
	}

	client := task.NewTaskServiceClient(conn)
	resp, err := client.SyncSubmit(context.Background(), &task.SubmitRequest{
		Id:     t.Id,
		JobId:  t.JobId,
		Name:   t.Name,
		Plugin: t.Plugin,
		Data:   t.Input,
	})

	if err != nil {
		t.Status = enum.ExceptionTaskStatus
		return err
	}

	t.Status = resp.Status
	t.Output = resp.Result

	if t.Status != enum.FinishJobStatus {
		return fmt.Errorf("status:%d,message:%s", t.Status, t.Output)
	}
	return nil
}

// 优选worker工作节点
func (s *ScheduleEngine) preferWorker(plugin string, list []WorkerNode) WorkerNode {
	pos := s.getAndIncr(plugin)
	i := pos % uint32(len(list))
	return list[i]
}

// 预选worker工作节点
func (s *ScheduleEngine) predicateWorker(plugin string) ([]WorkerNode, error) {
	workers := s.WorkerIndexer.GetPluginWorker(plugin)
	if len(workers) == 0 {
		return nil, errCode.ToGrpcErr(errCode.ErrPluginUnsupport, plugin)
	}
	return workers, nil
}

type InputTask struct {
	Task     *model.Task
	Callback func()
}

type workerConnMap struct {
	cache map[string]*grpc.ClientConn
	lock  sync.RWMutex
}

// 获取worker客户端连接
func (s *workerConnMap) Get(worker WorkerNode) (*grpc.ClientConn, error) {
	s.lock.RLock()
	conn, ok := s.cache[worker.NodeId]
	s.lock.RUnlock()
	if ok {
		return conn, nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	conn, ok = s.cache[worker.NodeId]
	if ok {
		return conn, nil
	}
	conn, err := grpc.Dial(worker.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(30*1024*1024)))
	if err != nil {
		return nil, err
	}
	s.cache[worker.NodeId] = conn
	return conn, nil
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
