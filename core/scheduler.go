package core

import (
	"context"
	"distributed-scheduler/dto"
	"distributed-scheduler/enum"
	"distributed-scheduler/model"
	"distributed-scheduler/proto/task"
	"distributed-scheduler/util"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/panjf2000/ants/v2"
	"github.com/ppkg/glog"
	"github.com/ppkg/kit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 调度引擎
type ScheduleEngine struct {
	// worker节点缓存器
	WorkerIndexer *WorkerIndexer
	lock          sync.RWMutex
	roundRobinMap map[string]*safeUint32

	// 调度器线程数
	threadCount  int
	schedulePool *ants.Pool

	workerConns workerConnMap
}

func NewScheduler(thread int) *ScheduleEngine {
	engine := &ScheduleEngine{
		threadCount:   thread,
		WorkerIndexer: NewWorkerIndexer(),
		roundRobinMap: make(map[string]*safeUint32),
		workerConns: workerConnMap{
			cache: make(map[string]*grpc.ClientConn),
		},
	}
	var err error
	engine.schedulePool, err = ants.NewPool(thread, ants.WithNonblocking(true))
	if err != nil {
		glog.Errorf("ScheduleEngine/NewScheduler 创建调度器异常,err:%+v", err)
	}

	return engine
}

// 推送任务
func (s *ScheduleEngine) processTask(task *model.Task) error {
	workers, err := s.predicateWorker(task.Plugin)
	if err != nil {
		glog.Errorf("ScheduleEngine/processTask 预选worker节点异常,taskId:%d,err:%+v", task.Id, err)
		return err
	}

	// 推送任务,如果推送失败则重推
	for i := 0; i < 5; i++ {
		myWorker := s.preferWorker(task.Plugin, workers)
		err = s.pushTask(myWorker, task)
		if err == nil {
			return nil
		}
		glog.Errorf("ScheduleEngine/processTask 第%d次推送任务异常,worker:%s,taskId:%d,err:%+v", i+1, kit.JsonEncode(myWorker), task.Id, err)
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
		return nil, fmt.Errorf("没有支持插件(%s)的worker可调度", plugin)
	}
	return workers, nil
}

type InputTask struct {
	Ctx      context.Context
	Task     *model.Task
	Callback func()
}
type CancelTaskKey struct{}
type CancelTaskParam struct {
	CancelFunc context.CancelFunc
	// 取消原因
	Reason string
	// 上下文是否已取消,0：否，1:系统错误而取消，2：用户手动取消
	IsCancel int32
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

// 添加需要调度的task
func (s *ScheduleEngine) Put(job *dto.JobInfo, tasks ...InputTask) {
	if len(tasks) == 0 {
		return
	}
	var err error
	for _, item := range tasks {
		err = s.run(job, item)
		if err != nil {
			s.cancelNotify(job, item, "调度器线程池已满，无法调度task")
			job.Job.Status = enum.SystemExceptionJobStatus
		}
	}
}

// 执行调度任务
func (s *ScheduleEngine) run(job *dto.JobInfo, task InputTask) error {
	err := s.schedulePool.Submit(func() {
		defer func() {
			if panic := recover(); panic != nil {
				errMsg := fmt.Sprintf("运行task(%d,%s) panic:%+v,trace:%s", task.Task.Id, task.Task.Name, panic, util.PanicTrace(10))
				task.Task.Status = enum.ExceptionTaskStatus
				task.Task.Output = errMsg
				s.cancelNotify(job, task, errMsg)
				job.Job.Status = enum.SystemExceptionJobStatus
				glog.Error(errMsg)
			}
		}()
		select {
		case <-task.Ctx.Done():
			cancelParam := task.Ctx.Value(CancelTaskKey{}).(*CancelTaskParam)
			// 如果有其他任务处理异常则直接跳过，不执行任何处理
			glog.Warningf("退出当前任务(%d,%s)，其他任务执行失败:%s", task.Task.Id, task.Task.Name, cancelParam.Reason)
			return
		default:
			err := s.processTask(task.Task)
			if err != nil {
				// 推送失败
				errMsg := fmt.Sprintf("推送task(%d,%s)失败,err:%+v", task.Task.Id, task.Task.Name, err)
				task.Task.Status = enum.ExceptionTaskStatus
				task.Task.Output = errMsg
				s.cancelNotify(job, task, errMsg)
				job.Job.Status = enum.PushFailJobStatus
				glog.Error(errMsg)
			}
			task.Callback()
		}
	})
	if err != nil {
		glog.Errorf("当前线程池满了,容量:%d,已使用:%d,剩余:%d,taskId:%d,err:%+v", s.schedulePool.Cap(), s.schedulePool.Running(), s.schedulePool.Free(), task.Task.Id, err)
		return err
	}
	return nil
}

// 取消通知
func (s *ScheduleEngine) cancelNotify(job *dto.JobInfo, task InputTask, reason string) {
	// 通知其他task执行取消操作
	cancelParam := task.Ctx.Value(CancelTaskKey{}).(*CancelTaskParam)
	cancelParam.Reason = reason
	if atomic.CompareAndSwapInt32(&cancelParam.IsCancel, 0, 1) {
		close(job.Done)
		cancelParam.CancelFunc()
	}
}
