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

	"github.com/panjf2000/ants/v2"
	"github.com/ppkg/glog"
	"github.com/ppkg/kit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 调度引擎
type scheduleEngine struct {
	// worker节点缓存器
	workerIndexer *workerIndexer
	lock          sync.RWMutex
	roundRobinMap map[string]*safeUint32

	// worker调度器线程数
	workerThreadCount int
	// worker线程池
	workerPools *workerPoolMap
	// worker连接维护
	workerConns *workerConnMap

	// task任务队列
	taskQueue chan func(worker WorkerNode)

	// 异步通知渠道
	NotifyChannel *workerNotifyChannel
}

func NewScheduler(workerThread int) *scheduleEngine {
	engine := &scheduleEngine{
		workerThreadCount: workerThread,
		workerPools:       NewWorkerPools(),
		workerIndexer:     NewWorkerIndexer(),
		roundRobinMap:     make(map[string]*safeUint32),
		workerConns:       NewWorkerConns(),
		NotifyChannel:     NewWorkerNotifyChannel(),
		taskQueue:         make(chan func(worker WorkerNode), 100000000),
	}
	return engine
}

func (s *scheduleEngine) AddWorker(worker WorkerNode) error {
	s.workerIndexer.AddWorker(worker)
	err := s.workerPools.Put(worker, s.workerThreadCount)
	if err != nil {
		glog.Errorf("scheduleEngine/AddWorker %v", err)
		return err
	}
	go s.startWork(worker)
	return nil
}

func (s *scheduleEngine) startWork(worker WorkerNode) {
	pool, closeCh, err := s.workerPools.Get(worker)
	if err != nil {
		glog.Errorf("scheduleEngine/startWork %v", err)
	}
	glog.Infof("scheduleEngine/startWork worker节点(%s,%s)开始工作", worker.NodeId, worker.Endpoint)
loop:
	for {
		select {
		case <-closeCh:
			break loop
		case fn := <-s.taskQueue:
			err = pool.Submit(func() {
				fn(worker)
			})
			if err == nil {
				continue
			}
			// 如果task提交给worker线程池失败则重新入列
			glog.Errorf("scheduleEngine/startWork worker节点(%s,%s)往线程池提交task失败,err:%+v", worker.NodeId, worker.Endpoint, err)
			s.taskQueue <- fn
		}
	}
	glog.Infof("scheduleEngine/startWork worker节点(%s,%s)停止工作", worker.NodeId, worker.Endpoint)
}

func (s *scheduleEngine) RemoveWorker(worker WorkerNode) {
	s.workerIndexer.RemoveWorker(worker)
	s.workerPools.Remove(worker)
}

// 更新worker索引
func (s *scheduleEngine) UpdateWorkerIndex(worker WorkerNode) {
	s.workerIndexer.RemoveWorker(worker)
	s.workerIndexer.AddWorker(worker)
}

// 批量更新worker索引
func (s *scheduleEngine) BatchUpdateWorkerIndex(list []WorkerNode) {
	for _, item := range list {
		worker, ok := s.workerIndexer.GetWorker(item.NodeId)
		if !ok {
			s.AddWorker(item)
			continue
		}
		// 如果worker支持的插件没有变化则跳过
		if util.EqualStringSlice(worker.PluginSet, item.PluginSet) {
			continue
		}
		s.UpdateWorkerIndex(item)
	}
	for _, worker := range s.workerIndexer.GetAllWorker() {
		isFound := false
		for _, item := range list {
			if worker.NodeId == item.NodeId {
				isFound = true
				break
			}
		}
		if isFound {
			continue
		}
		s.RemoveWorker(worker)
	}
}

// 推送任务
func (s *scheduleEngine) processTask(worker WorkerNode, task *model.Task) error {
	tryCount := 3
	// 优先给自己worker执行,不过要先判断自己是否支持当前插件运行
	if util.IsSupportPlugin(worker.PluginSet, task.Plugin) {
		err := s.pushTask(worker, task)
		if err == nil {
			return nil
		}
		glog.Errorf("ScheduleEngine/processTask 优先给自己worker推送异常,worker:%s,taskId:%d,err:%+v", kit.JsonEncode(worker), task.Id, err)
	} else {
		tryCount = 4
	}

	// 自己worker执行失败则交给其他worker来执行
	workers, err := s.predicateWorker(task.Plugin)
	if err != nil {
		glog.Errorf("ScheduleEngine/processTask 预选worker节点异常,taskId:%d,err:%+v", task.Id, err)
		return err
	}

	// 推送任务,如果推送失败则重推
	for i := 0; i < tryCount; i++ {
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
func (s *scheduleEngine) pushTask(worker WorkerNode, t *model.Task) error {
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
func (s *scheduleEngine) preferWorker(plugin string, list []WorkerNode) WorkerNode {
	pos := s.getAndIncr(plugin)
	i := pos % uint32(len(list))
	return list[i]
}

// 预选worker工作节点
func (s *scheduleEngine) predicateWorker(plugin string) ([]WorkerNode, error) {
	workers := s.workerIndexer.GetPluginWorker(plugin)
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

type workerConnMap struct {
	cache map[string]*grpc.ClientConn
	lock  sync.RWMutex
}

func NewWorkerConns() *workerConnMap {
	return &workerConnMap{
		cache: make(map[string]*grpc.ClientConn),
	}
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
func (s *scheduleEngine) getAndIncr(key string) uint32 {
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
func (s *scheduleEngine) Put(job *dto.JobInfo, tasks ...InputTask) {
	if len(tasks) == 0 {
		return
	}
	for _, item := range tasks {
		s.taskQueue <- s.buildQueueFunc(job, item)
	}
}

// 构建task队列入参
func (s *scheduleEngine) buildQueueFunc(job *dto.JobInfo, task InputTask) func(worker WorkerNode) {
	return func(worker WorkerNode) {
		defer func() {
			if panic := recover(); panic != nil {
				errMsg := fmt.Sprintf("运行task(%d,%s) panic:%+v,trace:%s", task.Task.Id, task.Task.Name, panic, util.PanicTrace(10))
				task.Task.Status = enum.ExceptionTaskStatus
				task.Task.Output = errMsg
				util.CancelNotify(task.Ctx, job, errMsg)
				job.Job.Status = enum.SystemExceptionJobStatus
				glog.Error(errMsg)
			}
		}()
		select {
		case <-task.Ctx.Done():
			cancelParam := task.Ctx.Value(dto.CancelTaskKey{}).(*dto.CancelTaskParam)
			// 如果有其他任务处理异常则直接跳过，不执行任何处理
			glog.Warningf("退出当前任务(%d,%s)，其他任务执行失败:%s", task.Task.Id, task.Task.Name, cancelParam.Reason)
			return
		default:
			err := s.processTask(worker, task.Task)
			if err != nil {
				// 推送失败
				errMsg := fmt.Sprintf("推送task(%d,%s)失败,err:%+v", task.Task.Id, task.Task.Name, err)
				task.Task.Status = enum.ExceptionTaskStatus
				task.Task.Output = errMsg
				util.CancelNotify(task.Ctx, job, errMsg)
				job.Job.Status = enum.PushFailJobStatus
				glog.Error(errMsg)
			}
			task.Callback()
		}
	}
}

// 分发job通知
func (s *scheduleEngine) DispatchNotify(job *dto.JobInfo) {
	defer func() {
		if panic := recover(); panic != nil {
			glog.Errorf("scheduleEngine/DispatchNotify 分发job(%d,%s)通知panic:%+v", job.Job.Id, job.Job.Name, panic)
		}
	}()
	list := s.NotifyChannel.GetAll()
	if len(list) == 0 {
		glog.Errorf("scheduleEngine/DispatchNotify 分发job(%d,%s)通知时找不到任何worker订阅通知", job.Job.Id, job.Job.Name)
		return
	}

	pos := s.getAndIncr("systemWorkerNotify")
	i := pos % uint32(len(list))
	channel := list[i]
	channel <- job
}

type workerPoolMap struct {
	pools         map[string]*ants.Pool
	closeChannels map[string]chan int
	lock          sync.RWMutex
}

func NewWorkerPools() *workerPoolMap {
	return &workerPoolMap{
		pools:         make(map[string]*ants.Pool),
		closeChannels: make(map[string]chan int),
	}
}

func (s *workerPoolMap) Get(worker WorkerNode) (*ants.Pool, <-chan int, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	val, ok := s.pools[worker.NodeId]
	if ok {
		return val, s.closeChannels[worker.NodeId], nil
	}
	return nil, nil, fmt.Errorf("工作节点worker(%s,%s)线程池未创建", worker.NodeId, worker.Endpoint)
}

func (s *workerPoolMap) Put(worker WorkerNode, size int) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	pool, err := ants.NewPool(size)
	if err != nil {
		return fmt.Errorf("工作节点worker(%s,%s)实例化线程池失败,err:%+v", worker.NodeId, worker.Endpoint, err)
	}
	s.pools[worker.NodeId] = pool
	s.closeChannels[worker.NodeId] = make(chan int)
	return nil
}

func (s *workerPoolMap) Remove(worker WorkerNode) {
	s.lock.Lock()
	defer s.lock.Unlock()
	pool, ok := s.pools[worker.NodeId]
	if !ok {
		return
	}
	delete(s.pools, worker.NodeId)
	pool.Release()
	ch := s.closeChannels[worker.NodeId]
	close(ch)
	delete(s.closeChannels, worker.NodeId)
}
