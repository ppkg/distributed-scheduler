package service

import (
	"context"
	"distributed-scheduler/core"
	"distributed-scheduler/dto"
	"distributed-scheduler/enum"
	"distributed-scheduler/errCode"
	"distributed-scheduler/model"
	"distributed-scheduler/proto/job"
	"distributed-scheduler/repository"
	"distributed-scheduler/repository/impl"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ppkg/glog"
	"github.com/ppkg/kit"
	"gorm.io/gorm"
)

type jobService struct {
	appCtx   *core.ApplicationContext
	jobRepo  repository.JobRepository
	taskRepo repository.TaskRepository
}

// 异步提交job
func (s *jobService) AsyncSubmit(_ job.JobService_AsyncSubmitServer) error {
	panic("not implemented") // TODO: Implement
}

// 同步提交job
func (s *jobService) SyncSubmit(stream job.JobService_SyncSubmitServer) error {
	jobInfo, err := s.receiveJobStream(stream)
	if err != nil {
		glog.Errorf("jobService/SyncSubmit 接收job数据异常,err:%+v", err)
		return err
	}
	jobInfo.Job.IsAsync = 1

	err = s.persistence(jobInfo)
	if err != nil {
		glog.Errorf("jobService/SyncSubmit 持久化job数据异常,err:%+v", err)
		return err
	}

	// 重新加载最新job信息
	jobInfo, err = s.reloadJobInfo(jobInfo)
	if err != nil {
		glog.Errorf("jobService/SyncSubmit 重新加载最新job信息异常,err:%+v", err)
		return err
	}
	jobInfo.InitDoneChannel()

	ctx, cancel := context.WithCancel(context.Background())
	cancelParam := &core.CancelTaskParam{
		CancelFunc: cancel,
	}
	ctx = context.WithValue(ctx, core.CancelTaskKey{}, cancelParam)
	defer func() {
		if atomic.CompareAndSwapInt32(&cancelParam.IsCancel, 0, 1) {
			cancelParam.CancelFunc()
		}
	}()

	s.appCtx.Scheduler.Put(jobInfo, s.buildTasks(ctx, jobInfo, jobInfo.TaskList)...)

	// job超时处理
	timeout := time.AfterFunc(time.Hour, func() {
		s.cancelNotify(ctx, jobInfo, fmt.Sprintf("job(%d,%s)处理超时终止退出", jobInfo.Job.Id, jobInfo.Job.Name))
		jobInfo.Job.Status = enum.RunningTimeoutJobStatus
	})
	defer timeout.Stop()

	for range jobInfo.Done {
		endTasks := jobInfo.FilterFinishEndTask()
		if jobInfo.Job.Size == int32(len(endTasks)) {
			jobInfo.Job.Status = enum.FinishJobStatus
			break
		}
	}

	// 记录取消原因
	switch cancelParam.IsCancel {
	case enum.ExceptionCancel:
		jobInfo.Job.Result = cancelParam.Reason
	}

	// 如果job是已完成状态则进行合并结果
	if jobInfo.Job.Status == enum.FinishJobStatus {
		result, err := jobInfo.Reduce()
		if err != nil {
			glog.Errorf("jobService/SyncSubmit 合并数据异常,%v", err)
			return err
		}
		jobInfo.Job.Result = result
	}

	// 保存job状态
	jobInfo.Job.FinishTime = time.Now()
	err = s.jobRepo.UpdateStatus(s.appCtx.Db, jobInfo.Job)
	if err != nil {
		glog.Errorf("jobService/SyncSubmit 更新job状态异常,id:%d,err:%+v", jobInfo.Job.Id, err)
		return err
	}

	return stream.SendAndClose(&job.SyncSubmitResponse{
		Id:     jobInfo.Job.Id,
		Status: jobInfo.Job.Status,
		Result: jobInfo.Job.Result,
	})
}

// 构建任务
func (s *jobService) buildTasks(ctx context.Context, jobInfo *dto.JobInfo, taskList []*model.Task) []core.InputTask {
	list := make([]core.InputTask, 0, len(taskList))
	for _, item := range taskList {
		list = append(list, core.InputTask{
			Ctx:      ctx,
			Task:     item,
			Callback: s.taskCallback(ctx, jobInfo, item),
		})
	}
	return list
}

func (s *jobService) taskCallback(ctx context.Context, job *dto.JobInfo, task *model.Task) func() {
	return func() {
		glog.Infof("当前任务执行结果,name:%s,id:%d,jobId:%d,status:%d(%s),nodeId:%s,plugin:%s,output:%s,input:%s", task.Name, task.Id, task.JobId, task.Status, enum.TaskStatusMap[task.Status], task.NodeId, task.Plugin, task.Output, task.Input)

		// 保存任务状态
		task.FinishTime = time.Now()
		err := s.taskRepo.UpdateStatus(s.appCtx.Db, task)
		if err != nil {
			glog.Errorf("jobService/taskCallback 持久化task状态失败,id:%d,err:%+v", task.Id, err)
			s.cancelNotify(ctx, job, fmt.Sprintf("持久化task(%d,%s)状态失败,%+v", task.Id, task.Name, err))
			job.Job.Status = enum.SystemExceptionJobStatus
			return
		}

		// 判断任务是否执行异常，异常则通知其他task停止执行
		if task.Status != enum.FinishTaskStatus {
			s.cancelNotify(ctx, job, fmt.Sprintf("task(%d,%s)业务处理失败,%s", task.Id, task.Name, task.Output))
			job.Job.Status = enum.BusinessExceptionJobStatus
			return
		}

		// 判断是否继续创建下一个
		pos := s.findPluginPos(job.Job.PluginSet, task.Plugin)
		if pos == -1 {
			glog.Errorf("jobService/taskCallback job找不到plugin(%s),id:%d", task.Plugin, task.Id)
			s.cancelNotify(ctx, job, fmt.Sprintf("job(%d,%s)不支持plugin(%s)", job.Job.Id, job.Job.Name, task.Plugin))
			job.Job.Status = enum.SystemExceptionJobStatus
			return
		}

		// 如果最后一个插件处理完毕就直接返回
		cancelParam := ctx.Value(core.CancelTaskKey{}).(*core.CancelTaskParam)
		if cancelParam.IsCancel == enum.NormalRuning && pos == len(strings.Split(job.Job.PluginSet, ","))-1 {
			job.Done <- 1
			return
		}

		// 创建新task并放入调度器执行
		newTask := &model.Task{
			JobId:    task.JobId,
			Sharding: task.Sharding,
			Name:     task.Name,
			Input:    task.Output,
			Plugin:   strings.Split(job.Job.PluginSet, ",")[pos+1],
		}
		// 持久化新task任务
		err = s.taskRepo.Save(s.appCtx.Db, newTask)
		if err != nil {
			glog.Errorf("jobService/taskCallback 持久化新task失败,task:%s,err:%+v", kit.JsonEncode(newTask), err)
			s.cancelNotify(ctx, job, fmt.Sprintf("新task(%s)持久化失败,jobId:%d,%+v", task.Name, newTask.JobId, err))
			job.Job.Status = enum.SystemExceptionJobStatus
			return
		}
		job.AppendSafeTask(newTask)
		// 调度新的task
		s.appCtx.Scheduler.Put(job, s.buildTasks(ctx, job, []*model.Task{newTask})...)
	}
}

func (s *jobService) findPluginPos(pluginSet string, plugin string) int {
	for i, item := range strings.Split(pluginSet, ",") {
		if item == plugin {
			return i
		}
	}
	return -1
}

// 取消通知
func (s *jobService) cancelNotify(ctx context.Context, job *dto.JobInfo, reason string) {
	// 通知未执行task取消操作
	cancelParam := ctx.Value(core.CancelTaskKey{}).(*core.CancelTaskParam)
	cancelParam.Reason = reason
	if atomic.CompareAndSwapInt32(&cancelParam.IsCancel, 0, 1) {
		close(job.Done)
		cancelParam.CancelFunc()
	}
}

// 重新加载job信息
func (s *jobService) reloadJobInfo(jobInfo *dto.JobInfo) (*dto.JobInfo, error) {
	if jobInfo == nil {
		jobInfo = &dto.JobInfo{}
	}
	var err error
	jobInfo.Job, err = s.jobRepo.FindById(s.appCtx.Db, jobInfo.Job.Id)
	if err != nil {
		return jobInfo, err
	}
	if jobInfo.Job == nil {
		return jobInfo, errCode.ToGrpcErr(errCode.ErrJobNotFound)
	}
	jobInfo.TaskList, err = s.taskRepo.List(s.appCtx.Db, map[string]interface{}{
		"jobId": jobInfo.Job.Id,
	})
	return jobInfo, err
}

// 持久化job信息
func (s *jobService) persistence(jobInfo *dto.JobInfo) error {
	return s.appCtx.Db.Transaction(func(tx *gorm.DB) error {
		err := s.jobRepo.Save(tx, jobInfo.Job)
		if err != nil {
			return err
		}
		for _, item := range jobInfo.TaskList {
			item.JobId = jobInfo.Job.Id
		}
		return s.taskRepo.BatchSave(tx, jobInfo.TaskList)
	})
}

// 接收job信息
func (s *jobService) receiveJobStream(stream job.JobService_SyncSubmitServer) (*dto.JobInfo, error) {
	jobInfo := &dto.JobInfo{}
	var firstPlugin string
	var sharding int32 = 0
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return jobInfo, err
		}
		if len(r.PluginSet) == 0 {
			return jobInfo, errCode.ToGrpcErr(errCode.ErrPluginSetEmpty)
		}
		// 初始化job数据
		if jobInfo.Job == nil {
			jobInfo.Job = &model.Job{
				Name:      r.Name,
				Type:      r.Type,
				PluginSet: strings.Join(r.PluginSet, ","),
			}
			firstPlugin = r.PluginSet[0]
		}
		jobInfo.AppendSafeTask(&model.Task{
			Sharding: sharding,
			Name:     fmt.Sprintf("%s-%d", jobInfo.Job.Name, sharding),
			Input:    r.Data,
			Plugin:   firstPlugin,
		})
		sharding++
	}

	jobInfo.Job.Size = int32(len(jobInfo.TaskList))

	return jobInfo, nil
}

func NewJobService(ctx *core.ApplicationContext) job.JobServiceServer {
	return &jobService{
		appCtx:   ctx,
		jobRepo:  impl.NewJobRepository(),
		taskRepo: impl.NewTaskRepository(),
	}
}
