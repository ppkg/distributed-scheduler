package service

import (
	"context"
	"database/sql"
	"distributed-scheduler/core"
	"distributed-scheduler/dto"
	"distributed-scheduler/enum"
	"distributed-scheduler/errCode"
	"distributed-scheduler/model"
	"distributed-scheduler/proto/job"
	"distributed-scheduler/repository"
	"distributed-scheduler/repository/impl"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ppkg/glog"
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

	s.appCtx.Scheduler.Put(jobInfo, s.buildTasks(ctx, jobInfo)...)

	for range jobInfo.Done {
		endTasks := jobInfo.FilterFinishEndTask()
		glog.Infof("当前job状态:%d,%s,共%d个task,已完成%d个", jobInfo.Job.Id, jobInfo.Job.Name, jobInfo.Job.Size, len(endTasks))
		if jobInfo.Job.Size == int32(len(endTasks)) {
			close(jobInfo.Done)
		}
	}

	switch cancelParam.IsCancel {
	case enum.ExceptionCancel:
		jobInfo.Job.Status = enum.ExceptionJobStatus
		jobInfo.Job.Result = cancelParam.Reason
	default:
		jobInfo.Job.Status = enum.FinishJobStatus
		result, err := jobInfo.Reduce()
		if err != nil {
			glog.Errorf("jobService/SyncSubmit 合并数据异常,%v", err)
			return err
		}
		jobInfo.Job.Result = result
		jobInfo.Job.FinishTime = sql.NullTime{
			Time: time.Now(),
		}
	}
	// 保存job状态
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
func (s *jobService) buildTasks(ctx context.Context, jobInfo *dto.JobInfo) []core.InputTask {
	taskList := make([]core.InputTask, 0, len(jobInfo.TaskList))
	for _, item := range jobInfo.TaskList {
		taskList = append(taskList, core.InputTask{
			Ctx:      ctx,
			Task:     item,
			Callback: s.taskCallback(ctx, jobInfo, item),
		})
	}
	return taskList
}

func (s *jobService) taskCallback(ctx context.Context, job *dto.JobInfo, task *model.Task) func() {
	return func() {
		glog.Infof("当前任务执行结果,name:%s,id:%d,jobId:%d,status:%d(%s),nodeId:%s,output:%s,input:%s", task.Name, task.Id, task.JobId, task.Status, enum.TaskStatusMap[task.Status], task.NodeId, task.Output, task.Input)

		err := s.taskRepo.UpdateStatus(s.appCtx.Db, task)
		if err != nil {
			glog.Errorf("jobService/taskCallback 持久化task状态失败,id:%d,err:%+v", task.Id, err)
			s.cancelNotify(ctx, job, task, err)
			return
		}

		// 判断任务是否执行异常，异常则通知其他task停止执行
		if task.Status != enum.FinishTaskStatus {
			s.cancelNotify(ctx, job, task, errors.New(task.Output))
			return
		}

		// 判断是否继续创建下一个
		pos := s.findPluginPos(job.Job.PluginSet, task.Plugin)
		if pos == -1 {
			glog.Errorf("jobService/taskCallback job找不到plugin(%s),id:%d", task.Plugin, task.Id)
			s.cancelNotify(ctx, job, task, err)
			return
		}
		// 如果最后一个插件处理完毕就直接返回
		cancelParam := ctx.Value(core.CancelTaskKey{}).(*core.CancelTaskParam)
		if cancelParam.IsCancel == enum.NormalRuning && pos == len(strings.Split(job.Job.PluginSet, ","))-1 {
			job.Done <- 1
			return
		}

		// 创建新task并放入调度器执行
		job.AppendSafeTask(&model.Task{
			Sharding: task.Sharding,
			Name:     task.Name,
			Input:    task.Output,
			Plugin:   strings.Split(job.Job.PluginSet, ",")[pos+1],
		})

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
func (s *jobService) cancelNotify(ctx context.Context, job *dto.JobInfo, task *model.Task, err error) {
	// 通知未执行task取消操作
	cancelParam := ctx.Value(core.CancelTaskKey{}).(*core.CancelTaskParam)
	cancelParam.Reason = fmt.Sprintf("任务(%d,%s)更新task状态失败,%+v", task.Id, task.Name, err)
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
