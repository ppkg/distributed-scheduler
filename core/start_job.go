package core

import (
	"context"
	"distributed-scheduler/dto"
	"distributed-scheduler/enum"
	dsModel "distributed-scheduler/model"
	"distributed-scheduler/util"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ppkg/glog"
	"github.com/ppkg/kit"
)

// 开始执行job
func (s *ApplicationContext) StartJob(jobInfo *dto.JobInfo) error {
	// 初始化job完成标识通道
	jobInfo.InitDoneChannel()

	ctx, cancel := context.WithCancel(context.Background())
	cancelParam := &dto.CancelTaskParam{
		CancelFunc: cancel,
	}
	ctx = context.WithValue(ctx, dto.CancelTaskKey{}, cancelParam)
	defer func() {
		if atomic.CompareAndSwapInt32(&cancelParam.IsCancel, 0, 1) {
			cancelParam.CancelFunc()
		}
	}()

	// job运行超时处理
	timeout := time.AfterFunc(3*time.Hour, func() {
		errMsg := fmt.Sprintf("job(%d,%s)处理超时终止运行,task数量:%d", jobInfo.Job.Id, jobInfo.Job.Name, jobInfo.Job.Size)
		util.CancelNotify(ctx, jobInfo, errMsg)
		jobInfo.Job.Status = enum.RunningTimeoutJobStatus
		glog.Error(errMsg)
	})
	defer timeout.Stop()

	// 添加job容器中并管理
	s.JobContainer.Put(jobInfo.Job.Id, &RunningJobItem{
		Ctx: ctx,
		Job: jobInfo,
	})
	defer s.JobContainer.Remove(jobInfo.Job.Id)

	// 构建task并移交给scheduler调度器来调度
	s.Scheduler.Put(jobInfo, s.buildTasks(ctx, jobInfo, jobInfo.TaskList)...)

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
			glog.Errorf("ApplicationContext/StartJob 合并task数据异常,%v", err)
			return err
		}
		jobInfo.Job.Result = result
	}

	// 保存job状态
	jobInfo.Job.FinishTime = time.Now()
	err := s.jobRepo.UpdateStatus(s.Db, jobInfo.Job)
	if err != nil {
		glog.Errorf("ApplicationContext/StartJob 更新job状态异常,id:%d,err:%+v", jobInfo.Job.Id, err)
		return err
	}
	return nil
}

// 构建任务
func (s *ApplicationContext) buildTasks(ctx context.Context, jobInfo *dto.JobInfo, taskList []*dsModel.Task) []InputTask {
	list := make([]InputTask, 0, len(taskList))
	for _, item := range taskList {
		list = append(list, InputTask{
			Ctx:      ctx,
			Task:     item,
			Callback: s.taskCallback(ctx, jobInfo, item),
		})
	}
	return list
}

// 任务执行完回调通知
func (s *ApplicationContext) taskCallback(ctx context.Context, job *dto.JobInfo, task *dsModel.Task) func() {
	return func() {
		glog.Infof("当前任务执行结果,name:%s,id:%d,jobId:%d,status:%d(%s),nodeId:%s,plugin:%s,output:%s,input:%s", task.Name, task.Id, task.JobId, task.Status, enum.TaskStatusMap[task.Status], task.NodeId, task.Plugin, task.Output, task.Input)

		// 保存任务状态
		task.FinishTime = time.Now()
		err := s.taskRepo.UpdateStatus(s.Db, task)
		if err != nil {
			glog.Errorf("ApplicationContext/taskCallback 持久化task状态失败,id:%d,err:%+v", task.Id, err)
			util.CancelNotify(ctx, job, fmt.Sprintf("持久化task(%d,%s)状态失败,%+v", task.Id, task.Name, err))
			job.Job.Status = enum.SystemExceptionJobStatus
			return
		}

		// 判断任务是否执行异常，异常则通知其他task停止执行
		if task.Status != enum.FinishTaskStatus {
			util.CancelNotify(ctx, job, fmt.Sprintf("task(%d,%s)业务处理失败,%s", task.Id, task.Name, task.Output))
			job.Job.Status = enum.BusinessExceptionJobStatus
			return
		}

		// 判断当前task是不是被取消了，如果是则直接return
		cancelParam := ctx.Value(dto.CancelTaskKey{}).(*dto.CancelTaskParam)
		if cancelParam.IsCancel != enum.NormalRuning {
			return
		}

		// 判断是否继续创建下一个
		pos := util.FindPluginPos(job.Job.PluginSet, task.Plugin)
		if pos == -1 {
			glog.Errorf("ApplicationContext/taskCallback job找不到plugin(%s),id:%d", task.Plugin, task.Id)
			util.CancelNotify(ctx, job, fmt.Sprintf("job(%d,%s)不支持plugin(%s)", job.Job.Id, job.Job.Name, task.Plugin))
			job.Job.Status = enum.SystemExceptionJobStatus
			return
		}

		// 如果最后一个插件处理完毕就直接返回
		if pos == len(strings.Split(job.Job.PluginSet, ","))-1 {
			job.Done <- 1
			return
		}

		// 创建新task并放入调度器执行
		newTask := &dsModel.Task{
			JobId:    task.JobId,
			Sharding: task.Sharding,
			Name:     task.Name,
			Input:    task.Output,
			Plugin:   strings.Split(job.Job.PluginSet, ",")[pos+1],
		}
		// 持久化新task任务
		err = s.taskRepo.Save(s.Db, newTask)
		if err != nil {
			glog.Errorf("ApplicationContext/taskCallback 持久化新task失败,task:%s,err:%+v", kit.JsonEncode(newTask), err)
			util.CancelNotify(ctx, job, fmt.Sprintf("新task(%s)持久化失败,jobId:%d,%+v", task.Name, newTask.JobId, err))
			job.Job.Status = enum.SystemExceptionJobStatus
			return
		}
		job.AppendSafeTask(newTask)
		// 调度新的task
		s.Scheduler.Put(job, s.buildTasks(ctx, job, []*dsModel.Task{newTask})...)
	}
}
