package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ppkg/distributed-scheduler/dto"
	"github.com/ppkg/distributed-scheduler/enum"
	"github.com/ppkg/distributed-scheduler/model"
	"github.com/ppkg/distributed-scheduler/util"

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
		if atomic.CompareAndSwapInt32(&cancelParam.State, int32(enum.NormalRuningState), -1) {
			cancelParam.CancelFunc()
		}
	}()

	// job运行超时处理
	timeout := time.AfterFunc(3*time.Hour, func() {
		errMsg := fmt.Sprintf("job(%d,%s)处理超时终止运行,task数量:%d", jobInfo.Job.Id, jobInfo.Job.Name, jobInfo.Job.Size)
		util.CancelNotify(ctx, jobInfo, errMsg)
		jobInfo.Job.Status = int32(enum.RunningTimeoutJobStatus)
		glog.Error(errMsg)
	})
	defer timeout.Stop()

	// 添加job容器中并管理
	s.jobContainer.Put(jobInfo.Job.Id, &RunningJobItem{
		Ctx: ctx,
		Job: jobInfo,
	})
	defer s.jobContainer.Remove(jobInfo.Job.Id)

	// job状态改为进行中
	jobInfo.Job.Status = int32(enum.DoingJobStatus)
	err := s.jobRepo.UpdateStatus(s.Db, jobInfo.Job)
	if err != nil {
		glog.Errorf("ApplicationContext/StartJob 更新job状态异常,id:%d,err:%+v", jobInfo.Job.Id, err)
		return err
	}

	// 判断有没有worker可以调度
	if s.Scheduler.IsEmptyWorker() {
		jobInfo.Job.Status = int32(enum.SystemExceptionJobStatus)
		jobInfo.Job.Message = fmt.Sprintf("系统没有worker节点提供给job(%d,%s)调度执行", jobInfo.Job.Id, jobInfo.Job.Name)
		glog.Error(jobInfo.Job.Message)
		err = s.jobRepo.UpdateStatus(s.Db, jobInfo.Job)
		if err != nil {
			glog.Errorf("ApplicationContext/StartJob 更新job状态异常,id:%d,err:%+v", jobInfo.Job.Id, err)
			return err
		}
		return errors.New(jobInfo.Job.Message)
	}

	// 构建task并移交给scheduler调度器来调度
	pluginSet := strings.Split(jobInfo.Job.PluginSet, ",")
	taskList := s.filterPendingTask(ctx, jobInfo, 0, pluginSet)
	s.Scheduler.DispatchTask(jobInfo, s.buildTasks(ctx, jobInfo, taskList)...)

	// 阻塞等待所有task执行完毕
	jobInfo.DoneLatch.Wait()
	// 判断是否task都执行完毕，如果是则改为已完成状态
	if enum.ContextState(cancelParam.State) == enum.NormalRuningState {
		endTasks := jobInfo.FilterFinishEndTask()
		if jobInfo.Job.Size == int32(len(endTasks)) {
			jobInfo.Job.Status = int32(enum.FinishJobStatus)
		} else {
			jobInfo.Job.Status = int32(enum.BusinessExceptionJobStatus)
			errTaskList := jobInfo.ExceptionTask.GetAll()
			msgList := make([]string, 0)
			msgList = append(msgList, fmt.Sprintf("共有%d个task执行失败", len(errTaskList)))
			for _, item := range errTaskList {
				if item.Message == "" {
					continue
				}
				msgList = append(msgList, fmt.Sprintf("task(%d,%s)->%s", item.Id, item.Name, item.Message))
			}
			jobInfo.Job.Message = strings.Join(msgList, ";")
		}
	} else if enum.ContextState(cancelParam.State) == enum.ExceptionCancelState {
		// 记录取消原因
		jobInfo.Job.Message = cancelParam.Reason
	}

	// 如果job是已完成状态则进行合并结果
	if enum.JobStatus(jobInfo.Job.Status) == enum.FinishJobStatus {
		result, err := jobInfo.Reduce()
		if err != nil {
			glog.Errorf("ApplicationContext/StartJob 合并task数据异常,%v", err)
			return err
		}
		jobInfo.Job.Result = result
	}

	// 保存job状态
	jobInfo.Job.FinishTime = time.Now()
	err = s.jobRepo.UpdateStatus(s.Db, jobInfo.Job)
	if err != nil {
		glog.Errorf("ApplicationContext/StartJob 更新job状态异常,id:%d,err:%+v", jobInfo.Job.Id, err)
		return err
	}

	// 如果是异步job并且需要通知则推送通知
	if jobInfo.Job.IsAsync == 1 && jobInfo.Job.IsNotify == 1 {
		s.Scheduler.DispatchJobNotify(jobInfo, func(job *dto.JobInfo, err error) {
			if err == nil {
				glog.Infof("ApplicationContext/StartJob 推送job回调通知成功,id:%d,name:%s", job.Job.Id, job.Job.Name)
				return
			}

			// 如果在推送出错前已经存在报错则不更改job状态及message信息
			if enum.JobStatus(job.Job.Status) != enum.FinishJobStatus {
				glog.Errorf("ApplicationContext/StartJob 推送job回调通知异常,id:%d,err:%+v", job.Job.Id, err)
				return
			}

			job.Job.Status = int32(enum.NotifyExceptionJobStatus)
			job.Job.Message = err.Error()
			myErr := s.jobRepo.UpdateStatus(s.Db, job.Job)
			if myErr != nil {
				glog.Errorf("ApplicationContext/StartJob job回调通知->更新job状态异常,id:%d,err:%+v", job.Job.Id, myErr)
			}
		})
	}
	return nil
}

// 构建任务
func (s *ApplicationContext) buildTasks(ctx context.Context, jobInfo *dto.JobInfo, taskList []*model.Task) []InputTask {
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

// 过滤出待处理task任务
func (s *ApplicationContext) filterPendingTask(ctx context.Context, job *dto.JobInfo, pos int, pluginSet []string) []*model.Task {
	if pos >= len(pluginSet) {
		return nil
	}
	var result []*model.Task
	taskList := job.TaskList.GetAll()
	list := util.FilterTaskByPlugin(taskList, pluginSet[pos])
	if len(list) == 0 {
		return result
	}
	// 递归查找
	result = append(result, s.filterPendingTask(ctx, job, pos+1, pluginSet)...)
	for _, item := range list {
		if enum.TaskStatus(item.Status) != enum.FinishTaskStatus {
			result = append(result, item)
			continue
		}
		if pos == len(pluginSet)-1 {
			continue
		}

		nextPluginTaskList := util.FilterTaskByPlugin(taskList, pluginSet[pos+1])
		isFound := false
		for _, nextTask := range nextPluginTaskList {
			if nextTask.Sharding == item.Sharding {
				isFound = true
				break
			}
		}
		if isFound {
			continue
		}
		// 如果下一个插件找不到相同的分片则下一个task没有创建需要创建新的
		newTask := s.createNewTask(ctx, job, item, pluginSet[pos+1])
		if newTask == nil {
			continue
		}
		result = append(result, newTask)
	}
	return result
}

// 任务执行完回调通知
func (s *ApplicationContext) taskCallback(ctx context.Context, job *dto.JobInfo, task *model.Task) func(err error) {
	return func(err error) {
		if err != nil {
			// 推送失败
			errMsg := fmt.Sprintf("推送task(%d,%s)失败,err:%+v", task.Id, task.Name, err)
			task.Status = int32(enum.ExceptionTaskStatus)
			task.Message = errMsg
			glog.Errorf("ApplicationContext/taskCallback %s", errMsg)

			if s.isNeedCancelJob(job, task) {
				job.Job.Status = int32(enum.PushTaskExceptionJobStatus)
				util.CancelNotify(ctx, job, errMsg)
			}
		}

		// 保存任务状态
		task.FinishTime = time.Now()
		err = s.taskRepo.UpdateStatus(s.Db, task)
		if err != nil {
			errMsg := fmt.Sprintf("持久化task(%d,%s)状态失败,%+v", task.Id, task.Name, err)
			task.Status = int32(enum.ExceptionTaskStatus)
			task.Message = errMsg
			glog.Errorf("ApplicationContext/taskCallback %s", errMsg)

			if s.isNeedCancelJob(job, task) {
				job.Job.Status = int32(enum.SystemExceptionJobStatus)
				util.CancelNotify(ctx, job, errMsg)
			}
			return
		}

		// 判断任务是否执行异常，异常则通知其他task停止执行
		if enum.TaskStatus(task.Status) != enum.FinishTaskStatus {
			errMsg := fmt.Sprintf("task(%d,%s)业务处理失败,%s", task.Id, task.Name, task.Message)
			if task.Message == "" {
				task.Message = "业务处理失败"
			}
			glog.Errorf("ApplicationContext/taskCallback %s", errMsg)

			if s.isNeedCancelJob(job, task) {
				job.Job.Status = int32(enum.BusinessExceptionJobStatus)
				util.CancelNotify(ctx, job, errMsg)
			}
			return
		}

		// 判断当前task是不是被取消了，如果是则直接return
		cancelParam := ctx.Value(dto.CancelTaskKey{}).(*dto.CancelTaskParam)
		if enum.ContextState(cancelParam.State) != enum.NormalRuningState {
			return
		}

		// 判断是否继续创建下一个
		pos := util.FindHandlerPos(job.Job.PluginSet, task.Plugin)
		if pos == -1 {
			glog.Errorf("ApplicationContext/taskCallback job找不到plugin(%s),id:%d", task.Plugin, task.Id)
			util.CancelNotify(ctx, job, fmt.Sprintf("job(%d,%s)不支持plugin(%s)", job.Job.Id, job.Job.Name, task.Plugin))
			job.Job.Status = int32(enum.SystemExceptionJobStatus)
			return
		}

		// 如果最后一个插件处理完毕就直接返回
		if pos == len(strings.Split(job.Job.PluginSet, ","))-1 {
			job.DoneLatch.Done()
			return
		}

		// 创建新task并放入调度器执行
		newTask := s.createNewTask(ctx, job, task, strings.Split(job.Job.PluginSet, ",")[pos+1])
		if newTask == nil {
			return
		}
		job.TaskList.Append(newTask)
		// 调度新的task
		s.Scheduler.DispatchTask(job, s.buildTasks(ctx, job, []*model.Task{newTask})...)
	}
}

// 当task执行异常是否需要取消job，如果job为继续执行模式则其他task会继续执行下去
func (s *ApplicationContext) isNeedCancelJob(job *dto.JobInfo, task *model.Task) bool {
	if enum.TaskExceptionOperation(job.Job.TaskExceptionOperation) == enum.ExitTaskExceptionOperation {
		return true
	}
	job.ExceptionTask.Append(task)
	job.DoneLatch.Done()
	return false
}

// 创建新task
func (s *ApplicationContext) createNewTask(ctx context.Context, job *dto.JobInfo, task *model.Task, plugin string) *model.Task {
	// 创建新task并放入调度器执行
	newTask := &model.Task{
		JobId:    task.JobId,
		Sharding: task.Sharding,
		Name:     task.Name,
		Input:    task.Output,
		Plugin:   plugin,
	}
	// 持久化新task任务
	err := s.taskRepo.Save(s.Db, newTask)
	if err != nil {
		glog.Errorf("ApplicationContext/createNewTask 持久化新task失败,task:%s,err:%+v", kit.JsonEncode(newTask), err)
		util.CancelNotify(ctx, job, fmt.Sprintf("新task(%s)持久化失败,jobId:%d,%+v", task.Name, newTask.JobId, err))
		job.Job.Status = int32(enum.SystemExceptionJobStatus)
		return nil
	}
	return newTask
}

// 重启未完成的异步job
func (s *ApplicationContext) restartUndoneAsyncJob() {
	list, err := s.loadUndoneAsyncJob()
	if err != nil {
		glog.Errorf("ApplicationContext/restartUndoneAsyncJob 加载未完成异步job异常,err:%+v", err)
		return
	}

	myList := make([]*dto.JobInfo, 0, len(list))
	for _, item := range list {
		// 如果job在容器中执行了则跳过
		if _, ok := s.jobContainer.Get(item.Job.Id); ok {
			continue
		}
		myList = append(myList, item)
	}

	if len(myList) == 0 {
		return
	}

	var logSlice []string
	for _, item := range myList {
		logSlice = append(logSlice, fmt.Sprintf("%d->%s", item.Job.Id, item.Job.Name))
	}
	glog.Infof("ApplicationContext/restartUndoneAsyncJob 共有%d个job重启,分别是:%s", len(myList), strings.Join(logSlice, ","))

	for _, item := range myList {
		go func(job *dto.JobInfo) {
			_ = s.StartJob(job)
		}(item)
	}
}

// 加载24小时内未完成异步工作
func (s *ApplicationContext) loadUndoneAsyncJob() ([]*dto.JobInfo, error) {
	endTime := time.Now()
	startTime := endTime.Add(-24 * time.Hour)
	jobList, err := s.jobRepo.List(s.Db, map[string]interface{}{
		"startTime": startTime,
		"endTime":   endTime,
		"isAsync":   1,
		"inStatus":  []enum.JobStatus{enum.PendingJobStatus, enum.DoingJobStatus, enum.SystemExceptionJobStatus, enum.PushTaskExceptionJobStatus, enum.BusinessExceptionJobStatus, enum.NotifyExceptionJobStatus},
	})
	if err != nil {
		return nil, err
	}
	if len(jobList) == 0 {
		return nil, nil
	}

	list := make([]*dto.JobInfo, 0, len(jobList))
	for _, item := range jobList {
		taskList, err := s.taskRepo.List(s.Db, map[string]interface{}{
			"jobId": item.Id,
		})
		if err != nil {
			return nil, err
		}
		job := dto.NewJobInfo(item)
		job.TaskList.Append(taskList...)
		list = append(list, job)
	}
	return list, nil
}
