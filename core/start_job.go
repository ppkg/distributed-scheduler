package core

import (
	"context"
	"distributed-scheduler/dto"
	"distributed-scheduler/enum"
	"distributed-scheduler/model"
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
		if atomic.CompareAndSwapInt32(&cancelParam.State, enum.NormalRuningState, -1) {
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
	s.jobContainer.Put(jobInfo.Job.Id, &RunningJobItem{
		Ctx: ctx,
		Job: jobInfo,
	})
	defer s.jobContainer.Remove(jobInfo.Job.Id)

	// job状态改为进行中
	jobInfo.Job.Status = enum.DoingJobStatus
	jobInfo.Job.Result = ""
	_ = s.jobRepo.UpdateStatus(s.Db, jobInfo.Job)

	// 构建task并移交给scheduler调度器来调度
	pluginSet := strings.Split(jobInfo.Job.PluginSet, ",")
	taskList := s.filterPendingTask(ctx, jobInfo, 0, pluginSet)
	s.Scheduler.DispatchTask(jobInfo, s.buildTasks(ctx, jobInfo, taskList)...)

	for range jobInfo.Done {
		endTasks := jobInfo.FilterFinishEndTask()
		if jobInfo.Job.Size == int32(len(endTasks)) {
			jobInfo.Job.Status = enum.FinishJobStatus
			break
		}
	}
	if cancelParam.State == enum.NormalRuningState && jobInfo.Job.Status != enum.FinishJobStatus {
		endTasks := jobInfo.FilterFinishEndTask()
		if jobInfo.Job.Size == int32(len(endTasks)) {
			jobInfo.Job.Status = enum.FinishJobStatus
		}
	}

	// 记录取消原因
	switch cancelParam.State {
	case enum.ExceptionCancelState:
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

	// 如果是异步job并且需要通知则推送通知
	if jobInfo.Job.IsAsync == 1 && jobInfo.Job.IsNotify == 1 {
		s.Scheduler.DispatchJobNotify(jobInfo)
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
	list := util.FilterTaskByPlugin(job.TaskList, pluginSet[pos])
	if len(list) == 0 {
		return result
	}
	// 递归查找
	result = append(result, s.filterPendingTask(ctx, job, pos+1, pluginSet)...)
	for _, item := range list {
		if item.Status != enum.FinishTaskStatus {
			result = append(result, item)
			continue
		}
		if pos == len(pluginSet)-1 {
			continue
		}

		nextPluginTaskList := util.FilterTaskByPlugin(job.TaskList, pluginSet[pos+1])
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
func (s *ApplicationContext) taskCallback(ctx context.Context, job *dto.JobInfo, task *model.Task) func() {
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
		if cancelParam.State != enum.NormalRuningState {
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
		newTask := s.createNewTask(ctx, job, task, strings.Split(job.Job.PluginSet, ",")[pos+1])
		if newTask == nil {
			return
		}
		job.AppendSafeTask(newTask)
		// 调度新的task
		s.Scheduler.DispatchTask(job, s.buildTasks(ctx, job, []*model.Task{newTask})...)
	}
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
		job.Job.Status = enum.SystemExceptionJobStatus
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
	if len(list) == 0 {
		return
	}
	var logSlice []string
	for _, item := range list {
		logSlice = append(logSlice, fmt.Sprintf("%d->%s", item.Job.Id, item.Job.Name))
	}
	glog.Infof("ApplicationContext/restartUndoneAsyncJob 共有%d个job重启,分别是:%s", len(list), strings.Join(logSlice, ","))

	for _, item := range list {
		go func (job *dto.JobInfo)  {
			_=s.StartJob(job)
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
		"inStatus":  []int32{enum.PendingJobStatus, enum.DoingJobStatus, enum.SystemExceptionJobStatus, enum.PushFailJobStatus},
	})
	if err != nil {
		return nil, err
	}
	if len(jobList) == 0 {
		return nil, nil
	}

	jobIds := make([]int64, 0, len(jobList))
	for _, jobItem := range jobList {
		_, ok := s.jobContainer.Get(jobItem.Id)
		if ok {
			continue
		}
		jobIds = append(jobIds, jobItem.Id)
	}

	taskList, err := s.taskRepo.List(s.Db, map[string]interface{}{
		"jobIds": jobIds,
	})
	if err != nil {
		return nil, err
	}

	taskMap := make(map[int64][]*model.Task)
	for _, item := range taskList {
		taskMap[item.JobId] = append(taskMap[item.JobId], item)
	}

	list := make([]*dto.JobInfo, 0, len(jobList))
	for _, jobItem := range jobList {
		list = append(list, &dto.JobInfo{
			Job:      jobItem,
			TaskList: taskMap[jobItem.Id],
		})
	}
	return list, nil
}
