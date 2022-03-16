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

	"github.com/maybgit/glog"
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
		glog.Errorf("ApplicationContext/StartJob %s", errMsg)
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
		glog.Errorf("ApplicationContext/StartJob %s", jobInfo.Job.Message)
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
				msgList = append(msgList, item.Message)
			}
			jobInfo.Job.Message = strings.Join(msgList, ";")
		}
	} else if enum.ContextState(cancelParam.State) == enum.ExceptionCancelState {
		// 记录取消原因
		jobInfo.Job.Message = cancelParam.Reason
	}

	// 合并task结果
	result, err := jobInfo.Reduce()
	if err != nil {
		glog.Errorf("ApplicationContext/StartJob 合并task数据异常,%v", err)
		return err
	}
	jobInfo.Job.Result = result

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
				glog.Errorf("ApplicationContext/StartJob %v,id:%d", err, job.Job.Id)
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
		newTasks := s.createNewTasks(ctx, job, item, pluginSet[pos+1])
		if newTasks == nil {
			continue
		}
		result = append(result, newTasks...)
		job.TaskList.Append(newTasks...)
	}
	return result
}

// 任务执行完回调通知
func (s *ApplicationContext) taskCallback(ctx context.Context, job *dto.JobInfo, task *model.Task) func(error) {
	return func(err error) {
		defer func() {
			// 保存任务状态
			task.FinishTime = time.Now()
			err = s.taskRepo.UpdateStatus(s.Db, task)
			if err != nil {
				errMsg := fmt.Sprintf("持久化task(%d,%s)状态失败,err:%+v", task.Id, task.Name, err)
				task.Status = int32(enum.ExceptionTaskStatus)
				task.Message = errMsg
				glog.Errorf("ApplicationContext/taskCallback %s", errMsg)

				if s.isNeedCancelJob(job, task) {
					job.Job.Status = int32(enum.SystemExceptionJobStatus)
					util.CancelNotify(ctx, job, errMsg)
				}
			}
		}()

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
			return
		}

		// 判断任务是否执行异常，异常则通知其他task停止执行
		if enum.TaskStatus(task.Status) != enum.FinishTaskStatus {
			errMsg := fmt.Sprintf("task(%d,%s)业务处理失败,%s", task.Id, task.Name, task.Message)
			task.Status = int32(enum.ExceptionTaskStatus)
			task.Message = errMsg
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

		// 判断当前任务是否为并行任务，如果是则需要继续判断其他并行任务是否已完成，否则直接跳过
		if dto.IsParallelPlugin(task.Plugin) {
			// 当其他并行任务没有完成则直接返回
			if !job.IsFinishParallelTask(task.Plugin, task.Sharding) {
				return
			}
			// 获取不到锁直接返回
			if !job.TryLockParallelTask(task) {
				return
			}
		}

		// 判断是否继续创建下一个
		pos := util.FindHandlerPos(job.Job.PluginSet, task.Plugin)
		if pos == -1 {
			glog.Errorf("ApplicationContext/taskCallback job找不到plugin(%s),id:%d", task.Plugin, task.Id)
			util.CancelNotify(ctx, job, fmt.Sprintf("job(%d,%s)系统不支持plugin(%s)", job.Job.Id, job.Job.Name, task.Plugin))
			job.Job.Status = int32(enum.SystemExceptionJobStatus)
			return
		}

		// 如果最后一个插件处理完毕就直接返回
		pluginSet := strings.Split(job.Job.PluginSet, ",")
		if pos == len(pluginSet)-1 {
			job.DoneLatch.Done()
			return
		}

		// 创建新task并放入调度器执行
		newTasks := s.createNewTasks(ctx, job, task, strings.Split(job.Job.PluginSet, ",")[pos+1])
		if newTasks == nil {
			return
		}
		job.TaskList.Append(newTasks...)
		// 调度新的task
		s.Scheduler.DispatchTask(job, s.buildTasks(ctx, job, newTasks)...)
	}
}

// 当task执行异常是否需要取消job，如果job为继续执行模式则其他task会继续执行下去
func (s *ApplicationContext) isNeedCancelJob(job *dto.JobInfo, task *model.Task) bool {
	if enum.TaskExceptionOperation(job.Job.TaskExceptionOperation) == enum.ExitTaskExceptionOperation {
		return true
	}
	// 如果存在异常记录则跳过，不用再次添加
	for _, item := range job.ExceptionTask.GetAll() {
		if item == task {
			return false
		}
	}
	job.ExceptionTask.Append(task)
	job.DoneLatch.Done()
	return false
}

// 创建新task
func (s *ApplicationContext) createNewTasks(ctx context.Context, job *dto.JobInfo, task *model.Task, plugin string) []*model.Task {
	var err error
	input := task.Output
	if dto.IsParallelPlugin(task.Plugin) {
		input, err = job.ReduceParallel(task.Plugin, task.Sharding)
		if err != nil {
			errMsg := fmt.Sprintf("合并并行task结果异常,当前task:%s,err:%+v", kit.JsonEncode(task), err)
			task.Status = int32(enum.ExceptionTaskStatus)
			task.Message = errMsg
			glog.Errorf("ApplicationContext/createNewTask %s", errMsg)

			if s.isNeedCancelJob(job, task) {
				util.CancelNotify(ctx, job, errMsg)
				job.Job.Status = int32(enum.SystemExceptionJobStatus)
			}
			return nil
		}
	}

	subPlugins := []string{""}
	if dto.IsParallelPlugin(plugin) {
		subPlugins = dto.SplitParallelPlugin(plugin)
	}
	// 创建新task并放入调度器执行
	list := make([]*model.Task, 0, len(subPlugins))
	for _, item := range subPlugins {
		list = append(list, &model.Task{
			JobId:     task.JobId,
			Sharding:  task.Sharding,
			Name:      task.Name,
			Input:     input,
			Plugin:    plugin,
			SubPlugin: item,
		})
	}
	// 持久化新task任务
	if len(list) == 1 {
		err = s.taskRepo.Save(s.Db, list[0])
	} else {
		err = s.taskRepo.BatchSave(s.Db, list)
	}
	if err != nil {
		errMsg := fmt.Sprintf("持久化新产生task异常,plugin:%s,新task:%s,%+v", plugin, kit.JsonEncode(list), err)
		task.Status = int32(enum.ExceptionTaskStatus)
		task.Message = errMsg
		glog.Errorf("ApplicationContext/createNewTask %s", errMsg)

		if s.isNeedCancelJob(job, task) {
			util.CancelNotify(ctx, job, errMsg)
			job.Job.Status = int32(enum.SystemExceptionJobStatus)
		}
		return nil
	}
	if len(list) > 1 {
		list, err = s.taskRepo.List(s.Db, map[string]interface{}{
			"jobId":    job.Job.Id,
			"sharding": task.Sharding,
			"plugin":   plugin,
		})
		if err != nil {
			errMsg := fmt.Sprintf("查询刚刚创建新task异常,plugin:%s,jobId:%d,err:%+v", plugin, job.Job.Id, err)
			task.Status = int32(enum.ExceptionTaskStatus)
			task.Message = errMsg
			glog.Errorf("ApplicationContext/createNewTask %s", errMsg)

			if s.isNeedCancelJob(job, task) {
				util.CancelNotify(ctx, job, errMsg)
				job.Job.Status = int32(enum.SystemExceptionJobStatus)
			}
			return nil
		}
	}
	return list
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
		glog.Infof("ApplicationContext/restartUndoneAsyncJob 没有任何job需要重启")
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

// 手动取消job
func (s *ApplicationContext) ManualCancelJob(jobId int64, reason ...string) error {
	jobObj, ok := s.jobContainer.Get(jobId)
	if !ok {
		return nil
	}
	s.jobContainer.Remove(jobId)
	cancelReason := "手动取消job"
	if len(reason) > 0 {
		cancelReason += "：" + reason[0]
	}
	util.CancelNotify(jobObj.Ctx, jobObj.Job, cancelReason)
	jobObj.Job.Job.Status = int32(enum.CancelJobStatus)
	err := s.jobRepo.UpdateStatus(s.Db, jobObj.Job.Job)
	if err != nil {
		glog.Errorf("ApplicationContext/ManualCancelJob 更新job状态异常,id:%d,err:%+v", jobObj.Job.Job.Id, err)
	}
	return nil
}
