package service

import (
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

	"github.com/ppkg/glog"
	"gorm.io/gorm"
)

type jobService struct {
	appCtx   *core.ApplicationContext
	jobRepo  repository.JobRepository
	taskRepo repository.TaskRepository
}

// 异步提交job
func (s *jobService) AsyncSubmit(stream job.JobService_AsyncSubmitServer) error {
	jobInfo, err := s.receiveAsyncJobStream(stream)
	if err != nil {
		glog.Errorf("jobService/AsyncSubmit 接收job数据异常,err:%+v", err)
		return err
	}

	err = s.persistence(jobInfo)
	if err != nil {
		glog.Errorf("jobService/AsyncSubmit 持久化job数据异常,err:%+v", err)
		return err
	}

	// 重新加载最新job信息
	jobInfo, err = s.reloadJobInfo(jobInfo)
	if err != nil {
		glog.Errorf("jobService/AsyncSubmit 重新加载最新job信息异常,err:%+v", err)
		return err
	}

	err = s.appCtx.StartJob(jobInfo)
	if err != nil {
		glog.Errorf("jobService/AsyncSubmit 执行job运算异常,id:%d,err:%+v", jobInfo.Job.Id, err)
		return err
	}

	return stream.SendAndClose(
		&job.AsyncSubmitResponse{
			Id: jobInfo.Job.Id,
		},
	)
}

// 异步通知
func (s *jobService) AsyncNotify(req *job.AsyncNotifyRequest, steam job.JobService_AsyncNotifyServer) error {
	if _, ok := s.appCtx.Scheduler.NotifyChannel.GetChannel(req.NodeId); ok {
		return fmt.Errorf("节点(%s)已注册订阅，不可用重复订阅", req.NodeId)
	}
	s.appCtx.Scheduler.NotifyChannel.RegisterChannel(req.NodeId)
	channel, _ := s.appCtx.Scheduler.NotifyChannel.GetChannel(req.NodeId)

	var err error
	var myJob *dto.JobInfo
	for item := range channel {
		err = steam.Send(&job.AsyncNotifyResponse{
			Id:     item.Job.Id,
			Name:   item.Job.Name,
			Type:   item.Job.Type,
			Status: item.Job.Status,
			Result: item.Job.Result,
		})
		if err == nil {
			// 更新推送状态
			s.jobRepo.UpdateNotifyStatus(s.appCtx.Db, item.Job.Id, enum.SuccessNotifyStatus)
			glog.Infof("jobService/AsyncNotify 通知worker(%s)成功,jobId:%d,jobName:%s", req.NodeId, item.Job.Id, item.Job.Name)
			continue
		}
		// 如果异步通知失败则关闭通道并重新发送给其他channel执行
		myJob = item
		break
	}

	// 如果是异常退出并关闭通道
	s.appCtx.Scheduler.NotifyChannel.RemoveAndCloseChannel(req.NodeId)

	myJob.NotifyCount++
	// 如果重试超过5次则不再重推通知,一下推送状态
	if myJob.NotifyCount > 5 {
		s.jobRepo.UpdateNotifyStatus(s.appCtx.Db, myJob.Job.Id, enum.FailNotifyStatus)
		return err
	}

	// 重新推送给其他worker通道
	s.appCtx.Scheduler.DispatchNotify(myJob)
	return err
}

// 同步提交job
func (s *jobService) SyncSubmit(stream job.JobService_SyncSubmitServer) error {
	jobInfo, err := s.receiveSyncJobStream(stream)
	if err != nil {
		glog.Errorf("jobService/SyncSubmit 接收job数据异常,err:%+v", err)
		return err
	}
	jobInfo.Job.IsAsync = 0

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

	err = s.appCtx.StartJob(jobInfo)
	if err != nil {
		glog.Errorf("jobService/SyncSubmit 执行job运算异常,id:%d,err:%+v", jobInfo.Job.Id, err)
		return err
	}

	return stream.SendAndClose(&job.SyncSubmitResponse{
		Id:     jobInfo.Job.Id,
		Status: jobInfo.Job.Status,
		Result: jobInfo.Job.Result,
	})
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

// 接收同步job流
func (s *jobService) receiveSyncJobStream(stream job.JobService_SyncSubmitServer) (*dto.JobInfo, error) {
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

// 接收异步job流
func (s *jobService) receiveAsyncJobStream(stream job.JobService_AsyncSubmitServer) (*dto.JobInfo, error) {
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
				IsAsync:   1,
			}
			if r.IsNotify {
				jobInfo.Job.IsNotify = 1
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
