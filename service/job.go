package service

import (
	"distributed-scheduler/core"
	"distributed-scheduler/dto"
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
	jobInfo, err = s.reloadJobInfo(jobInfo.Job.Id)
	if err != nil {
		glog.Errorf("jobService/SyncSubmit 重新加载最新job信息异常,err:%+v", err)
		return err
	}

	return stream.SendAndClose(&job.SyncSubmitResponse{})
}

// 重新加载job信息
func (s *jobService) reloadJobInfo(id int64) (dto.JobInfo, error) {
	var jobInfo dto.JobInfo
	var err error
	jobInfo.Job, err = s.jobRepo.FindById(s.appCtx.Db, id)
	if err != nil {
		return jobInfo, err
	}
	if jobInfo.Job == nil {
		return jobInfo, errCode.ToGrpcErr(errCode.ErrJobNotFound)
	}
	jobInfo.TaskList, err = s.taskRepo.List(s.appCtx.Db, map[string]interface{}{
		"jobId": id,
	})
	return jobInfo, err
}

// 持久化job信息
func (s *jobService) persistence(jobInfo dto.JobInfo) error {
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
func (s *jobService) receiveJobStream(stream job.JobService_SyncSubmitServer) (dto.JobInfo, error) {
	var jobInfo dto.JobInfo
	var firstPlugin string
	pos := 1
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
		jobInfo.TaskList = append(jobInfo.TaskList, &model.Task{
			Name:   fmt.Sprintf("%s-%d", jobInfo.Job.Name, pos),
			Input:  r.Data,
			Plugin: firstPlugin,
		})
		pos++
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
