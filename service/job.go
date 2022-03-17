package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/ppkg/distributed-scheduler/core"
	"github.com/ppkg/distributed-scheduler/dto"
	"github.com/ppkg/distributed-scheduler/enum"
	"github.com/ppkg/distributed-scheduler/errCode"
	"github.com/ppkg/distributed-scheduler/model"
	"github.com/ppkg/distributed-scheduler/proto/job"
	"github.com/ppkg/distributed-scheduler/repository"
	"github.com/ppkg/distributed-scheduler/repository/impl"
	"github.com/ppkg/kit"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/maybgit/glog"
	"gorm.io/gorm"
)

type jobService struct {
	appCtx   *core.ApplicationContext
	jobRepo  repository.JobRepository
	taskRepo repository.TaskRepository
}

// 异步结果通知
func (s *jobService) AsyncNotify(_ context.Context, _ *job.AsyncNotifyRequest) (*empty.Empty, error) {
	return nil, errors.New("not implemented")
}

// job开始执行通知
func (s *jobService) AsyncPostStart(_ context.Context, _ *job.AsyncPostStartRequest) (*empty.Empty, error) {
	return nil, errors.New("not implemented")
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

	go func() {
		_ = s.appCtx.StartJob(jobInfo)
	}()

	return stream.SendAndClose(
		&job.AsyncSubmitResponse{
			Id: jobInfo.Job.Id,
		},
	)
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

	resp := &job.SyncSubmitResponse{
		Id:     jobInfo.Job.Id,
		Status: jobInfo.Job.Status,
		Result: jobInfo.Job.Result,
		Mesage: jobInfo.Job.Message,
	}
	if jobInfo.Job.Meta != "" {
		_ = json.Unmarshal([]byte(jobInfo.Job.Meta), &resp.Meta)
	}
	return stream.SendAndClose(resp)
}

// 重新加载job信息
func (s *jobService) reloadJobInfo(jobInfo *dto.JobInfo) (*dto.JobInfo, error) {
	job, err := s.getJob(jobInfo.Job.Id)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, errCode.ToGrpcErr(errCode.ErrJobNotFound)
	}

	jobInfo = dto.NewJobInfo(job)
	taskList, err:=s.getExpectTaskList(jobInfo.Job.Id, job.Size)
	if err != nil {
		return nil, err
	}
	jobInfo.TaskList.Append(taskList...)
	return jobInfo, err
}

// 获取期望task列表
func (s *jobService) getExpectTaskList(jobId int64, size int32) ([]*model.Task, error) {
	taskList, err := s.taskRepo.List(s.appCtx.Db, map[string]interface{}{
		"jobId": jobId,
	})
	if err != nil {
		return nil, err
	}
	shardingMap := make(map[int32]struct{})
	for _, v := range taskList {
		if v.Sharding != 0 {
			continue
		}
		shardingMap[v.Sharding] = struct{}{}
	}
	if size == int32(len(shardingMap)) {
		return taskList, nil
	}

	// 当主从数据库存在延时时，需要睡眠一会再查询
	glog.Warningf("jobService/getExpectTaskList 主从数据库存在延时 jobId:%d,size:%d", jobId, size)
	time.Sleep(3 * time.Second)
	return s.taskRepo.List(s.appCtx.Db, map[string]interface{}{
		"jobId": jobId,
	})
}

// 查询分布式job信息
// 当主从同步数据延时严重导致最新数据查询不到
func (s *jobService) getJob(id int64) (*model.Job, error) {
	job, err := s.jobRepo.FindById(s.appCtx.Db, id)
	if err != nil {
		return nil, err
	}
	if job != nil {
		return job, nil
	}

	// 当主从数据库存在延时时，需要睡眠一会再查询
	glog.Warningf("jobService/getJob 主从数据库存在延时 %d", id)
	time.Sleep(3 * time.Second)
	return s.jobRepo.FindById(s.appCtx.Db, id)
}

// 持久化job信息
func (s *jobService) persistence(jobInfo *dto.JobInfo) error {
	return s.appCtx.Db.Transaction(func(tx *gorm.DB) error {
		err := s.jobRepo.Save(tx, jobInfo.Job)
		if err != nil {
			return err
		}
		taskList := jobInfo.TaskList.GetAll()
		for _, item := range taskList {
			item.JobId = jobInfo.Job.Id
		}
		return s.taskRepo.BatchSave(tx, taskList)
	})
}

// 接收同步job流
func (s *jobService) receiveSyncJobStream(stream job.JobService_SyncSubmitServer) (*dto.JobInfo, error) {
	var jobInfo *dto.JobInfo
	var firstPlugin string
	var sharding int32 = 0
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(r.PluginSet) == 0 {
			return nil, errCode.ToGrpcErr(errCode.ErrPluginSetEmpty)
		}
		// 初始化job数据
		if jobInfo == nil {
			jobInfo = dto.NewJobInfo(&model.Job{
				Name:                   r.Name,
				PluginSet:              strings.Join(r.PluginSet, ","),
				Label:                  r.Label,
				Source:                 r.Source,
				TaskExceptionOperation: r.TaskExceptionOperation,
			})
			if len(r.Meta) > 0 {
				jobInfo.Job.Meta = kit.JsonEncode(r.Meta)
			}
			firstPlugin = r.PluginSet[0]
		}

		var subPlugins []string
		if dto.IsParallelPlugin(firstPlugin) {
			subPlugins = dto.SplitParallelPlugin(firstPlugin)
		} else {
			subPlugins = append(subPlugins, "")
		}
		for _, item := range subPlugins {
			jobInfo.TaskList.Append(&model.Task{
				Sharding:  sharding,
				Name:      fmt.Sprintf("%s-%d", jobInfo.Job.Name, sharding),
				Input:     r.Data,
				Plugin:    firstPlugin,
				SubPlugin: item,
			})
		}
		jobInfo.Job.Size++
		sharding++
	}

	return jobInfo, nil
}

// 接收异步job流
func (s *jobService) receiveAsyncJobStream(stream job.JobService_AsyncSubmitServer) (*dto.JobInfo, error) {
	var jobInfo *dto.JobInfo
	var firstPlugin string
	var sharding int32 = 0
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(r.PluginSet) == 0 {
			return nil, errCode.ToGrpcErr(errCode.ErrPluginSetEmpty)
		}
		// 初始化job数据
		if jobInfo == nil {
			jobInfo = dto.NewJobInfo(&model.Job{
				Name:                   r.Name,
				Type:                   r.Type,
				PluginSet:              strings.Join(r.PluginSet, ","),
				IsAsync:                1,
				Label:                  r.Label,
				Source:                 r.Source,
				TaskExceptionOperation: r.TaskExceptionOperation,
			})
			if r.IsNotify {
				jobInfo.Job.IsNotify = 1
			}
			if len(r.Meta) > 0 {
				jobInfo.Job.Meta = kit.JsonEncode(r.Meta)
			}
			firstPlugin = r.PluginSet[0]
		}

		var subPlugins []string
		if dto.IsParallelPlugin(firstPlugin) {
			subPlugins = dto.SplitParallelPlugin(firstPlugin)
		} else {
			subPlugins = append(subPlugins, "")
		}

		for _, item := range subPlugins {
			jobInfo.TaskList.Append(&model.Task{
				Sharding:  sharding,
				Name:      fmt.Sprintf("%s-%d", jobInfo.Job.Name, sharding),
				Input:     r.Data,
				Plugin:    firstPlugin,
				SubPlugin: item,
			})
		}
		jobInfo.Job.Size++

		sharding++
	}
	return jobInfo, nil
}

// 手动取消job
func (s *jobService) ManualCancel(ctx context.Context, req *job.ManualCancelRequest) (*job.ManualCancelResponse, error) {
	if req.Id == 0 {
		return nil, errors.New("参数ID不能为空")
	}
	resp := &job.ManualCancelResponse{
		Status: int32(enum.SuccessCancelStatus),
	}
	job, err := s.jobRepo.FindById(s.appCtx.Db, req.Id)
	if err != nil {
		msg := fmt.Sprintf("查询job详情异常,err:%+v", err)
		glog.Errorf("jobService/ManualCancel %s", msg)
		resp.Status = int32(enum.FailCancelStatus)
		resp.Message = msg
		return resp, nil
	}
	// 处理job不存在情况
	if job == nil {
		resp.Status = int32(enum.NotFoundCancelStatus)
		return resp, nil
	}
	// 处理job已完成情况
	if job.Status == int32(enum.FinishJobStatus) {
		resp.Status = int32(enum.FinishJobCancelStatus)
		return resp, nil
	}
	var reason []string
	if req.Reason != "" {
		reason = append(reason, req.Reason)
	}
	err = s.appCtx.ManualCancelJob(req.Id, reason...)
	if err != nil {
		resp.Status = int32(enum.FailCancelStatus)
		resp.Message = err.Error()
	}
	return resp, nil
}

func NewJobService(ctx *core.ApplicationContext) job.JobServiceServer {
	return &jobService{
		appCtx:   ctx,
		jobRepo:  impl.NewJobRepository(),
		taskRepo: impl.NewTaskRepository(),
	}
}
