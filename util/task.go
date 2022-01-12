package util

import (
	"context"
	"distributed-scheduler/dto"
	"distributed-scheduler/enum"
	"distributed-scheduler/model"
	"sync/atomic"
)

// 取消通知
func CancelNotify(ctx context.Context, job *dto.JobInfo, reason string) {
	// 通知未执行task取消操作
	cancelParam := ctx.Value(dto.CancelTaskKey{}).(*dto.CancelTaskParam)
	cancelParam.Reason = reason
	if atomic.CompareAndSwapInt32(&cancelParam.State, enum.NormalRuningState, enum.ExceptionCancelState) {
		close(job.Done)
		cancelParam.CancelFunc()
	}
}

func FilterTaskByPlugin(list []*model.Task, plugin string) []*model.Task {
	var result []*model.Task
	for _, item := range list {
		if item.Plugin != plugin {
			continue
		}
		result = append(result, item)
	}
	return result
}
