package util

import (
	"context"
	"distributed-scheduler/dto"
	"sync/atomic"
)

// 取消通知
func CancelNotify(ctx context.Context, job *dto.JobInfo, reason string) {
	// 通知未执行task取消操作
	cancelParam := ctx.Value(dto.CancelTaskKey{}).(*dto.CancelTaskParam)
	cancelParam.Reason = reason
	if atomic.CompareAndSwapInt32(&cancelParam.IsCancel, 0, 1) {
		close(job.Done)
		cancelParam.CancelFunc()
	}
}
