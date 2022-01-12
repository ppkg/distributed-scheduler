package dto

import "context"

type CancelTaskKey struct{}
type CancelTaskParam struct {
	CancelFunc context.CancelFunc
	// 取消原因
	Reason string
	// 上下文状态,0：正常运行，1:系统错误而取消，2：用户手动取消
	State int32
}
