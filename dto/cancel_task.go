package dto

import "context"

type CancelTaskKey struct{}
type CancelTaskParam struct {
	CancelFunc context.CancelFunc
	// 取消原因
	Reason string
	// 上下文是否已取消,0：否，1:系统错误而取消，2：用户手动取消
	IsCancel int32
}
