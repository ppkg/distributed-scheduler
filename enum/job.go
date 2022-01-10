package enum

// job状态,0:待执行，1：执行中，2：执行完成，3：异常退出，4：已取消执行，5：推送失败
const (
	PendingJobStatus   int32 = 0
	DoingJobStatus     int32 = 1
	FinishJobStatus    int32 = 2
	ExceptionJobStatus int32 = 3
	CancelJobStatus    int32 = 4
	PushFailJobStatus  int32 = 5
)

const (
	// 异常退出
	ExceptionCancel int32 = 1
	// 手动取消
	ManualCancel int32 = 2
)
