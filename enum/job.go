package enum

// job状态,0:待执行，1：执行中，2：执行完成，3：取消执行，4：系统异常，5：调度task失败，6：运行超时，7：业务处理异常，8：回调通知异常
const (
	PendingJobStatus           int32 = 0
	DoingJobStatus             int32 = 1
	FinishJobStatus            int32 = 2
	CancelJobStatus            int32 = 3
	SystemExceptionJobStatus   int32 = 4
	PushTaskExceptionJobStatus int32 = 5
	RunningTimeoutJobStatus    int32 = 6
	BusinessExceptionJobStatus int32 = 7
	NotifyExceptionJobStatus   int32 = 8
)

const (
	// 正常运行
	NormalRuningState int32 = 0
	// 异常退出
	ExceptionCancelState int32 = 1
	// 手动取消
	ManualCancelState int32 = 2
)

// 通知状态，0：待通知，1：通知成功，2：通知失败
const (
	PendingNotifyStatus int32 = 0
	SuccessNotifyStatus int32 = 1
	FailNotifyStatus    int32 = 2
)
