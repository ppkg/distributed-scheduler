package enum

// job状态,0:待执行，1：执行中，2：执行完成，3：取消执行，4：系统异常，5：调度task失败，6：运行超时，7：业务处理异常，8：回调通知异常
const (
	PendingJobStatus           JobStatus = 0
	DoingJobStatus             JobStatus = 1
	FinishJobStatus            JobStatus = 2
	CancelJobStatus            JobStatus = 3
	SystemExceptionJobStatus   JobStatus = 4
	PushTaskExceptionJobStatus JobStatus = 5
	RunningTimeoutJobStatus    JobStatus = 6
	BusinessExceptionJobStatus JobStatus = 7
	NotifyExceptionJobStatus   JobStatus = 8
)

type JobStatus int32

const (
	// 正常运行
	NormalRuningState ContextState = 0
	// 异常退出
	ExceptionCancelState ContextState = 1
	// 手动取消
	ManualCancelState ContextState = 2
)

type ContextState int32

// 通知状态，0：待通知，1：通知成功，2：通知失败
const (
	PendingNotifyStatus NotifyStatus = 0
	SuccessNotifyStatus NotifyStatus = 1
	FailNotifyStatus    NotifyStatus = 2
)

type NotifyStatus int32
