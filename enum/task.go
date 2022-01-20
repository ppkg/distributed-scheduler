package enum

const (
	// task状态,0:待执行，1：执行中，2：执行完成，3：异常退出
	PendingTaskStatus   TaskStatus = 0
	DoingTaskStatus     TaskStatus = 1
	FinishTaskStatus    TaskStatus = 2
	ExceptionTaskStatus TaskStatus = 3
)

type TaskStatus int32

var TaskStatusMap = map[TaskStatus]string{
	PendingTaskStatus:   "待执行",
	DoingTaskStatus:     "执行中",
	FinishTaskStatus:    "执行完成",
	ExceptionTaskStatus: "异常退出",
}

// task异常操作，0：退出当前job，1：继续执行
const (
	ExitTaskExceptionOperation     TaskExceptionOperation = 0
	ContinueTaskExceptionOperation TaskExceptionOperation = 1
)

type TaskExceptionOperation int32
