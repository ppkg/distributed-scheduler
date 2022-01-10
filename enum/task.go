package enum

const (
	// task状态,0:待执行，1：执行中，2：执行完成，3：异常退出
	PendingTaskStatus   int32 = 0
	DoingTaskStatus     int32 = 1
	FinishTaskStatus    int32 = 2
	ExceptionTaskStatus int32 = 3
)

var TaskStatusMap=map[int32]string{
	PendingTaskStatus:"待执行",
	DoingTaskStatus:"执行中",
	FinishTaskStatus:"执行完成",
	ExceptionTaskStatus:"异常退出",
}