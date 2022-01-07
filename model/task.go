package model

import (
	"database/sql"
	"time"
)

type Task struct {
	Id    int64 `gorm:"primaryKey"`
	JobId int64
	// task名称
	Name string
	// 当前任务处理插件
	Plugin string
	// task状态,0:待执行，1：执行中，2：执行完成，3：异常退出
	Status int32
	// 输入参数
	Input string
	// 输出参数
	Output string
	// 分配执行节点ID
	NodeId string
	// 分配执行节点端点
	Endpoint string
	// 创建时间
	CreateTime time.Time `gorm:"->"`
	// 完成时间
	FinishTime sql.NullTime
}
