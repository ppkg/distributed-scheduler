package model

import (
	"time"
)

type Task struct {
	Id    int64 `gorm:"primaryKey"`
	JobId int64
	// task分片
	Sharding int32
	// task名称
	Name string
	// 当前任务处理插件
	Plugin string
	// 子插件，如果不为空则说明当前任务是并行任务
	SubPlugin string
	// task状态,0:待执行，1：执行中，2：执行完成，3：异常退出
	Status int32
	// 输入参数
	Input string
	// 输出参数
	Output string
	// job运行异常信息
	Message string
	// 分配执行节点ID
	NodeId string
	// 创建时间
	CreateTime time.Time `gorm:"->"`
	// 更新时间
	UpdateTime time.Time `gorm:"->"`
	// 完成时间
	FinishTime time.Time
}

func (s Task) TableName() string {
	return "task"
}
