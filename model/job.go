package model

import (
	"database/sql"
	"time"
)

type Job struct {
	Id int64 `gorm:"primaryKey"`
	// job名称
	Name string
	// job类型
	Type string
	// job状态,0:待执行，1：执行中，2：执行完成，3：异常退出，4：已取消执行，5：推送失败
	Status int32
	// 插件集合，多个以","分隔
	PluginSet string
	// 是否为异步job，0：否，1：是
	IsAsync int32
	// task数量
	Size int32
	// job运行结果
	Result string
	// 创建时间
	CreateTime time.Time `gorm:"->"`
	// 完成时间
	FinishTime sql.NullTime
}
