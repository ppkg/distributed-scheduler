package model

import (
	"time"
)

type Job struct {
	Id int64 `gorm:"primaryKey"`
	// job名称
	Name string
	// job类型
	Type string
	// job标签，便于job快速搜索
	Label string
	// 数据来源，提交job的应用名称
	Source string
	// task异常操作，0：退出当前job，1：继续执行
	TaskExceptionOperation int32
	// job状态,0:待执行，1：执行中，2：执行完成，3：取消执行，4：系统异常，5：task推送失败，6：运行超时，7：业务处理异常，8：回调通知异常
	Status int32
	// 插件集合，多个以","分隔
	PluginSet string
	// 是否为异步job，0：否，1：是
	IsAsync int32
	// job完成是否需要通知worker，0：不需要，1：需要
	IsNotify int32
	// task数量
	Size int32
	// job运行结果
	Result string
	// job运行异常信息
	Message string
	// 创建时间
	CreateTime time.Time `gorm:"->"`
	// 更新时间
	UpdateTime time.Time `gorm:"->"`
	// 完成时间
	FinishTime time.Time
}

func (s Job) TableName() string {
	return "job"
}
