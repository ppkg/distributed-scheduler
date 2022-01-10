package repository

import (
	"distributed-scheduler/model"

	"gorm.io/gorm"
)

type JobRepository interface {
	// 保存job信息
	Save(db *gorm.DB, model *model.Job) error
	// 根据ID查询job
	FindById(db *gorm.DB, id int64) (*model.Job, error)
	// 查询job列表
	List(db *gorm.DB, params map[string]interface{}) ([]*model.Job, error)
	// 更新job状态
	UpdateStatus(db *gorm.DB, model *model.Job) error
}
