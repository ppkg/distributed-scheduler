package repository

import (
	"github.com/ppkg/distributed-scheduler/model"

	"gorm.io/gorm"
)

type TaskRepository interface {
	// 批量保存task信息
	BatchSave(db *gorm.DB, list []*model.Task) error
	Save(db *gorm.DB, model *model.Task) error
	// 查询task列表
	List(db *gorm.DB, params map[string]interface{}) ([]*model.Task, error)
	// 更新task状态
	UpdateStatus(db *gorm.DB, model *model.Task) error
}
