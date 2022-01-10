package impl

import (
	"distributed-scheduler/model"
	"distributed-scheduler/repository"

	"github.com/ppkg/glog"
	"github.com/ppkg/kit"
	"gorm.io/gorm"
)

type taskRepositoryImpl struct {
}

// 批量保存task信息
func (s taskRepositoryImpl) BatchSave(db *gorm.DB, list []*model.Task) error {
	err := db.CreateInBatches(&list, 100).Error
	if err != nil {
		glog.Errorf("taskRepositoryImpl/BatchSave 批量保存task信息异常,参数:%s,err:%+v", kit.JsonEncode(list), err)
		return err
	}
	return nil
}

// 查询task列表
func (s taskRepositoryImpl) List(db *gorm.DB, params map[string]interface{}) ([]*model.Task, error) {
	var list []*model.Task
	if val, ok := params["jobId"]; ok {
		db.Where("job_id=?", val)
	}
	err := db.Find(&list).Error
	if err != nil {
		glog.Errorf("taskRepositoryImpl/List 查询task列表异常,参数:%s,err:%+v", kit.JsonEncode(params), err)
		return nil, err
	}
	return list, nil
}

// 更新task状态
func (s taskRepositoryImpl) UpdateStatus(db *gorm.DB, model *model.Task) error {
	err := db.Where("id=?", model.Id).Select("status,output,node_id,endpoint,finish_time").Updates(model).Error
	if err != nil {
		glog.Errorf("taskRepositoryImpl/UpdateStatus 更新task状态异常,参数:%s,err:%+v", kit.JsonEncode(model), err)
		return err
	}
	return nil
}

func NewTaskRepository() repository.TaskRepository {
	return taskRepositoryImpl{}
}
