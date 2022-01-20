package impl

import (
	"github.com/ppkg/distributed-scheduler/enum"
	"github.com/ppkg/distributed-scheduler/model"
	"github.com/ppkg/distributed-scheduler/repository"

	"github.com/ppkg/glog"
	"github.com/ppkg/kit"
	"gorm.io/gorm"
)

type taskRepositoryImpl struct {
}

// 批量保存task信息
func (s taskRepositoryImpl) BatchSave(db *gorm.DB, list []*model.Task) error {
	err := db.Omit("id", "output", "message", "finish_time").CreateInBatches(&list, 100).Error
	if err != nil {
		glog.Errorf("taskRepositoryImpl/BatchSave 批量保存task信息异常,参数:%s,err:%+v", kit.JsonEncode(list), err)
		return err
	}
	return nil
}

func (s taskRepositoryImpl) Save(db *gorm.DB, model *model.Task) error {
	err := db.Omit("id", "output", "message", "finish_time").Create(model).Error
	if err != nil {
		glog.Errorf("taskRepositoryImpl/Save 保存单个task信息异常,参数:%s,err:%+v", kit.JsonEncode(model), err)
		return err
	}
	return nil
}

// 查询task列表
func (s taskRepositoryImpl) List(db *gorm.DB, params map[string]interface{}) ([]*model.Task, error) {
	var list []*model.Task
	if val, ok := params["jobId"]; ok {
		db = db.Where("job_id=?", val)
	}
	if val, ok := params["jobIds"]; ok {
		db = db.Where("job_id in (?)", val)
	}
	err := db.Find(&list).Error
	if err != nil {
		glog.Errorf("taskRepositoryImpl/List 查询task列表异常,参数:%s,err:%+v", kit.JsonEncode(params), err)
		return nil, err
	}
	return list, nil
}

// 更新task状态
func (s taskRepositoryImpl) UpdateStatus(db *gorm.DB, data *model.Task) error {
	cols := []interface{}{
		"node_id",
	}
	if enum.TaskStatus(data.Status) == enum.FinishTaskStatus {
		cols = append(cols, "output", "finish_time")
	}
	if enum.TaskStatus(data.Status) == enum.ExceptionTaskStatus {
		cols = append(cols, "message")
	}
	err := db.Model(&model.Task{}).Where("id=?", data.Id).Select("status", cols...).Updates(data).Error
	if err != nil {
		glog.Errorf("taskRepositoryImpl/UpdateStatus 更新task状态异常,参数:%s,err:%+v", kit.JsonEncode(data), err)
		return err
	}
	return nil
}

func NewTaskRepository() repository.TaskRepository {
	return taskRepositoryImpl{}
}
