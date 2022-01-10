package impl

import (
	"distributed-scheduler/enum"
	"distributed-scheduler/model"
	"distributed-scheduler/repository"

	"github.com/ppkg/glog"
	"github.com/ppkg/kit"
	"gorm.io/gorm"
)

type jobRepositoryImpl struct {
}

// 保存job信息
func (s jobRepositoryImpl) Save(db *gorm.DB, model *model.Job) error {
	err := db.Create(model).Error
	if err != nil {
		glog.Errorf("jobRepositoryImpl/Save 保存job信息异常,err:%+v", err)
		return err
	}
	return nil
}

// 根据ID查询job
func (s jobRepositoryImpl) FindById(db *gorm.DB, id int64) (*model.Job, error) {
	list, err := s.List(db, map[string]interface{}{
		"id": id,
	})
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list[0], nil
}

// 查询job列表
func (s jobRepositoryImpl) List(db *gorm.DB, params map[string]interface{}) ([]*model.Job, error) {
	var list []*model.Job
	if val, ok := params["id"]; ok {
		db = db.Where("id=?", val)
	}
	err := db.Find(&list).Error
	if err != nil {
		glog.Errorf("jobRepositoryImpl/List 查询job列表异常,参数:%s,err:%+v", kit.JsonEncode(params), err)
		return nil, err
	}
	return list, nil
}

// 更新job状态
func (s jobRepositoryImpl) UpdateStatus(db *gorm.DB, data *model.Job) error {
	cols := []interface{}{
		"result",
	}
	if data.Status == enum.FinishJobStatus {
		cols = append(cols, "finish_time")
	}
	err := db.Model(&model.Job{}).Where("id=?", data.Id).Select("status", cols...).Updates(data).Error
	if err != nil {
		glog.Errorf("jobRepositoryImpl/UpdateStatus 更新job状态异常,参数:%s,err:%+v", kit.JsonEncode(data), err)
		return err
	}
	return nil
}

func NewJobRepository() repository.JobRepository {
	return jobRepositoryImpl{}
}
