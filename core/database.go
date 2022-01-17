package core

import (
	"time"

	"github.com/limitedlee/microservice/common/config"
	"github.com/ppkg/glog"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlog "gorm.io/gorm/logger"
)

func (s *ApplicationContext) initDatabase() error {
	var err error
	s.Db, err = gorm.Open(mysql.Open(config.GetString("mysql.github.com/ppkg/distributed-scheduler")), &gorm.Config{
		Logger: gormlog.Default.LogMode(gormlog.Info),
	})
	if err != nil {
		glog.Errorf("ApplicationContext/initDatabase 初始化数据库失败,err:%+v", err)
		return err
	}

	// 设置数据库连接池
	sqlDB, err := s.Db.DB()
	if err != nil {
		glog.Errorf("ApplicationContext/initDatabase 设置数据库连接池失败,err:%+v", err)
		return err
	}
	// SetMaxIdleConns 设置空闲连接池中连接的最大数量
	sqlDB.SetMaxIdleConns(10)

	// SetMaxOpenConns 设置打开数据库连接的最大数量。
	sqlDB.SetMaxOpenConns(1000)

	// SetConnMaxLifetime 设置了连接可复用的最大时间。
	sqlDB.SetConnMaxLifetime(time.Hour)
	return nil
}
