package dao

import "gorm.io/gorm"

func InitTables(db *gorm.DB) error {
	return db.AutoMigrate(
		&TaskCategory{},
		&Task{},
		&TaskExecution{},
		&TaskExecutionLog{},
	)
}
