package dao

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

type TaskCategory struct {
	ID          int64  `gorm:"column:id;type:bigint unsigned;primaryKey;autoIncrement"`
	TenantID    int64  `gorm:"column:tenant_id;type:bigint unsigned;not null;default:0;uniqueIndex:uniq_task_category_tenant_name,priority:1;index"`
	Name        string `gorm:"column:name;type:varchar(128);not null;uniqueIndex:uniq_task_category_tenant_name,priority:2"`
	Description string `gorm:"column:description;type:varchar(500);not null;default:''"`
	TaskCount   int64  `gorm:"-"`
	CTime       int64  `gorm:"column:ctime;type:bigint;not null"`
	UTime       int64  `gorm:"column:utime;type:bigint;not null"`
}

func (TaskCategory) TableName() string {
	return "task_categories"
}

type TaskCategoryDAO interface {
	Create(ctx context.Context, category TaskCategory) (int64, error)
	FindByID(ctx context.Context, id int64) (TaskCategory, error)
	List(ctx context.Context, bizID int64, keyword string) ([]TaskCategory, error)
	CountUncategorized(ctx context.Context, bizID int64) (int64, error)
	Update(ctx context.Context, category TaskCategory) (int64, error)
	Delete(ctx context.Context, id int64) (int64, error)
	ReplaceTasks(ctx context.Context, categoryID, bizID int64, taskIDs []int64) error
	ListTasks(ctx context.Context, bizID int64, categoryID *int64, query string, offset, limit int) ([]*Task, error)
	CountTasks(ctx context.Context, bizID int64, categoryID *int64, query string) (int64, error)
}

type GORMTaskCategoryDAO struct {
	db *gorm.DB
}

func NewGORMTaskCategoryDAO(db *gorm.DB) TaskCategoryDAO {
	return &GORMTaskCategoryDAO{db: db}
}

func (g *GORMTaskCategoryDAO) Create(ctx context.Context, category TaskCategory) (int64, error) {
	now := time.Now().UnixMilli()
	category.CTime, category.UTime = now, now
	err := g.db.WithContext(ctx).Create(&category).Error
	return category.ID, err
}

func (g *GORMTaskCategoryDAO) FindByID(ctx context.Context, id int64) (TaskCategory, error) {
	var category TaskCategory
	err := g.db.WithContext(ctx).Where("id = ?", id).First(&category).Error
	return category, err
}

func (g *GORMTaskCategoryDAO) List(ctx context.Context, bizID int64, keyword string) ([]TaskCategory, error) {
	var categories []TaskCategory
	query := g.db.WithContext(ctx).Model(&TaskCategory{}).Order("ctime ASC, id ASC")
	if keyword = strings.TrimSpace(keyword); keyword != "" {
		query = query.Where("name LIKE ? OR description LIKE ?", "%"+keyword+"%", "%"+keyword+"%")
	}
	if err := query.Find(&categories).Error; err != nil {
		return nil, err
	}
	for i := range categories {
		if err := g.db.WithContext(ctx).Model(&Task{}).
			Where("category_id = ? AND biz_id = ?", categories[i].ID, bizID).
			Count(&categories[i].TaskCount).Error; err != nil {
			return nil, err
		}
	}
	return categories, nil
}

func (g *GORMTaskCategoryDAO) CountUncategorized(ctx context.Context, bizID int64) (int64, error) {
	var count int64
	err := g.db.WithContext(ctx).Model(&Task{}).
		Where("biz_id = ? AND category_id IS NULL", bizID).
		Count(&count).Error
	return count, err
}

func (g *GORMTaskCategoryDAO) Update(ctx context.Context, category TaskCategory) (int64, error) {
	res := g.db.WithContext(ctx).Model(&TaskCategory{}).
		Where("id = ?", category.ID).
		Updates(map[string]any{
			"name":        category.Name,
			"description": category.Description,
			"utime":       time.Now().UnixMilli(),
		})
	return res.RowsAffected, res.Error
}

func (g *GORMTaskCategoryDAO) Delete(ctx context.Context, id int64) (int64, error) {
	res := g.db.WithContext(ctx).Delete(&TaskCategory{}, id)
	return res.RowsAffected, res.Error
}

func (g *GORMTaskCategoryDAO) ReplaceTasks(ctx context.Context, categoryID, bizID int64, taskIDs []int64) error {
	return g.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var category TaskCategory
		if err := tx.Where("id = ?", categoryID).First(&category).Error; err != nil {
			return err
		}

		if err := tx.Model(&Task{}).Where("category_id = ? AND biz_id = ?", categoryID, bizID).
			Updates(map[string]any{"category_id": nil, "utime": time.Now().UnixMilli()}).Error; err != nil {
			return err
		}
		if len(taskIDs) == 0 {
			return nil
		}

		var count int64
		if err := tx.Model(&Task{}).Where("biz_id = ? AND id IN ?", bizID, taskIDs).Count(&count).Error; err != nil {
			return err
		}
		if count != int64(len(taskIDs)) {
			return fmt.Errorf("one or more tasks do not exist or cannot be categorized")
		}

		return tx.Model(&Task{}).Where("biz_id = ? AND id IN ?", bizID, taskIDs).
			Updates(map[string]any{"category_id": categoryID, "utime": time.Now().UnixMilli()}).Error
	})
}

func (g *GORMTaskCategoryDAO) ListTasks(
	ctx context.Context,
	bizID int64,
	categoryID *int64,
	query string,
	offset, limit int,
) ([]*Task, error) {
	var tasks []*Task
	db := g.taskListQuery(ctx, bizID, categoryID, query).
		Preload("Category").
		Order("id DESC")
	if offset > 0 {
		db = db.Offset(offset)
	}
	if limit > 0 {
		db = db.Limit(limit)
	}
	err := db.Find(&tasks).Error
	return tasks, err
}

func (g *GORMTaskCategoryDAO) CountTasks(
	ctx context.Context,
	bizID int64,
	categoryID *int64,
	query string,
) (int64, error) {
	var count int64
	err := g.taskListQuery(ctx, bizID, categoryID, query).Count(&count).Error
	return count, err
}

func (g *GORMTaskCategoryDAO) taskListQuery(
	ctx context.Context,
	bizID int64,
	categoryID *int64,
	query string,
) *gorm.DB {
	db := g.db.WithContext(ctx).Model(&Task{}).Where("biz_id = ?", bizID)
	if categoryID != nil {
		if *categoryID == 0 {
			db = db.Where("category_id IS NULL")
		} else {
			db = db.Where("category_id = ?", *categoryID)
		}
	}
	if query = strings.TrimSpace(query); query != "" {
		db = db.Where("name LIKE ?", "%"+query+"%")
	}
	return db
}
