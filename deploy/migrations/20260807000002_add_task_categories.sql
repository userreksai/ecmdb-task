-- +goose Up
CREATE TABLE IF NOT EXISTS task_categories (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
    name VARCHAR(128) NOT NULL,
    description VARCHAR(500) NOT NULL DEFAULT '',
    ctime BIGINT NOT NULL,
    utime BIGINT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq_task_category_tenant_name (tenant_id, name),
    KEY idx_task_categories_tenant_id (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

SET @task_category_column_exists = (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tasks' AND COLUMN_NAME = 'category_id'
);
SET @task_category_column_sql = IF(
    @task_category_column_exists = 0,
    'ALTER TABLE tasks ADD COLUMN category_id BIGINT UNSIGNED NULL COMMENT ''任务分类ID''',
    'SELECT 1'
);
PREPARE task_category_column_stmt FROM @task_category_column_sql;
EXECUTE task_category_column_stmt;
DEALLOCATE PREPARE task_category_column_stmt;

SET @task_category_index_exists = (
    SELECT COUNT(*) FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tasks' AND INDEX_NAME = 'idx_tasks_category_id'
);
SET @task_category_index_sql = IF(
    @task_category_index_exists = 0,
    'ALTER TABLE tasks ADD INDEX idx_tasks_category_id (category_id)',
    'SELECT 1'
);
PREPARE task_category_index_stmt FROM @task_category_index_sql;
EXECUTE task_category_index_stmt;
DEALLOCATE PREPARE task_category_index_stmt;

SET @task_category_fk_exists = (
    SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS
    WHERE CONSTRAINT_SCHEMA = DATABASE()
      AND TABLE_NAME = 'tasks'
      AND CONSTRAINT_NAME = 'fk_tasks_category'
      AND CONSTRAINT_TYPE = 'FOREIGN KEY'
);
SET @task_category_fk_sql = IF(
    @task_category_fk_exists = 0,
    'ALTER TABLE tasks ADD CONSTRAINT fk_tasks_category FOREIGN KEY (category_id) REFERENCES task_categories(id) ON DELETE SET NULL',
    'SELECT 1'
);
PREPARE task_category_fk_stmt FROM @task_category_fk_sql;
EXECUTE task_category_fk_stmt;
DEALLOCATE PREPARE task_category_fk_stmt;

-- +goose Down
SET @task_category_fk_exists = (
    SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS
    WHERE CONSTRAINT_SCHEMA = DATABASE()
      AND TABLE_NAME = 'tasks'
      AND CONSTRAINT_NAME = 'fk_tasks_category'
      AND CONSTRAINT_TYPE = 'FOREIGN KEY'
);
SET @task_category_fk_sql = IF(
    @task_category_fk_exists > 0,
    'ALTER TABLE tasks DROP FOREIGN KEY fk_tasks_category',
    'SELECT 1'
);
PREPARE task_category_fk_stmt FROM @task_category_fk_sql;
EXECUTE task_category_fk_stmt;
DEALLOCATE PREPARE task_category_fk_stmt;

SET @task_category_index_exists = (
    SELECT COUNT(*) FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tasks' AND INDEX_NAME = 'idx_tasks_category_id'
);
SET @task_category_index_sql = IF(
    @task_category_index_exists > 0,
    'ALTER TABLE tasks DROP INDEX idx_tasks_category_id',
    'SELECT 1'
);
PREPARE task_category_index_stmt FROM @task_category_index_sql;
EXECUTE task_category_index_stmt;
DEALLOCATE PREPARE task_category_index_stmt;

SET @task_category_column_exists = (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tasks' AND COLUMN_NAME = 'category_id'
);
SET @task_category_column_sql = IF(
    @task_category_column_exists > 0,
    'ALTER TABLE tasks DROP COLUMN category_id',
    'SELECT 1'
);
PREPARE task_category_column_stmt FROM @task_category_column_sql;
EXECUTE task_category_column_stmt;
DEALLOCATE PREPARE task_category_column_stmt;

DROP TABLE IF EXISTS task_categories;
