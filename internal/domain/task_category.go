package domain

type TaskCategory struct {
	ID          int64
	TenantID    int64
	Name        string
	Description string
	TaskCount   int64
	CTime       int64
	UTime       int64
}
