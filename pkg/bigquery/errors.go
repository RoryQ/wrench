package bigquery

type ErrorCode int

const (
	ErrorCodeCreateClient ErrorCode = iota + 1
	ErrorCodeCloseClient
	ErrorCodeEnsureMigrationTables
	ErrorCodeExecuteMigrations
	ErrorCodeGetMigrationVersion
	ErrorCodeSetMigrationVersion
	ErrorCodeNoMigration
	ErrorCodeMigrationVersionDirty
	ErrorCodeRepairMigration
	ErrorCodeMigrationLock
	ErrorCodeLoadSchema
)

type Error struct {
	Code ErrorCode
	err  error
}

func (e *Error) Error() string {
	return e.err.Error()
}

func (e *Error) Unwrap() error {
	return e.err
}
