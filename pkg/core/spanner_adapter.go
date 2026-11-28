package core

import (
	"context"
	"errors"

	"github.com/roryq/wrench/pkg/migration"
	"github.com/roryq/wrench/pkg/spanner"
)

// spannerDriverAdapter adapts *spanner.Client to migration.Driver without altering
// any exported method signatures on *spanner.Client.
type spannerDriverAdapter struct {
	client *spanner.Client
}

var _ migration.Driver = (*spannerDriverAdapter)(nil)

// NewSpannerDriver wraps a *spanner.Client as a migration.Driver.
func NewSpannerDriver(client *spanner.Client) Driver {
	return &spannerDriverAdapter{client: client}
}

func (a *spannerDriverAdapter) EnsureMigrationTable(ctx context.Context, tableName string) error {
	return a.client.EnsureMigrationTable(ctx, tableName)
}

func (a *spannerDriverAdapter) EnsureRepeatableMigrationTable(ctx context.Context, tableName string) error {
	return a.client.EnsureRepeatableMigrationTable(ctx, tableName)
}

func (a *spannerDriverAdapter) SetupMigrationLock(ctx context.Context, tableName string) error {
	return a.client.SetupMigrationLock(ctx, tableName)
}

func (a *spannerDriverAdapter) GetMigrationLock(ctx context.Context, tableName, lockIdentifier string) (migration.MigrationLock, error) {
	return a.client.GetMigrationLock(ctx, tableName, lockIdentifier)
}

func (a *spannerDriverAdapter) ExecuteMigrations(ctx context.Context, migrations migration.Migrations, opts migration.ExecuteOptions) error {
	status, err := a.client.DetermineUpgradeStatus(ctx, opts.VersionTableName)
	if err != nil {
		return err
	}

	switch status {
	case spanner.ExistingMigrationsUpgradeStarted:
		_, err = a.client.UpgradeExecuteMigrations(ctx, migrations, opts.Limit, opts.VersionTableName, opts.ProtoDescriptors, opts.FFMigrations)
		return err
	case spanner.ExistingMigrationsUpgradeCompleted:
		_, err = a.client.ExecuteMigrations(ctx, migrations, opts.Limit, opts.VersionTableName, opts.PartitionedDMLConcurrency, opts.ProtoDescriptors, opts.FFMigrations)
		return err
	default:
		return errors.New("migration in undetermined state")
	}
}

func (a *spannerDriverAdapter) ExecuteRepeatableMigrations(ctx context.Context, migrations migration.Migrations, tableName string, opts migration.ExecuteOptions) error {
	_, err := a.client.ExecuteRepeatableMigrations(ctx, migrations, tableName, opts.PartitionedDMLConcurrency, opts.ProtoDescriptors)
	return err
}

func (a *spannerDriverAdapter) GetSchemaMigrationVersion(ctx context.Context, tableName string) (uint, bool, error) {
	return a.client.GetSchemaMigrationVersion(ctx, tableName)
}

func (a *spannerDriverAdapter) GetMigrationHistory(ctx context.Context, tableName string) ([]migration.MigrationHistoryRecord, error) {
	return a.client.GetMigrationHistory(ctx, tableName)
}

func (a *spannerDriverAdapter) GetRepeatableMigrationHistory(ctx context.Context, tableName string) ([]migration.RepeatableMigrationHistoryRecord, error) {
	return a.client.GetRepeatableMigrationHistory(ctx, tableName)
}

func (a *spannerDriverAdapter) RepairMigration(ctx context.Context, tableName string) error {
	return a.client.RepairMigration(ctx, tableName)
}

func (a *spannerDriverAdapter) Close() error {
	return a.client.Close()
}
