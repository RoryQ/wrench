# wrench

This is a fork of https://github.com/cloudspannerecosystem/wrench with the following improvements

- Records timestamped history of applied migrations, not just the current version number.
- Supports out of order migrations. Similar to [FlywayDB](https://flywaydb.org/documentation/commandline/migrate#outOfOrder), addresses [golang-migrate/migrate/#278](https://github.com/golang-migrate/migrate/issues/278)
- Migration locking. Prevents multiple wrench processes from applying the same migration.
- Automated release builds. Each release has prebuilt binary for multiple os/arch that can be downloaded to your CI environment without requiring golang to build from source.
- Supports INSERT statements in migration DML scripts. (Not just partitioned DML)
- Custom intervals for migration sequences. Generated migration files can be numbered by 10s, 100s etc. E.g. `[00010.sql, 00020.sql, 00030.sql]` This is allows hotfixes to be inserted inbetween applied migrations.
- Export schema to discrete files. Instead of a `schema.sql` containing all the objects. If this is checked into source control this makes diff-ing more consistent as it follows a hierarchy vs moving around in a single file. e.g. `[table/table1.sql, table/table2.sql, index/index1.sql]`
- Export static data tables by specifying in a `static_data_tables.txt` or `wrench.json` file.
- Automatically upgrades tracking tables used by [cloudspannerecosystem/wrench](https://github.com/cloudspannerecosystem/wrench) or [golang-migrate/migrate](https://github.com/golang-migrate/migrate) to this version.
- Skip Versions. Flag `--skip-versions` can be set to skip migrations. Useful for working around unsupported features in the emulator during local development.
- Repair dirty migrations. If a migration fails the version is marked as dirty. Any partial changes should be reverted manually and the history cleaned
using `migrate repair`.

## Onboarding existing databases to wrench

This fork of wrench uses two additional tables for tracking migrations, `SchemaMigrationsHistory` for all scripts
applied and `SchemaMigrationsLock` to limit wrench migrations to a single invocation.
If coming from a database managed by `golang-migrate` or the `cloudspannerecosystem/wrench` then you will already have a
`SchemaMigrations` table and no work is needed. You can proceed to use this version of wrench and during the next migration
it will detect that the `SchemaMigrationsHistory` table is missing, then create and backfill the "history" data.
Subsequent `migrate up` invocations will use the history table instead of the `SchemaMigrations` table to detect unapplied
migrations.

If you have an existing database that is not controlled by any migration tools then you should export the current schema
(you can use `wrench load`) and use this as the baseline version by saving as `000001.sql` and manually creating a
`SchemaMigrations` table with a `1` entry. This will initiate the backfill process, skipping the migration for existing
databases but recreating for new databases.

### If you wish to go back to `golang-migrate` or `cloudspannerecosystem/wrench`
You can simply drop the `SchemaMigrationsHistory` and `SchemaMigrationsLock` table as the `SchemaMigrations` will be in sync.
___

## Installation

With go 1.21 or higher:

```shell
go install github.com/roryq/wrench@latest
```

## Usage

### Prerequisite

```sh
export SPANNER_PROJECT_ID=your-project-id
export SPANNER_INSTANCE_ID=your-instance-id
export SPANNER_DATABASE_ID=your-database-id
```

You can also specify project id, instance id and database id by passing them as command arguments.

<!--usage-shell-->
```
Usage:
  wrench [command]

Available Commands:
  create        Create database with tables described in schema file
  drop          Drop database
  reset         Equivalent to drop and then create
  load          Load schema from server to file
  load-discrete Load schema from server to discrete files per object
  schema        Runs the migrations against a dockerised spanner emulator, then loads the schema and static data to disk. (Requires docker)
  apply         Apply DDL file to database
  migrate       Migrate database
  truncate      Truncate all tables without deleting a database
  help          Help about any command
  completion    Generate the autocompletion script for the specified shell

Migrate database

Usage:
  wrench migrate [command]

Available Commands:
  create      Create a set of sequential up migrations in directory
  up          Apply all or N up migrations
  version     Print current migration version
  history     Print migration version history
  setup-lock  Initialise or reset the migration lock
  repair      If a migration has failed, clean up any schema changes manually then repair the history with this command

Flags:
      --credentials-file string          Specify Credentials File
      --database string                  Cloud Spanner database name (optional. if not set, will use $SPANNER_DATABASE_ID value)
      --detect-partitioned-dml           Automatically detect when a migration contains only Partitioned DML statements, and apply the statements in statement-level transactions via the PartitionedDML API. (optional. if not set, will use $WRENCH_DETECT_PARTITIONED_DML or default to false)
      --directory string                 Directory that schema file placed (required)
  -h, --help                             help for wrench
      --instance string                  Cloud Spanner instance name (optional. if not set, will use $SPANNER_INSTANCE_ID value)
      --lock-identifier string           Random identifier used to lock migration operations to a single wrench process. (optional. if not set then it will be generated) (default "58a4394a-19f9-4dbf-880d-20b6cf169d46")
      --project string                   GCP project id (optional. if not set, will use $SPANNER_PROJECT_ID or $GOOGLE_CLOUD_PROJECT value)
      --schema-file string               Name of schema file (optional. if not set, will use default 'schema.sql' file name)
      --sequence-interval uint16         Used to generate the next migration id. Rounds up to the next interval. (optional. if not set, will use $WRENCH_SEQUENCE_INTERVAL or default to 1) (default 1)
      --static-data-tables-file string   File containing list of static data tables to track (optional)
      --stmt-timeout duration            Set a non-default timeout for statement execution
      --verbose                          Used to indicate whether to output Migration information during a migration
  -v, --version                          version for wrench

Use "wrench [command] --help" for more information about a command.
```