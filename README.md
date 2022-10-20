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

[![cloudspannerecosystem](https://circleci.com/gh/cloudspannerecosystem/wrench.svg?style=svg)](https://circleci.com/gh/cloudspannerecosystem/wrench)

`wrench` is a schema management tool for [Cloud Spanner](https://cloud.google.com/spanner/).

Please feel free to report issues and send pull requests, but note that this
application is not officially supported as part of the Cloud Spanner product.

```sh
$ cat ./_examples/schema.sql
CREATE TABLE Singers (
  SingerID STRING(36) NOT NULL,
  FirstName STRING(1024),
) PRIMARY KEY(SingerID);

# create database with ./_examples/schema.sql
$ wrench create --directory ./_examples

# create migration file
$ wrench migrate create --directory ./_examples
_examples/migrations/000001.sql is created

# edit _examples/migrations/000001.sql
$ cat ./_examples/migrations/000001.sql
ALTER TABLE Singers ADD COLUMN LastName STRING(1024);

# execute migration
$ wrench migrate up --directory ./_examples

# load ddl from database to file ./_examples/schema.sql
$ wrench load --directory ./_examples

# show time and date of migrations
$ wrench migrate history
Version	Dirty	Created					Modified
1	false	2020-06-16 08:07:11.763755 +0000 UTC	2020-06-16 08:07:11.76998 +0000 UTC

# finally, we have successfully migrated database!
$ cat ./_examples/schema.sql
CREATE TABLE Singers (
  SingerID STRING(36) NOT NULL,
  FirstName STRING(1024),
  LastName STRING(1024),
) PRIMARY KEY(SingerID);

CREATE TABLE SchemaMigrations (
  Version INT64 NOT NULL,
  Dirty BOOL NOT NULL,
) PRIMARY KEY(Version);

CREATE TABLE SchemaMigrationsHistory (
  Version INT64 NOT NULL,
  Dirty BOOL NOT NULL,
  Created TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
  Modified TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY(Version);
```

## Installation

Get binary from [release page](https://github.com/cloudspannerecosystem/wrench/releases).
Or, you can use Docker container: [mercari/wrench](https://hub.docker.com/r/mercari/wrench).

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
  apply         Apply DDL file to database
  migrate       Migrate database
  truncate      Truncate all tables without deleting a database
  help          Help about any command
  completion    Generate the autocompletion script for the specified shell

Flags:
      --credentials-file string          Specify Credentials File
      --database string                  Cloud Spanner database name (optional. if not set, will use $SPANNER_DATABASE_ID value)
      --directory string                 Directory that schema file placed (required)
  -h, --help                             help for wrench
      --instance string                  Cloud Spanner instance name (optional. if not set, will use $SPANNER_INSTANCE_ID value) (default "test-instance")
      --lock-identifier string           Random identifier used to lock migration operations to a single wrench process. (optional. if not set then it will be generated) (default "7da31609-e4c8-4aae-9f7c-b9db98c231e1")
      --project string                   GCP project id (optional. if not set, will use $SPANNER_PROJECT_ID or $GOOGLE_CLOUD_PROJECT value) (default "my-project")
      --schema-file string               Name of schema file (optional. if not set, will use default 'schema.sql' file name)
      --sequence-interval uint16         Used to generate the next migration id. Rounds up to the next interval. (optional. if not set, will use $WRENCH_SEQUENCE_INTERVAL or default to 1) (default 1)
      --static-data-tables-file string   File containing list of static data tables to track (optional)
  -v, --version                          version for wrench

Use "wrench [command] --help" for more information about a command.
```