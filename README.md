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

### Create database

```sh
$ wrench create --directory ./_examples
```

This creates the database with `./_examples/schema.sql`.

### Drop database

```sh
$ wrench drop
```

This just drops the database.

### Reset database

```sh
wrench reset --directory ./_examples
```

This drops the database and then re-creates with `./_examples/schema.sql`. Equivalent to `drop` and then `create`.

### Load schema from database to file

```sh
$ wrench load --directory ./_examples
```

This loads schema DDL from database and writes it to `./_examples/schema.sql`.

### Create migration file

```sh
$ wrench migrate create --directory ./_examples
```

This creates a next migration file like `_examples/migrations/000001.sql`. You will write your own migration DDL to this file.

You can optionally set `$WRENCH_SEQUENCE_INTERVAL` or pass `--sequence_interval` with an integer e.g. `10` which will be used
to generate the next sequence number by rounding up to the next sequence interval.

```sh
$ wrench migrate create --sequence_interval 10
```
This will create migration scripts like `000010.sql`, `000020.sql`, `000030.sql` etc. These gaps allow hotfixes to be
inserted into the sequence if needed.

### Execute migrations

```sh
$ wrench migrate up --directory ./_examples
```

This executes migrations. This also creates `SchemaMigrations` & `SchemaMigrationsHistory` tables in your database to manage schema version if it does not exist.

### Migrations history
```sh
$ wrench migrate history
```
This displays the history of migrations applied to your database, ordered by when they were first attempted.
Migrations left in a dirty state and subsequently retried are reflected in the Modified timestamp.

### Apply single DDL/DML

```sh
$ wrench apply --ddl ./_examples/ddl.sql
```

This applies single DDL or DML.

Use `wrench [command] --help` for more information about a command.

## Contributions

Please read the [contribution guidelines](CONTRIBUTING.MD) before submitting
pull requests.

## License

Copyright 2019 Mercari, Inc.

Licensed under the MIT License.
