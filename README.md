# pg_merge_table

This is a simple script to merge one table into another in different PostgreSQL servers (or, at least,
in different schemas on the same server).

The "from" and "to" tables must have the same name, and identical schemas. The "from" table is scanned
in its entirety, and any records found are copied to the "to" table. If a record exists already with the
same primary key in the "to" table, the values from the "from" table are used to update that record; if
no such record exists, it is inserted.

Optionally, after all inserts/updates are done, any records (by primary key) which do not exist in the "from"
table but which do exist in the "to" table can be deleted.

Currently, the script will only operate on tables with a single-column primary key.

## Examples

    ./pg_merge_table.py --from="host=10.2.3.1 dbname=prod" --to="host=10.2.4.12 dbname=archive" sch.tab

This will merge the entire contents of table "sch.tab" (schema "sch", table name "tab") from the PostgreSQL
server on host 10.2.3.1, database `prod`, into the one on 10.2.4.12, database `archive`. No records will be
deleted from `archive`.

   ./pg_merge_table.py --delete --from="host=10.2.3.1 dbname=prod" --to="host=10.2.4.12 dbname=archive" tbl

This will merge the entire contents of table "tab" (schema "public", table name "tab") from the PostgreSQL
server on host 10.2.3.1, database `prod`, into the one on 10.2.4.12, database `archive`. Any records (using
the primary key) which do not exist on `prod` will be deleted from "tab" on `archive`.

   ./pg_merge_table.py --dry-run --delete --from="host=10.2.3.1 dbname=prod" --to="host=10.2.4.12 dbname=archive" tbl

This will do a dry run of the merge operation above. No records will be changed on `archive`, but a summary
of how many records *would* be inserted, updated, and deleted will be printed at the end.

## Limitations and Quirks

* Currently, only single-column primary keys are accepted.

* When running in "live" mode (not dry-run), the number of records updated and inserted are not available, as an update
cannot be distinguished from an insert in live mode. The number of updated vs inserted is available in dry-run mode.

* Columns that are of a type that cannot be successfully cast to a portable format (such as text) and back again cannot be
copied using this tool. All in-core and contrib/ PostgreSQL types can be so cast, but it is possible that some extension
and user-defined types might fail.

## Synopsys

```$ ./pg_merge_table.py --help
usage: pg_merge_table.py [-h] [--from FROM_CONNECTION_STRING]
                         [--to TO_CONNECTION_STRING] [--delete] [--dry-run]
                         [--progress]
                         fq_table_name

Merge a table from one PostgreSQL database to another.

positional arguments:
  fq_table_name         name of table to merge (optionally schema-qualified,
                        must exist on both servers)

optional arguments:
  -h, --help            show this help message and exit
  --from FROM_CONNECTION_STRING
                        connection string for "from" server (defaults to
                        "host=localhost")
  --to TO_CONNECTION_STRING
                        connection string for "to" server (defaults to
                        "host=localhost")
  --delete              delete entries in "to" table missing in "from" table"
  --dry-run             write counts of affected rows, but do not actually
                        copy any rows
  --progress            write progress every 1,000 rows```