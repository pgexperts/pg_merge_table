#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from __future__ import print_function

import sys
import argparse

import psycopg2

from introspection import *

# Parse and assign arguments

parser = argparse.ArgumentParser(description='Merge a table from one PostgreSQL database to another.')

parser.add_argument('fq_table_name',
                    help='name of table to merge (optionally schema-qualified, must exist on both servers)')

parser.add_argument('--from', dest='from_connection_string', action='store',
                    default="host=localhost",
                    help='connection string for "from" server (defaults to "host=localhost")')
parser.add_argument('--to', dest='to_connection_string', action='store',
                    default="host=localhost",
                    help='connection string for "to" server (defaults to "host=localhost")')

parser.add_argument('--key', dest='key', action='store',
                    default="",
                    help='key column for matching; it must exist on both tables, and the only column in a unique index')

parser.add_argument('--delete', dest='delete_missing', action='store_true',
                    default=False,
                    help='delete entries in "to" table missing in "from" table"')
parser.add_argument('--execute', dest='execute', action='store_true',
                    default=False,
                    help='actually run the merge; by default, does a dry run only')
parser.add_argument('--progress', dest='progress', action='store_true',
                    default=False,
                    help='write progress every 1,000 rows')

args = parser.parse_args()

fq_table_name = args.fq_table_name

from_connnection_string = args.from_connection_string
to_connection_string = args.to_connection_string

key = args.key

delete_missing = args.delete_missing
execute = args.execute
progress = args.progress

#

from_connection = psycopg2.connect(from_connnection_string)
from_connection.autocommit = False

to_connection = psycopg2.connect(to_connection_string)
to_connection.autocommit = False

# Decompose table name into schema, name

components = fq_table_name.split('.')
if len(components) == 1:
    schema_name = 'public'
    table_name = components[0]
elif len(components) == 2:
    schema_name = components[0]
    table_name = components[1]
else:
    print('Table name "%s" contains more than one dot.' % fq_table_name, file=sys.stderr)
    exit(1)

# Verify table exists on both sides

if not table_exists(from_connection, schema_name, table_name):
    print('Table "%s.%s" does not exist on "from" server.' % (schema_name, table_name), file=sys.stderr)
    exit(1)

if not table_exists(to_connection, schema_name, table_name):
    print('Table "%s.%s" does not exist on "to" server.' % (schema_name, table_name), file=sys.stderr)
    exit(1)

# Retrieve the primary key column name, and all table columns

if key:
    key_type = type_for_column(from_connection, schema_name, table_name, key)

    if not key_type:
        print('Table "%s.%s" coolumn %s does not exist on "from" server.' % (schema_name, table_name, key), file
              =sys.stderr)
        exit(1)

    to_key_type = type_for_column(to_connection, schema_name, table_name, key)
    if not to_key_type:
        print('Table "%s.%s" column "%s" does not exist on "to" server.' % (schema_name, table_name, key), file
              =sys.stderr)
        exit(1)

    if key_type != to_key_type:
        print('Table "%s.%s" column "%s" is not the same type on the "from" (%s) and "to" (%s) server.' % \
              (schema_name, table_name, key, key_type, to_key_type), file=sys.stderr)
        exit(1)

    from_curs = from_connection.cursor()

    from_curs.execute("""
        SELECT COUNT(DISTINCT i.indexrelid)
            FROM pg_class c
            JOIN pg_namespace ns ON c.relnamespace = ns.oid 
            JOIN pg_attribute a ON a.attrelid = c.oid
            JOIN pg_index i ON i.indrelid = c.oid
            WHERE nspname = %s
              AND relname = %s
              AND attname = %s
              AND indisunique
              AND attnum = ANY (indkey::integer[])
              AND array_length(indkey::integer[], 1) = 1
                  """, (schema_name, table_name, key))

    from_key_index_count = int(from_curs.fetchone()[0])

    if from_key_index_count == 0:
        print('Table "%s.%s" column "%s" does not appear by itself in a UNIQUE index on the "from" server.' % \
              (schema_name, table_name, key), file=sys.stderr)
        exit(1)

    if from_key_index_count > 1:
        print('Table "%s.%s" column "%s" appears by itself in more than one UNIQUE index on the "from" server.' % \
              (schema_name, table_name, key), file=sys.stderr)
        exit(1)

    from_curs.close()

    to_curs = to_connection.cursor()

    to_curs.execute("""
        SELECT COUNT(DISTINCT i.indexrelid)
            FROM pg_class c
            JOIN pg_namespace ns ON c.relnamespace = ns.oid 
            JOIN pg_attribute a ON a.attrelid = c.oid
            JOIN pg_index i ON i.indrelid = c.oid
            WHERE nspname = %s
              AND relname = %s
              AND attname = %s
              AND indisunique
              AND attnum = ANY (indkey::integer[])
              AND array_length(indkey::integer[], 1) = 1
                  """, (schema_name, table_name, key))

    to_key_index_count = int(to_curs.fetchone()[0])

    if to_key_index_count == 0:
        print('Table "%s.%s" column "%s" does not appear by itself in a UNIQUE index on the "to" server.' % \
              (schema_name, table_name, key), file=sys.stderr)
        exit(1)

    if to_key_index_count > 1:
        print('Table "%s.%s" column "%s" appears by itself in more than one UNIQUE index on the "to" server.' % \
              (schema_name, table_name, key), file=sys.stderr)
        exit(1)

    to_curs.close()


else:
    primary_key_from = list(primary_key_for_table(from_connection, schema_name, table_name))
    primary_key_to = list(primary_key_for_table(to_connection, schema_name, table_name))

    if len(primary_key_from) == 0:
        print('Table "%s.%s" does not have a primary key on "from" server.' % (schema_name, table_name), file
              =sys.stderr)
        exit(1)
    elif len(primary_key_from) > 1:
        print('Table "%s.%s" has a multi-column primary key on "from" server, currently not supported.' % (
        schema_name, table_name), file=sys.stderr)
        exit(1)

    if len(primary_key_to) == 0:
        print('Table "%s.%s" does not have a primary key on "to" server.' % (schema_name, table_name), file=sys.stderr)
        exit(1)
    elif len(primary_key_to) > 1:
        print('Table "%s.%s" has a multi-column primary key on "to" server, currently not supported.' % (
        schema_name, table_name), file=sys.stderr)
        exit(1)

    if primary_key_from[0] != primary_key_to[0]:
        print('Table "%s.%s" has different primary key column names on "from" (%s) and "to" (%s)servers.' % \
              (schema_name, table_name, primary_key_from[0], primary_key_to[0],), file=sys.stderr)
        exit(1)

    key = primary_key_from[0]
    key_type = type_for_column(from_connection, schema_name, table_name, key)

columns = list(table_columns(from_connection, schema_name, table_name))

# For convenience, we make sure the primary key is the first column in our list.

if columns[0] != key:
    columns.remove(key)
    columns.insert(0, key)

columns = ['"' + c + '"' for c in columns]

# These are used to build the SQL for SELECT, INSERT, and UPDATE operations.

column_list_string = ', '.join(columns)
replacement_string = ', '.join(['%s'] * len(columns))

select_statement = 'SELECT %s FROM "%s"."%s"' % (column_list_string, schema_name, table_name)
insert_statement = """
    INSERT INTO "%s"."%s"(%s) VALUES(%s)
        ON CONFLICT (%s) DO UPDATE SET %s
        RETURNING (xmax = 0) AS inserted
    """ % (schema_name,
           table_name,
           column_list_string,
           replacement_string,
           key,
           ', '.join([column + ' = EXCLUDED.' + column for column in columns[1:]]))

# To avoid memory issues, we use a server-side cursor rather than just issuing what might
# be a truly gigantic SELECT statement

from_curs = from_connection.cursor(name='from_cursor')
from_curs.execute(select_statement)

to_curs = to_connection.cursor()

# If we will be deleting rows, create a temporary table to hold the ones we did insert/update.

if delete_missing:
    tracking_table_name = "delete_track_" + table_name

    to_curs.execute("""
        CREATE TEMPORARY TABLE %s (
            pk %s PRIMARY KEY
            ) ON COMMIT DROP
        """ % (tracking_table_name, key_type,))

    tracking_table_insert = "INSERT INTO " + tracking_table_name + "(pk) VALUES(%s)"
else:
    tracking_table_name = ""
    tracking_table_insert = ""

probe_statement = """
  SELECT COUNT(*) FROM "%s"."%s" WHERE %s=
  """ % (schema_name, table_name, key,)
probe_statement += '%s'

rows_processed = 0
rows_updated = 0
rows_deleted = 0
rows_inserted = 0

for row in from_curs:
    # This iterates through the input in "gulps" of the default itersize, which is 2000 in psycopg2.

    rows_processed += 1

    if tracking_table_insert:
        to_curs.execute(tracking_table_insert, row[:1])

    if not execute:
        to_curs.execute(probe_statement, row[:1])
        already_exists = int(to_curs.fetchone()[0])
        if already_exists:
            rows_updated += 1
        else:
            rows_inserted += 1

    else:
        to_curs.execute(insert_statement, row)
        inserted = to_curs.fetchone()[0]
        if inserted:
            rows_inserted += 1
        else:
            rows_updated += 1

    if progress and (rows_processed % 1000) == 0:
        print("%s rows processed, %s updated, %s inserted" % \
              (rows_processed, rows_updated, rows_inserted,), file=sys.stdout)

from_curs.close()

if delete_missing and progress:
    print("deleting.", file=sys.stdout)

    if not execute:
        to_curs.execute("""
            SELECT COUNT(*) FROM "%s"."%s" WHERE %s NOT IN (SELECT pk FROM %s)
            """ % (schema_name, table_name, key, tracking_table_name))

        rows_deleted = int(to_curs.fetchone()[0])
    else:
        to_curs.execute("""
            DELETE FROM "%s"."%s" WHERE %s NOT IN (SELECT pk FROM %s)
            """ % (schema_name, table_name, key, tracking_table_name))

        rows_deleted = to_curs.rowcount

from_connection.rollback()

if not execute:
    to_connection.rollback()
else:
    to_connection.commit()
    to_connection.autocommit = True
    if progress:
        print("vacuuming.", file=sys.stdout)
    to_curs.execute("""VACUUM ANALYZE "%s"."%s" """ % (schema_name, table_name,))

to_curs.close()

if not execute:
    print("dry run estimates: %s rows processed, %s updated, %s inserted, %s deleted" % \
          (rows_processed, rows_updated, rows_inserted, rows_deleted), file=sys.stdout)
else:
    print("%s rows processed, %s updated, %s inserted, %s deleted" % \
          (rows_processed, rows_updated, rows_inserted, rows_deleted), file=sys.stdout)

exit(0)