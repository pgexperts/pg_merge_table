# -*- coding: utf-8 -*-
from __future__ import unicode_literals

#

def table_exists(connection, schema_name, table_name):
    curs = connection.cursor()
    curs.execute("""
        SELECT COUNT(*)
          FROM information_schema.tables
         WHERE table_name=%(table_name)s AND table_schema=%(schema_name)s
        """, {'schema_name': schema_name, 'table_name': table_name})
    
    counts = int(curs.fetchone()[0])
    curs.close()
    
    return counts > 0


def primary_key_for_table(connection, schema_name, table_name):
    fq_table_name = '%s.%s' % (schema_name, table_name,)

    curs = connection.cursor()
    curs.execute("""
    SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                             AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = %(fq_table_name)s::regclass
        AND    i.indisprimary
        """, { 'fq_table_name': fq_table_name})
    
    for row in curs.fetchall():
        yield row[0]
    
    curs.close()


def table_columns(connection, schema_name, table_name):
    curs = connection.cursor()
    curs.execute("""
        SELECT 
            c.column_name
        FROM 
             information_schema.columns as c 
        WHERE
            c.table_schema = %(schema_name)s AND
            c.table_name = %(table_name)s
        ORDER BY c.ordinal_position
    """, {'schema_name': schema_name, 'table_name': table_name})
    
    for row in curs.fetchall():
        yield row[0]
        
    curs.close()

    
def type_for_column(connection, schema_name, table_name, column_name):
    curs = connection.cursor()
    curs.execute("""
        SELECT 
            c.data_type,
            c.character_maximum_length
        FROM 
             information_schema.columns as c 
        WHERE
            c.table_schema = %(schema_name)s AND
            c.table_name = %(table_name)s AND
            c.column_name = %(column_name)s
    """, {'schema_name': schema_name, 'table_name': table_name, 'column_name': column_name})
    
    row = curs.fetchone()
    curs.close()
    
    if not row:
        return None
    
    if not row[1]:
        return row[0]
    
    return '%s(%s)' % (row[0], row[1])