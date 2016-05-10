#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
This module is a wrapper for sqlite3 that has methods to make database 
transactions or other database operations and queries.  
'''

import os
import sys
import sqlite3

from exceptions import *


SQLITE_TYPES = {'NULL', 'INTEGER', 'TEXT', 'REAL', 'BLOB', 'INTEGER PRIMARY KEY'}

ALL = Ellipsis


class Quikql(object):
    '''
    The wrapper class
    '''
    def __init__(self, filename):
        '''
        When called pass in a file path, to set for all future calls
        on wrapper-object.
    
        @type filename: <type 'str'>
        @param filename: File path to .db for object to use.
        '''
        self._filename = filename
        self._conn = sqlite3.connect(self._filename)
        self._execute('PRAGMA FOREIGN_KEYS=1')

    def _execute(self, command, items=None, many=False, valueiter=()):
        '''
        Private method to dispatch all queries to database.  The context 
        manager will handle commits and closing of database connections.

        @type command: <type 'str'>
        @param command: A string command to be executed.

        @type items: <type 'Quikql Constant'> or <type 'int'>
        @param items: Denotes the total amount of values the query is meant
                      to return.

        @type many: <type 'bool'>
        @param many: Flag for whether the command is to be run over a sequence
                     of values.

        @type valueiter: <type 'iter'>
        @param valueiter: The iterable sequence of values to run with command.
        '''
        with self._conn:
            cursor = self._conn.cursor()
            if many: 
                cursor.executemany(command, valueiter)
            else:
                cursor.execute(command)
            fetch_values = self._fetch(cursor, items)
        return fetch_values

    def _fetch(self, cursor, items=None):
        '''
        Private method to retrieve values after a query is made.

        @type cursor: <type 'Cursor object'>
        @param cursor: The current connection's cursor.

        @type items: <type 'NoneType'> or <type 'int'>
        @param items: Denotes the total amount of values the query is meant to 
                      return.
        '''
        if items is None:
            return cursor.fetchone()
        elif items == ALL:
            return cursor.fetchall()
        return cursor.fetchmany(items)

    def _field_value_stubs(self, field_values):
        return ' AND '.join('"{}"="{}"' for _ in field_values)

    def _repr(self, values):
        return dict(map(repr, i) for i in values.items())

    def create_table(self, table_name, columns, pkey=(), fkey=None):
        '''
        Method to create new tables.
        
        @type table_name: <type 'str'>
        @param table_name: The name of the new table to create.

        @type columns: <type 'dict'>
        @param columns: Keyword argument/s that set column names for table. 

        @type pkey: <type 'tuple'>
        @param pkey: A 'tuple' of 'str's showing which columns will also be
                     primary keys.

        @type fkey: <type 'NoneType' or 'dict'>
        @param fkey: The key-value pairs here are, type <'str'> for the key 
                     representing the foreign key's column name from the table
                     and the values are of type <'tuple'> indicatting 
                     ('table-name', 'column-name'), the column-name being a
                     primary key.
        '''
        create_table_statement = 'CREATE TABLE IF NOT EXISTS %s ' % table_name
        table_columns = self._create_columns(columns, pkey=pkey, fkey=fkey)
        self._execute(create_table_statement + table_columns)

    def _create_columns(self, columns, pkey=(), fkey=None):
        '''
        Private method to create column schema for a new table.

        @type columns: <type 'dict'>
        @param columns: The key-value pairs to match identifier-type for the 
                        new table schema.

        @type pkey: <type 'tuple'>
        @param pkey: A 'tuple' of 'str's showing which columns will also be
                     primary keys.

        @type fkey: <type 'NoneType' or 'dict'>
        @param fkey: The key-value pairs here are, type <'str'> for the key 
                     representing the foreign key's column name from the table
                     and the values are of type <'tuple'> indicatting 
                     ('table-name', 'column-name'), the column-name being a
                     primary key.
        '''
        if not SQLITE_TYPES.issuperset(columns.values()):
            invalid_args = ' '.join(map(str, columns.values())) 
            raise InvalidSQLType(invalid_args)
        columns_statement = ', '.join([' '.join(i) if i[0] not in pkey else
                                       ' '.join(i) + ' PRIMARY KEY' for i in 
                                        columns.items()])
        if fkey is not None:
            columns_statement += self._create_foreign_key(fkey)
        return '({})'.format(columns_statement)

    def _create_foreign_key(self, fkeys):
        '''
        Private method to create a foreign key request.

        @type fkey: <type 'NoneType' or 'dict'>
        @param fkey: The key-value pairs here are, type <'str'> for the key 
                     representing the foreign key's column name from the table
                     and the values are of type <'tuple'> indicatting 
                     ('table-name', 'column-name'), the column-name being a
                     primary key.
        '''
        foreignkey_statement = ''
        for fkey in fkeys:
            foreignkey_statement += ', FOREIGN KEY({}) REFERENCES {}({})'.format(
                                                              fkey, *fkeys[fkey])
        return foreignkey_statement

    def attach(self, database_name, schema_name):
        '''
        Method to attach another database the current sqlite connection.

        @type database_name: <type 'str'>
        @param database_name: The database filename to add to the current connection.

        @type schema_name: <type 'str'>
        @param schema_name: The name that sqlite will use internally to 
                            reference this database.
        '''
        attach_command = ('ATTACH DATABASE "%s" AS %s' % 
                         (database_name, schema_name))
        self._execute(attach_command)

    def detach(self, schema_name):
        '''
        Method to detach a previously attached database from the current
        connection.

        @type schema_name: <type 'str'>
        @param schema_name: The name sqlite used to refer to this database.
                            internally (see `attach`).
        '''
        detach_command = 'DETACH DATABASE %s' % schema_name
        self._execute(detach_command)
 
    def attached(self):
        '''
        Method to list all the current databases attached to the current
        connection.
        '''
        attached_command = 'PRAGMA DATABASE_LIST'
        return dict(database[1:] for database in 
                    self._execute(attached_command, items=ALL))

    def delete_table(self, table):
        '''
        Method to delete a table.

        @type table: <type 'str'>
        @param table: The name of the table to delete.
        '''
        delete_table_command = 'DROP TABLE IF EXISTS %s' % table
        self._execute(delete_table_command)
    
    def delete_row(self, table, field_values):
        '''
        Method to query table for values to remove.

        @type table: <type 'str'>
        @param table: Name of table to query.

        @type field_values: <type 'dict'>
        @param field_values: The key-value pairs corresponding to the field and 
                             value to be matched for deletion.
        '''
        if not isinstance(field_values, dict):
            raise InvalidArg(type(field_values))
        del_row_cmd = 'DELETE FROM %s WHERE ' % table
        format_values = [v for k in field_values.items() for v in k]
        del_row_cmd += self._field_value_stubs(field_values)
        self._execute(del_row_cmd.format(*format_values))
    
    def update_row(self, table, columns, row=None): 
        '''
        Update a column value to a new value.

        @type table: <type 'str'>
        @param table: The table to update row in.

        @type columns: <type 'dict'>
        @param columns: The columns to update the values of.

        @type row: optional argument of <type 'dict'>
        @param row: The key-value pairs  to match to a row.
        '''
        repr_column = self._repr(columns)
        column_update = ', '.join(map('='.join, repr_column.items()))
        update_cmd = 'UPDATE %s SET %s' % (table, column_update)
        fields = []
        if row is not None:
            update_cmd += ' WHERE ' + self._field_value_stubs(row)
            fields = [v for k in row.items() for v in k]
        self._execute(update_cmd.format(*fields))

    def insert_row(self, table, values):
        '''
        Replace or insert a row into given table.

        @type table: <type 'str'>
        @param table: The table to replace/insert into.

        @type values: <type 'dict'>
        @param values: The key-value pairs of the column-value being inserted.
        '''
        if not isinstance(values, dict):
            raise InvalidArg(type(values))
        repr_insert = self._repr(values)
        columns, row_values = map(', '.join, zip(*repr_insert.items()))
        insert_command = ('INSERT OR REPLACE INTO %s(%s) VALUES(%s)' % 
                          (table, columns, row_values))
        self._execute(insert_command)

    def insert_rows(self, table, *values):
        '''
        Replace or insert multiple rows into given table.

        @type table: <type 'str'>
        @param table: The table to replace/insert into.

        @type values: A variadic number of <type 'dict'>
        @param values: Any number of rows to be inserted.
        '''
        schema = [i[1] for i in self.get_schema(table)]
        insert_command = 'INSERT OR REPLACE INTO %s VALUES' % table
        value_stubs = '(' + ','.join(['?' for _ in schema]) + ')'
        insert_command += value_stubs
        insert_values = [[value.get(i) for i in schema] for value in values]
        self._execute(insert_command, many=True, valueiter=insert_values)

    def get_row(self, table, field_values, size=None): 
        '''
        Method to retrieve row from a specified table.
    
        @type table: <type 'str'> 
        @param table: the table to retrieve row from

        @type field_values: <type 'dict'>
        @param field_values: A key-value pair to match and retrieve all other 
                             adjacent values in the corresponding row.

        @type size: <type 'int'>
        @param size: Number of entries to retrieve.
        '''
        if not isinstance(field_values, dict):
            raise InvalidArg(type(field_values))
        row_cmd = 'SELECT * FROM %s WHERE ' % table
        format_values = [v for k in field_values.items() for v in k]
        row_cmd += self._field_value_stubs(field_values)
        return self._execute(row_cmd.format(*format_values), items=size)

    def get_column(self, table, column):
        '''
        Method to retrieve column from a specified table and column.

        @type table: <type 'str'>
        @param table: The table name to be queried for column.

        @type column: <type 'str'>
        @param column: The column name to be retrieve from table.
        '''
        get_column_cmd = 'SELECT %s FROM %s' % (column, table)
        return self._execute(get_column_cmd, items=ALL)

    def count(self, table, field):
        '''
        Method to count the number of non-none fields of a specified field.

        @type table: <type 'str'>
        @param table: The table name to be queried for field count.

        @type field: <type 'str'>
        @param field: The name of the field to match for count.
        '''
        if not isinstance(field, str):
            raise InvalidArg(type(field))
        count_cmd = 'SELECT COUNT(%s) FROM %s' % (field, table)
        return self._execute(count_cmd) 

    def min(self, table, field):
        '''
        Method to find the minimum value of the specified field.

        @type table: <type 'str'>
        @param table: The table name to be queried for field minimum.

        @type field: <type 'str'>
        @param field: The name of the field to find the minimum value for.
        '''
        if not isinstance(field, str):
            raise InvalidArg(type(field))
        minimum_cmd = 'SELECT MIN(%s) FROM %s' % (field, table)
        return self._execute(minimum_cmd)

    def max(self, table, field):
        '''
        Method to find the maximum value of the specified field.

        @type table: <type 'str'>
        @param table: The table name to be queried for field maximum.

        @type field: <type 'str'>
        @param field: The name of the field to find the maximum value for.
        '''
        if not isinstance(field, str):
            raise InvalidArg(type(field))
        maximum_cmd = 'SELECT MAX(%s) FROM %s' % (field, table)
        return self._execute(maximum_cmd)

    def sum(self, table, field):
        '''
        Method to find the total sum of the values from the specified field.

        @type table: <type 'str'>
        @param table: The table name to find the sum of fields for.

        @type field: <type 'str'>
        @param field: The name of the field to find the sum for.
        '''
        if not isinstance(field, str):
            raise InvalidArg(type(field))
        sum_cmd = 'SELECT SUM(%s) FROM %s' % (field, table)
        return self._execute(sum_cmd)

    def dump_table(self, table, order=None):
        '''
        Method to return entire table contents.

        @type table: <type 'str'>
        @param table: The table to dump contents of.

        @type order: <type 'NoneType'> or <type 'str'>
        @param order: Optional argument to return contents ordered by a column.
        '''
        table_cmd = 'SELECT * FROM %s' % table
        if order is not None:
            table_cmd += ' ORDER BY %s' % order
        return self._execute(table_cmd, items=ALL) 

    def table_size(self, table):
        '''
        Method to find the byte-size of the supplied table.

        @type table: <type 'str'>
        @param table: The table to find the byte-size for.
        '''
        table_data = self.dump_table(table)
        return sys.getsizeof(table_data)

    def get_tables(self):
        '''
        Method to return all the tables in database object.
        '''
        table_cmd = 'SELECT name FROM sqlite_master WHERE type="table"'
        return self._execute(table_cmd, items=ALL)
    
    def get_schema(self, table):
        '''
        Method to return the schema of a table.

        @type table: <type 'str'>
        @param table: A table name to search for in database object.
        '''
        schema_cmd = 'PRAGMA TABLE_INFO(%s)' % table
        return self._execute(schema_cmd, items=ALL)
