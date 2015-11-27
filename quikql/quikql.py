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

    def _fetch(self, cursor, items=None):
        '''
        Private method to retrieve values after a query is made.

            @type cursor: <type 'Cursor object'>
            @param cursor: The current connection's cursor.

            @type items: <type 'Quikql Constant'> or <type 'int'>
            @param items: Denotes the total amount of values the query is meant
                          to return.
        '''
        if items is None:
            return cursor.fetchone()
        elif items == ALL:
            return cursor.fetchall()
        return cursor.fetchmany(items)

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

    def create_table(self, table_name, columns, key=None):
        '''
        Method to create new tables.
        
            @type table_name: <type 'str'>
            @param table_name: The name of the new table to create.

            @type columns: <type 'dict'>
            @param columns: Keyword argument/s that set column names for table. 

            @type key: <type 'dict'>
            @param key: The key-value pairs for the new foreign key.
        '''
        name = self.create_table.__name__
        if len(columns) == 0:
            raise InsufficientArgs(name, 2, 1)
        create_table_statement = 'CREATE TABLE IF NOT EXISTS %s ' % table_name
        table_columns = self.create_columns(**columns)
        self._execute(create_table_statement + table_columns)

    def create_columns(self, key=None, **columns):
        '''
        Method to create column schema for a new table.

        @type columns: <type 'dict'>
        @param columns: The key-value pairs to match identifier-type for the 
                        new table schema.
        '''
        if not SQLITE_TYPES.issuperset(columns.values()):
            invalid_args = ' '.join(map(str, columns.values())) 
            raise InvalidType(name, invalid_args)
        columns = ', '.join(map(' '.join, columns.items()))
        if key is not None:
            columns += self.create_foreign_key(key)
        return '(%s)' % columns 

    def create_foreign_key(self, keys):
        '''
        Method to create a foreign key request.

        @type keys: <type 'dict'>
        @param keys: The key-value pairs representing the key and reference for
                     a table schema.
        '''
        foreignkey, references = key.popitem()
        foreignkey_statement = ', FOREIGN KEY(%s)' % foreignkey
        reference_statement = ' REFERENCES %s(%s)' % tuple(references)
        return foreignkey_statement + reference_statement

    def delete_table(self, table):
        '''
        Method to delete a table.

            @type table: <type 'str'>
            @param table: The name of the table to delete.
        '''
        delete_table_command = 'DROP TABLE IF EXISTS %s' % table
        self._execute(delete_table_command)
    
    def delete_row(self, table, column, row):
        '''
        Method to query table for values to remove.

            @type table: <type 'str'>
            @param table: Name of table to query.

            @type column: <type 'str'>
            @param column: Table column to find values.

            @type row: Any acceptable sqlite3 type.
            @param row: Value in row to search and remove.
        '''
        delete_row_command = 'DELETE FROM %s WHERE %s="%s"' % (table, column, row)
        self._execute(delete_row_command)
    
    def update_row(self, table, columns, row=None): 
        '''
        Update a column value to a new value.

            @type table: <type 'str'>
            @param table: The table to update row in.
    
            @type columns: <type 'dict'>
            @param columns: The columns to update the values of.

            @type value: <type 'dict'>
            @param value: The value to insert new.
        '''
        name = self.update_row.__name__
        column_update = ', '.join(map('='.join, columns.items())) 
        if row is not None:
            row_values = ' AND '.join(map('='.join, row.items()))
        update_cmd = ('UPDATE %s SET %s WHERE %s' % 
                     (table, column_update, row_values))
        self._execute(update_cmd)

    def insert_row(self, table, column, values=None):
        '''
        Replace or Insert a row into given table.

        @type table: <type 'str'>
        @param table: The table to replace/insert into.

        @type column: <type 'list'> or an iterable with index.
        @param column: The columns to operate on.

        @type value: <type 'list'>
        @param value: The values being inserted.
        '''
        name = self.insert_row.__name__
        if not hasattr(column, '__iter__') or not hasattr(values, '__iter__'):
            raise InvalidArg(name, 'column', 'list or tuple')
        column = ', '.join(column)
        row_values = ', '.join(values)
        replace_command = ('INSERT OR REPLACE INTO %s(%s) VALUES(%s)' % 
                          (table, column, row_values))
        self._execute(replace_command)

    def get_row(self, table, values, size=None): 
        '''
        Method to retrieve row from a specified table.
    
            @type table: <type 'str'> 
            @param table: the table to retrieve row from
    
            @type value: <type 'dict'>
            @param value: A key-value pair to match and retrieve all other
                          adjacent values in the corresponding row.

            @type size: <type 'int'>
            @param size: Number of entries to retrieve.
        '''
        name = self.get_row.__name__
        row_values = ' AND '.join(map('='.join, values.items()))
        row_cmd = 'SELECT * FROM %s WHERE %s' % (table, row_values)
        return self._execute(row_cmd, items=size)

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

    def dump_table(self, table, order=None):
        '''
        Method to return entire table contents.

            @type table: <type 'str'>
            @param table: The table to dump contents of.

            @type order: <type 'NoneType'> or <type 'str'>
            @param order: Optional argument to return contents ordered by a 
                          column.
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
        table_cmd = ('SELECT name FROM sqlite_master WHERE type="table"')
        return self._execute(table_cmd, items=ALL)
    
    def get_schema(self, table):
        '''
        Method to return the schema of a table.

            @type table: <type 'str'>
            @param table: A table name to search for in database object.
        '''
        schema_cmd = ('PRAGMA TABLE_INFO(%s)' % table)
        return self._execute(schema_cmd, items=ALL)
