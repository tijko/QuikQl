#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
This module is a wrapper for sqlite3 that has methods to make database 
transactions or other database operations and queries.  
'''

import os
import sqlite3
import sys

from exceptions import *


SQLITE_TYPES = {'NULL', 'INTEGER', 'TEXT', 'REAL', 'BLOB', 'INTEGER PRIMARY KEY'}

FALL = 'ALL'
FONE = 'ONE'


class Quikql(object):
    '''
    The wrapper class
    '''
    def __init__(self, filename):
        '''
        When called pass in a file path, to set for all future calls
        on wrapper-object.
    
            @type filename: <type 'str'>
            @param filename: file path to .db for object to use.
        '''
        self._filename = filename
        self._conn = sqlite3.connect(self._filename)

    def _fetch(self, cursor, items):
        '''
        Private method to retrieve values after a query is made.

            @type cursor: <type 'Cursor object'>
            @param cursor: the current connection's cursor.

            @type items: <type 'Quikql Constant'> or <type 'int'>
            @param items: denotes the total amount of values the query is meant
                          to return.
        '''
        if not items: return
        if items == FONE:
            return cursor.fetchone()
        elif items == FALL:
            return cursor.fetchall()
        return cursor.fetchmany(items)

    def _execute(self, command, items=0, many=False, valueiter=()):
        '''
        Private method to dispatch all queries to database.  The context 
        manager will handle commits and closing of database connections.

        @type command: <type 'str'>
        @param command: a string command to be executed.

        @type items: <type 'Quikql Constant'> or <type 'int'>
        @param items: denotes the total amount of values the query is meant
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

    def create_table(self, table_name, key=None, **columns):
        '''
        Method to create new tables.
        
            @type table_name: <type 'str'>
            @param table_name: the name of the new table to create.

            @type key: <type 'dict'>
            @param key: the key-value pairs for the new foreign key.

            @type columns: <type 'dict'>
            @param columns: keyword argument/s that set column names for table. 
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
        @param columns: key-value pairs to match identifier-type for the new
                        table schema.
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
        @param keys: key-value pairs representing the key and reference for a
                     table schema.
        '''
        foreignkey, references = key.popitem()
        foreignkey_statement = ', FOREIGN KEY(%s)' % foreignkey
        reference_statement = ' REFERENCES %s(%s)' % tuple(references)
        return foreignkey_statement + reference_statement

    def delete_table(self, table):
        '''
        Method to delete a table.

            @param table: the table to delete.
        '''
        delete_table_command = 'DROP TABLE IF EXISTS %s' % table
        self._execute(delete_table_command)
    
    def delete_row(self, table, column, value):
        '''
        Method to query table for values to remove.

            @type table: <type 'str'>
            @param table: name of table to query.

            @type column: <type 'str'>
            @param column: table column to find values.

            @type value: any acceptable sqlite3 type.
            @param value: value to search and remove.
        '''
        delete_row_command = 'DELETE FROM %s WHERE %s="%s"' % (table, column, value)
        self._execute(delete_row_command)
    
    def update_row(self, table, cur_cols=dict(), update=dict()): 
        '''
        Update a column value to a new value.

            @type table: <type 'str'>
            @param table: the table to update row in
    
            @type column: <type 'dict'>
            @param column: the column to update row in

            @type value: <type 'dict'>
            @param value: the value to insert new.
        '''
        name = self.update_row.__name__
        try:
            update = getattr(update, 'items')
            update = [(i, repr(v)) for i,v in update()]
            curr = getattr(cur_cols, 'items')
            curr = [(i, repr(v)) for i,v in curr()]
            curr = ' AND '.join(v for v in ('='.join(i) for i in curr))
            update = ', '.join(v for v in ('='.join(i) for i in update))
        except AttributeError:
            raise InvalidArg(name, 'cur_cols and update', 'dict')
        update_cmd = ('UPDATE %s SET %s WHERE %s' % (table, update, curr))
        self._execute(update_cmd)

    def insert_row(self, table, column=None, value=None):
        '''
        Replace or Insert a row into given table

        @type table: <type 'str'>
        @param table: the table to replace/insert into

        @type column: <type 'list'> or an iterable with index
        @param column: the columns to operate on

        @type value: <type 'list'>
        @param value: the values being inserted
        '''
        name = self.insert_row.__name__
        if isinstance(column, tuple) or isinstance(column, list):
            column = ', '.join(column)
        else:
            raise InvalidArg(name, 'column', 'list or tuple')
        if isinstance(value, tuple) or isinstance(value, list):
            value = [repr(i) for i in value]
            value = ', '.join(value)
        else:
            raise InvalidArg(name, 'value', 'list or tuple')
        replace = ('INSERT OR REPLACE INTO %s(%s) VALUES(%s)' % 
                   (table, column, value))
        self._execute(replace)

    def get_row(self, table, columns=None, value=None, size=None): 
        '''
        Method to retrieve row from a specified table.
    
            @type table: <type 'str'> 
            @param table: the table to retrieve row from
    
            @type column: <type 'NoneType'> or an iterable with index
            @param column: the column to find the row value.  If none are
                           supplied, '*' will be defaulted to and all columns
                           will be returned
        
            @type value: <type 'NoneType'> or <type 'dict'>
            @param value: a value to match and retrieve

            @type size: <type 'int'>
            @param size: number of entries to retrieve
        '''
        name = self.get_row.__name__
        if not columns and not value:
            args = len([i for i in (table, columns, value, size) if i])
            raise InsufficientArgs(name, 3, args)
        if columns:
            if isinstance(columns, tuple) or isinstance(columns, list):
                columns = ', '.join(columns)
            else:
                raise InvalidArg(name, 'columns', 'list or tuple')
        else:
            columns = '*'
        if value:
            try:
                values = getattr(value, 'items')
                values = [(i,repr(v)) for i,v in values()]
                value = ' AND '.join(v for v in ('='.join(i) for i in values))
                row_cmd = ('SELECT %s FROM %s WHERE %s' % (columns, table, value))
            except AttributeError:
                raise InvalidArg(name, 'value', 'dict')
        else:
            row_cmd = ('SELECT %s FROM %s' % (columns, table))
        if not size:
            size = FALL
        return self._execute(row_cmd, items=size)

    def dump_table(self, table, columns=None, order=None):
        '''
        Method to return entire table contents.

            @type table: <type 'str'>
            @param table: the table to dump contents of

            @type columns: <type 'NoneType'> or an iterable with an index 
            @param columns: columns to return data entries from.  If none are
                            supplied, '*' will be defaulted to and all columns
                            returned

            @type order: <type 'NoneType'> or <type 'str'>
            @param order: optional argument to return contents ordered by a 
                          column
        '''
        if columns:
            if isinstance(columns, tuple) or isinstance(columns, list):            
                columns = ', '.join(columns)
            else:
                name = self.dump_table.__name__
                raise InvalidArg(name, 'columns', 'list or tuple')
        else:
            columns = '*'
        if not order:
            table_cmd = ('SELECT %s FROM %s' % (columns, table))
        else:
            table_cmd = ('SELECT %s FROM %s ORDER BY %s' % (columns, table, order))
        return self._execute(table_cmd, items=FALL) 

    def table_size(self, table):
        '''
        Method to find the byte-size of the supplied table.

            @type table: <type 'str'>
            @param table: the table to find the byte-size for.
        '''
        table_data = self.dump_table(table)
        return sys.getsizeof(table_data)

    def get_tables(self):
        '''
        Method to return all the tables in database object.
        '''
        table_cmd = ('SELECT name FROM sqlite_master WHERE type="table"')
        return self._execute(table_cmd, items=FALL)
    
    def get_schema(self, table):
        '''
        Method to return the schema of a table.

            @type table: <type 'str'>
            @param table: a table name to search for in database object
        '''
        schema_cmd = ('PRAGMA TABLE_INFO(%s)' % table)
        return self._execute(schema_cmd, items=FALL)
