#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
This module is a wrapper for sqlite3 that has methods to make database 
transactions or other database operations and queries.  
'''

import os
import sqlite3
import sys

from exceptions import InsufficientArgs, InvalidArg, InvalidType


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
        self.SQLITE_TYPES = ['NULL', 'INTEGER', 'TEXT', 'REAL', 
                             'BLOB', 'INTEGER PRIMARY KEY'
                            ]
        self._filename = filename
        self._conn = sqlite3.connect(self._filename)

    def _commit_action(self, cmd, size=None, execute_type=None):
        '''
        Method to dispatch all queries to database.  Context_Manager will 
        handle commits and closing of database connections.

            @type cmd: <type 'str'>
            @param cmd: valid parsed query
    
            @type size: <type 'NoneType'> or <type 'int'>
            @param size: return size on a query

            @type execute_type: <type 'str'>
            @param execute_type: specifies what kind of commit
        '''
        with self._conn:
            cur = self._conn.cursor()
            if not execute_type:
                cur.execute(cmd)
            else:
                cur.executemany(cmd, values)
            if not size:
                return
            elif size == 'all':
                db_values = cur.fetchall()
            else:
                if size > 1:
                    db_values = cur.fetchmany(size)
                else:
                    db_values = cur.fetchone()
        return db_values

    def create_table(self, table, key=None, **columns):
        '''
        Method to create new tables.
        
            @type table: <type 'str'>
            @param table: the name of the new table to create.

            @type columns: <type 'dict'>
            @param columns: keyword argument/s that set column names for table. 
        '''
        name = self.create_table.__name__
        if any(v for v in columns.values() if v not in self.SQLITE_TYPES):
            invalid_arg = [v for v in columns.values() 
                           if v not in self.SQLITE_TYPES]
            raise InvalidType(name, invalid_arg[0])
        new_table = 'CREATE TABLE IF NOT EXISTS %s ' % table
        if columns:
            columns = ', '.join(' '.join(i) for i in columns.items())
            if key:
                fk = key.keys()[0]
                ref = key.values()[0]
                foreign_key = (', FOREIGN KEY(%s) REFERENCES %s(%s)' % 
                              (fk, ref[0], ref[1]))
                columns = '(' + columns + foreign_key + ')'
            else:
                columns = '(' + columns + ')'
            new_table += columns
        else:
            raise InsufficientArgs(name, 2, 1)
        with self._conn:
            cur = self._conn.cursor()
            cur.execute(new_table)            
        return

    def delete_table(self, table):
        '''
        Method to delete a table.

            @param table: the table to delete.
        '''
        with self._conn:
            cur = self._conn.cursor()
            cur.execute('DROP TABLE IF EXISTS %s' % table)
        return            
    
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
        with self._conn:
            cur = self._conn.cursor()
            cur.execute('DELETE FROM %s WHERE %s="%s"' % (table, column, value))
        return
    
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
        self._commit_action(update_cmd)
        return

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
        self._commit_action(replace)
        return

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
            size = 'all'
        row = self._commit_action(row_cmd, size=size)
        return row

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
        table_content = self._commit_action(table_cmd, size='all') 
        return table_content

    def table_size(self, table):
        '''
        Method to find the byte-size of the supplied table.

            @type table: <type 'str'>
            @param table: the table to find the byte-size for.
        '''
        table_data = self.dump_table(table)
        byte_size = sys.getsizeof(table_data)
        return byte_size

    def get_tables(self):
        '''
        Method to return all the tables in database object.
        '''
        table_cmd = ('SELECT name FROM sqlite_master WHERE type="table"')
        table = self._commit_action(table_cmd, size='all')
        return table
    
    def get_schema(self, table):
        '''
        Method to return the schema of a table.

            @type table: <type 'str'>
            @param table: a table name to search for in database object
        '''
        schema_cmd = ('PRAGMA TABLE_INFO(%s)' % table)
        schema = self._commit_action(schema_cmd, size='all')
        return schema
