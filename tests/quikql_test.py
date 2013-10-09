#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import os

from quikql import Quikql as ql


class QuikqlTest(unittest.TestCase):

    def setUp(self):
        path = os.getcwd()
        self.testdb = ql(path + '/test.db')
        self.testdb.create_table('test_table', ids='INTEGER')

    def test_create_database(self):
        test_table = 'test_table'
        test_retrieve = self.testdb.get_tables()[0]
        self.assertEqual((test_table,), test_retrieve)

    def test_retrieve_tables(self):
        tables = ('test_table',)
        test_retrieve = self.testdb.get_tables()[0]
        self.assertEqual(tables, test_retrieve)

    def test_delete_table(self):
        self.testdb.delete_table('test_table')
        tables = self.testdb.get_tables()
        self.assertEqual([], tables)

    def test_retrieve_schema(self):
        test_schema = [(0, u'ids', u'INTEGER', 0, None, 0)]
        schema = self.testdb.get_schema('test_table')
        self.assertEqual(test_schema, schema)

    def test_insert_row(self):
        self.testdb.insert_row('test_table', ['ids'], ['2001'])
        
    def test_get_row(self):
        row = [(2001,)]
        self.testdb.insert_row('test_table', ['ids'], ['2001'])
        testrow = self.testdb.get_row('test_table', ('ids',), {'ids':'2001'})
        self.assertEqual(testrow, row) 

    def test_delete_row(self):
        self.testdb.insert_row('test_table', ['ids'], ['2001'])
        self.testdb.delete_row('test_table', 'ids', '2001')
        table = self.testdb.dump_table('test_table')
        self.assertEqual([], table)

    def test_retrieve_table_content(self):
        table = [(2001,)]
        self.testdb.delete_table('test_table')
        self.testdb.create_table('test_table', ids='INTEGER')
        self.testdb.insert_row('test_table', ['ids'], ['2001'])
        testtable = self.testdb.dump_table('test_table')
        self.assertEqual(testtable, table)
 
if __name__ == '__main__':
    unittest.main()                
