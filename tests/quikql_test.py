#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import unittest

from quikql import *


class QuikqlTest(unittest.TestCase):

    def setUp(self):
        self.testdb = Quikql(path)
        self.tablename = 'Music'
        self.schema = {'artist':'TEXT', 'title':'TEXT', 
                       'duration':'INTEGER', 'track_number':'INTEGER'}
        self.testdb.create_table(self.tablename, self.schema)

    def test_create_database(self):
        test_table = self.tablename
        test_retrieve = self.testdb.get_tables()[0]
        self.assertIn(test_table, test_retrieve)

    def test_retrieve_tables(self):
        tables = self.tablename
        test_retrieve = self.testdb.get_tables()[0]
        self.assertIn(tables, test_retrieve)

    def test_delete_table(self):
        self.testdb.delete_table(self.tablename)
        tables = self.testdb.get_tables()
        self.assertFalse(tables)

    def test_retrieve_schema(self):
        schema = self.testdb.get_schema(self.tablename)
        schema_map = {i[1]:i[2] for i in schema}
        self.assertEqual(self.schema, schema_map)

    def test_insert_row(self):
        self.testdb.insert_row(self.tablename, {'artist':'Lifetones', 
                                                'title':'Good Sides'})

    def test_get_row(self):
        row = {'artist':'Lifetones', 'title':'Good Sides'}
        self.testdb.insert_row(self.tablename, row)
        testrow = self.testdb.get_row(self.tablename, row, size=ALL)
        retrieved_row = {'artist':testrow[0][2], 'title':testrow[0][1]}
        self.assertEqual(row, retrieved_row)

    def test_delete_row(self):
        row = {'artist':'Neal Howard', 'title':'The gathering'}
        row_retrieve = (None, u'The gathering', u'Neal Howard', None,)
        self.testdb.insert_row(self.tablename, row)
        table_before_del = self.testdb.dump_table(self.tablename)
        self.testdb.delete_row(self.tablename, row)
        table_after_del = self.testdb.dump_table(self.tablename)
        self.assertIn(row_retrieve, table_before_del)
        self.assertNotIn(row_retrieve, table_after_del)

    def test_retrieve_table_content(self):
        self.testdb.delete_table(self.tablename)
        self.testdb.create_table(self.tablename, self.schema)
        row = {'artist':'Neal Howard', 'title':'The gathering'}
        table_retrieve = [(None, u'The gathering', u'Neal Howard', None,)]
        self.testdb.insert_row(self.tablename, row)
        testtable = self.testdb.dump_table(self.tablename)
        self.assertEqual(testtable, table_retrieve)

 
if __name__ == '__main__':
    path = os.getcwd() + '/test.db'
    if os.path.isfile(path):
        os.unlink(path)
    unittest.main(verbosity=3) 
