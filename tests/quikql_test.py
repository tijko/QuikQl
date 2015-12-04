#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import unittest

from quikql import *


class QuikqlTest(unittest.TestCase):

    def setUp(self):
        testdb.create_table(tablename, schema)

    def test_create_database(self):
        test_table = tablename
        test_retrieve = testdb.get_tables()[0]
        self.assertIn(test_table, test_retrieve)

    def test_retrieve_tables(self):
        tables = tablename
        test_retrieve = testdb.get_tables()[0]
        self.assertIn(tables, test_retrieve)

    def test_delete_table(self):
        testdb.delete_table(tablename)
        tables = testdb.get_tables()
        self.assertFalse(tables)

    def test_retrieve_schema(self):
        test_schema = testdb.get_schema(tablename)
        schema_map = {i[1]:i[2] for i in test_schema}
        self.assertEqual(schema, schema_map)

    def test_insert_row(self):
        testdb.insert_row(tablename, {'artist':'Lifetones', 
                                      'title':'Good Sides'})

    def test_get_row(self):
        row = {'artist':'Lifetones', 'title':'Good Sides'}
        testdb.insert_row(tablename, row)
        testrow = testdb.get_row(tablename, row, size=ALL)
        retrieved_row = {'artist':testrow[0][2], 'title':testrow[0][1]}
        self.assertEqual(row, retrieved_row)

    def test_delete_row(self):
        row = {'artist':'Neal Howard', 'title':'The gathering'}
        row_retrieve = (None, u'The gathering', u'Neal Howard', None,)
        testdb.insert_row(tablename, row)
        table_before_del = testdb.dump_table(tablename)
        testdb.delete_row(tablename, row)
        table_after_del = testdb.dump_table(tablename)
        self.assertIn(row_retrieve, table_before_del)
        self.assertNotIn(row_retrieve, table_after_del)

    def test_retrieve_table_content(self):
        testdb.delete_table(tablename)
        testdb.create_table(tablename, schema)
        row = {'artist':'Neal Howard', 'title':'The gathering'}
        table_retrieve = [(None, u'The gathering', u'Neal Howard', None,)]
        testdb.insert_row(tablename, row)
        testtable = testdb.dump_table(tablename)
        self.assertEqual(testtable, table_retrieve)

 
if __name__ == '__main__':
    path = os.getcwd() + '/test.db'
    if os.path.isfile(path):
        os.unlink(path)
    testdb = Quikql(path)
    tablename = 'Music'
    schema = {'artist':'TEXT', 'title':'TEXT', 
              'duration':'INTEGER', 'track_number':'INTEGER'}
    unittest.main(verbosity=3) 
