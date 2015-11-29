#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import unittest

from quikql import *


class QuikqlTest(unittest.TestCase):

    def setUp(self):
        self.testdb = Quikql(path)
        self.testdb.create_table('Music', {'artist':'TEXT', 
                                           'title':'TEXT', 
                                           'duration':'INTEGER', 
                                           'track_number':'INTEGER'})

    def test_create_database(self):
        test_table = 'Music'
        test_retrieve = self.testdb.get_tables()[0]
        self.assertIn(test_table, test_retrieve)

    def test_retrieve_tables(self):
        tables = 'Music'
        test_retrieve = self.testdb.get_tables()[0]
        self.assertIn(tables, test_retrieve)

    def test_delete_table(self):
        self.testdb.delete_table('Music')
        tables = self.testdb.get_tables()
        self.assertFalse(tables)

    def test_retrieve_schema(self):
        test_schema = {u'artist':u'TEXT', u'title':u'TEXT',
                       u'duration':u'INTEGER', u'track_number':u'INTEGER'}
        schema = self.testdb.get_schema('Music')
        schema_map = {i[1]:i[2] for i in schema}
        self.assertEqual(test_schema, schema_map)

    def test_insert_row(self):
        self.testdb.insert_row('Music', {'artist':'Lifetones', 
                                         'title':'Good Sides'})
        
    """
    def test_get_row(self):
        row = [(2001,)]
        self.testdb.insert_row('Music', ['ids'], ['2001'])
        testrow = self.testdb.get_row('Music', {'ids':'2001'}, size=ALL)
        self.assertEqual(testrow, row) 

    def test_delete_row(self):
        self.testdb.insert_row('Music', ['ids'], ['2001'])
        self.testdb.delete_row('Music', 'ids', '2001')
        table = self.testdb.dump_table('Music')
        self.assertEqual([], table)

    def test_retrieve_table_content(self):
        table = [(2001,)]
        self.testdb.delete_table('Music')
        self.testdb.create_table('Music', {'ids':'INTEGER'})
        self.testdb.insert_row('Music', ['ids'], ['2001'])
        testtable = self.testdb.dump_table('Music')
        self.assertEqual(testtable, table)

    def test_table_size(self):
        size = 48
        self.testdb.delete_table('Music')
        self.testdb.create_table('Music', {'ids':'INTEGER'})
        self.testdb.insert_row('Music', ['ids'], ['2001'])
        test_table_size = self.testdb.table_size('Music')
        self.assertEqual(test_table_size, size)
    """
 
if __name__ == '__main__':
    path = os.getcwd() + '/test.db'
    if os.path.isfile(path):
        os.unlink(path)
    unittest.main()                
