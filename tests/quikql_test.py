#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import unittest

from quikql import *


class QuikqlTest(unittest.TestCase):

    def setUp(self):
        testdb.create_table(tablename, schema)
        testdb.insert_rows(tablename, *entries)

    def tearDown(self):
        testdb.delete_table(tablename)

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

    def test_get_schema(self):
        test_schema = testdb.get_schema(tablename)
        schema_map = {i[1]:i[2] for i in test_schema}
        self.assertEqual(schema, schema_map)

    def test_get_tables(self):
        test_tables = testdb.get_tables()
        self.assertIn(tablename, test_tables[0])

    def test_insert_row(self):
        row = {'artist':'Lifetones', 'title':'Good Sides'}
        testdb.insert_row(tablename, row)

    def test_insert_rows(self):
        rows = [{'artist':'damu', 'title':'How its suppose to be'},
                {'artist':'Nightmare on Wax', 'title':'You Wish'},
                {'artist':'Deep Space House', 'duration':12423},
                {'artist':'Bonobo', 'title':'Black sands', 'track_number':3}]
        testdb.insert_rows(tablename, *rows)

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
        fields = [i[1] for i in testdb.get_schema('Music')]
        current_entries = [tuple(entry.get(field) for field in fields) 
                           for entry in entries]
        testtable = testdb.dump_table(tablename)
        self.assertEqual(testtable, current_entries)

    def test_row_insert_raisesexception(self):
        invalid_row_insert = [('artist', 'Diplo')]
        self.assertRaises(InvalidArg, testdb.insert_row, 
                          tablename, invalid_row_insert)
 

if __name__ == '__main__':
    path = os.getcwd() + '/test.db'
    if os.path.isfile(path):
        os.unlink(path)
    testdb = Quikql(path)
    tablename = 'Music'
    schema = {'artist':'TEXT', 'title':'TEXT', 
              'duration':'INTEGER', 'track_number':'INTEGER'}
    entries = [{'artist':'Pryda', 'title':'opus', 'duration':532},
               {'artist':'Deadmau5', 'title':'everything after', 'track_number':3},
               {'artist':'Steve Angello', 'title':'voices', 'duration':531},
               {'artist':'Gramatik', 'title':'prophet2.0'},
               {'artist':'MF Doom', 'title':'Safed Musli'}]
    unittest.main(verbosity=3) 
