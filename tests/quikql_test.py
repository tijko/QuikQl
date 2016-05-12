#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import unittest

from quikql import *


class QuikqlTest(unittest.TestCase):

    def setUp(self):
        testdb.create_table(tablename, schema, pkey=primary_key)
        testdb.insert_rows(tablename, *entries)

    def tearDown(self):
        tables = testdb.get_tables()
        for table in tables:
            testdb.delete_table(table)

    def test_create_table(self):
        testtable = 'Open_Source_Software'
        schema = {'name':'TEXT', 'language':'TEXT', 'loc':'INTEGER'}
        testdb.create_table(testtable, schema)
        tables = [i[0] for i in testdb.get_tables()]
        self.assertIn(testtable, tables)

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
        row = {'artist':'Lifetones', 'title':'Good Sides', 'artistid':8}
        testdb.insert_row(tablename, row)

    def test_insert_rows(self):
        rows = [{'artist':'damu', 'title':'How its suppose to be', 'artistid':9},
                {'artist':'Nightmare on Wax', 'title':'You Wish', 'artistid':10},
                {'artist':'Deep Space House', 'duration':12423, 'artistid':11},
                {'artist':'Bonobo', 'title':'Black sands', 
                 'track_number':3, 'artistid':12}]
        testdb.insert_rows(tablename, *rows)
        table = testdb.dump_table(tablename)
        rows.extend(entries)
        current_entries = []
        for entry in table:
            row = {}
            for i in range(len(entry)):
                if entry[i] is not None:
                    row[fields[i]] = entry[i]
            if row:
                current_entries.append(row)
        for row in rows:
            self.assertIn(row, current_entries)

    def test_get_row(self):
        row = {'artist':'Lifetones', 'title':'Good Sides'}
        testdb.insert_row(tablename, row)
        testrow = testdb.get_row(tablename, row, size=ALL)
        retrieved_row = {'artist':testrow[0][4], 'title':testrow[0][3]}
        self.assertEqual(row, retrieved_row)

    def test_delete_row(self):
        row = {'artist':'Neal Howard', 'title':'The gathering', 'artistid':8}
        row_retrieve = (None, None, 8, u'The gathering', u'Neal Howard',)
        testdb.insert_row(tablename, row)
        table_before_del = testdb.dump_table(tablename)
        testdb.delete_row(tablename, row)
        table_after_del = testdb.dump_table(tablename)
        self.assertIn(row_retrieve, table_before_del)
        self.assertNotIn(row_retrieve, table_after_del)

    def test_delete_invalid_row(self):
        row = {'artist':'The Doors', 'title':'Soul Kitchen', 'artistid':7}
        table_before_delete = testdb.dump_table(tablename)
        testdb.delete_row(tablename, row)
        table_after_delete = testdb.dump_table(tablename)
        self.assertEqual(table_before_delete, table_after_delete)

    def test_count_field(self):
        field_counts = {'artist':5, 'title':5, 'duration':2, 
                        'track_number':1, 'artistid':5}
        for field in field_set:
            self.assertEqual(field_counts[field], 
                             testdb.count(tablename, field)[0])

    def test_count_InvalidArg(self):
        self.assertRaises(InvalidArg, testdb.count,
                          tablename, ['field1', 'field2'])

    def test_minimum_field(self):
        for field in field_set:
            field_minimum = min([entry[field] for entry in entries 
                                              if entry.get(field)])
            self.assertEqual(field_minimum, testdb.min(tablename, field)[0])

    def test_minimum_InvalidArg(self):
        self.assertRaises(InvalidArg, testdb.min, tablename, ['field1', 
                                                              'field2'])

    def test_maximum_field(self):
        for field in field_set:
            field_maximum = max([entry[field] for entry in entries 
                                 if entry.get(field)])
            self.assertEqual(field_maximum, testdb.max(tablename, field)[0])

    def test_maximum_InvalidArg(self):
        self.assertRaises(InvalidArg, testdb.max, tablename, ['field1', 
                                                              'field2'])

    def test_sum(self):
        duration_total = sum([entry['duration'] for entry in 
                              entries if entry.get('duration')])
        self.assertEqual(duration_total, testdb.sum(tablename, 'duration')[0])

    def test_sum_InvalidArg(self):
        self.assertRaises(InvalidArg, testdb.sum, tablename, ['field1', 
                                                              'field2'])

    def test_retrieve_table_content(self):
        fields = [i[1] for i in testdb.get_schema('Music')]
        current_entries = [tuple(entry.get(field) for field in fields) 
                           for entry in entries]
        testtable = testdb.dump_table(tablename)
        self.assertEqual(testtable, current_entries)

    def test_update_row(self):
        update_row = {'artist':'Deadmau5', 'track_number':3}
        update_column = {'title':'ID', 'track_number':4}
        new_row = (None, u'ID', u'Deadmau5', 4)
        testdb.update_row(tablename, update_column, update_row)
        table_dump = testdb.dump_table(tablename)
        self.assertIn(new_row, table_dump)

    def test_update_invalid_row(self):
        invalid_update_row = {'artist':'Led Zeppelin', 'artistid':6}
        update_column = {'title':'Misty Mountain Top', 'track_number':3}
        new_row = (None, u'Misty Mountain Top', 'Led Zeppelin', 3)
        testdb.update_row(tablename, update_column, invalid_update_row)
        table_dump = testdb.dump_table(tablename)
        self.assertNotIn(new_row, table_dump)

    def test_insert_InvalidArg(self):
        invalid_row_insert = [('artist', 'Diplo')]
        self.assertRaises(InvalidArg, testdb.insert_row, 
                          tablename, invalid_row_insert)

    def test_delete_InvalidArg(self):
        invalid_row_delete = [('artist', 'Frank Sinatra')]
        self.assertRaises(InvalidArg, testdb.delete_row,
                          tablename, invalid_row_delete)

    def test_get_InvalidArg(self):
        invalid_row_get = [('artist', 'Franz Schubert')]
        self.assertRaises(InvalidArg, testdb.get_row,
                          tablename, invalid_row_get)
 
    def test_create_InvalidSQLType(self):
        table = 'Foo'
        schema = {'Bar':'Baz'}
        self.assertRaises(InvalidSQLType, testdb.create_table, table, schema)

def remove_db(path):
    if os.path.isfile(path):
        os.unlink(path)

if __name__ == '__main__':
    path = os.getcwd() + '/test.db'
    remove_db(path)
    testdb = Quikql(path)
    tablename = 'Music'
    fields = ['duration', 'title', 'artist', 'track_number', 'artistid']
    schema = {'artist':'TEXT', 'title':'TEXT', 'artistid':'INTEGER',
              'duration':'INTEGER', 'track_number':'INTEGER'}
    primary_key = ('artistid',)
    entries = [{'artist':'Pryda', 'title':'opus', 'duration':532, 'artistid':1},
               {'artist':'Deadmau5', 'title':'everything after', 
                'track_number':3, 'artistid':2},
               {'artist':'Steve Angello', 'title':'voices', 
                'duration':531, 'artistid':3},
               {'artist':'Gramatik', 'title':'prophet2.0', 'artistid':4},
               {'artist':'MF Doom', 'title':'Safed Musli', 'artistid':5}]
    field_set = {k for e in entries for k in e.keys()}
    unittest.main(verbosity=3) 
    remove_db(path)

