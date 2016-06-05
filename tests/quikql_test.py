#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import unittest

from quikql import *
from sqlite3 import IntegrityError


class QuikqlTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.path = os.getcwd() + '/radio.db'
        remove_db(self.path)
        self.testdb = Quikql('radio.db')
        self.testdb.create_table('artists', artists_schema, pkey=('artist',))
        self.testdb.create_table('music', music_schema, 
                             fkey={'artist':('artists', 'artist')})
        with open('recordings.txt') as f:
            raw_json_str = f.read()
        self.json_data = json.loads(raw_json_str)
        for artist in self.json_data['artists']:
            self.testdb.insert_row('artists', {'artist':artist})
            for title in self.json_data['artists'][artist]['titles']:
                self.testdb.insert_row('music', {'artist':artist,
                                                 'track':title}) 

    @classmethod
    def tearDownClass(self):
        remove_db(self.path)

    def test_create_table(self):
        tables_before = [i[0] for i in self.testdb.get_tables()]
        self.assertNotIn(test_table, tables_before)
        self.testdb.create_table(test_table, test_schema)
        tables_after = [i[0] for i in self.testdb.get_tables()]
        self.assertIn(test_table, tables_after)
        self.testdb.delete_table(test_table)

    def test_delete_table(self):
        self.testdb.create_table(test_table, test_schema)
        tables_before = [i[0] for i in self.testdb.get_tables()]
        self.assertIn(test_table, tables_before)
        self.testdb.delete_table(test_table)
        tables_after = [i[0] for i in self.testdb.get_tables()]
        self.assertNotIn(test_table, tables_after)

    def test_get_schema(self):
        test_artists_schema = self.testdb.get_schema('artists')
        test_music_schema = self.testdb.get_schema('music')
        artists_schema_map = {i[1]:i[2] for i in test_artists_schema}
        music_schema_map = {i[1]:i[2] for i in test_music_schema}
        self.assertEqual(artists_schema, artists_schema_map)
        self.assertEqual(music_schema, music_schema_map)

    def test_get_tables(self):
        test_tables = self.testdb.get_tables()
        self.assertSequenceEqual(['artists', 'music'], 
                                 [t[0] for t in test_tables])

    def test_insert_row(self):
        artists_row = {'artist':'Lifetones'}
        get_field = ('Lifetones',)
        get_before = self.testdb.get_row('artists', artists_row)
        self.assertIsNone(get_before)
        self.testdb.insert_row('artists', artists_row)
        get_after = self.testdb.get_row('artists', artists_row)
        self.assertEqual(get_field, get_after)
        self.testdb.delete_row('artists', artists_row)

    def test_insert_rows(self):
        artists_rows = [{'artist':'Leon Vynehall'},
                        {'artist':'JaimeXX'},
                        {'artist':'Caribou'},
                        {'artist':'Dusky'}] 
        for artist in artists_rows:
            get_before = self.testdb.get_row('artists', artist)
            self.assertIsNone(get_before)
        self.testdb.insert_rows('artists', *artists_rows)
        for artist in artists_rows:
            get_after = self.testdb.get_row('artists', artist)
            self.assertEqual(artist['artist'], *get_after)
            self.testdb.delete_row('artists', artist) 
        
    def test_get_row(self):
        artist_row = {'artist':'Lifetones'}
        self.testdb.insert_row('artists', artist_row)
        test_get = self.testdb.get_row('artists', artist_row, size=ALL)
        self.assertEqual(('Lifetones',), test_get[0])
        self.testdb.delete_row('artists', artist_row)

    def test_delete_row(self):
        row = {'artist':'Neal Howard'}
        self.testdb.insert_row('artists', row)
        row_before = self.testdb.get_row('artists', row)
        self.assertIsNotNone(row_before)
        self.testdb.delete_row('artists', row)
        row_after = self.testdb.get_row('artists', row)
        self.assertIsNone(row_after)

    def test_delete_row_InvalidRow(self):
        del_row = {'artist':'The Doors'}
        table_before_delete = self.testdb.dump_table('artists')
        self.testdb.delete_row('artists', del_row)
        table_after_delete = self.testdb.dump_table('artists')
        self.assertEqual(table_before_delete, table_after_delete)

    def test_count_field(self):
        artist_count = len(self.json_data['artists'])
        self.assertEqual(artist_count, self.testdb.count('artists', 'artist')[0])

    def test_count_InvalidArg(self):
        self.assertRaises(InvalidArg, self.testdb.count,
                          'artists', ['field1', 'field2'])

    def test_minimum_field(self):
        min_artist = 'aphex twin'
        self.testdb.insert_row('artists', {'artist':min_artist})
        self.assertEqual(min_artist, self.testdb.min('artists', 'artist')[0])
        self.testdb.delete_row('artists', {'artist':min_artist})

    def test_minimum_InvalidArg(self):
        self.assertRaises(InvalidArg, self.testdb.min, 'artists', ['field1', 
                                                                   'field2'])
    def test_maximum_field(self):
        max_artist = 'zz top'
        self.testdb.insert_row('artists', {'artist':max_artist})
        self.assertEqual(max_artist, self.testdb.max('artists', 'artist')[0])
        self.testdb.delete_row('artists', {'artist':max_artist})

    def test_maximum_InvalidArg(self):
        self.assertRaises(InvalidArg, self.testdb.max, 'artists', ['field1', 
                                                                   'field2'])

    def test_sum(self):
        duration_updates = [({'duration':4.02}, {'track':'Blue Moon'}),
                            ({'duration':8.12}, {'track':'Player'}),
                            ({'duration':2.08}, {'track':'Nettle Leaves'})]
        test_total = sum([i[0]['duration'] for i in duration_updates])
        for update in duration_updates:
            self.testdb.update_row('music', update[0], row=update[1])
        self.assertEqual(test_total, self.testdb.sum('music', 'duration')[0])

    def test_sum_InvalidArg(self):
        self.assertRaises(InvalidArg, self.testdb.sum, 'artists', ['field1', 
                                                                   'field2'])

    def test_retrieve_table_content(self):
        artists = [entry for entry in self.json_data['artists']]
        table_artists = [i[0] for i in self.testdb.dump_table('artists')]
        self.assertEqual(artists, table_artists)

    def test_update_row(self):
        update_row = {'artist':'deadmau5', 'track':'Fallen'}
        update_column = {'duration':2.31}
        before_duration = self.testdb.get_row('music', update_row) 
        self.assertNotIn(2.31, before_duration)
        self.testdb.update_row('music', update_column, update_row)
        after_duration = self.testdb.get_row('music', update_row)
        self.assertIn(2.31, after_duration)

    def test_update_row_InvalidRow(self):
        invalid_update_row = {'artist':'Led Zeppelin'}
        update_column = {'track':'Misty Mountain Top'}
        self.testdb.update_row('music', update_column, invalid_update_row)
        get_row = self.testdb.get_row('music', {'artist':'Led Zeppelin',
                                           'track':'Misty Mountain Top'})
        self.assertIsNone(get_row)

    def test_insert_row_InvalidArg(self):
        invalid_insert_row = [('artist', 'Diplo')]
        self.assertRaises(InvalidArg, self.testdb.insert_row, 
                          'artists', invalid_insert_row)

    def test_delete_row_InvalidArg(self):
        invalid_delete_row = [('artist', 'Frank Sinatra')]
        self.assertRaises(InvalidArg, self.testdb.delete_row,
                          'artists', invalid_delete_row)

    def test_get_row_InvalidArg(self):
        invalid_get_row = [('artist', 'Franz Schubert')]
        self.assertRaises(InvalidArg, self.testdb.get_row,
                          'artists', invalid_get_row)

    def test_create_table_InvalidSQLType(self):
        table = 'Foo'
        schema = {'Bar':'Baz'}
        self.assertRaises(InvalidSQLType, self.testdb.create_table, 
                                                     table, schema)
    
    def test_insert_row_InvalidForeignKey(self):
        invalid_foreignkey = {'artist':'foo', 'track':'bar', 'album':'baz'}
        self.assertRaises(IntegrityError, self.testdb.insert_row, 
                          'music', invalid_foreignkey)
        

def remove_db(path):
    if os.path.isfile(path):
        os.unlink(path)


if __name__ == '__main__':
    artists_schema = {'artist':TEXT}
    music_schema = {'track':TEXT, 'duration':REAL,
                    'album':TEXT, 'artist':TEXT}
    test_table = 'Open_Source_Software'
    test_schema = {'name':TEXT, 'language':TEXT, 'loc':INTEGER}
    unittest.main(verbosity=3) 
