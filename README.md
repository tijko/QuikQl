Quikql
======

[![Build Status](https://travis-ci.org/tijko/Quikql.svg?branch=master)](https://travis-ci.org/tijko/Quikql)

A simple wrapper for [sqlite3](https://docs.python.org/2/library/sqlite3.html).  

This was made as an educational project to learn about deploying custom 
python modules as well as basic database operations.  Having later learned about
[sqlalchemy](http://www.sqlalchemy.org/) as an Object-Relational Mapping which 
is all that Quikql is and much more, would have been the way to go if not for 
only learning purposes.  Nevertheless it was still helpful educationally and 
practically for deploying a quick and lightweight database ORM module.

With Quikql you can create and operate on databases using sqlite3 without the 
need to manually input the database command strings, create connections, deal 
with cursor objects, or use context mangers to execute the commands. 

##Usage

To use Quikql, you create a sqlite3 session by passing in the path to the 
database you want to use:

    >>> from quikql import Quikql as ql
    >>>
    >>> session = ql('/path/to/your/database.db')

To create a new table, call your Quikql objects `create_table` method, passing 
in a `string` for the name of your table as the first argument and a `dict` of
any number of column names as the keys and a valid sqlite3 type as the value:

    >>> session.create_table('Employees', {'name':'TEXT', 'id':'INTEGER'})

You can return a list of all the current tables by using the `get_tables`
method:

    >>> current_tables = session.get_tables()
    >>> current_tables
    [(u'Employees',)]

You can quickly return a tables schema by calling the `get_schema` method,
just pass in the name of the table you want the schema for:

    >>> schema = session.get_schema('Employees')
    >>> schema
    [(0, u'name', u'TEXT', 0, None, 0), (1, u'id', u'INTEGER', 0, None, 0)]

To insert a row, call your Quikql objects `insert_row` method, passing in the
name of the table as your first argument, then a `list` or `tuple` with the 
columns you want to insert into followed by a `list` or `tuple` of those values:

    >>> session.insert_row('Employees', ['name', 'id'], ['Bob', '123'])
    
The `dump_table` method will return all of a tables content:

    >>> contents = session.dump_table('Employees')
    >>> contents
    [(u'Bob', 123)]

To query the table by value you can use Quikql's `get_row`.  Pass in the 
table's name with a row key-value pair matching which row to retrieve:

    >>> session.get_row('Employees', {'id':'123'})
    [(u'Bob',)]

Delete a table by calling `delete_table` method with the table you want to
delete:

    >>> session.delete_table('Employees')

##Installation

Download the zip file and unzip in the directory it is located:

    cd /path/to/Quikql

    unzip Quikql-master

    cd Quikql-master

Then run the `setup.py` file with root privileges:

    sudo python setup.py

