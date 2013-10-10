QuikQl
======

A simple wrapper for sqlite3.  

With QuikQl you can create databases with sqlite3 without the need to manually 
inputting the database operations. 

##Usage

To use QuikQl, you create a sqlite3 session by passing in the path to the 
database you want to use:

    >>> from quikql import Quikql as ql
    >>>
    >>> session = ql('/path/to/your/database.db')

To create a new table, call your QuikQl objects `create_table` method, passing 
in a `string` for the name of your table as the first argument and any number 
of `kwargs` with a valid sqlite3 type as the value:

    >>> session.create_table('Employees', name='TEXT', id='INTEGER')

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

To insert a row, call your QuikQl objects `insert_row` method, passing in the
name of the table as your first argument, then a `list` or `tuple` with the 
columns you want to insert into followed by a `list` or `tuple` of those values:

    >>> session.insert_row('Employees', ['name', 'id'], ['Bob', '123'])
    
The `dump_table` method will return all of a tables content:

    >>> contents = session.dump_table('Employees')
    >>> contents
    [(u'Bob', 123)]

To query the table by value you can use QuikQl's `get_row`.  Pass in the 
table's name with a `list` of columns you want retrieved from the row and
optionally a `dict` parameter of matching values:

    >>> session.get_row('Employees', ['name'], {'id':'123'})
    [(u'Bob',)]

Delete a table by calling `delete_table` method with the table you want to
delete:

    >>> session.delete_table('Employees')

##Installation

Download the zip file and unzip in the directory it is located:

    cd /path/to/Quikql

    unzip QuikQl-master

    cd Quikql-master

Then run the `setup.py` file with root privileges:

    sudo python setup.py

