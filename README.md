QuikQl
======

A simple wrapper for sqlite3.  

With QuikQl you can create databases with sqlite3 without the need to manually 
inputting the database operations. 

##Usage

To use QuikQl, you create a sqlite3 session by passing in the path to your 
database you want to work with:

    >>> from quikql import Quikql as ql

    >>> session = ql('/path/to/your/database.db')

To create a new table, call your QuikQl objects `create_table` method, passing 
in a `string` for the name of your table as the first argument and any number 
of `kwargs` with a valid sqlite3 type string as the value:

    >>> session.create_table('Employees', name='TEXT', id='INTEGER')

You can return a list of all the current tables by using the `get_tables`
method.  

    >>> current_tables = session.get_tables()
    >>> current_tables
    ['Employees']
    
Delete a table by calling `delete_table` method with the table you want to
delete.

    >>> session.delete_table('Employees')


To insert a row, call your QuikQl objects `insert_row` method, passing in:
the name of the table as your first argument, then a `list` or `tuple` with
the columns you want to insert into followed by a `list` or `tuple` of those
values.

    session.insert_row('Employees', ['name', 'id'], ['Bob', '123'])


##Installation

Download the zip file and unzip in the directory it is located:

    cd /path/to/Quikql

    unzip QuikQl-master

    cd Quikql-master

Then run the `setup.py` file with root privileges:

    sudo python setup.py

