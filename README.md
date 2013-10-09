QuikQl
======

A simple wrapper for sqlite3.  

With QuikQl you can create databases with sqlite3 without the need to manually 
inputting the database operations. 

##Usage

To use QuikQl, you create a sqlite3 session by passing in the path to your database you want to work with:

    from quikql import Quikql as ql

    session = ql('/path/to/your/database.db')

To create a table:

    session.create_table('Employees', name='TEXT', id='INTEGER')

To insert a row:

    session.insert_row('Employees', ('name', 'id',), ('Bob', '123',))

##Installation

Download the zip file and unzip in the directory it is located:

    cd /path/to/Quikql

    unzip QuikQl-master

    cd Quikql-master

Then run the `setup.py` file with root privileges:

    sudo python setup.py

