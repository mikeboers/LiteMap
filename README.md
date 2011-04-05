**LiteMap** is a Python class which behaves like a dictionary, but persists to
a table within a SQLite database.

The `LiteMap` will only map strings to other strings. If you would like to use more complex values then wrap this object in a `shelve.Shelf`. If you would like to use more complex keys and values then wrap this object in a `SerialView`.

Example:
    
    from litemap import LiteMap
    map = LiteMap('/path/to/db.sqlite', 'table_name')
    # use map like a dictionary
