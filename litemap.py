# encoding: utf8

import sqlite3
import cPickle as pickle
import collections
import threading
import ast

class LiteMap(collections.MutableMapping):
    """Persistant mapping class backed by SQLite."""
    
    def __init__(self, path, table='__bucket__'):
        self._path = os.path.abspath(os.path.expanduser(path))
        self._table = self._escape(table)
        self._local = threading.local()
        
        with self._conn:
            cur = self._conn.cursor()
            cur.execute('''CREATE TABLE IF NOT EXISTS %s (
                key   STRING UNIQUE ON CONFLICT REPLACE,
                value BLOB
            )''' % self._table)
            index_name = self._escape(table + '_index')
            cur.execute('''CREATE INDEX IF NOT EXISTS %s on %s (key)''' % (index_name, self._table))
    
    def _escape(self, v):
        """Escapes a SQLite identifier."""
        # HACK: there must be a better way to do this (but this does appear to
        # work just fine as long as there is no null byte).
        return '"%s"' % v.replace('"', '""')
    
    @property
    def _conn(self):
        conn = getattr(self._local, 'conn', None)
        if not hasattr(self._local, 'conn'):
            self._local.conn = sqlite3.connect(self._path)
            self._local.conn.text_factory = str
        return self._local.conn
    
    # Overide these in child classes to change the serializing behaviour. By
    # dumping everything to a buffer SQLite will store the data as a BLOB,
    # therefore preserving binary data. If it was stored as a STRING then it
    # would truncate at the first null byte.
    _dump_key = buffer
    _load_key = str
    _dump_value = buffer
    _load_value = str
    
    
    def setmany(self, items):
        with self._conn:
            self._conn.executemany('''INSERT INTO %s VALUES (?, ?)''' % self._table, (
                (self._dump_key(key), self._dump_value(value)) for key, value in items
            ))
    
    def __setitem__(self, key, value):
        self.setmany([(key, value)])

    def __getitem__(self, key):
        cur = self._conn.cursor()
        cur.execute('''SELECT value FROM %s WHERE key = ?''' % self._table, (self._dump_key(key), ))
        res = cur.fetchone()
        if not res:
            raise KeyError(key)
        return self._load_value(res[0])
    
    def __contains__(self, key):
        cur = self._conn.cursor()
        cur.execute('''SELECT COUNT(*) FROM %s WHERE key = ?''' % self._table, (self._dump_key(key), ))
        res = cur.fetchone()
        return bool(res[0])
    
    def getmany(self, keys, *args):
        cur = self._conn.cursor()
        cur.execute('''SELECT key, value FROM %s WHERE key IN (%s)''' % (
            self._table, ','.join(['?'] * len(keys))), tuple(self._dump_key(x) for x in keys))
        map = dict(cur.fetchall())
        res = [self._load_value(map.get(x, *args)) for x in keys]
        return res
    
    def __delitem__(self, key):
        cur = self._conn.cursor()
        with self._conn:
            cur.execute('''DELETE FROM %s WHERE key = ?''' % self._table, (self._dump_key(key), ))
        if not cur.rowcount:
            raise KeyError(key)
    
    def clear(self):
        with self._conn:
            self._conn.execute('''DELETE FROM %s''' % self._table)

    def __len__(self):
        with self._conn:
            cur = self._conn.cursor()
            cur.execute('''SELECT count(*) FROM %s''' % self._table)
            return cur.fetchone()[0]

    def iteritems(self):
        cur = self._conn.cursor()
        cur.execute('''SELECT key, value FROM %s''' % self._table)
        for row in cur:
            yield self._load_key(row[0]), self._load_value(row[1])
    
    def __iter__(self):
        cur = self._conn.cursor()
        cur.execute('''SELECT key FROM %s''' % self._table)
        for row in cur:
            yield self._load_key(row[0])

    iterkeys = __iter__

    def itervalues(self):
        cur = self._conn.cursor()
        cur.execute('''SELECT value FROM %s''' % self._table)
        for row in cur:
            yield self._load_value(row[0])
    
    items = lambda self: list(self.iteritems())
    keys = lambda self: list(self.iterkeys())
    values = lambda self: list(self.itervalues())


def _is_reprable(key):
    t = type(key)
    return t in (int, str, unicode) or (t is tuple and all(
        _is_reprable(x) for x in key))

class PickleMap(LiteMap):
    """Value-pickling LiteMap.
    
    Keys may consist of strings, unicode, ints, and tuples (of these objects).
    We are using repr to serialize the key and we are assuming that it is
    deterministic.
    
    Because of the repr-ing, this does not have all of the same lookup
    behaviours of a normal dict. Ints and longs of the same value are not
    considered equal, nor are strings and unicode objects.
    
    We know this will not be deterministic across the 32/64bit platform
    boundary when using integers > 2**32.
    
    """
        
    @staticmethod
    def _dump_key(key):
        if not _is_reprable(key):
            raise ValueError('cannot deterministically serialize key %r' % key)
        return repr(key)
    
    _load_key = staticmethod(lambda x: ast.literal_eval(x))
    
    _dump_value = staticmethod(lambda x: buffer(pickle.dumps(x, protocol=-1)))
    _load_value = staticmethod(lambda x: pickle.loads(str(x)))


def test_thread_safe():
    
    import os
    from threading import Thread
    import random
    import time
    
    path = ':memory:'
    store = Litemap(path)
    
    def target():
        for i in xrange(100):
            items = [(os.urandom(5), os.urandom(10)) for j in xrange(5)]
            for k, v in items:
                store[k] = v
            for k, v in items:
                assert store[k] == v
    
    threads = [Thread(target=target) for i in xrange(5)]
    for x in threads:
        x.start()
    for x in threads:
        x.join()

    
    
if __name__ == '__main__':
    
    from time import clock as time
    # import bsddb
    import os
    
    store = PickleMap(':memory:')
    store.clear()
    
    start_time = time()
    
    store['key'] = 'whatever'
    print store['key']
    print 'key' in store
    print 'not' in store
    store[('tuple', 1)] = 'tuple_1'
    print store[('tuple', 1)]
    for i in range(100):
        key = os.urandom(5)
        value = os.urandom(10)
        store[key] = value
        assert store[key] == value, '%r != %r' % (repr(store[key]), value)
    
    print len(store)
    #     print store.keys()
    # print store.items()
    
    # test_thread_safe()
    
    print time() - start_time
