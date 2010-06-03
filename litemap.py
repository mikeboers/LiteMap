# encoding: utf8

import os
import sqlite3
import cPickle as pickle
import collections
import threading
import hashlib

import dehash


class LiteMap(collections.MutableMapping):
    """Persistant mapping class backed by SQLite."""
    
    def __init__(self, path, table='__bucket__'):
        self._path = os.path.abspath(os.path.expanduser(path))
        self._table = self._escape(table)
        self._local = threading.local()
        
        with self._conn:
            cur = self._conn.cursor()
            cur.execute('''CREATE TABLE IF NOT EXISTS %s (
                hash  BLOB UNIQUE ON CONFLICT REPLACE,
                key   BLOB,
                value BLOB
            )''' % self._table)
            index_name = self._escape(table + '_index')
            cur.execute('''CREATE INDEX IF NOT EXISTS %s on %s (hash)''' % (index_name, self._table))
    
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
    _hash_method = hashlib.md5
    _hash = classmethod(lambda cls, x: buffer(dehash.dehash(x, cls._hash_method)))
    _dump = staticmethod(lambda x: buffer(pickle.dumps(x, protocol=-1)))
    _load = staticmethod(lambda x: pickle.loads(str(x)))
    
    def setmany(self, items):
        with self._conn:
            self._conn.executemany('''INSERT INTO %s VALUES (?, ?, ?)''' % self._table, (
                (self._hash(key), self._dump(key), self._dump(value)) for key, value in items
            ))
    
    def __setitem__(self, key, value):
        self.setmany([(key, value)])

    def __getitem__(self, key):
        cur = self._conn.cursor()
        cur.execute('''SELECT value FROM %s WHERE hash = ?''' % self._table, (self._hash(key), ))
        res = cur.fetchone()
        if not res:
            raise KeyError(key)
        return self._load(res[0])
    
    def __contains__(self, key):
        cur = self._conn.cursor()
        cur.execute('''SELECT COUNT(*) FROM %s WHERE hash = ?''' % self._table, (self._hash(key), ))
        res = cur.fetchone()
        return bool(res[0])
    
    # def getmany(self, keys, *args):
    #     cur = self._conn.cursor()
    #     cur.execute('''SELECT key, value FROM %s WHERE key IN (%s)''' % (
    #         self._table, ','.join(['?'] * len(keys))), tuple(self._dump_key(x) for x in keys))
    #     map = dict(cur.fetchall())
    #     res = [self._load(map.get(x, *args)) for x in keys]
    #     return res
    
    def __delitem__(self, key):
        cur = self._conn.cursor()
        with self._conn:
            cur.execute('''DELETE FROM %s WHERE hash = ?''' % self._table, (self._hash(key), ))
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
            yield self._load(row[0]), self._load(row[1])
    
    def __iter__(self):
        cur = self._conn.cursor()
        cur.execute('''SELECT key FROM %s''' % self._table)
        for row in cur:
            yield self._load(row[0])

    iterkeys = __iter__

    def itervalues(self):
        cur = self._conn.cursor()
        cur.execute('''SELECT value FROM %s''' % self._table)
        for row in cur:
            yield self._load(row[0])
    
    items = lambda self: list(self.iteritems())
    keys = lambda self: list(self.iterkeys())
    values = lambda self: list(self.itervalues())




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
    
    store = LiteMap('~/Desktop/test.sqlite')
    
    start_time = time()
    
    store['key'] = 'whatever'
    assert store['key'] == 'whatever'
    assert 'key' in store
    del store['key']
    assert 'key' not in store
    try:
        del store['key']
    except KeyError:
        pass
    else:
        assert False
    
    for i, word in enumerate('this is a sequence of words'.split()):
        store[('word', i)] = word
    
    assert 'not' not in store
    
    store[('tuple', 1)] = 'tuple_1'
    assert store[('tuple', 1)] == 'tuple_1'
    
    for i in range(100):
        key = os.urandom(5)
        value = os.urandom(10)
        store[key] = value
        assert store[key] == value, '%r != %r' % (repr(store[key]), value)
    
    print time() - start_time
