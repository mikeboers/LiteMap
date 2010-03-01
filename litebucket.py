# encoding: utf8

import sqlite3
import cPickle as pickle
import collections
import base64
import threading

class Bucket(collections.MutableMapping):
    """Persistant mapping class.
    
    The keys and values will be truncated to null bytes. If you need a binary
    safe Bucket, use the BinaryBucket or PickleBucket.
    
    """
    
    def __init__(self, path, name='keystore'):
        self._path = path
        self._table = "'%s'" % name.replace("'", "''")
        self._local = threading.local()
        self._create_table()
    
    @property
    def _conn(self):
        conn = getattr(self._local, 'conn', None)
        if not hasattr(self._local, 'conn'):
            self._local.conn = sqlite3.connect(self._path)
            self._local.conn.text_factory = str
        return self._local.conn
        
    def _create_table(self):    
        with self._conn:
            cur = self._conn.cursor()
            cur.execute('''CREATE TABLE IF NOT EXISTS %s (
                key   BLOB UNIQUE ON CONFLICT REPLACE,
                value BLOB
            )''' % self._table)
            cur.execute('''CREATE INDEX IF NOT EXISTS key_index on %s (key)''' % self._table)
    
    @staticmethod
    def _dump_key(key):
        if not isinstance(key, str):
            raise TypeError('key must be str')
        return key
    _load_key = staticmethod(lambda x: x)
    _dump_value = _dump_key
    _load_value = _load_key
    
    def setmany(self, items):
        with self._conn:
            self._conn.executemany('''INSERT INTO %s VALUES (?, ?)''' % self._table, (
                (self._dump_key(key), self._dump_value(value)) for key, value in items
            ))
    def __setitem__(self, key, value):        
        # print '__setitem__', repr(key), repr(value)
        self.setmany([(key, value)])

    def __getitem__(self, key):
        cur = self._conn.cursor()
        cur.execute('''SELECT value FROM %s WHERE key = ?''' % self._table, (self._dump_key(key), ))
        res = cur.fetchone()
        if not res:
            raise KeyError(key)
        # print '__getitem__', repr(key), repr(res[0])
        return self._load_value(res[0])
    
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


class BinaryBucket(Bucket):
    """Binary safe Bucket."""
    _dump_value = staticmethod(lambda x: base64.b64encode(x))
    _load_value = staticmethod(lambda x: base64.b64decode(x))
    
class PickleBucket(Bucket):
    """Value-pickling Bucket."""
    _dump_value = staticmethod(lambda x: base64.b64encode(pickle.dumps(x, protocol=-1)))
    _load_value = staticmethod(lambda x: pickle.loads(base64.b64decode(x)))


def test_thread_safe():
    
    import os
    from threading import Thread
    import random
    import time
    
    path = '/tmp/keystore_test.sqlite'
    store = BinaryBucket(path)
    
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
    import bsddb
    
    store = PickleBucket('testing.sqlite')
    store.clear()
    
    start_time = time()
    
    # for i in range(1000):
    #         store[str(i)] = str(i ** 2)
    # store.setmany((str(i), str(i**2)) for i in range(1000))
    store['key'] = 'whatever'
    print store['key']
    
    for i in range(10):
        store[str(i)] = str(i)
        print store[str(i)]
    
    print len(store)
    #     print store.keys()
    #     print store.items()
    
    # test_thread_safe()
    
    print time() - start_time
