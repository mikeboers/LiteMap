# encoding: utf8

from __future__ import print_function
import datetime
import hashlib

def derepr(x):
    if x in (True, False, None):
        return repr(x)
    t = type(x)
    if t is int:
        return repr(x)
    if t is long:
        return repr(x).rstrip('L')
    if t is bytes:
        return "'%s'" % x.encode('string-escape')
    if t is unicode:
        return "u'%s'" % x.encode('unicode-escape').replace("'", "\\'")
    if t is tuple:
        return '(%s)' % ', '.join(derepr(y) for y in x)
    if t is set:
        return 'set([%s])' % ', '.join(derepr(y) for y in x)
    # Below here is non-standard.
    if t is list:
        return '[%s]' % ', '.join(derepr(y) for y in x)
    if t is dict:
        return '{%s}' % ', '.join(sorted('%s: %s' % (derepr(k), derepr(v)) for (k, v) in x.iteritems()))
    raise TypeError('cannot serialize type %r' % t)

def dehash(x, hash=hashlib.sha256):
    return hash(derepr(x)).digest()
    


if __name__ == '__main__':
    for x in [
        0,
        1,
        2**32,
        2**64,
        'hello',
        u'hello',
        "single'quote",
        'double"quote',
        u"single'quote",
        u'double"quote',
        'both quotes \'"\0 and junk',
        u'both quotes \'"\0 and junk',
        u'¡™£¢∞§¶•ªº',
        '¡™£¢∞§¶•ªº',
        ''.join(map(chr, range(256))),
        u''.join(map(unichr, range(256))),
        (0, 1, 2),
        (),
        (0, 'hello', (0, 1, u'inner')),
        range(5),
        {'a': 1, 'b': 2, 'list': range(5), 'tuple': (1, 2, (3, 4, 'inner'))},
        True,
        False,
        None,
        (True, False, (1, 2, None)),
        ]:
            print(repr(x))
            print(derepr(x))
            print(dehash(x, hashlib.md5).encode('hex'))
            print()