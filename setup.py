
from distutils.core import setup

setup(
    name='LiteMap',
    version='0.2.1',
    description='Mapping class which stores in SQLite database.',
    url='http://github.com/mikeboers/LiteMap',
    py_modules=['litemap'],
    
    author='Mike Boers',
    author_email='litemap@mikeboers.com',
    license='BSD-3',
    
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
