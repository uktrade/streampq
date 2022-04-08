# streampq [![CircleCI](https://circleci.com/gh/uktrade/streampq.svg?style=shield)](https://circleci.com/gh/uktrade/streampq) [![Test Coverage](https://api.codeclimate.com/v1/badges/d96c6b7b6f8cf6ecfd9c/test_coverage)](https://codeclimate.com/github/uktrade/streampq/test_coverage)

Stream results of multi-statement PostgreSQL queries from Python without a server-side cursor.

For complex situations where multiple statements are needed, but also where results are too big to store in memory at once. Existing Python PostgreSQL drivers don't seem to handle this case well - typically they require a server-side cursor, but SQL syntax doesn't allow a server-side cursor to "wrap" multiple SQL statements. This library side steps that problem by using libpq's _single-row mode_.

CTRL+C is respected during queries: even in the middle of a slow query, a KeyboardInterrupt is raised and bubbles up as is typical in Python programs. Other Python PostgreSQL drivers don't do this - CTRL+C can appear to be ignored. This is done by using libpq's _Asynchronous Command Processing_ to wait for query results using Python's [selectors](https://docs.python.org/3/library/selectors.html) module rather than in blocking libpq functions.


## Installation

```bash
pip install streampq
```

The `libpq` binary library is also required. This is typically either already installed, or installed by: 

- macOS + brew: `brew install libpq`
- Linux (Debian): `apt install libpq5`
- Linux (Red Hat):`yum install postgresql-libs`


## Usage

```python
from streampq import streampq_connect

# libpq connection paramters
# https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
#
# Any can be ommitted and environment variables will be used instead
# https://www.postgresql.org/docs/current/libpq-envars.html
connection_params = (
    ('host', 'localhost'),
    ('port', '5432'),
    ('dbname', 'postgres'),
    ('user', 'postgres'),
    ('password', 'password'),
)

# SQL statement(s) - if more than one, separate by ;
sql = '''
SELECT * FROM my_table;
SELECT * FROM my_other_table;
'''

with streampq_connect(connection_params) as query:
    for (columns, rows) in query(sql):
        for row in rows:
            pass
```
