# streampq [![CircleCI](https://circleci.com/gh/uktrade/streampq.svg?style=shield)](https://circleci.com/gh/uktrade/streampq) [![Test Coverage](https://api.codeclimate.com/v1/badges/d96c6b7b6f8cf6ecfd9c/test_coverage)](https://codeclimate.com/github/uktrade/streampq/test_coverage)

Stream results of multi-statement PostgreSQL queries from Python. Has these benefits over other Python PostgreSQL drivers:

- Streams results from complex multi-statement queries even though SQL doesn't allow server-side cursors for such queries - suitable for large amounts of results that don't fit in memory.

- CTRL+C behaves as expected during queries - a `KeyboardInterrupt` is raised.

- Every effort is made to cancel running queries on CTRL+C or errors - so the server doesn't continue needlessly using resources.


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
