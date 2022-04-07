# streampq

Stream results of multi statement PostgreSQL queries from Python

> Work in progress. This README serves as a rough design spec


## Installation

```bash
pip install streampq
```

The `libpq` binary library is also required.


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
