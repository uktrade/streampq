# streampq [![CircleCI](https://circleci.com/gh/uktrade/streampq.svg?style=shield)](https://circleci.com/gh/uktrade/streampq) [![Test Coverage](https://api.codeclimate.com/v1/badges/d96c6b7b6f8cf6ecfd9c/test_coverage)](https://codeclimate.com/github/uktrade/streampq/test_coverage)

Stream results of multi-statement PostgreSQL queries from Python without server-side cursors. Has benefits over some other Python PostgreSQL libraries:

- Streams results from complex multi-statement queries even though SQL doesn't allow server-side cursors for such queries - suitable for large amounts of results that don't fit in memory.

- CTRL+C (SIGINT) by default behaves as expected even during slow queries - a `KeyboardInterrupt` is raised and quickly bubbles up through streampq code. Unless client code prevents it, the program will exit.

- Every effort is made to cancel queries on `KeyboardInterrupt`, `SystemExit`, or errors - the server doesn't continue needlessly using resources.

Particularly useful when temporary tables are needed to store intermediate results in multi-statement SQL scripts, or when using a server-side cursor would make a query slower.


## Installation

```bash
pip install streampq
```

The `libpq` binary library is also required. This is typically either already installed, or installed by: 

- macOS + brew: `brew install libpq`
- Linux (Debian): `apt install libpq5`
- Linux (Red Hat):`yum install postgresql-libs`

The only runtime dependencies are `libpq` and Python itself.


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

# Connection and querying is via a context manager
with streampq_connect(connection_params) as query:
    for (columns, rows) in query(sql):
        print(columns)  # Tuple of column names (see below)

        for row in rows:
            print(row)  # Tuple of row values
```

### Chunked Pandas dataframes of SQL query results

You can create chunked Pandas dataframes of SQL query results, similar to the output of using the `chunksize` parameter of the Pandas `read_csv` function. This uses a helper function to do the chunking.

```python
import itertools
import pandas as pd
from streampq import streampq_connect

connection_params = (
    ('host', 'localhost'),
    ('port', '5432'),
    ('dbname', 'postgres'),
    ('user', 'postgres'),
    ('password', 'password'),
)

sql = '''
    SELECT * FROM my_table;
    SELECT * FROM my_other_table;
'''

def query_chunked_dfs(query, sql, chunk_size):

    def _chunked_df(columns, rows):
        it = iter(rows)
        while True:
            df = pd.DataFrame.from_records(itertools.islice(it, chunk_size), columns=columns)
            if len(df) == 0:
                break
            yield df

    for columns, rows in query(sql):
        yield _chunked_df(columns, rows)

with streampq_connect(connection_params) as query:
    for chunked_dfs in query_chunked_dfs(query, sql, chunk_size=10000):
        for df in chunked_dfs:
            print(df)
```

### Column names

Each column is an instance of streampq.Column, a subclass of str with two extra attributes:

- **type_id**

  The PostgresSQL type id of the column. For example 25 indicates text.

- **type_modifier**

  The PostgreSQL type modifier of the column. This is often -1 to indicate no modification.

While it's not typical to subclass str in Python, it's much more frequent to only need the column name for query results than the PostgreSQL type ID and modifier.

### PostgreSQL types to Python type decoding

There are [194 built-in PostgreSQL data types (including array types)](https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat), and streampq converts them to Python types. In summary:

| PostgreSQL types                                       | Python type                                                                                                   |
|:-------------------------------------------------------| :-------------------------------------------------------------------------------------------------------------|
| null                                                   | None                                                                                                          |
| text (e.g. varchar), xml, network addresses, and money | str                                                                                                           |
| byte (e.g. bytea)                                      | bytes                                                                                                         |
| integer (e.g. int4)                                    | int                                                                                                           |
| inexact real number (e.g. float4)                      | float                                                                                                         |
| exact real number (e.g. numeric)                       | Decimal                                                                                                       |
| date                                                   | date<br>(+ and - infinity are mapped to date.max and date.min respectively)                                   |
| timestamp                                              | datetime<br>(without timezone, + and - infinity are mapped to datetime.max and datetime.min respectively)     |
| timestamptz                                            | datetime<br>(with offset timezone, + and - infinity are mapped to datetime.max and datetime.min respectively) |
| json and jsonb                                         | output of json.loads                                                                                          |
| interval                                               | [streampq.Interval](#streampqinterval)                                                                        |
| range (e.g. daterange)                                 | [streampq.Range](#streampqrange)                                                                              |
| multirange (e.g. datemultirange)                       | tuples of [streampq.Range](#streampqrange)                                                                    |
| arrays and vectors                                     | tuple (of any of the above types, or of nested tuples)                                                        |

To customise these, override the default value of the `get_decoders` parameter of the `streampq_connect` function in [streampq.py](./streampq.py).

In general, built-in types are preferred over custom types, and immutable types are preferred over mutable.

#### streampq.Interval

The Python built-in timedelta type is not used for PostgreSQL interval since timedelta does not offer a way to store PostgreSQL intervals of years or months, other than converting to days which would be a loss of information.

Instead, a [namedtuple](https://docs.python.org/3/library/collections.html#collections.namedtuple) is defined, streampq.Interval, with members:

| Member  | Type    |
|:--------|:--------|
| years   | int     |
| months  | int     |
| days    | int     |
| hours   | int     |
| minutes | int     |
| seconds | Decimal |


#### streampq.Range

There is no Python built-in type for a PosgreSQL range. So for these, a [namedtuple](https://docs.python.org/3/library/collections.html#collections.namedtuple) is defined, streampq.Range, with members:

| Member  | Type                                                                       |
|:--------|:---------------------------------------------------------------------------|
| lower   | int, date, datetime (without timezone), or datetime (with offset timezone) |
| upper   | int, date, datetime (without timezone), or datetime (with offset timezone) |
| bounds  | str - one of `()`, `(]`, `[)`, or `[]`                                     |


### Bind parameters - literals

Dynamic SQL literals can be bound using the `literals` parameter of the query function. It must be an iterable of key-value pairs.

```python
sql = '''
    SELECT * FROM my_table WHERE my_col = {my_col_value};
'''

with streampq_connect(connection_params) as query:
    for (columns, rows) in query(sql, literals=(
        ('my_col_value', 'my-value'),
    )):
        for row in rows:
            pass
```


### Bind parameters - identifiers

Dynamic SQL identifiers, e.g. column names, can be bound using the `identifiers` parameter of the query function. It must be an iterable of key-value pairs.

```python
sql = '''
    SELECT * FROM my_table WHERE {column_name} = 'my-value';
'''

with streampq_connect(connection_params) as query:
    for (columns, rows) in query(sql, identifiers=(
        ('column_name', 'my_col'),
    )):
        for row in rows:
            pass
```

Identifiers and literals use different escaping rules - hence the need for 2 different parameters.


### Single-statement SQL queries

While this library is specialsed for multi-statement queries, it works fine when there is only one. In this case the iterable returned from the query function yields only a single `(columns, rows)` pair.


## Exceptions

Exceptions derive from `streampq.StreamPQError`. If there is any more information available on the error, it's added as a string in its `args` property. This is included in the string representation of the exception by default.


## Exception hierarchy

  - **StreamPQError**

    Base class for all explicitly-thrown exceptions

    - **ConnectionError**

      An error occurred while attempting to connect to the database.

    - **QueryError**

      An error occurred while attempting to run a query. Typically this is due to a syntax error or a missing column.

    - **CancelError**

      An error occurred while attempting to cancel a query.

    - **CommunicationError**

      An error occurred communicating with the database after successful connection.
