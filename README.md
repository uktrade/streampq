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

connection_params = '...'
sql = '...'

with streampq_connect(connection_params) as query:
    for (columns, rows) in query(sql):
        for row in rows:
            pass

```
