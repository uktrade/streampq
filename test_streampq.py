from datetime import date
from decimal import Decimal

import pytest

from streampq import streampq_connect


@pytest.fixture
def params():
    yield (
        ('host', 'localhost'),
        ('port', '5432'),
        ('dbname', 'postgres'),
        ('user', 'postgres'),
        ('password', 'password'),
    )


def test_streampy(params):
    sql = '''
        SELECT 1 as "first";
        SELECT NULL,1,'2',3.3,'2021-01-01'::date,'{"a":2}'::jsonb,'{"a":2}'::json;
    '''
    with streampq_connect(params) as query: 
        results = [
            (cols, list(rows))
            for cols, rows in query(sql)
        ]

    assert results[0][0] == ('first',)
    assert results[0][1] == [(1,)]
    assert results[1][0] == ('?column?',) * 4 + ('date',) + ('jsonb',) + ('json',)
    assert results[1][1] == [(None, 1, '2', Decimal('3.3'), date(2021, 1, 1), {'a': 2}, {'a': 2})]
