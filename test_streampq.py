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


def test_multiple_queries(params):
    sql = '''
        SELECT 1 as "first";
        SELECT 2,'3';
    '''
    with streampq_connect(params) as query: 
        results = tuple(
            (cols, tuple(rows))
            for cols, rows in query(sql)
        )

    assert results == (
        (('first',), ((1,),)),
        (('?column?','?column?'), ((2,'3'),)),
    )


def test_types(params):
    sql_to_python_mapping = (
        ("NULL", None),
        ("1", 1),
        ("'2'", '2'),
        ("3.3", Decimal('3.3')),
        ("'2021-01-01'::date", date(2021, 1, 1)),
        ("'{\"a\":2}'::jsonb", {'a': 2}),
        ("'{\"b\":2}'::json", {'b': 2}),
    )
    with streampq_connect(params) as query:
        results = tuple(
            tuple(rows)[0][0]
            for sql_value, _ in sql_to_python_mapping
            for cols, rows in query(f'SELECT {sql_value}')
        )

    assert results == tuple(
        python_value for _, python_value in sql_to_python_mapping
    )


def test_syntax_error(params):
    sql = '''
        SELECT 1;
        SELECTa;
    '''
    with streampq_connect(params) as query:
        with pytest.raises(Exception):
            next(iter(query(sql)))


def test_missing_column(params):
    sql = '''
        SELECT 1;
        SELECT a;
    '''
    with streampq_connect(params) as query:
        results = iter(query(sql))
        cols, rows = next(results)
        next(rows)
        with pytest.raises(Exception):
            next(rows)
