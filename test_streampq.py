from datetime import date
from decimal import Decimal
from multiprocessing import Process, Event
from time import sleep
import signal
import uuid
import os
import sys

import pytest

from streampq import streampq_connect, ConnectionError, QueryError


@pytest.fixture
def params():
    yield (
        ('host', 'localhost'),
        ('port', '5432'),
        ('dbname', 'postgres'),
        ('user', 'postgres'),
        ('password', 'password'),
    )


def test_connection_error(params):
    with pytest.raises(ConnectionError, match='could not translate host name "does-not-exist" to address'):
        streampq_connect((('host', 'does-not-exist'),)).__enter__()



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


def test_literals(params):
    sql = '''
        SELECT {first} as "first", {second} as "second";
    '''
    with streampq_connect(params) as query:
        results = tuple(
            (cols, tuple(rows))
            for cols, rows in query(sql, literals=(
                ('first', '🍰'),
                ('second', 'an\'"other'),
            ))
        )

    assert results == (
        (('first', 'second'),
        (('🍰', 'an\'"other'),)),
    )


def test_identifiers(params):
    sql = '''
        SELECT 'first' as {first}, 'second' as {second};
    '''
    with streampq_connect(params) as query:
        results = tuple(
            (cols, tuple(rows))
            for cols, rows in query(sql, identifiers=(
                ('first', '🍰'),
                ('second', 'an\'"other'),
            ))
        )

    assert results == (
        (('🍰', 'an\'"other'),
        (('first', 'second'),)),
    )


def test_types(params):
    sql_to_python_mapping = (
        ("NULL", None),
        ("1", 1),
        ("1::int8", 1),
        ("'🍰'", '🍰'),
        ("3.3", Decimal('3.3')),
        ("'2021-01-01'::date", date(2021, 1, 1)),
        ("'{{\"a\":2}}'::jsonb", {'a': 2}),
        ("'{{\"b\":2}}'::json", {'b': 2}),
        ("'{{\"one \\\"and\",\"2\"}}'::text[]", ('one "and', '2')),
        ("'{{\"NULL\"}}'::text[]", ('NULL',)),
        ("'{{{{{{1,2}},{{1,2}}}},{{{{1,2}},{{1,2}}}}}}'::int4[]", (((1,2),(1,2)),((1,2),(1,2)))),
        ("'{{NULL}}'::int4[]", (None,)),
        ("NULL::int4[]", None),
        ("'{{}}'::int4[]", ()),
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
        with pytest.raises(QueryError, match='syntax error at or near "SELECTa"'):
            next(iter(query(sql)))


def test_missing_column(params):
    sql = '''
        SELECT 1;
        SELECT a;
    '''
    with streampq_connect(params) as query:
        results = iter(query(sql))
        _, rows = next(results)
        next(rows)
        with pytest.raises(QueryError, match='column "a" does not exist'):
            next(rows)


def test_large_query(params):
    string = '-' * 100_000_000
    sql = f'''
        SELECT '{string}'
    '''

    returned_string = None

    with streampq_connect(params) as query:
        for columns, rows in query(sql):
            for row in rows:
                returned_string = row[0]

    assert string == returned_string


def run_query(params, sql, about_to_run_query, exception_bubbled, exception_type):
    def sigterm_handler(_, __):
        sys.exit(0)
    signal.signal(signal.SIGTERM, sigterm_handler)

    try:
        with streampq_connect(params) as query:
            about_to_run_query.set()
            next(iter(query(sql)))
    except exception_type:
        exception_bubbled.set()


@pytest.mark.parametrize("signal_type,exception_type", [
    (signal.SIGINT, KeyboardInterrupt),
    (signal.SIGTERM, SystemExit),
])
def test_keyboard_interrupt(params, signal_type, exception_type):
    about_to_run_query = Event()
    exception_bubbled = Event()

    unique_str = str(uuid.uuid4())
    sql = f'''
        SELECT '{unique_str}', pg_sleep(120)
    '''

    p = Process(target=run_query, args=(params, sql, about_to_run_query, exception_bubbled, exception_type))
    p.start()
    about_to_run_query.wait(timeout=60)

    # Try to make sure that the query is really running and we would be blocking in libpq
    # where keyboard interrupt wouldn't be responded to
    sleep(2)

    os.kill(p.pid, signal_type)
    exception_bubbled.wait(timeout=2)
    p.join(timeout=2)

    sql = f'''
        SELECT count(*)
        FROM pg_stat_activity
        WHERE query LIKE '%{unique_str}%' AND pid != pg_backend_pid()
    '''

    count = None

    with streampq_connect(params) as query:
        for cols, rows in query(sql):
            for row in rows:
                count = row[0]

    assert count == 0
