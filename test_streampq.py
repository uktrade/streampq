from datetime import date
from decimal import Decimal
from multiprocessing import Process, Event
from time import sleep
import signal
import uuid
import os

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
        ("1::int8", 1),
        ("'🍰'", '🍰'),
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
        _, rows = next(results)
        next(rows)
        with pytest.raises(Exception):
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


def run_query(params, sql, about_to_run_query, keyboard_interrupt_bubbled):
    try:
        with streampq_connect(params) as query:
            about_to_run_query.set()
            next(iter(query(sql)))
    except KeyboardInterrupt:
        keyboard_interrupt_bubbled.set()


def test_keyboard_interrupt(params):
    about_to_run_query = Event()
    keyboard_interrupt_bubbled = Event()

    unique_str = str(uuid.uuid4())
    sql = f'''
        SELECT '{unique_str}', pg_sleep(120)
    '''

    p = Process(target=run_query, args=(params, sql, about_to_run_query, keyboard_interrupt_bubbled))
    p.start()
    about_to_run_query.wait(timeout=60)

    # Try to make sure that the query is really running and we would be blocking in libpq
    # where keyboard interrupt wouldn't be responded to
    sleep(2)

    os.kill(p.pid, signal.SIGINT)
    keyboard_interrupt_bubbled.wait(timeout=2)
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
