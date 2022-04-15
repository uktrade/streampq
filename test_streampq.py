from datetime import date, datetime, timedelta, timezone
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


def test_multiple_queries_with_params_from_iterable(params):
    sql = '''
        SELECT 1 as "first";
        SELECT 2,'3';
    '''

    def params_it():
        yield from dict(params).items()

    with streampq_connect(params_it()) as query:
        results = tuple(
            (cols, tuple(rows))
            for cols, rows in query(sql)
        )

    assert results == (
        (('first',), ((1,),)),
        (('?column?','?column?'), ((2,'3'),)),
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


def test_literal_escaping(params):
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


def test_identifier_escaping(params):
    sql = '''
        SELECT 'first' as {first}, 'second' as {second}, 'third' as {third};
    '''
    with streampq_connect(params) as query:
        results = tuple(
            (cols, tuple(rows))
            for cols, rows in query(sql, identifiers=(
                ('first', '🍰'),
                ('second', 'an\'"other'),
                ('third', (1,2)),  # Would be very odd to have a column name like this
            ))
        )

    assert results == ((
        ('🍰', 'an\'"other', '(1, 2)'),
        (('first', 'second', 'third'),)
    ),)


@pytest.mark.parametrize("sql_value,python_value", [
    # No explicit types - so could depend on database defaults?
    ("NULL", None),
    ("TRUE", True),
    ("FALSE", False),
    ("1", 1),
    ("3.3", Decimal('3.3')),
    ("'🍰'", '🍰'),
    # With explicit types - using _type rather than type[] for array
    # since that seems to use the right array type oid
    ("'TRUE'::bool", True),
    ("'{{TRUE,FALSE}}'::_bool", (True, False)),
    ("'\\xDE00BEEF'::bytea", b'\xde\x00\xbe\xef'),
    ("'{{\"\\\\xDE00BEEF\"}}'::_bytea", (b'\xde\x00\xbe\xef',)),
    ("'a'::char", 'a'),
    ("'{{a}}'::_char", ('a',)),
    ("'a'::name", 'a'),
    ("'{{a}}'::_name", ('a',)),
    ("1::int8", 1),
    ("'{{1}}'::_int8", (1,)),
    ("1::int2", 1),
    ("'{{1}}'::_int2", (1,)),
    ("'1 2'::int2vector", (1,2)),
    ("'{{\"1 2\",\"3 4\"}}'::_int2vector", ((1,2),(3,4))),
    ("'boolin'::regproc", 'boolin'),
    ("'{{boolin}}'::_regproc", ('boolin',)),
    ("1::int4", 1),
    ("'{{{{{{1,2}},{{1,2}}}},{{{{1,2}},{{1,2}}}}}}'::_int4", (((1,2),(1,2)),((1,2),(1,2)))),
    ("'{{NULL}}'::_int4", (None,)),
    ("NULL::_int4", None),
    ("'{{}}'::_int4", ()),
    ("'🍰'::text", '🍰'),
    ("'{{\"one \\\"and\",\"2\"}}'::_text", ('one "and', '2')),
    ("'{{\"NULL\"}}'::_text", ('NULL',)),
    ("'1'::oid", 1),
    ("'{{1}}'::_oid", (1,)),
    ("'(1,2)'::tid", (1,2)),
    ("'{{\"(1,2)\",\"(3,4)\"}}'::_tid", ((1,2),(3,4))),
    ("'5'::xid", 5),
    ("'{{5}}'::_xid", (5,)),
    ("'5'::cid", 5),
    ("'{{5}}'::_cid", (5,)),
    ("'1 2'::oidvector", (1,2)),
    ("'{{\"1 2\",\"3 4\"}}'::_oidvector", ((1,2),(3,4))),
    ("1.5::float4", 1.5),
    ("'{{1.5}}'::_float4", (1.5,)),
    ("1.5::float8", 1.5),
    ("'{{1.5}}'::_float8", (1.5,)),
    ("'{{\"b\":2}}'::json", {'b': 2}),
    ("'🍰'::varchar", '🍰'),
    ("'{{\"one \\\"and\",\"2\"}}'::_varchar", ('one "and', '2')),
    ("'2021-01-01'::date", date(2021, 1, 1)),
    ("'{{\"2021-01-01\"}}'::_date", (date(2021, 1, 1),)),
    ("'2021-01-01 10:10:01'::timestamp", datetime(2021, 1, 1, 10, 10, 1)),
    ("'{{\"2021-01-01 10:10:01\"}}'::_timestamp", (datetime(2021, 1, 1, 10, 10, 1),)),
    ("'2021-01-01 05:10:01'::timestamptz", datetime(2021, 1, 1, 5, 10, 1, tzinfo=timezone(timedelta(hours=-10)))),
    ("'{{\"2021-01-01 05:10:01\"}}'::_timestamptz", (datetime(2021, 1, 1, 5, 10, 1, tzinfo=timezone(timedelta(hours=-10))),)),
    ("1.2::numeric", Decimal('1.2')),
    ("'{{1.2}}'::_numeric", (Decimal('1.2'),)),
    ("'{{\"a\":2}}'::jsonb", {'a': 2}),
    ("'{{\\{{\\\"a\\\":2\\}}}}'::_jsonb", ({'a': 2},)),
])
def test_decoders(params, sql_value, python_value):
    with streampq_connect(params) as query:
        list(query("SET TIMEZONE TO -10"))

        result = tuple(
            tuple(rows)[0][0]
            for cols, rows in query(f'SELECT {sql_value}')
        )[0]

    assert result == python_value


@pytest.mark.parametrize("python_value,sql_value_as_python", [
    (None, None),
    (True, True),
    (False, False),
    ('A string', 'A string'),
    (1, 1),
    (3.3, Decimal('3.3')),
    (Decimal('3.3'), Decimal('3.3')),
    (date(2021, 1, 1), '2021-01-01'),
    (datetime(2021, 1, 1), '2021-01-01 00:00:00'),
    (Exception, "<class 'Exception'>"),  # No encoder specified so defaults to str
    (('a','b'), ('a','b')),
    ((('a"','\\b'),('a','b')), (('a"','\\b'),('a','b'))),
    ((True, False), (True,False)),
])
def test_encoders(params, python_value, sql_value_as_python):
    with streampq_connect(params) as query:
        result = tuple(
            tuple(rows)[0][0]
            for cols, rows in query('SELECT {value} as "col"', literals=(
                ('value', python_value),
            ))
        )[0]

    assert result == sql_value_as_python


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


def test_temporary_table(params):
    sql = '''
        CREATE TEMPORARY TABLE my_temp_table AS SELECT a, b
        FROM (
            values ('foo', 1), ('bar',2 ), ('baz', 3)
        ) s(a,b);
        SELECT * FROM my_temp_table;
    '''

    with streampq_connect(params) as query:
        results = tuple(
            (cols, tuple(rows))
            for cols, rows in query(sql)
        )

    assert results == (
        (
            ('a', 'b'),
            (('foo', 1), ('bar', 2), ('baz', 3)),
        ),
    )


def test_empty_queries_among_others(params):
    sql = '''
        ;;
        SELECT 1 as "col"
        ;; ;;;
        SELECT 2 as "col"
    '''

    with streampq_connect(params) as query:
        results = tuple(
            (cols, tuple(rows))
            for cols, rows in query(sql)
        )

    assert results == (
        (('col',), ((1,),)),
        (('col',), ((2,),)),
    )


def test_series(params):
    sql = '''
        SELECT * FROM generate_series(1,10000000);
    '''

    count = 0

    with streampq_connect(params) as query:
        for cols, rows in query(sql):
            for row in rows:
                count += 1

    assert count == 10000000


def test_notice(params):
    notice = f'NOTICE--' * 10000
    sql = f'''
        CREATE OR REPLACE FUNCTION r(i integer) RETURNS integer AS $$
        BEGIN
            FOR i IN 1..10000 LOOP
                RAISE notice '{notice}';
            END LOOP;
            RETURN i + 1;
        END;
        $$ LANGUAGE plpgsql;
        SELECT r(1);
        SELECT * FROM generate_series(1,100000);
    '''

    count = 0

    with streampq_connect(params) as query:
        for cols, rows in query(sql):
            for row in rows:
                count += 1

    assert count == 100001


def test_sending_notify_from_another_connection(params):
    string = '-' * 100_000_000
    sql = f'''
        SELECT '{string}'
    '''

    with \
            streampq_connect(params) as query_1, \
            streampq_connect(params) as query_2:

        list(query_1('LISTEN channel'))
        for _ in range(0, 10000):
            list(query_2(f"NOTIFY channel, '{'-' * 1000}'"))

        for columns, rows in query_1(sql):
            for row in rows:
                returned_string = row[0]

    assert string == returned_string


def test_incomplete_iteration_same_query(params):
    # Not completely sure if this is desired or not - if calling
    # code doesn't complete iteration of results of an individual
    # statment, then the groupby under the hood seems to continue
    # to iterate through all its remaining results, which means
    # results continue to be fetched from the server

    sql = '''
        SELECT a, b
        FROM (
            values ('foo', 1), ('bar',2 )
        ) s(a,b);
        SELECT a
        FROM (
            values ('foo'), ('bar')
        ) s(a);
    '''

    count = 0
    with streampq_connect(params) as query:
        all_results = query(sql)
        next(all_results)
        cols, rows = next(all_results)
        second_query_results = tuple(rows)

    assert second_query_results == (('foo',), ('bar',))


def test_incomplete_iteration_different_query(params):
    # Not completely sure if this is desired or not - should the
    # existing command be cancelled and the new one allowed to run?

    sql = '''
        SELECT a, b
        FROM (
            values ('foo', 1), ('bar',2 )
        ) s(a,b);
    '''

    count = 0
    with streampq_connect(params) as query:
        all_results = query(sql)
        cols, rows = next(all_results)
        next(rows)

        with pytest.raises(QueryError, match='another command is already in progress'):
            query(sql)
