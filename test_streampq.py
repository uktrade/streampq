from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from contextlib import contextmanager
from ctypes import cdll
import itertools
from multiprocessing import Process, Event
from multiprocessing.synchronize import Event as EventClass
from time import sleep
import signal
import uuid
import os
import sys
from types import FrameType
from typing import Type, TypeVar, Optional, Iterable, Tuple, Any

import pandas as pd
import pytest

from streampq import streampq_connect, Interval, Range, StreamPQError, ConnectionError, QueryError, Query


@pytest.fixture
def params() -> Iterable[Iterable[Tuple[str, str]]]:
    yield (
        ('host', 'localhost'),
        ('port', '5432'),
        ('dbname', 'postgres'),
        ('user', 'postgres'),
        ('password', 'password'),
    )


def test_connection_error(params: Iterable[Tuple[str, str]]) -> None:
    with pytest.raises(ConnectionError, match='could not translate host name "does-not-exist" to address'):
        streampq_connect((('host', 'does-not-exist'),)).__enter__()


def test_multiple_queries_with_params_from_iterable(params: Iterable[Tuple[str, str]]) -> None:
    sql = '''
        SELECT 1 as "first";
        SELECT 2,'3';
    '''

    def params_it() -> Iterable[Tuple[str, str]]:
        yield from dict(params).items()

    with streampq_connect(params_it()) as query:
        results = tuple(
            (cols, tuple(rows))
            for cols, rows in query(sql)
        )

    assert results == (
        (('first',), ((1,),)),
        (('?column?', '?column?'), ((2,'3'),)),
    )


def test_multiple_queries(params: Iterable[Tuple[str, str]]) -> None:
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
        (('?column?', '?column?'), ((2,'3'),)),
    )


def test_literal_escaping(params: Iterable[Tuple[str, str]]) -> None:
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


def test_identifier_escaping(params: Iterable[Tuple[str, str]]) -> None:
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
    ("'{{\"b\":2}}'::json", {'b': 2}),
    ("'<a>b</a>'::xml", '<a>b</a>'),
    ("ARRAY['<a>b</a>']::_xml", ('<a>b</a>',)),
    ("'1.2.0.0/24'::cidr", '1.2.0.0/24'),
    ("ARRAY['1.2.0.0/24']::_cidr", ('1.2.0.0/24',)),
    ("1.5::float4", 1.5),
    ("'{{1.5}}'::_float4", (1.5,)),
    ("1.5::float8", 1.5),
    ("'{{1.5}}'::_float8", (1.5,)),
    ("'08:00:2b:01:02:03:04:05'::macaddr8", '08:00:2b:01:02:03:04:05'),
    ("ARRAY['08:00:2b:01:02:03:04:05']::_macaddr8", ('08:00:2b:01:02:03:04:05',)),
    ("1.2::money", '$1.20'),
    ("'{{1.2}}'::_money", ('$1.20',)),
    ("'08:00:2b:01:02:03'::macaddr", '08:00:2b:01:02:03'),
    ("ARRAY['08:00:2b:01:02:03']::_macaddr", ('08:00:2b:01:02:03',)),
    ("'1.2.0.0/24'::inet", '1.2.0.0/24'),
    ("ARRAY['1.2.0.0/24']::_inet", ('1.2.0.0/24',)),
    ("'🍰'::varchar", '🍰'),
    ("'{{\"one \\\"and\",\"2\"}}'::_varchar", ('one "and', '2')),
    ("'2021-01-01'::date", date(2021, 1, 1)),
    ("'-infinity'::date", date.min),
    ("'infinity'::date", date.max),
    ("'{{\"2021-01-01\"}}'::_date", (date(2021, 1, 1),)),
    ("'2021-01-01 10:10:01'::timestamp", datetime(2021, 1, 1, 10, 10, 1)),
    ("'-infinity'::timestamp", datetime.min),
    ("'infinity'::timestamp", datetime.max),
    ("'{{\"2021-01-01 10:10:01\"}}'::_timestamp", (datetime(2021, 1, 1, 10, 10, 1),)),
    ("'2021-01-01 05:10:01'::timestamptz", datetime(2021, 1, 1, 5, 10, 1, tzinfo=timezone(timedelta(hours=-10)))),
    ("'-infinity'::timestamptz",  datetime.min.replace(tzinfo=timezone(timedelta()))),
    ("'infinity'::timestamptz", datetime.max.replace(tzinfo=timezone(timedelta()))),
    ("'{{\"2021-01-01 05:10:01\"}}'::_timestamptz", (datetime(2021, 1, 1, 5, 10, 1, tzinfo=timezone(timedelta(hours=-10))),)),
    ("interval '1 year 3 months 5 weeks 3 days 1 hours 2 minutes 6.3 seconds ago'", Interval(years=-1, months=-3, days=-38, hours=-1, minutes=-2, seconds=Decimal('-6.3'))),
    ("interval '1 year ago'", Interval(years=-1)),
    ("interval '2 minutes ago'", Interval(minutes=-2)),
    ("ARRAY[interval '2 minutes ago']", (Interval(minutes=-2),)),
    ("1.2::numeric", Decimal('1.2')),
    ("'{{1.2}}'::_numeric", (Decimal('1.2'),)),
    ("'{{\"a\":2}}'::jsonb", {'a': 2}),
    ("'{{\\{{\\\"a\\\":2\\}}}}'::_jsonb", ({'a': 2},)),
    ("'[1,2]'::int4range", Range(1,3,'[)')),
    ("ARRAY['[1,2]']::_int4range", (Range(1,3,'[)'),)),
    ("'[1,2]'::numrange", Range(Decimal('1'),Decimal('2'),'[]')),
    ("ARRAY['[1,2]']::_numrange", (Range(Decimal('1'),Decimal('2'),'[]'),)),
    ("'[2021-01-01,2021-01-04)'::tsrange", Range(datetime(2021, 1, 1),datetime(2021, 1, 4),'[)')),
    ("ARRAY['[2021-01-01,2021-01-04)']::_tsrange", (Range(datetime(2021, 1, 1),datetime(2021, 1, 4),'[)'),)),
    ("'[2021-01-01,2021-01-04)'::tstzrange", Range(datetime(2021, 1, 1, tzinfo=timezone(timedelta(hours=-10))),datetime(2021, 1, 4, tzinfo=timezone(timedelta(hours=-10))),'[)')),
    ("ARRAY['[2021-01-01,2021-01-04)']::_tstzrange", (Range(datetime(2021, 1, 1, tzinfo=timezone(timedelta(hours=-10))),datetime(2021, 1, 4, tzinfo=timezone(timedelta(hours=-10))),'[)'),)),
    ("'[2021-01-01,2021-01-04)'::daterange", Range(date(2021, 1, 1),date(2021, 1, 4),'[)')),
    ("ARRAY['[2021-01-01,2021-01-04)']::_daterange", (Range(date(2021, 1, 1),date(2021, 1, 4),'[)'),)),
    ("'[1,2]'::int8range", Range(1,3,'[)')),
    ("ARRAY['[1,2]']::_int8range", (Range(1,3,'[)'),)),
])
def test_decoders(params: Iterable[Tuple[str, str]], sql_value: str, python_value: Any) -> None:
    with streampq_connect(params) as query:
        list(query("SET TIMEZONE TO -10"))

        result = tuple(
            tuple(rows)[0][0]
            for cols, rows in query(f'SELECT {sql_value}')
        )[0]

    assert result == python_value


@pytest.mark.parametrize("sql_value,python_value", [
    ("interval '1 year ago'", Interval(years=-1)),
    ("interval '2 minutes ago'", Interval(minutes=-2)),
])
def test_interval_decoders_no_sign_seconds(params: Iterable[Tuple[str, str]], sql_value: str, python_value: Any) -> None:
    # It's a bit easy to get Decimal('-0') which is as "right" as  Decimal('0'), but usually not expected
    with streampq_connect(params) as query:
        result = tuple(
            tuple(rows)[0][0]
            for cols, rows in query(f'SELECT {sql_value}')
        )[0]

    assert result == python_value
    assert not result.seconds.is_signed()


@pytest.mark.skipif(float(os.environ.get('POSTGRES_VERSION', '0')) < 14.0, reason='multiranges not available before PostgreSQL 14')
@pytest.mark.parametrize("sql_value,python_value", [
    ("'{{[1,2],[5,6]}}'::int4multirange", (Range(1,3,'[)'),Range(5,7,'[)'))),
    ("ARRAY['{{[1,2]}}']::_int4multirange", ((Range(1,3,'[)'),),)),
    ("'{{[1,2]}}'::nummultirange", (Range(Decimal('1'),Decimal('2'),'[]'),)),
    ("ARRAY['{{[1,2]}}']::_nummultirange", ((Range(Decimal('1'),Decimal('2'),'[]'),),)),
    ("'{{[2021-01-01,2021-01-04)}}'::tsmultirange", (Range(datetime(2021, 1, 1),datetime(2021, 1, 4),'[)'),)),
    ("ARRAY['{{[2021-01-01,2021-01-04)}}']::_tsmultirange", ((Range(datetime(2021, 1, 1),datetime(2021, 1, 4),'[)'),),)),
    ("'{{[2021-01-01,2021-01-04)}}'::tstzmultirange", (Range(datetime(2021, 1, 1, tzinfo=timezone(timedelta(hours=-10))),datetime(2021, 1, 4, tzinfo=timezone(timedelta(hours=-10))),'[)'),)),
    ("ARRAY['{{[2021-01-01,2021-01-04)}}']::_tstzmultirange", ((Range(datetime(2021, 1, 1, tzinfo=timezone(timedelta(hours=-10))),datetime(2021, 1, 4, tzinfo=timezone(timedelta(hours=-10))),'[)'),),)),
    ("'{{[2021-01-01,2021-01-04),[2022-01-01,2022-01-04)}}'::datemultirange", (Range(date(2021, 1, 1),date(2021, 1, 4),'[)'),Range(date(2022, 1, 1),date(2022, 1, 4),'[)'))),
    ("ARRAY['{{[2021-01-01,2021-01-04)}}']::_datemultirange", ((Range(date(2021, 1, 1),date(2021, 1, 4),'[)'),),)),
    ("'{{[1,2],[1,2]}}'::int8multirange", (Range(1,3,'[)'),)),
    ("ARRAY['{{[1,2]}}']::_int8multirange", ((Range(1,3,'[)'),),)),
])
def test_multirange_decoders(params: Iterable[Tuple[str, str]], sql_value: str, python_value: Any) -> None:
    with streampq_connect(params) as query:
        list(query("SET TIMEZONE TO -10"))

        result = tuple(
            tuple(rows)[0][0]
            for cols, rows in query(f'SELECT {sql_value}')
        )[0]

    assert result == python_value


def test_bootstreap_rowtypes(params: Iterable[Tuple[str, str]]) -> None:
    # We only check that an exception isn't raised and that most likely
    # We don't decode the rowtype data itself since not sure on use case,
    # and so don't assert on more since not sure there is a use case
    with streampq_connect(params) as query:
        result = tuple(
            tuple(rows)
            for cols, rows in query("""
                SELECT pg_type FROM pg_type;
                SELECT pg_attribute FROM pg_attribute;
                SELECT pg_class FROM pg_class;
                SELECT pg_proc FROM pg_proc;
            """)
        )
    for i in range(0, 4):
        assert isinstance(result[i][0], tuple)
        assert isinstance(result[i][0][0], str)


@pytest.mark.skipif(float(os.environ.get('POSTGRES_VERSION', '0')) < 14.0, reason='rowtype arrays not available before PostgreSQL 14')
def test_bootstreap_rowtypes_arrays(params: Iterable[Tuple[str, str]]) -> None:
    # We only check that an exception isn't raised and that most likely
    # the array types are decoded properly into tuples. We don't decode
    # the rowtype data itself, and so don't assert on more since not sure
    # there is a use case
    with streampq_connect(params) as query:
        result = tuple(
            tuple(rows)
            for cols, rows in query("""
                SELECT ARRAY[pg_type] FROM pg_type;
                SELECT ARRAY[pg_attribute] FROM pg_attribute;
                SELECT ARRAY[pg_class] FROM pg_class;
                SELECT ARRAY[pg_proc] FROM pg_proc;
            """)
        )
    for i in range(0, 4):
        assert isinstance(result[i][0][0], tuple)
        assert isinstance(result[i][0][0][0], str)


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
def test_encoders(params: Iterable[Tuple[str, str]], python_value: Any, sql_value_as_python: Any) -> None:
    with streampq_connect(params) as query:
        result = tuple(
            tuple(rows)[0][0]
            for cols, rows in query('SELECT {value} as "col"', literals=(
                ('value', python_value),
            ))
        )[0]

    assert result == sql_value_as_python


def test_syntax_error(params: Iterable[Tuple[str, str]]) -> None:
    sql = '''
        SELECT 1;
        SELECTa;
    '''
    with streampq_connect(params) as query:
        with pytest.raises(QueryError, match='syntax error at or near "SELECTa"') as exc_info:
            next(iter(query(sql)))

    error_fields = dict(exc_info.value.fields)
    assert error_fields['severity'] == 'ERROR'
    assert error_fields['severity_nonlocalized'] == 'ERROR'
    assert error_fields['sqlstate'] == '42601'
    assert error_fields['message_primary'] == 'syntax error at or near "SELECTa"'
    assert error_fields['statement_position'] == '28'
    assert error_fields['source_file'] is not None
    assert error_fields['source_line'] is not None
    assert error_fields['source_function'] is not None


def test_missing_column(params: Iterable[Tuple[str, str]]) -> None:
    sql = '''
        SELECT 1;
        SELECT a;
    '''
    with streampq_connect(params) as query:
        results = iter(query(sql))
        _, rows = next(results)
        rows_it = iter(rows)
        next(rows_it)
        with pytest.raises(QueryError, match='column "a" does not exist') as exc_info:
            next(rows_it)

    error_fields = dict(exc_info.value.fields)
    assert error_fields['severity'] == 'ERROR'
    assert error_fields['severity_nonlocalized'] == 'ERROR'
    assert error_fields['sqlstate'] == '42703'
    assert error_fields['message_primary'] == 'column "a" does not exist'
    assert error_fields['statement_position'] == '35'
    assert error_fields['source_file'] is not None
    assert error_fields['source_line'] is not None
    assert error_fields['source_function'] is not None


def test_large_query(params: Iterable[Tuple[str, str]]) -> None:
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


BaseExceptionType = TypeVar('BaseExceptionType', bound=BaseException)

def run_query(params: Iterable[Tuple[str, str]], sql: str, about_to_run_query: EventClass, exception_bubbled: EventClass, exception_type: Type[BaseExceptionType]) -> None:
    def sigterm_handler(_: int, __: Optional[FrameType]) -> None:
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
def test_keyboard_interrupt(params: Iterable[Tuple[str, str]], signal_type: int, exception_type: Any) -> None:
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

    assert p.pid is not None
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


def test_temporary_table(params: Iterable[Tuple[str, str]]) -> None:
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


def test_empty_queries_among_others(params: Iterable[Tuple[str, str]]) -> None:
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


def test_with_type_modifier(params: Iterable[Tuple[str, str]]) -> None:
    sql = '''
        SELECT 'a'::varchar(8) as "col"
    '''

    with streampq_connect(params) as query:
        results = tuple(
            (cols, tuple(rows))
            for cols, rows in query(sql)
        )

    assert results == (
        (('col',), (('a',),)),
    )
    assert results[0][0][0].type_id == 1043
    assert results[0][0][0].type_modifier == 12


def test_series(params: Iterable[Tuple[str, str]]) -> None:
    sql = '''
        SELECT * FROM generate_series(1,10000000);
    '''

    count = 0

    with streampq_connect(params) as query:
        for cols, rows in query(sql):
            for row in rows:
                count += 1

    assert count == 10000000


def test_series_pandas_chunked(params: Iterable[Tuple[str, str]]) -> None:
    sql = '''
        SELECT * FROM generate_series(1,1005);
    '''

    def query_chunked_dfs(query: Query, sql: str, chunk_size: int) -> Iterable[Iterable[pd.DataFrame]]:

        def _chunked_df(columns: Tuple[str,...], rows: Iterable[Tuple[Any,...]]) -> Iterable[pd.DataFrame]:
            it = iter(rows)
            while True:
                df = pd.DataFrame(itertools.islice(it, chunk_size), columns=columns)
                if len(df) == 0:
                    break
                yield df

        for cols, rows in query(sql):
            yield _chunked_df(cols, rows)

    lengths = []
    with streampq_connect(params) as query:
        for chunked_dfs in query_chunked_dfs(query, sql, chunk_size=100):
            for chunk_df in chunked_dfs:
                lengths.append(len(chunk_df))

    assert lengths == [100] * 10 + [5]


def test_series_pandas(params: Iterable[Tuple[str, str]]) -> None:
    sql = '''
        SELECT * FROM generate_series(1,1005) AS s(my_col);
    '''

    lengths = []
    columns = []
    with streampq_connect(params) as query:
        for _columns, rows in query(sql):
            df = pd.DataFrame(rows, columns=_columns)
            lengths.append(len(df))
            columns.append(df.columns)

    assert lengths == [1005]
    assert columns == [['my_col']]


def test_notice(params: Iterable[Tuple[str, str]]) -> None:
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


def test_sending_notify_from_another_connection(params: Iterable[Tuple[str, str]]) -> None:
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


def test_incomplete_iteration_same_query(params: Iterable[Tuple[str, str]]) -> None:
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
        all_resuylts_it = iter(all_results)
        next(all_resuylts_it)
        cols, rows = next(all_resuylts_it)
        second_query_results = tuple(rows)

    assert second_query_results == (('foo',), ('bar',))


def test_incomplete_iteration_different_query(params: Iterable[Tuple[str, str]]) -> None:
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
        all_results_it = iter(all_results)
        cols, rows = next(all_results_it)
        rows_it = iter(rows)
        next(rows_it)

        with pytest.raises(QueryError, match='another command is already in progress'):
            query(sql)


@contextmanager
def fail_malloc(fail_in, search_string):
    cdll.LoadLibrary('./test_override_malloc.so').set_fail_in(fail_in, search_string)
    try:
        yield
    finally:
        cdll.LoadLibrary('./test_override_malloc.so').set_fail_in(-1, b'')


@pytest.mark.skipif(not sys.platform.startswith('linux'), reason='Test for malloc failure is linux-specific')
def test_connection_malloc_failure(params: Iterable[Tuple[str, str]]) -> None:
    with \
            fail_malloc(0, b'PQconnectStartParams'), \
            pytest.raises(ConnectionError, match='Unable to create connection object. Might be out of memory'):
        streampq_connect(params).__enter__()


def run(query, sql, identifiers=(), literals=()):
    for cols, rows in query(sql, identifiers=identifiers, literals=literals):
        for row in rows:
            pass


@pytest.mark.skipif(not sys.platform.startswith('linux'), reason='Test for malloc failure is linux-specific')
@pytest.mark.parametrize("fail_after", list(i for i in range(1, 40)))
def test_malloc_failure(params: Iterable[Tuple[str, str]], fail_after) -> None:

    sql = '''
        SELECT *, {value} AS {column} FROM generate_series(1,10);
        SELECT * FROM generate_series(1,10);
    '''

    # Mostly for test coverage reasons to make sure that all of `run` is run
    if fail_after in (34, 35, 39):
        with streampq_connect(params) as query:
            run(query, sql, identifiers=(('column', 'column'),), literals=(('value', 'value'),))
    else:
        with \
                fail_malloc(fail_after, b'libpq.so'), \
                pytest.raises(StreamPQError, match='memory|Memory|localhost'):  # Some memory issues are failure to resolve

            with streampq_connect(params) as query:
                run(query, sql, identifiers=(('column', 'column'),), literals=(('value', 'value'),))


@pytest.mark.skipif(not sys.platform.startswith('linux'), reason='Test for malloc failure is linux-specific')
@pytest.mark.parametrize("fail_after", list(i for i in range(0, 15)))
def test_malloc_failure_once_returning_results(params: Iterable[Tuple[str, str]], fail_after) -> None:

    sql = '''
        SELECT * FROM generate_series(1, 5);
        SELECT repeat(a::text, 10000) FROM generate_series(1, 100000) AS t(a);
    '''

    with \
            pytest.raises(StreamPQError, match='memory|Memory|localhost'), \
            streampq_connect(params) as query:

        for cols, rows in query(sql):
            with fail_malloc(fail_after, b'libpq.so'):
                for row in rows:
                    pass
