from collections import namedtuple
from collections.abc import Generator
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from json import loads as json_loads
import re
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE
from ctypes import cdll, create_string_buffer, cast, c_char_p, c_void_p, c_int, c_size_t
from ctypes.util import find_library
from itertools import groupby

from typing import Protocol, Generator, Callable, Iterable, Tuple, Dict, Set, Any


# A protocol is needed to use keyword arguments
class Query(Protocol):
    def __call__(self, sql: str, literals: Iterable[Tuple[str, Any]]=(), identifiers: Iterable[Tuple[str, Any]]=()) -> \
        Iterable[Tuple[Tuple[str,...], Iterable[Tuple[Any, ...]]]]: ...


@contextmanager
def streampq_connect(
        params: Iterable[Tuple[str, str]]=(),
        get_literal_encoders_array_types=lambda: get_default_literal_encoders_array_types(),
        get_literal_encoders=lambda: get_default_literal_encoders(),
        get_decoders=lambda: get_default_decoders(),
        get_libpq=lambda: cdll.LoadLibrary(find_library('pq') or 'libpq.so'),
) -> Generator[Query, None, None]:
    _create_string_buffer = create_string_buffer
    _cast = cast
    _c_char_p = c_char_p
    _c_void_p = c_void_p
    _c_int = c_int
    _c_size_t = c_size_t

    _dict = dict
    _len = len
    _object = object
    _range = range
    _tuple = tuple
    bytes_decode = bytes.decode
    str_encode = str.encode
    dict_get = dict.get

    pq = get_libpq()
    sel = DefaultSelector()
    sel_register = sel.register
    sel_unregister = sel.unregister
    sel_select = sel.select

    literal_encoders_array_types_set = set(get_literal_encoders_array_types())
    literal_encoders_dict = _dict(get_literal_encoders())
    decoders_dict = _dict(get_decoders())

    # Identifier encoding is not configurable - no known use case.
    # Since these are empty then we fall through to passing values through `str`
    identifier_encoders_array_types_set: Set = set()
    identifier_encoders_dict: Dict = dict()

    identity = lambda v: v

    PQconnectdbParams = pq.PQconnectdbParams
    PQconnectdbParams.restype = _c_void_p
    PQsocket = pq.PQsocket
    PQsocket.argtypes = (_c_void_p,)
    PQsetnonblocking = pq.PQsetnonblocking
    PQsetnonblocking.argtypes = (_c_void_p, _c_int)
    PQflush = pq.PQflush
    PQflush.argtypes = (_c_void_p,)
    PQconsumeInput = pq.PQconsumeInput
    PQconsumeInput.argtypes = (_c_void_p,)
    PQisBusy = pq.PQisBusy
    PQisBusy.argtypes = (_c_void_p,)
    PQfinish = pq.PQfinish
    PQfinish.argtypes = (_c_void_p,)
    PQstatus = pq.PQstatus
    PQstatus.argtypes = (_c_void_p,)

    PQgetCancel = pq.PQgetCancel
    PQgetCancel.argtypes = (_c_void_p,)
    PQgetCancel.restype = _c_void_p
    PQcancel = pq.PQcancel
    PQcancel.argtypes = (_c_void_p, _c_char_p, _c_int)
    PQfreeCancel = pq.PQfreeCancel
    PQfreeCancel.argtypes = (_c_void_p,)

    PQescapeLiteral = pq.PQescapeLiteral
    PQescapeLiteral.argtypes = (_c_void_p, _c_char_p, _c_size_t)
    PQescapeLiteral.restype = c_void_p
    PQescapeIdentifier = pq.PQescapeIdentifier
    PQescapeIdentifier.argtypes = (_c_void_p, _c_char_p, _c_size_t)
    PQescapeIdentifier.restype = _c_void_p
    PQfreemem = pq.PQfreemem
    PQfreemem.argtypes = (_c_void_p,)

    PQsendQuery = pq.PQsendQuery
    PQsendQuery.argtypes = (_c_void_p, _c_char_p)
    PQsetSingleRowMode = pq.PQsetSingleRowMode
    PQsetSingleRowMode.argtypes = (_c_void_p,)
    PQgetResult = pq.PQgetResult
    PQgetResult.argtypes = (_c_void_p,)
    PQgetResult.restype = _c_void_p
    PQresultStatus = pq.PQresultStatus
    PQresultStatus.argtypes = (_c_void_p,)
    PQnfields= pq.PQnfields
    PQnfields.argtypes = (_c_void_p,)
    PQfname = pq.PQfname
    PQfname.argtypes = (_c_void_p, _c_int)
    PQfname.restype = _c_char_p
    PQgetisnull = pq.PQgetisnull
    PQgetisnull.argtypes = (_c_void_p, _c_int, _c_int)
    PQgetvalue = pq.PQgetvalue
    PQgetvalue.argtypes = (_c_void_p, _c_int, _c_int)
    PQgetvalue.restype = _c_char_p
    PQftype = pq.PQftype
    PQftype.argtypes = (_c_void_p, _c_int)
    PQclear = pq.PQclear
    PQclear.argtypes = (_c_void_p,)

    PQerrorMessage = pq.PQerrorMessage
    PQerrorMessage.argtypes = (_c_void_p,)
    PQerrorMessage.restype = _c_char_p

    PGRES_COMMAND_OK = 1
    PGRES_TUPLES_OK = 2
    PGRES_SINGLE_TUPLE = 9

    def as_null_terminated_array(strings):
        char_ps = _tuple(_c_char_p(str_encode(string, 'utf-8')) for string in strings) + (None,)
        return (_c_char_p * _len(char_ps))(*char_ps)

    params_tuple = _tuple(params)
    keywords = as_null_terminated_array((key for key, value in params_tuple))
    values = as_null_terminated_array((value for key, value in params_tuple))

    @contextmanager
    def get_conn():
        conn = _c_void_p(0)
        try:
            conn = PQconnectdbParams(keywords, values, 0)
            if not conn:
                raise ConnectionError()

            status = PQstatus(conn)
            if status:
                raise ConnectionError(bytes_decode(PQerrorMessage(conn), 'utf-8'))

            ok = PQsetnonblocking(conn, 1)
            if ok != 0:
                raise ConnectionError(bytes_decode(PQerrorMessage(conn), 'utf-8'))

            socket = PQsocket(conn)
            yield socket, conn
        finally:
            PQfinish(conn)

    @contextmanager
    def cancel_query(socket, conn):
        query_running = False

        def set_needs_cancel(new_query_running):
            nonlocal query_running
            query_running = new_query_running

        try:
            yield set_needs_cancel
        finally:
            if query_running:
                pg_cancel = _c_void_p(0)
                try:
                    pg_cancel = PQgetCancel(conn)
                    if not pg_cancel:
                        raise CancelError(bytes_decode(PQerrorMessage(conn), 'utf-8'))
                    buf = _create_string_buffer(256)
                    ok = PQcancel(pg_cancel, buf, 256)
                    if not ok:
                        raise CancelError(bytes_decode(buf.raw.rstrip(b'\x00'), 'utf-8'))
                finally:
                    PQfreeCancel(pg_cancel)

    # Blocking using select rather than in libpq allows signals to be caught by Python.
    # Notably SIGINT will result in a KeyboardInterrupt as expected
    @contextmanager
    def get_blocker(socket, events):

        def block():
            while True:
                for _, mask in sel_select():
                    for event in events:
                        if event & mask:
                            return event

        to_register = 0
        for ev in events:
            to_register |= ev

        sel_register(socket, to_register)
        try:
            yield block
        finally:
            sel_unregister(socket)

    def flush_write(block_write_read, conn):
        while True:
            incomplete = PQflush(conn)
            if incomplete == -1:
                raise CommunicationError(bytes_decode(PQerrorMessage(conn), 'utf-8'))
            if not incomplete:
                break
            ready_for = block_write_read()
            if ready_for == EVENT_READ:
                ok = PQconsumeInput(conn)
                if not ok:
                    raise CommunicationError(bytes_decode(PQerrorMessage(conn), 'utf-8'))

    def flush_read(block_read, conn):
        while True:
            is_busy = PQisBusy(conn)
            if not is_busy:
                break
            block_read()
            ok = PQconsumeInput(conn)
            if not ok:
                raise CommunicationError(bytes_decode(PQerrorMessage(conn), 'utf-8'))

    def encode(conn, value, func, array_types_set, encoders_dict):
        def _value_encode(value):
            must_escape, encoder = dict_get(encoders_dict, type(value), (True, str))
            string_encoded = encoder(value)
            if not must_escape:
                return string_encoded
            string_encoded_bytes = str_encode(string_encoded, 'utf-8')
            escaped_p = _c_void_p(0)
            try:
                escaped_p = func(conn, string_encoded_bytes, _len(string_encoded_bytes))
                if not escaped_p:
                    raise StreamPQError(bytes_decode(PQerrorMessage(conn), 'utf-8'))
                escaped_str = bytes_decode(_cast(escaped_p, _c_char_p).value, 'utf-8')
            finally:
                PQfreemem(escaped_p)

            return escaped_str

        def _array_encode(array, depth):
            return (('[' if depth else 'ARRAY[') + ','.join(
                _array_encode(value, depth+1) if type(value) in array_types_set else _value_encode(value)
                for value in array
            ) + ']')

        return \
            _array_encode(value, 0) if type(value) in array_types_set else \
            _value_encode(value)


    def query(socket, conn, set_query_running, sql, literals, identifiers):
        set_query_running(True)

        ok = PQsendQuery(conn, str_encode(sql.format(**_dict(
            _tuple((key, encode(conn, value, PQescapeLiteral, literal_encoders_array_types_set, literal_encoders_dict)) for key, value in literals) +
            _tuple((key, encode(conn, value, PQescapeIdentifier, identifier_encoders_array_types_set, identifier_encoders_dict)) for key, value in identifiers)
        )), 'utf-8'));
        if not ok:
            raise QueryError(bytes_decode(PQerrorMessage(conn), 'utf-8'))

        with get_blocker(socket, (EVENT_WRITE,EVENT_READ)) as block_write_read:
            flush_write(block_write_read, conn)

        ok = PQsetSingleRowMode(conn);
        if not ok:
            raise QueryError(bytes_decode(PQerrorMessage(conn), 'utf-8'))

        def get_results() -> Iterable:
            # So we can use groupby to separate rows for different statements
            # in multi-statment queries
            group_key = _object()
            result = _c_void_p(0)
            num_columns = 0
            column_names: Tuple[str, ...] = ()
            column_decoders: Tuple[Callable[[str], Any], ...] = ()
            null_decoder = dict_get(decoders_dict, None, identity)

            with get_blocker(socket, (EVENT_READ,)) as block_read:
                while True:
                    flush_read(block_read, conn)

                    try:
                        result = PQgetResult(conn)
                        if not result:
                            break

                        status = PQresultStatus(result)
                        if status in (PGRES_COMMAND_OK, PGRES_TUPLES_OK):
                            group_key = _object()
                            num_columns = 0
                            column_names = ()
                            column_decoders = ()
                            continue

                        if status != PGRES_SINGLE_TUPLE:
                            raise QueryError(bytes_decode(PQerrorMessage(conn), 'utf-8'))

                        if num_columns == 0:
                            num_columns = PQnfields(result)
                            column_names = _tuple(
                                bytes_decode(PQfname(result, i), 'utf-8')
                                for i in _range(0, num_columns)
                            )
                            column_decoders = _tuple(
                                dict_get(decoders_dict, PQftype(result, i), identity)
                                for i in _range(0, num_columns)
                            )

                        values = _tuple(
                            null_decoder('NULL') if PQgetisnull(result, 0, i) else column_decoders[i](bytes_decode(PQgetvalue(result, 0, i), 'utf-8'))
                            for i in _range(0, num_columns)
                        )

                        yield (group_key, column_names), values
                    finally:
                        PQclear(result)
                        result = _c_void_p(0)

            set_query_running(False)

        def get_columns(grouped_results):
            for (_, column_names), rows in grouped_results:
                yield column_names, (row[1] for row in rows)

        results = get_results()
        grouped_results = groupby(results, key=lambda key_columns_row: key_columns_row[0])
        with_columns = get_columns(grouped_results)

        return with_columns

    # Avoid partial to make type checking more sensitive
    def get_query_func(socket, conn, set_query_running) -> Query:
        def _query(sql, literals=(), identifiers=()):
            return query(socket, conn, set_query_running, sql, literals=literals, identifiers=identifiers)

        return _query

    with \
            get_conn() as (socket, conn), \
            cancel_query(socket, conn) as set_query_running:

        yield get_query_func(socket, conn, set_query_running)


def get_default_literal_encoders():
    return (
        # type, (must escape, encoder)
        (type(None), (False, lambda _: 'NULL')),
        (type(True), (False, lambda value: 'TRUE' if value else 'FALSE')),
        (type(1), (False, str)),
        (type(1.0), (False, str)),
        (Decimal, (False, str)),
        (type(''), (True, lambda value: value)),
    )


def get_default_literal_encoders_array_types():
    return (type(()), type([]))


def get_default_decoders():
    # Returns tuple of oid, array_oid, and decoder

    # Avoid dots
    fromhex = bytes.fromhex
    lstrip = str.lstrip
    strip = str.strip
    split = str.split
    identity = lambda v: v

    # Avoid globals
    _int = int
    _tuple = tuple

    return \
        ((None, lambda _: None),) + \
        sum(_tuple((
            (oid, value_decoder),
            (array_oid, get_array_decoder(value_decoder)),
        ) for oid, array_oid, value_decoder in (
            (16, 1000, lambda v: v == 't'),                                             # bool
            (17, 1001, lambda v: fromhex(lstrip(v, '\\x'))),                            # bytea
            (18, 1002, identity),                                                       # char
            (19, 1003, identity),                                                       # name
            (20, 1016, int),                                                            # int8
            (21, 1005, int),                                                            # int2
            (22, 1006, lambda v: _tuple(_int(i) for i in split(v))),                    # int2vector
            (23, 1007, int),                                                            # int4
            (24, 1008, identity),                                                       # regproc
            (25, 1009, identity),                                                       # text
            (26, 1028, int),                                                            # oid
            (27, 1010, lambda v: _tuple(_int(i) for i in split(strip(v, '()'), ','))),  # tid
            (28, 1011, int),                                                            # xid
            (29, 1012, int),                                                            # cid
            (30, 1013, lambda v: _tuple(int(i) for i in split(v))),                     # oidvector
            (114, 199, json_loads),                                                     # json
            (142, 143, identity),                                                       # xml
            (650, 651, identity),                                                       # cidr
            (700, 1021, float),                                                         # float4
            (701, 1022, float),                                                         # float8
            (774, 775, identity),                                                       # macaddr8
            (790, 791, identity),                                                       # money
            (829, 1040, identity),                                                      # macaddr
            (869, 1041, identity),                                                      # inet
            (1043, 1015, identity),                                                     # varchar
            (1082, 1182, get_date_decoder()),                                           # date
            (1114, 1115, get_timestamp_decoder()),                                      # timestamp
            (1184, 1185, get_timestamptz_decoder()),                                    # timestamptz
            (1186, 1187, get_interval_decoder()),                                       # interval
            (1700, 1231, Decimal),                                                      # numeric
            (3802, 3807, json_loads),                                                   # jsonb
            (3904, 3905, get_range_decoder(int)),                                       # int4range
            (3906, 3907, get_range_decoder(Decimal)),                                   # numrange
            (3908, 3909, get_range_decoder(get_timestamp_decoder())),                   # tsrange
            (3910, 3911, get_range_decoder(get_timestamptz_decoder())),                 # tstzrange
            (3912, 3913, get_range_decoder(get_date_decoder())),                        # daterange
            (3926, 3927, get_range_decoder(int)),                                       # int8range
            (4451, 6150, get_multirange_decoder(int)),                                  # int4multirange
            (4532, 6151, get_multirange_decoder(Decimal)),                              # nummultirange
            (4533, 6152, get_multirange_decoder(get_timestamp_decoder())),              # tsmultirange
            (4534, 6153, get_multirange_decoder(get_timestamptz_decoder())),            # tstzmultirange
            (4535, 6155, get_multirange_decoder(get_date_decoder())),                   # datemultirange
            (4536, 6157, get_multirange_decoder(int)),                                  # int8multirange
        )), ())

# It's not perfect to map infinity to min/max for date/datetimes, but it's
# probably fine in most cases

def get_date_decoder():
    date_min = date.min
    date_max = date.max
    fromisoformat = date.fromisoformat

    def decode(raw):
        return \
            date_min if raw == '-infinity' else \
            date_max if raw == 'infinity' else \
            fromisoformat(raw)
    return decode


def get_timestamp_decoder():
    datetime_min = datetime.min
    datetime_max = datetime.max
    strptime = datetime.strptime

    def decode(raw):
        return \
            datetime_min if raw == '-infinity' else \
            datetime_max if raw == 'infinity' else \
            strptime(raw, '%Y-%m-%d %H:%M:%S')
    return decode


def get_timestamptz_decoder():
    datetime_min = datetime.min.replace(tzinfo=timezone(timedelta())) 
    datetime_max = datetime.max.replace(tzinfo=timezone(timedelta()))
    strptime = datetime.strptime
    str_format = str.format

    def decode(raw):
        # Infinities don't come with a timezone, so we just use 0-offset
        return \
            datetime_min if raw == '-infinity' else \
            datetime_max if raw == 'infinity' else \
            strptime(str_format('{:<024}', raw), '%Y-%m-%d %H:%M:%S%z')
    return decode


def get_interval_decoder():
    interval_regex = re.compile(r'((?P<years>-?\d+) years?)?( ?(?P<months>-?\d+) mons?)?( ?(?P<days>-?\d+) days?)?( ?(?P<sign>-)(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>.*))?')
    re_match = re.match
    group = re.Match.group
    _int = int
    _Decimal = Decimal
    _Interval = Interval

    def decode(raw):
        m = re_match(interval_regex, raw)
        sign = -1 if group(m, 'sign') == '-' else 1
        return _Interval(*(
            _int(group(m, 'years') or '0'),
            _int(group(m, 'months') or '0'),
            _int(group(m, 'days') or '0'),
            _int(group(m, 'hours') or '0') * sign,
            _int(group(m, 'minutes') or '0') * sign,
            _Decimal(group(m, 'seconds') or '0') * sign or _Decimal('0'),
        ))

    return decode


def get_range_decoder(value_decoder):
    OUT = object()
    IN_UNQUOTED = object()
    IN_QUOTED = object()

    append = list.append
    join = str.join
    _Range = Range

    def decode(raw):
        state = OUT
        bounds = []
        values = []
        value = []

        for c in raw:
            if state is OUT:
                if c in '([])':
                    append(bounds, c)
                elif c == '"':
                    state = IN_QUOTED
                elif c == ',':
                    pass
                else:
                    append(value, c)
                    state = IN_UNQUOTED
            elif state is IN_UNQUOTED:
                if c in ')]':
                    append(values, value_decoder(join('', value)) if value else None)
                    append(bounds, c)
                    value = []
                    state = OUT
                elif c in ',':
                    append(values, value_decoder(join('', value)) if value else None)
                    value = []
                    state = OUT
                else:
                    append(value, c)
            elif state is IN_QUOTED:
                if c == '"':
                    append(values, value_decoder(join('', value)))
                    value = []
                    state = OUT
                else:
                    append(value, c)

        return _Range(lower=values[0], upper=values[1], bounds=join('', bounds))

    return decode


def get_multirange_decoder(value_decoder):
    OUT = object()
    IN_UNQUOTED = object()
    IN_QUOTED = object()

    append = list.append
    join = str.join
    _tuple = tuple
    _Range = Range

    def decode(raw):
        state = OUT
        ranges = []
        bounds = []
        values = []
        value = []

        for c in raw:
            if state is OUT:
                if c in '([':
                    append(bounds, c)
                elif c in '])':
                    append(bounds, c)
                    append(ranges, _Range(lower=values[0], upper=values[1], bounds=join('', bounds)))
                    bounds = []
                    values = []
                elif c == '"':
                    state = IN_QUOTED
                elif c in '{,}':
                    pass
                else:
                    append(value, c)
                    state = IN_UNQUOTED
            elif state is IN_UNQUOTED:
                if c in ')]':
                    append(values, value_decoder(join('', value)) if value else None)
                    append(bounds, c)
                    append(ranges, _Range(lower=values[0], upper=values[1], bounds=''.join(bounds)))
                    bounds = []
                    values = []
                    value = []
                    state = OUT
                elif c in ',':
                    append(values, value_decoder(''.join(value)) if value else None)
                    value = []
                    state = OUT
                else:
                    append(value, c)
            elif state is IN_QUOTED:
                if c == '"':
                    append(values, value_decoder(''.join(value)))
                    value = []
                    state = OUT
                else:
                    append(value, c)

        return _tuple(ranges)

    return decode



def get_array_decoder(value_decoder):
    OUT = object()
    IN_UNQUOTED = object()
    IN_QUOTED = object()
    IN_QUOTED_ESCAPE = object()

    append = list.append
    pop = list.pop
    join = str.join
    _tuple = tuple

    def decode(raw):
        state = OUT
        stack = [[]]
        value = []

        for c in raw:
            if state is OUT:
                if c == '{':
                    append(stack, [])
                elif c == '}':
                    append(stack[-2], _tuple(pop(stack)))
                elif c == ',':
                    pass
                elif c == '"':
                    state = IN_QUOTED
                else:
                    append(value, c)
                    state = IN_UNQUOTED
            elif state is IN_UNQUOTED:
                if c == '}':
                    value_str = join('', value)
                    value = []
                    append(stack[-1], None if value_str == 'NULL' else value_decoder(value_str))
                    append(stack[-2], _tuple(pop(stack)))
                    state = OUT
                elif c == ',':
                    value_str = join('', value)
                    value = []
                    append(stack[-1], None if value_str == 'NULL' else value_decoder(value_str))
                    state = OUT
                else:
                    append(value, c)
            elif state is IN_QUOTED:
                if c == '"':
                    value_str = join('', value)
                    value = []
                    append(stack[-1], value_decoder(value_str))
                    state = OUT
                elif c == '\\':
                    state = IN_QUOTED_ESCAPE
                else:
                    append(value, c)
            elif state is IN_QUOTED_ESCAPE:
                append(value, c)
                state = IN_QUOTED

        return stack[0][0]

    return decode


Interval = namedtuple('Interval', ('years', 'months', 'days', 'hours', 'minutes', 'seconds'), defaults=(0, 0, 0, 0, 0, Decimal('0')))
Range = namedtuple('Range', ('lower', 'upper', 'bounds'))


class StreamPQError(Exception):
    pass


class ConnectionError(StreamPQError):
    pass


class QueryError(StreamPQError):
    pass


class CancelError(StreamPQError):
    pass


class CommunicationError(StreamPQError):
    pass
