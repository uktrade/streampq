from collections import namedtuple
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import partial
from json import loads as json_loads
import re
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE
from ctypes import cdll, create_string_buffer, byref, cast, c_char_p, c_void_p, c_int, c_size_t
from ctypes.util import find_library
from itertools import groupby


@contextmanager
def streampq_connect(
        params=(),
        get_literal_encoders_array_types=lambda: get_default_literal_encoders_array_types(),
        get_literal_encoders=lambda: get_default_literal_encoders(),
        get_decoders=lambda: get_default_decoders(),
        get_libpq=lambda: cdll.LoadLibrary(find_library('pq')),
):
    pq = get_libpq()

    literal_encoders_array_types_set = set(get_literal_encoders_array_types())
    literal_encoders_dict = dict(get_literal_encoders())
    decoders_dict = dict(get_decoders())

    # Identifier encoding is not configurable - no known use case.
    # Since these are empty then we fall through to passing values through `str`
    identifier_encoders_array_types_set = set()
    identifier_encoders_dict = dict()

    identity = lambda v: v

    pq.PQconnectdbParams.restype = c_void_p
    pq.PQsocket.argtypes = (c_void_p,)
    pq.PQsetnonblocking.argtypes = (c_void_p, c_int)
    pq.PQflush.argtypes = (c_void_p,)
    pq.PQconsumeInput.argtypes = (c_void_p,)
    pq.PQisBusy.argtypes = (c_void_p,)
    pq.PQfinish.argtypes = (c_void_p,)
    pq.PQstatus.argtypes = (c_void_p,)

    pq.PQgetCancel.argtypes = (c_void_p,)
    pq.PQgetCancel.restype = c_void_p
    pq.PQcancel.argtypes = (c_void_p, c_char_p, c_int)
    pq.PQfreeCancel.argtypes = (c_void_p,)

    pq.PQescapeLiteral.argtypes = (c_void_p, c_char_p, c_size_t)
    pq.PQescapeLiteral.restype = c_void_p
    pq.PQescapeIdentifier.argtypes = (c_void_p, c_char_p, c_size_t)
    pq.PQescapeIdentifier.restype = c_void_p
    pq.PQfreemem.argtypes = (c_void_p,)

    pq.PQsendQuery.argtypes = (c_void_p, c_char_p)
    pq.PQsetSingleRowMode.argtypes = (c_void_p,)
    pq.PQgetResult.argtypes = (c_void_p,)
    pq.PQgetResult.restype = c_void_p
    pq.PQresultStatus.argtypes = (c_void_p,)
    pq.PQnfields.argtypes = (c_void_p,)
    pq.PQfname.argtypes = (c_void_p, c_int)
    pq.PQfname.restype = c_char_p
    pq.PQgetisnull.argtypes = (c_void_p, c_int, c_int)
    pq.PQgetvalue.argtypes = (c_void_p, c_int, c_int)
    pq.PQgetvalue.restype = c_char_p
    pq.PQftype.argtypes = (c_void_p, c_int)
    pq.PQclear.argtypes = (c_void_p,)

    pq.PQerrorMessage.argtypes = (c_void_p,)
    pq.PQerrorMessage.restype = c_char_p

    PGRES_COMMAND_OK = 1
    PGRES_TUPLES_OK = 2
    PGRES_SINGLE_TUPLE = 9

    def as_null_terminated_array(strings):
        char_ps = tuple(c_char_p(string.encode('utf-8')) for string in strings) + (None,)
        return (c_char_p * len(char_ps))(*char_ps)

    params_tuple = tuple(params)
    keywords = as_null_terminated_array((key for key, value in params_tuple))
    values = as_null_terminated_array((value for key, value in params_tuple))

    @contextmanager
    def get_conn():
        conn = c_void_p(0)
        try:
            conn = pq.PQconnectdbParams(keywords, values, 0)
            if not conn:
                raise ConnectionError()

            status = pq.PQstatus(conn)
            if status:
                raise ConnectionError(pq.PQerrorMessage(conn).decode('utf-8'))

            ok = pq.PQsetnonblocking(conn, 1)
            if ok != 0:
                raise ConnectionError(pq.PQerrorMessage(conn).decode('utf-8'))

            socket = pq.PQsocket(conn)
            sel = DefaultSelector()
            yield sel, socket, conn
        finally:
            pq.PQfinish(conn)

    @contextmanager
    def cancel_query(sel, socket, conn):
        query_running = False

        def set_needs_cancel(new_query_running):
            nonlocal query_running
            query_running = new_query_running

        try:
            yield set_needs_cancel
        finally:
            if query_running:
                pg_cancel = c_void_p(0)
                try:
                    pg_cancel = pq.PQgetCancel(conn)
                    if not pg_cancel:
                        raise CancelError(pq.PQerrorMessage(conn).decode('utf-8'))
                    buf = create_string_buffer(256)
                    ok = pq.PQcancel(pg_cancel, buf, 256)
                    if not ok:
                        raise CancelError(buf.raw.rstrip(b'\x00').decode('utf-8'))
                finally:
                    pq.PQfreeCancel(pg_cancel)

    # Blocking using select rather than in libpq allows signals to be caught by Python.
    # Notably SIGINT will result in a KeyboardInterrupt as expected
    def block_until(sel, socket, events):
        to_register = 0
        for ev in events:
            to_register |= ev
        sel.register(socket, to_register)
        while True:
            for key, mask in sel.select():
                for event in events:
                    if event & mask:
                        sel.unregister(socket)
                        return event

    def flush_write(sel, socket, conn):
        while True:
            incomplete = pq.PQflush(conn)
            if incomplete == -1:
                raise CommunicationError(pq.PQerrorMessage(conn).decode('utf-8'))
            if not incomplete:
                break
            ready_for = block_until(sel, socket, (EVENT_WRITE, EVENT_READ))
            if ready_for == EVENT_READ:
                ok = pq.PQconsumeInput(conn)
                if not ok:
                    raise CommunicationError(pq.PQerrorMessage(conn).decode('utf-8'))

        block_until(sel, socket, (EVENT_READ,))

    def flush_read(sel, socket, conn):
        while True:
            is_busy = pq.PQisBusy(conn)
            if not is_busy:
                break
            block_until(sel, socket, (EVENT_READ,))
            ok = pq.PQconsumeInput(conn)
            if not ok:
                raise CommunicationError(pq.PQerrorMessage(conn).decode('utf-8'))

    def encode(conn, value, func, array_types_set, encoders_dict):
        def _value_encode(value):
            must_escape, encoder = encoders_dict.get(type(value), (True, str))
            string_encoded = encoder(value)
            if not must_escape:
                return string_encoded
            string_encoded_bytes = string_encoded.encode('utf-8')
            escaped_p = c_void_p(0)
            try:
                escaped_p = func(conn, string_encoded_bytes, len(string_encoded_bytes))
                if not escaped_p:
                    raise StreamPQError(pq.PQerrorMessage(conn).decode('utf-8'))
                escaped_str = cast(escaped_p, c_char_p).value.decode('utf-8')
            finally:
                pq.PQfreemem(escaped_p)

            return escaped_str

        def _array_encode(array, depth):
            return (('[' if depth else 'ARRAY[') + ','.join(
                _array_encode(value, depth+1) if type(value) in array_types_set else _value_encode(value)
                for value in array
            ) + ']')

        return \
            _array_encode(value, 0) if type(value) in array_types_set else \
            _value_encode(value)


    def query(sel, socket, conn, set_query_running, sql, literals=(), identifiers=()):
        set_query_running(True)

        ok = pq.PQsendQuery(conn, sql.format(**dict(
            tuple((key, encode(conn, value, pq.PQescapeLiteral, literal_encoders_array_types_set, literal_encoders_dict)) for key, value in literals) +
            tuple((key, encode(conn, value, pq.PQescapeIdentifier, identifier_encoders_array_types_set, identifier_encoders_dict)) for key, value in identifiers)
        )).encode('utf-8'));
        if not ok:
            raise QueryError(pq.PQerrorMessage(conn).decode('utf-8'))

        flush_write(sel, socket, conn)

        ok = pq.PQsetSingleRowMode(conn);
        if not ok:
            raise QueryError(pq.PQerrorMessage(conn).decode('utf-8'))

        def get_results():
            # So we can use groupby to separate rows for different statements
            # in multi-statment queries
            group_key = object()
            result = c_void_p(0)

            while True:
                flush_read(sel, socket, conn)

                try:
                    result = pq.PQgetResult(conn)
                    if not result:
                        break

                    status = pq.PQresultStatus(result)
                    if status in (PGRES_COMMAND_OK, PGRES_TUPLES_OK):
                        group_key = object()
                        continue

                    if status != PGRES_SINGLE_TUPLE:
                        raise QueryError(pq.PQerrorMessage(conn).decode('utf-8'))

                    num_columns = pq.PQnfields(result)
                    columns = tuple(
                        pq.PQfname(result, i).decode('utf-8')
                        for i in range(0, num_columns)
                    )
                    values = tuple(
                        decoders_dict.get(
                            None if pq.PQgetisnull(result, 0, i) else \
                            pq.PQftype(result, i), identity)(pq.PQgetvalue(result, 0, i).decode('utf-8'))
                        for i in range(0, num_columns)
                    )

                    yield (group_key, columns), values
                finally:
                    pq.PQclear(result)
                    result = c_void_p(0)

            set_query_running(False)

        def get_columns(grouped_results):
            for (_, columns), rows in grouped_results:
                yield columns, (row[1] for row in rows)

        results = get_results()
        grouped_results = groupby(results, key=lambda key_columns_row: key_columns_row[0])
        with_columns = get_columns(grouped_results)

        return with_columns

    with \
            get_conn() as (sel, socket, conn), \
            cancel_query(sel, socket, conn) as set_query_running:

        yield partial(query, sel, socket, conn, set_query_running)


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
    # Returns tuple of oid, decoder pairs
    return \
        ((None, lambda _: None),) + \
        sum(tuple((
            (oid, value_decoder),
            (array_oid, get_array_decoder(value_decoder)),
        ) for oid, array_oid, value_decoder in (
            (16, 1000, lambda v: v == 't'),                                         # bool
            (17, 1001, lambda v: bytes.fromhex(v.lstrip('\\x'))),                   # bytea
            (18, 1002, lambda v: v),                                                # char
            (19, 1003, lambda v: v),                                                # name
            (20, 1016, int),                                                        # int8
            (21, 1005, int),                                                        # int2
            (22, 1006, lambda v: tuple(int(i) for i in v.split())),                 # int2vector
            (23, 1007, int),                                                        # int4
            (24, 1008, lambda v: v),                                                # regproc
            (25, 1009, lambda v: v),                                                # text
            (26, 1028, int),                                                        # oid
            (27, 1010, lambda v: tuple(int(i) for i in v.strip('()').split(','))),  # tid
            (28, 1011, int),                                                        # xid
            (29, 1012, int),                                                        # cid
            (30, 1013, lambda v: tuple(int(i) for i in v.split())),                 # oidvector
            (114, 199, json_loads),                                                 # json
            (142, 143, lambda v: v),                                                # xml
            (650, 651, lambda v: v),                                                # cidr
            (700, 1021, float),                                                     # float4
            (701, 1022, float),                                                     # float8
            (774, 775, lambda v: v),                                                # macaddr8
            (790, 791, lambda v: v),                                                # money
            (829, 1040, lambda v: v),                                               # macaddr
            (869, 1041, lambda v: v),                                               # inet
            (1043, 1015, lambda v: v),                                              # varchar
            (1082, 1182, get_date_decoder()),                                       # date
            (1114, 1115, get_timestamp_decoder()),                                  # timestamp
            (1184, 1185, get_timestamptz_decoder()),                                # timestamptz
            (1186, 1187, get_interval_decoder()),                                   # interval
            (1700, 1231, Decimal),                                                  # numeric
            (3802, 3807, json_loads),                                               # jsonb
            (3904, 3905, get_range_decoder(int)),                                   # int4range
            (3906, 3907, get_range_decoder(Decimal)),                               # numrange
            (3908, 3909, get_range_decoder(get_timestamp_decoder())),               # tsrange
            (3910, 3911, get_range_decoder(get_timestamptz_decoder())),             # tstzrange
            (3912, 3913, get_range_decoder(get_date_decoder())),                    # daterange
            (3926, 3927, get_range_decoder(int)),                                   # int8range
            (4451, 6150, get_multirange_decoder(int)),                              # int4multirange
            (4532, 6151, get_multirange_decoder(Decimal)),                          # nummultirange
            (4533, 6152, get_multirange_decoder(get_timestamp_decoder())),          # tsmultirange
            (4534, 6153, get_multirange_decoder(get_timestamptz_decoder())),        # tstzmultirange
            (4535, 6155, get_multirange_decoder(get_date_decoder())),               # datemultirange
            (4536, 6157, get_multirange_decoder(int)),                              # int8multirange
        )), ())

# It's not perfect to map infinity to min/max for date/datetimes, but it's
# probably fine in most cases

def get_date_decoder():
    def decode(raw):
        return \
            date.min if raw == '-infinity' else \
            date.max if raw == 'infinity' else \
            date.fromisoformat(raw)
    return decode


def get_timestamp_decoder():
    def decode(raw):
        return \
            datetime.min if raw == '-infinity' else \
            datetime.max if raw == 'infinity' else \
            datetime.strptime(raw, '%Y-%m-%d %H:%M:%S')
    return decode


def get_timestamptz_decoder():
    def decode(raw):
        # Inifinities don't come with a timezone, so we just use 0-offset
        return \
            datetime.min.replace(tzinfo=timezone(timedelta())) if raw == '-infinity' else \
            datetime.max.replace(tzinfo=timezone(timedelta())) if raw == 'infinity' else \
            datetime.strptime('{:<024}'.format(raw), '%Y-%m-%d %H:%M:%S%z')
    return decode


def get_interval_decoder():
    interval_regex = re.compile(r'((?P<years>-?\d+) years?)?( ?(?P<months>-?\d+) mons?)?( ?(?P<days>-?\d+) days?)?( ?(?P<sign>-)(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>.*))?')
    def decode(raw):
        m = re.match(interval_regex, raw)
        years, months, days, hours, minutes, seconds = (
            func(m.group(name)) if m.group(name) else 0
            for func, name in ((int, 'years'), (int, 'months'), (int, 'days'), (int, 'hours'), (int, 'minutes'), (Decimal, 'seconds'))
        )
        sign = -1 if m.group('sign') == '-' else 1
        return Interval(years=years, months=months, days=days, hours=sign*hours, minutes=sign*minutes, seconds=sign*seconds)

    return decode


def get_range_decoder(value_decoder):
    OUT = object()
    IN_UNQUOTED = object()
    IN_QUOTED = object()

    def decode(raw):
        state = OUT
        bounds = []
        values = []
        value = []

        for c in raw:
            if state is OUT:
                if c in '([])':
                    bounds.append(c)
                elif c == '"':
                    state = IN_QUOTED
                elif c == ',':
                    pass
                else:
                    value.append(c)
                    state = IN_UNQUOTED
            elif state is IN_UNQUOTED:
                if c in ')]':
                    values.append(value_decoder(''.join(value)) if value else None)
                    bounds.append(c)
                    value = []
                    state = OUT
                elif c in ',':
                    values.append(value_decoder(''.join(value)) if value else None)
                    value = []
                    state = OUT
                else:
                    value.append(c)
            elif state is IN_QUOTED:
                if c == '"':
                    values.append(value_decoder(''.join(value)))
                    value = []
                    state = OUT
                else:
                    value.append(c)

        return Range(lower=values[0], upper=values[1], bounds=''.join(bounds))

    return decode


def get_multirange_decoder(value_decoder):
    OUT = object()
    IN_UNQUOTED = object()
    IN_QUOTED = object()

    def decode(raw):
        state = OUT
        ranges = []
        bounds = []
        values = []
        value = []

        for c in raw:
            if state is OUT:
                if c in '([':
                    bounds.append(c)
                elif c in '])':
                    bounds.append(c)
                    ranges.append(Range(lower=values[0], upper=values[1], bounds=''.join(bounds)))
                    bounds = []
                    values = []
                elif c == '"':
                    state = IN_QUOTED
                elif c in '{,}':
                    pass
                else:
                    value.append(c)
                    state = IN_UNQUOTED
            elif state is IN_UNQUOTED:
                if c in ')]':
                    values.append(value_decoder(''.join(value)) if value else None)
                    bounds.append(c)
                    ranges.append(Range(lower=values[0], upper=values[1], bounds=''.join(bounds)))
                    bounds = []
                    values = []
                    value = []
                    state = OUT
                elif c in ',':
                    values.append(value_decoder(''.join(value)) if value else None)
                    value = []
                    state = OUT
                else:
                    value.append(c)
            elif state is IN_QUOTED:
                if c == '"':
                    values.append(value_decoder(''.join(value)))
                    value = []
                    state = OUT
                else:
                    value.append(c)

        return tuple(ranges)

    return decode



def get_array_decoder(value_decoder):
    OUT = object()
    IN_UNQUOTED = object()
    IN_QUOTED = object()
    IN_QUOTED_ESCAPE = object()

    def decode(raw):
        state = OUT
        stack = [[]]
        value = []

        for c in raw:
            if state is OUT:
                if c == '{':
                    stack.append([])
                elif c == '}':
                    stack[-2].append(tuple(stack.pop()))
                elif c == ',':
                    pass
                elif c == '"':
                    state = IN_QUOTED
                else:
                    value.append(c)
                    state = IN_UNQUOTED
            elif state is IN_UNQUOTED:
                if c == '}':
                    value_str = ''.join(value)
                    value = []
                    stack[-1].append(None if value_str == 'NULL' else value_decoder(value_str))
                    stack[-2].append(tuple(stack.pop()))
                    state = OUT
                elif c == ',':
                    value_str = ''.join(value)
                    value = []
                    stack[-1].append(None if value_str == 'NULL' else value_decoder(value_str))
                    state = OUT
                else:
                    value.append(c)
            elif state is IN_QUOTED:
                if c == '"':
                    value_str = ''.join(value)
                    value = []
                    stack[-1].append(value_decoder(value_str))
                    state = OUT
                elif c == '\\':
                    state = IN_QUOTED_ESCAPE
                else:
                    value.append(c)
            elif state is IN_QUOTED_ESCAPE:
                value.append(c)
                state = IN_QUOTED

        return stack[0][0]

    return decode


Interval = namedtuple('Interval', ['years', 'months', 'days', 'hours', 'minutes', 'seconds'], defaults=(0,)*6)
Range = namedtuple('Interval', ['lower', 'upper', 'bounds'])


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
