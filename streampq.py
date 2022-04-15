from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from functools import partial
from json import loads as json_loads
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE
from ctypes import cdll, create_string_buffer, byref, cast, c_char_p, c_void_p, c_int, c_size_t
from ctypes.util import find_library
from itertools import groupby


@contextmanager
def streampq_connect(
        params=(),
        get_encoders=lambda: get_default_encoders(),
        get_decoders=lambda: get_default_decoders(),
        get_libpq=lambda: cdll.LoadLibrary(find_library('pq')),
):
    pq = get_libpq()

    encoders_dict = dict(get_encoders())
    decoders_dict = dict(get_decoders())
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

    def escape(conn, value, func, allow_unescaped):
        must_escape, encoder = encoders_dict.get(type(value), (True, str))
        string_encoded = encoder(value)
        if allow_unescaped and not must_escape:
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

    def query(sel, socket, conn, set_query_running, sql, literals=(), identifiers=()):
        set_query_running(True)

        ok = pq.PQsendQuery(conn, sql.format(**dict(
            tuple((key, escape(conn, value, pq.PQescapeLiteral, True)) for key, value in literals) +
            tuple((key, escape(conn, value, pq.PQescapeIdentifier, False)) for key, value in identifiers)
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


def get_default_encoders():
    return (
        # type, (must escape, encoder)
        (type(None), (False, lambda _: 'NULL')),
        (type(True), (False, lambda value: 'TRUE' if value else 'FALSE')),
        (type(''), (True, lambda value: value)),
    )


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
            (700, 1021, float),                                                     # float4
            (701, 1022, float),                                                     # float8
            (114, 199, json_loads),                                                 # json
            (1043, 1015, lambda v: v),                                              # varchar
            (1082, 1182, date.fromisoformat),                                       # date
            (1114, 1115, lambda v: datetime.strptime(v, '%Y-%m-%d %H:%M:%S')),      # timestamp
            (1700, 1231, Decimal),                                                  # numeric
            (3802, 3807, json_loads),                                               # jsonb
        )), ())


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
