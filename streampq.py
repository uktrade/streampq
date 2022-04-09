from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from functools import partial
from json import loads as json_loads
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE
from ctypes import cdll, create_string_buffer, byref, c_char_p, c_void_p, c_int
from ctypes.util import find_library
from itertools import groupby


@contextmanager
def streampq_connect(
        params=(),
        get_decoders=lambda: get_default_decoders(),
        get_libpq=lambda: cdll.LoadLibrary(find_library('pq')),
):
    pq = get_libpq()

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

    PGRES_TUPLES_OK = 2
    PGRES_SINGLE_TUPLE = 9

    def as_null_terminated_array(strings):
        char_ps = tuple(c_char_p(string.encode('utf-8')) for string in strings) + (None,)
        return (c_char_p * len(char_ps))(*char_ps)

    keywords = as_null_terminated_array((param[0] for param in params))
    values = as_null_terminated_array((param[1] for param in params))

    @contextmanager
    def get_conn():
        conn = c_void_p(0)
        try:
            conn = pq.PQconnectdbParams(keywords, values, 0)
            if not conn:
                raise ConnectionError()

            status = pq.PQstatus(conn)
            if status:
                raise ConnectionError(pq.PQerrorMessage(conn))

            ok = pq.PQsetnonblocking(conn, 1)
            if ok != 0:
                raise ConnectionError(pq.PQerrorMessage(conn))

            socket = pq.PQsocket(conn)
            sel = DefaultSelector()
            yield sel, socket, conn
        finally:
            pq.PQfinish(conn)

    @contextmanager
    def cancel_query(sel, socket, conn):
        try:
            yield
        finally:
            pg_cancel = c_void_p(0)
            try:
                pg_cancel = pq.PQgetCancel(conn)
                if not pg_cancel:
                    raise CancelError(pq.PQerrorMessage(conn))
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
                raise CommunicationError(pq.PQerrorMessage(conn))
            if not incomplete:
                break
            ready_for = block_until(sel, socket, (EVENT_WRITE, EVENT_READ))
            if ready_for == EVENT_WRITE:
                continue
            if ready_for == EVENT_READ:
                ok = PQconsumeInput(conn)
                if not ok:
                    raise CommunicationError(pq.PQerrorMessage(conn))

        block_until(sel, socket, (EVENT_READ,))

    def flush_read(sel, socket, conn):
        while True:
            is_busy = pq.PQisBusy(conn)
            if not is_busy:
                break
            block_until(sel, socket, (EVENT_READ,))
            ok = pq.PQconsumeInput(conn)
            if not ok:
                raise CommunicationError(pq.PQerrorMessage(conn))

    def query(sel, socket, conn, sql):
        ok = pq.PQsendQuery(conn, sql.encode('utf-8'));
        if not ok:
            raise CommunicationError(pq.PQerrorMessage(conn))

        flush_write(sel, socket, conn)

        ok = pq.PQsetSingleRowMode(conn);
        if not ok:
            raise CommunicationError(pq.PQerrorMessage(conn))

        def get_results():
            # So we can use groupby to separate rows for different statements
            # in multi-statment queries
            group_key = object()
            while True:
                flush_read(sel, socket, conn)

                result = pq.PQgetResult(conn)
                if not result:
                    break

                try:
                    status = pq.PQresultStatus(result)
                    if status == PGRES_TUPLES_OK:
                        group_key = object()
                        continue

                    if status != PGRES_SINGLE_TUPLE:
                        raise QueryError(pq.PQerrorMessage(conn))

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

        def get_columns(grouped_results):
            for (_, columns), rows in grouped_results:
                yield columns, (row[1] for row in rows)

        results = get_results()
        grouped_results = groupby(results, key=lambda key_columns_row: key_columns_row[0])
        with_columns = get_columns(grouped_results)

        return with_columns

    with \
            get_conn() as (sel, socket, conn), \
            cancel_query(sel, socket, conn):

        yield partial(query, sel, socket, conn)


def get_default_decoders():
    return (
        (None, lambda _: None),             # null
        (20, int),                          # int8
        (23, int),                          # int4
        (25, lambda v: v),                  # text
        (114, json_loads),                  # json
        (1007, decode_array(int)),          # int4[]
        (1009, decode_array(lambda v: v)),  # text[]
        (1082, date.fromisoformat),         # date
        (1700, Decimal),                    # numeric
        (3802, json_loads),                 # jsonb
    )


def decode_array(value_decoder):
    OUT = object()
    IN_UNQUOTED = object()
    IN_QUOTED = object()
    IN_QUOTED_ESCAPE = object()

    def parse(raw):
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

    return parse


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
