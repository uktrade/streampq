from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from functools import partial
from json import loads as json_loads
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE
from ctypes import cdll, c_char_p, c_void_p, c_int
from ctypes.util import find_library
from itertools import groupby


@contextmanager
def streampq_connect(
        params=(),
        encoders=(
            (None, lambda _: None),     # null
            (23, int),                  # int4
            (25, lambda v: v),          # text
            (114, json_loads),          # json
            (1082, date.fromisoformat), # date
            (1700, Decimal),            # numeric
            (3802, json_loads),         # jsonb
        ),
        get_libpq=lambda: cdll.LoadLibrary(find_library('pq')),
):
    pq = get_libpq()

    encoders_dict = dict(encoders)
    identity = lambda v: v

    pq.PQconnectdbParams.restype = c_void_p
    pq.PQsocket.argtypes = (c_void_p,)
    pq.PQsetnonblocking.argtypes = (c_void_p, c_int)
    pq.PQflush.argtypes = (c_void_p,)
    pq.PQconsumeInput.argtypes = (c_void_p,)
    pq.PQisBusy.argtypes = (c_void_p,)
    pq.PQfinish.argtypes = (c_void_p,)
    pq.PQstatus.argtypes = (c_void_p,)
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

    PGRES_TUPLES_OK = 2
    PGRES_SINGLE_TUPLE = 9

    def as_null_terminated_array(strings):
        char_ps = tuple(c_char_p(string.encode('utf-8')) for string in strings) + (None,)
        arr = (c_char_p * len(char_ps))()
        arr[:] = char_ps
        return arr

    keywords = as_null_terminated_array(tuple(param[0] for param in params))
    values = as_null_terminated_array(tuple(param[1] for param in params))

    @contextmanager
    def get_conn():
        conn = pq.PQconnectdbParams(keywords, values, 0)
        if not conn:
            raise Exception()

        try:
            status = pq.PQstatus(conn)
            if status:
                raise Exception()

            ok = pq.PQsetnonblocking(conn, 1)
            if ok != 0:
                raise Exception()

            socket = pq.PQsocket(conn)
            sel = DefaultSelector()
            yield sel, socket, conn
        finally:
            pq.PQfinish(conn)

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
                raise Exception()
            if not incomplete:
                break
            ready_for = block_until(sel, socket, (EVENT_WRITE, EVENT_READ))
            if ready_for == EVENT_WRITE:
                continue
            if ready_for == EVENT_READ:
                ok = PQconsumeInput(conn)
                if not ok:
                    raise Exception()

        block_until(sel, socket, (EVENT_READ,))

    def flush_read(sel, socket, conn):
        while True:
            is_busy = pq.PQisBusy(conn)
            if not is_busy:
                break
            block_until(sel, socket, (EVENT_READ,))
            ok = pq.PQconsumeInput(conn)
            if not ok:
                raise Exception()

    def query(sel, socket, conn, sql):
        ok = pq.PQsendQuery(conn, sql.encode('utf-8'));
        if not ok:
            raise Exception()

        flush_write(sel, socket, conn)

        ok = pq.PQsetSingleRowMode(conn);
        if not ok:
            raise Exception()

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
                        raise Exception(status)

                    num_columns = pq.PQnfields(result)
                    columns = tuple(
                        pq.PQfname(result, i).decode('utf-8')
                        for i in range(0, num_columns)
                    )
                    values = tuple(
                        encoders_dict.get(
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

    with get_conn() as (sel, socket, conn):
        yield partial(query, sel, socket, conn)
