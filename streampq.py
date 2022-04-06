from contextlib import contextmanager
from functools import partial
from ctypes import cdll, c_char_p, c_void_p, c_int
from ctypes.util import find_library
from itertools import groupby


@contextmanager
def streampq_connect(params=(), get_libpq=lambda: cdll.LoadLibrary(find_library('pq'))):
    pq = get_libpq()

    pq.PQconnectdbParams.restype = c_void_p
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
    pq.PQgetvalue.argtypes = (c_void_p, c_int, c_int)
    pq.PQgetvalue.restype = c_char_p
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
            yield conn
        finally:
            pq.PQfinish(conn)

    def query(conn, sql):
        ok = pq.PQsendQuery(conn, sql.encode('utf-8'));
        if not ok:
            raise Exception()

        ok = pq.PQsetSingleRowMode(conn);
        if not ok:
            raise Exception()

        def get_results():
            # So we can use groupby to separate rows for different statements
            # in multi-statment queries
            group_key = object()

            while True:
                result = pq.PQgetResult(conn)
                if not result:
                    break

                try:
                    status = pq.PQresultStatus(result)
                    if status == PGRES_TUPLES_OK:
                        group_key = object()
                        continue

                    if status != PGRES_SINGLE_TUPLE:
                        continue

                    num_columns = pq.PQnfields(result)
                    columns = tuple(
                        pq.PQfname(result, i).decode('utf-8')
                        for i in range(0, num_columns)
                    )
                    values = tuple(
                        pq.PQgetvalue(result, 0, i)
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

    with get_conn() as conn:
        yield partial(query, conn)
