from contextlib import contextmanager
from ctypes import cdll, c_char_p, c_void_p
from ctypes.util import find_library

@contextmanager
def streampq_connect(params=(), get_libpq=lambda: cdll.LoadLibrary(find_library('pq'))):
    pq = get_libpq()

    pq.PQconnectdbParams.restype = c_void_p
    pq.PQfinish.argtypes = (c_void_p,)
    pq.PQstatus.argtypes = (c_void_p,)
    pq.PQexec.argtypes = (c_void_p, c_char_p)
    pq.PQexec.restype = c_void_p
    pq.PQclear.argtypes = (c_void_p,)

    def as_null_terminated_array(strings):
        char_ps = tuple(c_char_p(string.encode('utf-8')) for string in strings) + (None,)
        arr = (c_char_p * len(char_ps))()
        arr[:] = char_ps
        return arr

    keywords = as_null_terminated_array(tuple(param[0] for param in params))
    values = as_null_terminated_array(tuple(param[1] for param in params))

    conn = c_void_p(pq.PQconnectdbParams(keywords, values, 0))
    if not conn:
        raise Exception()

    status = pq.PQstatus(conn)
    if status:
        raise Exception()

    @contextmanager
    def query(sql):
        pg_result = pq.PQexec(conn, sql.encode('utf-8'));
        try:
            yield pg_result
        finally:
            pq.PQclear(pg_result)

    try:
        yield query
    finally:
        pq.PQfinish(conn)
