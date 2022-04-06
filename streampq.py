from contextlib import contextmanager
from ctypes import cdll
from ctypes.util import find_library

@contextmanager
def streampq_connect(get_libpq=lambda: cdll.LoadLibrary(find_library('pq'))):
	pq = get_libpq()

	def query():
		return pq.PQconnectdbParams

	yield query
