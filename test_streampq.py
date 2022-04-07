from datetime import date
from decimal import Decimal
from streampq import streampq_connect

def test_streampy():
    params = (
        ('host', 'localhost'),
        ('port', '5432'),
        ('dbname', 'postgres'),
        ('user', 'postgres'),
        ('password', 'password'),
    )
    sql = '''
        SELECT 1 as "first";
        SELECT 1,'2',3.3,'2021-01-01'::date,'{"a":2}'::jsonb;
    '''
    with streampq_connect(params) as query: 
        results = [
            (cols, list(rows))
            for cols, rows in query(sql)
        ]

    assert results[0][0] == ('first',)
    assert results[0][1] == [(1,)]
    assert results[1][0] == ('?column?',) * 3 + ('date',) + ('jsonb',)
    assert results[1][1] == [(1, '2', Decimal('3.3'), date(2021, 1, 1), {'a': 2})]
