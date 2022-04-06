from streampq import streampq_connect

def test_streampy():
    params = (
        ('host', 'localhost'),
        ('port', '5432'),
        ('dbname', 'postgres'),
        ('user', 'postgres'),
        ('password', 'password'),
    )
    sql = 'SELECT 1 as "first"; SELECT 1,2'
    with streampq_connect(params) as query: 
        results = [
            (cols, list(rows))
            for cols, rows in query(sql)
        ]

    assert results[0][0] == ('first',)
    assert len(results[0][1]) == 1
    assert results[1][0] == ('?column?', '?column?')
    assert len(results[1][1]) == 1
