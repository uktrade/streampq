from streampq import streampq_connect

def test_streampy():
    params = (
        ('host', 'localhost'),
        ('port', '5432'),
        ('dbname', 'postgres'),
        ('user', 'postgres'),
        ('password', 'password'),
    )
    count = 0
    with streampq_connect(params) as query:
        for cols, rows in query('SELECT 1; SELECT 2'):
            for row in rows:
                count += 1

    assert count == 2
