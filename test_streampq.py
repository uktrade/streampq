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
        with query('SELECT 1; SELECT 2') as rows:
            for row in rows:
                count += 1

    assert count == 2
