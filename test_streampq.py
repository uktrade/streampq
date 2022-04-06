from streampq import streampq_connect

def test_streampy():
    with streampq_connect((('host', 'localhost'),)) as query:
        assert query()
