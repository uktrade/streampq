from streampq import streampq_connect

def test_streampy():
    with streampq_connect() as query:
        assert query()
