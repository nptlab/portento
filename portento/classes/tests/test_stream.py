import pytest
from pandas import Interval, Timestamp

from portento import Stream
from portento.utils import Link, compute_presence


@pytest.fixture
def stream(links):
    return Stream(links)


@pytest.fixture
def stream_2(links_2):
    return Stream(links_2)


@pytest.fixture
def links():
    return [Link(Interval(1.0, 3.0, 'both'), 'a', 'b'),
            Link(Interval(7.0, 8.0, 'both'), 'a', 'b'),
            Link(Interval(4.5, 7.5, 'both'), 'a', 'c'),
            Link(Interval(6.0, 9.0, 'both'), 'b', 'c'),
            Link(Interval(2.0, 3.0, 'both'), 'b', 'd')]


@pytest.fixture
def links_2():
    return [Link(Interval(0.0, 4.0, 'both'), 'a', 'b'),
            Link(Interval(6.0, 9.0, 'both'), 'a', 'b'),
            Link(Interval(2.0, 5.0, 'both'), 'a', 'c'),
            Link(Interval(1.0, 8.0, 'both'), 'b', 'c'),
            Link(Interval(7.0, 10.0, 'both'), 'b', 'd'),
            Link(Interval(6.0, 9.0, 'both'), 'c', 'd')]


class TestStream:

    def test_add(self, stream, stream_2):
        assert set(stream.dict_view) - set(stream.tree_view) == set()
        assert set(stream_2.dict_view) - set(stream_2.tree_view) == set()

        assert set(stream.nodes) - {'a', 'b', 'c', 'd'} == set()
        assert set(stream_2.nodes) - {'a', 'b', 'c', 'd'} == set()

        assert set(stream.node_presence('a')) - \
               {Interval(1.0, 3.0, closed='both'), Interval(4.5, 8.0, closed='both')} == set()
        assert set(stream_2.node_presence('a')) - \
               {Interval(0.0, 5.0, closed='both'), Interval(6.0, 9.0, closed='both')} == set()

    def test_graph_presence(self, stream, stream_2):
        assert stream.stream_presence_len() == 6.5
        assert stream_2.stream_presence_len() == 10.0

    # TODO time_instants with Timedelta
    """def test_graph_presence_timestamp(self):
        links = [Link(Interval(Timestamp('2017-01-01 00:00:00'), Timestamp('2018-01-01 00:00:00')), 'a', 'b'),
                 Link(Interval(Timestamp('2018-01-01 00:00:00'), Timestamp('2019-01-01 00:00:00')), 'a', 'b'),
                 Link(Interval(Timestamp('2019-01-01 00:00:00'), Timestamp('2020-01-01 00:00:00')), 'a', 'b')]
        s = Stream(links)
        assert s.stream_presence_len() == Interval(Timestamp('2017-01-01 00:00:00'),
                                                   Timestamp('2020-01-01 00:00:00')).length.total_seconds()"""

    def test_presence(self, stream, stream_2):
        assert stream.stream_presence_len() == compute_presence(stream.stream_presence, stream.interval_type)
        assert stream_2.stream_presence_len() == compute_presence(stream_2.stream_presence, stream_2.interval_type)

        for s in [stream, stream_2]:
            for u in s.nodes:
                assert s.node_presence_len(u) == compute_presence(s.node_presence(u), s.interval_type)
                if u in s.edges:
                    for v in s.edges[u]:
                        assert s.link_presence_len(u, v) == compute_presence(s.link_presence(u, v), s.interval_type)
