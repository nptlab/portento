import pytest
from pandas import Interval
from itertools import combinations, chain

from portento import Stream, Link, \
    filter_stream, filter_by_time, filter_by_nodes, \
    NoFilter, TimeFilter, NodeFilter


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


class TestFilter:

    @pytest.mark.parametrize('time_filter', [
        TimeFilter([Interval(2.5, 4.5)]),
        TimeFilter([Interval(4.5, 6.5), Interval(0, 1)]),
        TimeFilter([Interval(7.0, 10.0)]),
        TimeFilter([Interval(5.0, 5.0, 'both')])
    ])
    def test_time_filter(self, stream, stream_2, time_filter):
        for s in [stream, stream_2]:
            assert [i for i in filter_by_time(s.tree_view, time_filter)] == \
                   [i for i in filter_stream(s, NoFilter(), time_filter, 'time')] == \
                   [i for i in filter_stream(s, NoFilter(), time_filter, 'node')]

    def test_node_filter(self, stream, stream_2):
        for s in [stream, stream_2]:
            for three_nodes in combinations(s.nodes, 3):
                node_filter = NodeFilter(lambda x: x in three_nodes)
                assert [i for i in filter_by_nodes(s.dict_view, node_filter)] == \
                       [i for i in filter_stream(s, node_filter, NoFilter(), 'time')] == \
                       [i for i in filter_stream(s, node_filter, NoFilter(), 'node')]

    @pytest.mark.parametrize('time_filter', [
        TimeFilter([Interval(2.5, 4.5)]),
        TimeFilter([Interval(4.5, 6.5), Interval(0, 1)]),
        TimeFilter([Interval(7.0, 10.0)]),
        TimeFilter([Interval(5.0, 5.0, 'both')])
    ])
    def test_stream_filter(self, stream, stream_2, time_filter):
        for s in [stream, stream_2]:
            nodes = s.nodes.keys()
            iterables = []
            for n in range(len(nodes)):
                iterables.append(combinations(nodes, n))
            iterables = chain.from_iterable(iterables)
            for bunch_nodes in iterables:
                node_filter = NodeFilter(lambda x: x in bunch_nodes)
                assert [i for i in filter_stream(s, node_filter, time_filter, 'time')] == \
                       [i for i in filter_stream(s, node_filter, time_filter, 'node')]
