import pytest
from pandas import Interval

from portento import StreamDict
from portento.utils import Link


@pytest.fixture
def stream(links):
    return StreamDict(links)


@pytest.fixture
def stream_2(links_2):
    return StreamDict(links_2)


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


class TestStreamDict:

    # TODO parametrize with fixtures
    def test_iter(self, links, links_2):
        assert [link for link in StreamDict(links)] == sorted(links)
        assert [link for link in StreamDict(links_2)] == sorted(links_2)

    def test_getitem(self, links, links_2):
        stream = StreamDict(links)
        stream_2 = StreamDict(links_2)
        assert list(stream_2['a']) == sorted([Link(Interval(0.0, 4.0, 'both'), 'a', 'b'),
                                        Link(Interval(2.0, 5.0, 'both'), 'a', 'c'),
                                        Link(Interval(6.0, 9.0, 'both'), 'a', 'b')])
        assert list(stream['c']) == sorted([Link(Interval(4.5, 7.5, 'both'), 'a', 'c'),
                                      Link(Interval(6.0, 9.0, 'both'), 'b', 'c')])
        assert list(stream['b', 'a']) == sorted([Link(Interval(1.0, 3.0, 'both'), 'a', 'b'),
                                           Link(Interval(7.0, 8.0, 'both'), 'a', 'b')])

    @pytest.mark.parametrize('new_link,result,node', [
        (Link(Interval(3.0, 6.0, 'neither'), 'b', 'c'), [Interval(1.0, 3.0, 'both'),
                                                         Interval(3.0, 6.0, 'neither'),
                                                         Interval(6.0, 9.0, 'both')], 'b'),
        (Link(Interval(3.0, 9.0, 'neither'), 'c', 'b'), [Interval(3.0, 9.0, 'right')], 'c')
    ])
    def test_add_nodes(self, links, new_link, result, node):
        s = StreamDict(links)
        s.add(new_link)
        assert [interval for interval in s.nodes[node]] == result

    @pytest.mark.parametrize('new_link,result,node_1,node_2', [
        (Link(Interval(4.0, 6.0, 'both'), 'b', 'a'), [Link(Interval(0.0, 9.0, 'both'), 'a', 'b')], 'a', 'b'),
        (Link(Interval(5.0, 6.0, 'left'), 'd', 'c'), [Link(Interval(5.0, 6.0, 'left'), 'c', 'd'),
                                                      Link(Interval(6.0, 9.0, 'both'), 'c', 'd')], 'c', 'd')
    ])
    def test_add_edges(self, links_2, new_link, result, node_1, node_2):
        s = StreamDict(links_2)
        s.add(new_link)
        assert [link for link in s.edges[node_1][node_2]] == result
