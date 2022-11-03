import pytest
from pandas import Interval

from portento import StreamTree
from portento.utils import Link, cut_interval


@pytest.fixture
def stream(links):
    return StreamTree(links)


@pytest.fixture
def stream_2(links_2):
    return StreamTree(links_2)


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


class TestStreamTree:

    def test_iter(self, links, links_2):
        assert list(StreamTree(links)) == sorted(links)
        assert list(StreamTree(links_2)) == sorted(links_2)

    @pytest.mark.parametrize('new_link,result', [
        (Link(Interval(3.0, 7.0, 'both'), 'a', 'b'), [Link(interval=Interval(1.0, 8.0, closed='both'), u='a', v='b'),
                                                      Link(interval=Interval(2.0, 3.0, closed='both'), u='b', v='d'),
                                                      Link(interval=Interval(4.5, 7.5, closed='both'), u='a', v='c'),
                                                      Link(interval=Interval(6.0, 9.0, closed='both'), u='b', v='c')]),
        (Link(Interval(3.0, 7.0), 'a', 'b'), [Link(interval=Interval(1.0, 3.0, closed='both'), u='a', v='b'),
                                              Link(interval=Interval(2.0, 3.0, closed='both'), u='b', v='d'),
                                              Link(interval=Interval(3.0, 8.0, closed='right'), u='a', v='b'),
                                              Link(interval=Interval(4.5, 7.5, closed='both'), u='a', v='c'),
                                              Link(interval=Interval(6.0, 9.0, closed='both'), u='b', v='c')]),
        (Link(Interval(4.0, 5.0), 'a', 'b'), [Link(interval=Interval(1.0, 3.0, closed='both'), u='a', v='b'),
                                              Link(interval=Interval(2.0, 3.0, closed='both'), u='b', v='d'),
                                              Link(interval=Interval(4.0, 5.0, closed='right'), u='a', v='b'),
                                              Link(interval=Interval(4.5, 7.5, closed='both'), u='a', v='c'),
                                              Link(interval=Interval(6.0, 9.0, closed='both'), u='b', v='c'),
                                              Link(interval=Interval(7.0, 8.0, closed='both'), u='a', v='b')])])
    def test_add_links(self, links, new_link, result):
        tree = StreamTree(links)
        tree.add(new_link)
        assert list(tree) == sorted(result)
