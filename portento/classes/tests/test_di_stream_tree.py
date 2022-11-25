import pytest

from pandas import Interval

from portento import DiStreamTree
from portento.utils import DiLink


class TestDiStreamTree:

    def test_di_stream_tree(self):
        di_link_1 = DiLink(interval=Interval(0, 2), u=2, v=1)
        di_link_2 = DiLink(interval=Interval(1, 3), u=1, v=2)
        di_stream_tree = DiStreamTree([di_link_1, di_link_2])
        assert list(iter(di_stream_tree)) == [di_link_1, di_link_2]
