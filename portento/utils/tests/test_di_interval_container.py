import pytest

from pandas import Interval
from functools import partial

from portento.utils import DiLink, DiIntervalContainer


class TestDiIntervalContainer:

    def test_di_interval_container(self):
        DiLink_10_5 = partial(DiLink, u=10, v=5)
        di_links = [DiLink_10_5(interval=Interval(0, 1)), DiLink_10_5(interval=Interval(1, 2, 'both'))]
        di_interval_container = DiIntervalContainer(10, 5)
        for link in di_links:
            di_interval_container.add(link)
        assert list(iter(di_interval_container)) == [DiLink(Interval(0, 2), 10, 5)]

