import pytest
import pandas as pd

from portento.utils import merge_interval
from portento.exception import PortentoMergeException


class TestIntervalMerge:

    @pytest.mark.parametrize("interval_1,interval_2,args", [
        (pd.Interval(4, 6, "neither"), pd.Interval(4, 6, "both"), (4, 6, 'both')),
        (pd.Interval(0, 4, "left"), pd.Interval(0, 4, "right"), (0, 4, 'both')),
        (pd.Interval(3, 4, "neither"), pd.Interval(3, 4, "left"), (3, 4, 'left')),
        (pd.Interval(0, 4, "right"), pd.Interval(3, 5, "left"), (0, 5, 'neither')),
        (pd.Interval(0, 0, "both"), pd.Interval(0, 5, "both"), (0, 5, 'both'))
    ])
    def test_interval_merge(self, interval_1, interval_2, args):
        left, right, close = args
        assert merge_interval(interval_1, interval_2) == pd.Interval(left, right, close)

    @pytest.mark.parametrize('intervals,args', [
        ((pd.Interval(0, 1, 'neither'), pd.Interval(4, 5, 'left'), pd.Interval(2, 3, 'both')), (0, 5, 'neither')),
        ((pd.Interval(5, 10, 'both'), pd.Interval(5, 6, 'neither'), pd.Interval(9, 10, 'neither')), (5, 10, 'both')),
    ])
    def test_interval_merge_iter(self, intervals, args):
        left, right, close = args
        assert merge_interval(*intervals) == pd.Interval(left, right, close)
