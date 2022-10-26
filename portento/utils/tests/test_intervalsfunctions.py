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
