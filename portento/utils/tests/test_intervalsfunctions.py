import pytest
import pandas as pd
import numpy as np

from portento.utils import merge_interval, split_in_instants


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


class TestIntervalSplit:

    @pytest.mark.parametrize('interval,instant_duration,res', [
        (pd.Interval(0, 10, 'neither'), 1, (1, 10, 1)),
        (pd.Interval(0, 1, 'right'), 0.1, (0.1, 1.1, 0.1)),
        (pd.Interval(0, 1, 'left'), 0.2, (0, 1, 0.2))
    ])
    def test_interval_split(self, interval, instant_duration, res):
        n_digits = len((str(instant_duration) + ".").split(".")[1])
        res_list = []
        start, end, step = res
        counter = start
        while counter < end:
            res_list.append(counter)
            counter = round(counter + step, ndigits=n_digits)

        assert list(split_in_instants(interval, instant_duration)) == res_list
