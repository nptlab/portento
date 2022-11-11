import pandas as pd
from typing import Iterable
from functools import reduce, partial
from itertools import tee
import operator


def compute_closure(closed_left, closed_right):
    closed = 'neither'
    if closed_left:
        if closed_right:
            closed = "both"
        else:
            closed = "left"
    else:
        if closed_right:
            closed = "right"
    return closed


def merge_interval(*intervals):
    """Merge pandas Interval objects

        Parameters
        ----------
        intervals: Iterable[pd.Interval]

        Returns
        -------
        merged_interval : pandas Interval
        """
    it_min, it_max = tee(filter(lambda x: bool(x), intervals), 2)
    min_interval = min(it_min, key=lambda x: (x.left, 0 if x.closed_left else 1))
    max_interval = max(it_max, key=lambda x: (x.right, 1 if x.closed_right else 0))

    closed = compute_closure(min_interval.closed_left, max_interval.closed_right)

    return pd.Interval(min_interval.left, max_interval.right, closed)


def cut_interval(interval: pd.Interval, cutting_interval: pd.Interval) -> pd.Interval:
    """

    Parameters
    ----------
    interval
    cutting_interval

    Returns
    -------

    """
    new_left = interval.left
    new_right = interval.right
    closed_left = interval.closed_left
    closed_right = interval.closed_right

    if cutting_interval.left > interval.left:
        new_left = cutting_interval.left
        closed_left = cutting_interval.closed_left
    elif cutting_interval.left == interval.left:
        closed_left = min(cutting_interval.closed_left, closed_left)

    if cutting_interval.right < interval.right:
        new_right = cutting_interval.right
        closed_right = cutting_interval.closed_right
    elif cutting_interval.right == interval.right:
        closed_right = min(cutting_interval.closed_right, closed_right)

    closed = compute_closure(closed_left, closed_right)

    return pd.Interval(new_left, new_right, closed)


def _mapping_function(datum, interval_type):
    if isinstance(datum, pd.Interval):
        return datum.length
    else:
        return _mapping_function(datum.interval, interval_type)


def compute_presence(intervals: Iterable[pd.Interval], interval_type):
    """Sum of all lengths of intervals in the iterable

    If the type of the boundaries is Timestamp or Timedelta, the result return is the total seconds.

    Parameters
    ----------
    intervals : Iterable
        Iterable of pandas Interval objects
    interval_type : type
        The type of the intervals boundaries

    Returns
    -------
    float
    """
    if intervals:
        return reduce(operator.add, map(partial(_mapping_function, interval_type=interval_type), intervals))
    else:
        return 0
