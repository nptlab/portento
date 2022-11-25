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


def _left_tuple(interval):
    return interval.left, 0 if interval.closed_left else 1


def _right_tuple(interval):
    return interval.right, 1 if interval.closed_right else 0


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
    min_interval = min(it_min, key=lambda x: _left_tuple(x))
    max_interval = max(it_max, key=lambda x: _right_tuple(x))

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


def contains_interval(container_interval, contained_interval):
    return _left_tuple(container_interval) <= _left_tuple(contained_interval) and \
           _right_tuple(container_interval) >= _right_tuple(contained_interval)


def _mapping_function(datum):
    if isinstance(datum, pd.Interval):
        return datum.length
    else:
        return _mapping_function(datum.interval)


def compute_presence(intervals: Iterable[pd.Interval]):
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
        return reduce(operator.add, map(_mapping_function, intervals))
    else:
        return 0
