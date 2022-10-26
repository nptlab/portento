import pandas as pd
from typing import Iterable
from functools import reduce, partial
import operator


def merge_interval(interval_1: pd.Interval, interval_2: pd.Interval):
    """Merge two pandas Interval objects if they do overlap

    Parameters
    ----------
    interval_1 : pandas Interval
    interval_2 : pandas Interval

    Returns
    -------
    merged_interval : pandas Interval
    """

    if interval_1.left < interval_2.left:
        left_border = interval_1.left
        is_closed_left = interval_1.closed_left
    elif interval_1.left == interval_2.left:
        left_border = interval_1.left
        is_closed_left = interval_1.closed_left or interval_2.closed_left
    else:
        left_border = interval_2.left
        is_closed_left = interval_2.closed_left

    if interval_1.right > interval_2.right:
        right_border = interval_1.right
        is_closed_right = interval_1.closed_right
    elif interval_1.right == interval_2.right:
        right_border = interval_1.right
        is_closed_right = interval_1.closed_right or interval_2.closed_right
    else:
        right_border = interval_2.right
        is_closed_right = interval_2.closed_right

    if is_closed_left:
        if is_closed_right:
            closed = 'both'
        else:
            closed = 'left'
    else:
        if is_closed_right:
            closed = 'right'
        else:
            closed = 'neither'

    return pd.Interval(left_border, right_border, closed)


def intervaltree_boundaries(interval, precision):
    """Prepare the interval for intervaltree

    Parameters
    ----------
    interval
    precision

    Returns
    -------

    """
    # assume '+' operand defined between interval borders and precision
    begin = interval.left
    end = interval.right
    if interval.open_left:
        begin += precision
    if interval.closed_right:
        end += precision

    return begin, end


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

    closed = "neither"
    if closed_left:
        if closed_right:
            closed = "both"
        else:
            closed = "left"
    else:
        if closed_right:
            closed = "right"

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
