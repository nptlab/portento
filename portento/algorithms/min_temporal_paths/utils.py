from typing import List
from itertools import repeat, tee
from functools import singledispatch
from heapq import merge
from operator import itemgetter
from bisect import bisect_right
from pandas import Interval
from sortedcontainers import SortedKeyList

from portento.utils import split_in_instants, DiLink, Link
from portento.classes import Stream, DiStream, filter_by_time, TimeFilter, StreamTree


@singledispatch
def prepare_for_path_computation(stream, time_bound: List[Interval], reverse=False):
    pass


@prepare_for_path_computation.register
def _(stream: DiStream, time_bound, reverse=False):
    return _create_edge_representation(stream.tree_view, stream.instant_duration, time_bound, reverse, DiLink)


@prepare_for_path_computation.register
def _(stream: Stream, time_bound, reverse=False):
    edge_repr, edge_repr_rev = tee(_create_edge_representation(stream.tree_view,
                                                               stream.instant_duration, time_bound, reverse, Link), 2)

    return merge(edge_repr, map(lambda x: (x[0], {"u": x[1]["v"], "v": x[1]["u"]}), edge_repr_rev),
                 key=itemgetter(0), reverse=reverse)


def _create_edge_representation(stream_tree: StreamTree, instant_duration, time_bound, reverse, link_type):
    split_order = lambda x: reversed(list(x)) if reverse else list(x)
    instants = merge(*map(lambda x: split_order(zip(split_in_instants(x.interval, instant_duration),
                                                    repeat(({"u": x.u,
                                                             "v": x.v})))),
                          filter_by_time(stream_tree, TimeFilter(time_bound), link_type)),
                     key=itemgetter(0), reverse=reverse)

    return instants


def find_le_idx(a, x):
    """Find rightmost value less than or equal to x

    The code is based on the documentation of the bisect module
    """
    i = a.bisect_key_right(x)
    return max(i - 1, 0)


def find_le(a, x):
    """Find rightmost value less than or equal to x

    The code is based on the documentation of the bisect module
    """

    return a[find_le_idx(a, x)]


def filter_out_candidate(candidate_tuples, min_key):
    """Filter out candidates in which their key is less than min_key"""
    # TODO the min key is not correct
    return SortedKeyList(candidate_tuples.irange_key(min_key=min_key), key=candidate_tuples.key)


def update_on_new_candidate(candidate_tuples, candidate, neg=False):
    """Returns the updated SortedKeyList according to the new candidate"""
    mult = 1
    if neg:
        mult = -1

    candidate_val, candidate_a = candidate

    # get the tail of the SortedKeyList. If empty, return a default value that is always dominated
    tail_val, tail_a = candidate_tuples[-1] if len(candidate_tuples) > 0 else (candidate_val, candidate_a)

    candidate_val *= mult
    tail_val *= mult

    if not (tail_val <= candidate_val and tail_a <= candidate_a):  # the tail does not dominate the new tuple

        if candidate_val <= tail_val and candidate_a <= tail_a:  # the new candidate tuple dominates the tail
            candidate_tuples.discard((tail_val * mult, tail_a))

        candidate_tuples.add((candidate_val * mult, candidate_a))

    return candidate_tuples
