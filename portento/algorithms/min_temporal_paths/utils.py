from typing import List
from itertools import repeat, tee
from functools import singledispatch
from heapq import merge
from operator import itemgetter
from pandas import Interval
from sortedcontainers import SortedKeyList

from portento.utils import split_in_instants, DiLink, Link
from portento.classes import Stream, DiStream, StreamTree
from portento.slicing import slice_by_time, TimeFilter


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
                          slice_by_time(stream_tree, TimeFilter(time_bound), link_type)),
                     key=itemgetter(0), reverse=reverse)

    return instants


def dominates(tuple_1, tuple_2, neg=True):
    tuple_1_val, tuple_1_a = tuple_1
    tuple_2_val, tuple_2_a = tuple_2

    if neg:
        tuple_1_val *= -1
        tuple_2_val *= -1

    return ((tuple_1_val < tuple_2_val and tuple_1_a <= tuple_2_a) or
            (tuple_1_val == tuple_2_val and tuple_1_a < tuple_2_a))


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


def filter_out_candidate(candidate_tuples, dominating_idx):
    """Filter out candidates in which their key is less than min_key"""

    return SortedKeyList(candidate_tuples[dominating_idx:], key=candidate_tuples.key)


def get_idx_filtered_candidates(candidate_tuples, t):
    """Find the index of the tuple with max a that is <= t. Also return dominated indexes of dominated tuples"""
    to_remove_idx = []
    tuple_idx, tuple_arrival = 0, float('-inf')
    for i, (distance, arrival) in enumerate(candidate_tuples):
        if arrival < t:
            if tuple_arrival < arrival:
                tuple_arrival = arrival
                tuple_idx = i
        else:
            to_remove_idx.append(i)

    return tuple_idx, to_remove_idx


def update_on_new_candidate(candidate_tuples, candidate, neg=True):
    """Returns the updated SortedKeyList according to the new candidate"""

    # get the tail of the SortedKeyList. If empty, return a default value that is always dominated
    tail = candidate_tuples.pop() if len(candidate_tuples) > 0 else (float('inf') * (-1 if neg else 1), float('inf'))

    if dominates(candidate, tail, neg):  # the candidate tuple dominates the tail
        candidate_tuples.add(candidate)
    elif dominates(tail, candidate, neg):  # the tail dominates the candidate tuple
        candidate_tuples.add(tail)
    elif tail != candidate:  # there's no domination and the two tuples are different
        candidate_tuples.add(tail)
        candidate_tuples.add(candidate)
    else:
        candidate_tuples.add(tail)

    return candidate_tuples
