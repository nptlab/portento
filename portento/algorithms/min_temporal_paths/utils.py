from typing import List
from itertools import repeat, tee
from functools import singledispatch
from heapq import merge
from operator import itemgetter
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


def remove_dominated_elements(tuples_list: SortedKeyList, new_tuple, dominated_by=lambda x, y: x >= y):
    """Suppose the tuple list is a SortedKeyList (key is the first element of the tuple)"""
    new_tuples_list = tuples_list.copy()
    # if the new tuple is dominated or already in the list, do nothing
    for candidate in new_tuples_list.irange_key(min_key=0, max_key=new_tuple[0], reverse=True):
        if dominated_by(new_tuple, candidate):
            return new_tuples_list

    for candidate in new_tuples_list.irange_key(min_key=new_tuple[0]):
        if dominated_by(candidate, new_tuple):
            new_tuples_list.remove(candidate)

    new_tuples_list.add(new_tuple)

    return new_tuples_list




