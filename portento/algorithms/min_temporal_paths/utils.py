from itertools import repeat
from functools import singledispatch
from heapq import merge
from operator import itemgetter
from pandas import Interval

from portento.utils import split_in_instants, DiLink
from portento.classes import Stream, DiStream, filter_by_time, TimeFilter


@singledispatch
def _prepare_for_path_computation(stream, time_bound: Interval = None, reverse=False):
    pass


@_prepare_for_path_computation.register
def _(stream: DiStream, time_bound=None, reverse=False):
    if not time_bound:
        time_bound = [stream.stream_presence.root.full_interval]

    instants = merge(*map(lambda x: zip(split_in_instants(x.interval, stream.instant_duration),
                                        repeat(({"u": x.u,
                                                 "v": x.v}))),
                          filter_by_time(stream.tree_view, TimeFilter(time_bound), DiLink)),
                     key=itemgetter(0), reverse=reverse)

    return instants


@_prepare_for_path_computation.register
def _(stream: Stream, time_bound=None, reverse=False):
    if not time_bound:
        time_bound = stream.stream_presence.root.full_interval
