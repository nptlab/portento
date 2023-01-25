from collections.abc import Hashable
from functools import singledispatch
from pandas import Interval
from operator import itemgetter
from sortedcontainers import SortedKeyList
from portento.classes import Stream, DiStream
from portento.utils import get_start_end
from .earliest_arrival import earliest_arrival_time
from .utils import prepare_for_path_computation, find_le, find_le_idx, update_on_new_candidate, filter_out_candidate


def fastest_path_duration(stream: Stream, source: Hashable, time_bound: Interval = None):
    """Compute the fastest path duration from the source node to each other node in the predefined time boundaries.

    Parameters
    ----------
    stream : Stream or DiStream.

    source : Node
        The starting node.

    time_bound : Interval
        The time to take into account. Needed to perform a time slice over the stream.
        Default is None.
        If time_bound is None, use the whole stream.

    Returns
    -------
    path_duration : dict
        The dictionary of the form {node : duration of the fastest path from source node}

    """
    if not (source in stream):
        raise AttributeError("The source node must be present in the stream.")

    if not time_bound:
        time_bound = stream.stream_presence.root.full_interval

    start, end = get_start_end(time_bound, stream.instant_duration)
    path_duration = dict(((u, 0 if u == source else float('inf')) for u in stream.nodes))
    pairs_start_arrival_time = dict(((u, SortedKeyList(key=itemgetter(1))) for u in stream.nodes))

    for t, nodes in prepare_for_path_computation(stream, [time_bound]):
        if t >= start and t + stream.instant_duration <= end:
            u, v = nodes["u"], nodes["v"]
            if u == source:
                pairs_start_arrival_time[source].add((t, t))

            pairs_u, pairs_v = pairs_start_arrival_time[u].copy(), pairs_start_arrival_time[v].copy()

            # extract the pair with the largest arrival time that is less than or equal to t
            if len(pairs_u) > 0:

                starting_u, arrival_u = find_le(pairs_u, t)
                starting_v, arrival_v = starting_u, t + stream.instant_duration

                # filter out dominated candidates
                filter_out_candidate(pairs_u, arrival_u)

                # compare selected pair with the tail of pairs in v
                update_on_new_candidate(pairs_v, (starting_v, arrival_v))

                pairs_start_arrival_time[u], pairs_start_arrival_time[v] = pairs_u, pairs_v

                path_duration[v] = min(path_duration[v], arrival_v - starting_v)

        elif t >= end:
            break

    return path_duration


@singledispatch
def fastest_path_duration_multipass(stream, source: Hashable, time_bound: Interval = None):
    """Compute the fastest path duration from the source node to each other node in the predefined time boundaries.

    DEPRECATED
    This implementation calls the earliest_arrival_time method for each instant in the time bound.
    This is kept for performance comparisons with the standard implementation fastest_path_duration.

    Parameters
    ----------
    stream : Stream or DiStream.

    source : Node
        The starting node.

    time_bound : Interval
        The time to take into account. Needed to perform a time slice over the stream.
        Default is None.
        If time_bound is None, use the whole stream.

    Returns
    -------
    path_duration : dict
        The dictionary of the form {node : duration of the fastest path from source node}

    """
    if not (source in stream):
        raise AttributeError("The source node must be present in the stream.")


@fastest_path_duration_multipass.register
def _(stream: DiStream, source: Hashable, time_bound: Interval = None):
    return _fastest_path_call_earliest_arrival(stream, source, time_bound, lambda x: x.u == source)


@fastest_path_duration_multipass.register
def _(stream: Stream, source: Hashable, time_bound: Interval = None):
    return _fastest_path_call_earliest_arrival(stream, source, time_bound)


def _fastest_path_call_earliest_arrival(stream, source, time_bound, filter_links=lambda x: x):
    if not time_bound:
        time_bound = stream.stream_presence.root.full_interval

    start, end = get_start_end(time_bound, stream.instant_duration)
    path_duration = dict(((u, 0 if u == source else float('inf')) for u in stream.nodes))

    for t in map(lambda link: get_start_end(link.interval, stream.instant_duration)[0],
                 filter(filter_links, stream[source])):
        if t >= start and t + stream.instant_duration <= end:
            earliest_arrival = earliest_arrival_time(stream, source, Interval(t, end, 'both'))

            path_duration = dict(((u, min(path_duration[u], earliest_arrival[u] - t)) for u in stream.nodes))

    return path_duration
