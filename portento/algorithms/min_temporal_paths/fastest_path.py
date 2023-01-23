from collections.abc import Hashable
from functools import singledispatch
from pandas import Interval
from portento.classes import Stream, DiStream, StreamDict, DiStreamDict, filter_by_time, TimeFilter
from portento.utils import get_start_end, Link, DiLink
from .earliest_arrival import earliest_arrival_time
from .utils import prepare_for_path_computation


def fastest_path_duration(stream: Stream, source: Hashable, time_bound: Interval = None):
    """Compute the latest departure time from each node to the target node in the predefined time boundaries.

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
    if not time_bound:
        time_bound = stream.stream_presence.root.full_interval

    start, end = get_start_end(time_bound, stream.instant_duration)


@singledispatch
def fastest_path_duration_multipass(stream, source: Hashable, time_bound: Interval = None):
    """Compute the latest departure time from each node to the target node in the predefined time boundaries.

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
    pass


@fastest_path_duration_multipass.register
def _(stream: DiStream, source: Hashable, time_bound: Interval = None):
    if not time_bound:
        time_bound = stream.stream_presence.root.full_interval
    else:
        start_out_edges = (stream[source])
        stream = DiStream(filter_by_time(stream.tree_view, TimeFilter([time_bound]), DiLink))

    start, end = get_start_end(time_bound, stream.instant_duration)
    path_duration = dict(((u, 0 if u == source else float('inf')) for u in stream.nodes))


@fastest_path_duration_multipass.register
def _(stream: Stream, source: Hashable, time_bound: Interval = None):
    if not time_bound:
        time_bound = stream.stream_presence.root.full_interval

    start, end = get_start_end(time_bound, stream.instant_duration)
    path_duration = dict(((u, 0 if u == source else float('inf')) for u in stream.nodes))

    for t in map(lambda link: get_start_end(link.interval, stream.instant_duration)[0], stream[source]):
        pass
