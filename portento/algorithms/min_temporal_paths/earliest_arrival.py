"""

"""
from collections.abc import Hashable
from pandas import Interval
from portento.classes import Stream
from portento.utils import get_start_end
from .utils import prepare_for_path_computation


def earliest_arrival_time(stream: Stream, source: Hashable, time_bound: Interval = None):
    """Compute the earliest arrival time from node source to each other node in the predefined time boundaries.

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
    arrival_time : dict
        The dictionary of the form {node : earliest arrival time from source node}

    """
    if not time_bound:
        time_bound = stream.stream_presence.root.full_interval

    start, end = get_start_end(time_bound, stream.instant_duration)

    arrival_time = dict(((u, start if u == source else float('inf')) for u in stream.nodes))

    for t, nodes in prepare_for_path_computation(stream, [time_bound]):
        t_plus_trav = t + stream.instant_duration
        if t_plus_trav <= end and t >= arrival_time[nodes["u"]]:
            if t_plus_trav < arrival_time[nodes["v"]]:
                arrival_time[nodes["v"]] = t_plus_trav

        elif t >= end:
            break

    return arrival_time
