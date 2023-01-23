from collections.abc import Hashable
from pandas import Interval
from portento.classes import Stream
from portento.utils import get_start_end
from .utils import prepare_for_path_computation


def latest_departure_time(stream: Stream, target: Hashable, time_bound: Interval = None):
    """Compute the latest departure time from each node to the target node in the predefined time boundaries.

    Parameters
    ----------
    stream : Stream or DiStream.

    target : Node
        The ending node.

    time_bound : Interval
        The time to take into account. Needed to perform a time slice over the stream.
        Default is None.
        If time_bound is None, use the whole stream.

    Returns
    -------
    departure_time : dict
        The dictionary of the form {node : latest departure time to reach target node}

    """
    if not time_bound:
        time_bound = stream.stream_presence.root.full_interval

    start, end = get_start_end(time_bound, stream.instant_duration)

    departure_time = dict(((u, end if u == target else float('-inf')) for u in stream.nodes))

    for t, nodes in prepare_for_path_computation(stream, [time_bound], reverse=True):
        if t >= start:
            if t + stream.instant_duration <= departure_time[nodes["v"]]:
                if t > departure_time[nodes["u"]]:
                    departure_time[nodes["u"]] = t
        else:
            break

    return departure_time
