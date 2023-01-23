"""

"""
from collections.abc import Hashable
from pandas import Interval
from portento.classes import Stream
from portento.utils import get_start_end
from .utils import prepare_for_path_computation


def earliest_arrival_time(stream: Stream, source: Hashable, time_bound: Interval = None):
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
