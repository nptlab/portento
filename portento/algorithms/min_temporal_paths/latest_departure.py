from collections.abc import Hashable
from pandas import Interval
from portento.classes import Stream
from portento.utils import get_start_end
from .utils import prepare_for_path_computation


def latest_departure_time(stream: Stream, target: Hashable, time_bound: Interval = None):
    if not time_bound:
        time_bound = stream.stream_presence.root.full_interval

    start, end = get_start_end(time_bound, stream.instant_duration)

    arrival_time = dict(((u, end if u == target else float('-inf')) for u in stream.nodes))

    for t, nodes in prepare_for_path_computation(stream, [time_bound], reverse=True):
        if t >= start:
            if t + stream.instant_duration <= arrival_time[nodes["v"]]:
                if t > arrival_time[nodes["u"]]:
                    arrival_time[nodes["u"]] = t
        else:
            break

    return arrival_time
