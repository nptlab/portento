from collections.abc import Hashable
from pandas import Interval
from portento.classes import Stream
from portento.utils import get_start_end
from .utils import prepare_for_path_computation


def fastest_path_duration(stream: Stream, source: Hashable, time_bound: Interval = None):
    if not time_bound:
        time_bound = stream.stream_presence.root.full_interval

    start, end = get_start_end(time_bound, stream.instant_duration)
