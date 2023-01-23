"""

"""
from pandas import Interval
from portento.classes import Stream
from .utils import _prepare_for_path_computation


def earliest_arrival_time(stream: Stream, time_bound: Interval = None):
    if not time_bound:
        time_bound = [stream.stream_presence.root.full_interval]
    iterator = _prepare_for_path_computation(stream, time_bound)


