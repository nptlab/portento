from collections.abc import Hashable
from pandas import Interval
from operator import itemgetter
from sortedcontainers import SortedKeyList
from portento.classes import Stream
from portento.utils import get_start_end
from .utils import prepare_for_path_computation


def shortest_path_distance(stream: Stream, source: Hashable, time_bound: Interval = None):
    """Compute the distance (number of hops in the shortest path) from source node to each other node
    in the predefined time boundaries.

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
    distance : dict
        The dictionary of the form {node : distance from the source}

    """
    pass
