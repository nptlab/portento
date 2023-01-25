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
    if not (source in stream):
        raise AttributeError("The source node must be present in the stream.")

    if not time_bound:
        time_bound = stream.stream_presence.root.full_interval

    start, end = get_start_end(time_bound, stream.instant_duration)
    path_distance = dict(((u, 0 if u == source else float('inf')) for u in stream.nodes))
    pairs_distance_arrival_time = dict(((u, SortedKeyList(key=itemgetter(1))) for u in stream.nodes))

    for t, nodes in prepare_for_path_computation(stream, [time_bound]):
        if t >= start and t + stream.instant_duration <= end:
            u, v = nodes["u"], nodes["v"]
            if u == source:
                pairs_distance_arrival_time[source].add((0, t))

            distance_u, _ = min(pairs_distance_arrival_time[u].irange_key(min_key=0, max_key=t),
                                default=(float('inf'), float('inf')))
            distance_v = distance_u + stream.instant_duration
            arrival_v = (t + stream.instant_duration) if distance_v < float('inf') else float('inf')
            pairs_distance_arrival_time[v].add((distance_v, arrival_v))

            # TODO remove dominated elements
            path_distance[v] = min(path_distance[v], distance_v)

        elif t >= end:
            break

    return path_distance
