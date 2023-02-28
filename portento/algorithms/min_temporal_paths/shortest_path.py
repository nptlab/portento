from collections.abc import Hashable
from pandas import Interval
from operator import itemgetter
from sortedcontainers import SortedKeyList
from portento.classes import Stream
from portento.utils import get_start_end
from .utils import prepare_for_path_computation, update_on_new_candidate, get_idx_filtered_candidates


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
    pairs_distance_arrival_time = dict(((u, SortedKeyList(key=itemgetter(0))) for u in stream.nodes))

    for t, nodes in prepare_for_path_computation(stream, [time_bound]):
        t_plus_trav = t + stream.instant_duration
        if t_plus_trav <= end:
            u, v = nodes["u"], nodes["v"]
            if u == source:
                pairs_distance_arrival_time[source].add((0, t))

            pairs_u, pairs_v = pairs_distance_arrival_time[u].copy(), pairs_distance_arrival_time[v].copy()

            if len(pairs_u) > 0:
                # extract the pair with the largest arrival time that is less than or equal to t
                tuple_idx, to_remove_idx = get_idx_filtered_candidates(pairs_u, t)
                distance_u, arrival_u = pairs_u[tuple_idx]

                if arrival_u < t_plus_trav:
                    distance_v, arrival_v = distance_u + stream.instant_duration, t_plus_trav

                    # filter out dominated candidates
                    for i in to_remove_idx:
                        pairs_u.pop(i)

                    # compare selected pair with the tail of pairs in v
                    pairs_v = update_on_new_candidate(pairs_v, (distance_v, arrival_v), neg=False)

                    pairs_distance_arrival_time[u], pairs_distance_arrival_time[v] = pairs_u, pairs_v
                    path_distance[v] = min(path_distance[v], distance_v)

        elif t >= end:
            break

    return path_distance
