"""Functional interface to Stream methods

For metrics the interface follows the notation of

Latapy, Matthieu, Tiphaine Viard, and Clémence Magnien.
"Stream graphs and link streams for the modeling of interactions over time."
Social Network Analysis and Mining 8.1 (2018): 1-29.

"""
from collections.abc import Hashable

import pandas as pd
from operator import truediv
from functools import partial
from itertools import chain, combinations, permutations
from more_itertools import flatten, unzip

from portento.utils import IntervalTree
from .stream import Stream, DiStream
from .filters import filter_by_time, TimeFilter

pair_permutations = partial(permutations, r=2)
pair_combinations = partial(combinations, r=2)


# TODO implement functional interface for non-modifying methods of Stream

def V(stream: Stream):
    """The set of nodes

    """
    return stream.nodes.keys()


def card_V(stream: Stream):
    """Cardinality of the set of nodes.

    """
    return len(stream.nodes)


def V_t(stream: Stream, t: pd.Interval):
    """Set of nodes that are present in a certain time instant.

    """
    return set(
        flatten(
            list(
                map(lambda link: [link.u, link.v],
                    filter_by_time(stream.tree_view, TimeFilter([t]))))))


def card_V_t(stream: Stream, t: pd.Interval):
    """Cardinality of the set of nodes that are present in a certain time instant.

    """
    return len(V_t(stream, t))


def node_contribution_of_t(stream: Stream, t: pd.Interval):
    """Contribution of the time instant referring to nodes in the stream.

    """
    return truediv(card_V_t(stream, t), card_V(stream))


def E(stream: Stream):
    """The set of temporal links.

    """
    return stream.edges


def card_E(stream: Stream):
    """Cardinality of the set of temporal links as the sum of the amount of time instants in which a link is present.
    NOT equivalent to len(E(stream)).

    """
    return sum(stream.link_presence_len(u, v) for u in E(stream) for v in E(stream)[u])


def E_t(stream: Stream, t: pd.Interval):
    """Set of links that are present in a certain time instant.

    """
    return set(
        list(
            map(lambda link: {link.u, link.v},
                filter_by_time(stream.tree_view, TimeFilter([t])))))


def card_E_t(stream: Stream, t: pd.Interval):
    """Cardinality of the set of links that are present in a certain time instant.

    """
    return len(E_t(stream, t))


def link_contribution_of_t(stream: Stream, t: pd.Interval):
    """Contribution of the time instant referring to links in the stream.

    """
    return truediv(card_E_t(stream, t), card_E(stream))


def T(stream: Stream):
    """The set of intervals in which the stream is present (at least a link is present).

    """
    return stream.stream_presence


def card_T(stream: Stream):
    """The number of time instants in which the stream is present (at least a link is present).

    """
    return stream.stream_presence_len()


def T_u(stream: Stream, u: Hashable):
    """Node presence as the intervals in which a node is present (is at least in a link).

    """
    return stream.node_presence(u)


def card_T_u(stream: Stream, u: Hashable):
    """The number of time instants in which a node is present (is at least in an link).

    """
    return stream.node_presence_len(u)


def contribution_of_node(stream: Stream, node: Hashable):
    """Contribution of the node in the stream.

    """
    return truediv(card_T_u(stream, node), card_T(stream))


def T_u_v(stream: Stream, u: Hashable, v: Hashable):
    """Node presence as the intervals in which a node is present (is at least in a link).

    """
    return stream.link_presence(u, v)


def card_T_u_v(stream: Stream, u: Hashable, v: Hashable):
    """The number of time instants in which a node is present (is at least in a link).

    """
    return stream.link_presence_len(u, v)


def contribution_of_link(stream: Stream, u: Hashable, v: Hashable):
    """Contribution of the link in the stream.

    """
    return truediv(card_T_u_v(stream, u, v), card_T(stream))


def W(stream: Stream):
    """The set of temporal nodes.

    """
    return stream.nodes


def card_W(stream: Stream):
    """Cardinality of the set of temporal nodes as the sum of the amount of time instants in which a node is present.
    NOT equivalent to len(W(stream)).

    """
    return sum(stream.node_presence_len(node) for node in stream.nodes)


def coverage(stream: Stream):
    """The coverage of the stream

    """
    return truediv(card_W(stream), (card_T(stream) * card_V(stream)))


def number_of_nodes(stream: Stream):
    """The summation of the contributions of all nodes.

    """
    return truediv(card_W(stream), card_T(stream))


def number_of_links(stream: Stream):
    """The summation of the contributions of all links.

    """
    return truediv(card_E(stream), card_T(stream))


def node_duration(stream: Stream):
    """Duration of the stream where each time instant contributes proportionally to the number of nodes
    present at this time.

    """
    return truediv(card_W(stream), card_V(stream))


def link_duration(stream: Stream):
    """Duration of the stream where each time instant contributes proportionally to the number of links
    present at this time.

    """
    card_v_x_v = _card_set_unordered_pairs_distinct_elements(card_V(stream))
    return truediv(card_E(stream), card_v_x_v)


def uniformity_of_nodes(stream: Stream, u: Hashable, v: Hashable):
    """Uniformity of two nodes in the stream.
    Denotes the capability of two nodes to be linked together considering their presence.

    """
    return truediv(*_intersection_and_union(stream, u, v))


def uniformity(stream: Stream):
    """Uniformity of the stream.
    If the stream has uniformity 1 it means all nodes are present at the same times.
    """
    f = partial(_intersection_and_union, stream=stream)

    return truediv(
        *map(
            lambda x: sum(x),
            unzip((f(u=u, v=v) for u, v in _all_possible_links(stream)))))


def compactness(stream: Stream):
    """Compute the compactness of the stream.
    It's like the coverage with T = [min(t), max(t)]

    """
    t_min_max = stream.stream_presence.root.full_interval.length
    return truediv(card_W(stream), (t_min_max * card_V(stream)))


def density(stream: Stream):
    """Density of the stream.
    Indicates the probability that, taking a time and two nodes, the link exists.

    """
    sum_intersect_t_u_t_v = sum(_card_intervals_intersection(stream, u, v)
                                for u, v in _all_possible_links(stream))

    if sum_intersect_t_u_t_v:
        return truediv(card_E(stream),
                       sum_intersect_t_u_t_v)
    return 0


def density_of_pair(stream: Stream, u: Hashable, v: Hashable):
    """Density of a pair of nodes.
    Denotes the probability that a link between u and v exists when both nodes exist.
    """
    return truediv(card_T_u_v(stream, u, v), _card_intervals_intersection(stream, u, v))


def density_of_node(stream: Stream, node: Hashable):
    """Density of a node.
    The probability that the node is involved in a link when it exists.
    """
    return truediv(
        *map(lambda x: sum(x),
             unzip((card_T_u_v(stream, u, node), _card_intervals_intersection(stream, u, node)) for u in V(stream) if
                   u != node)))


def density_of_time(stream: Stream, t: pd.Interval):
    """Density of a time interval.
    Denotes the probability that a link exists among two nodes present in time t.
    """
    return truediv(card_E_t(stream, t), _card_set_unordered_pairs_distinct_elements(card_V_t(stream, t)))


def _all_possible_links(stream: Stream):
    if isinstance(stream, DiStream):
        return pair_permutations(iterable=V(stream))

    return pair_combinations(iterable=V(stream))


def _card_intervals_union(intervals_1, intervals_2):
    """Compute the cardinality of the union of two iterables of intervals.

    """
    tree = IntervalTree(chain(intervals_1, intervals_2))
    return tree.length


def _card_intervals_intersection(stream: Stream, u: Hashable, v: Hashable):
    return card_T_u(stream, u) + card_T_u(stream, v) - _card_intervals_union(card_T_u(stream, u), card_T_u(stream, v))


def _intersection_and_union(stream: Stream, u: Hashable, v: Hashable):
    """Compute the cardinality of the intersection and the union of two sets of time instants.
    The sets of time instants are red black trees of pandas intervals.

    """
    t_u = T_u(stream, u)
    t_v = T_u(stream, v)
    card_union = _card_intervals_union(t_u, t_v)
    card_intersection = card_T_u(stream, u) + card_T_u(stream, v) - card_union
    return card_intersection, card_union


def _card_set_unordered_pairs_distinct_elements(card_set):
    """Cardinality of the set of unordered pairs of distinct elements.

    """
    return truediv((card_set * (card_set - 1)), 2)
