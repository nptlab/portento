import pytest
from pandas import Interval
from itertools import cycle
from random import choices
from portento.classes.tests.random_stream import generate_stream
from portento.utils import Link, DiLink, contains_interval
from portento.metrics.metrics import *
from portento.metrics.metrics import _card_set_unordered_pairs_distinct_elements, \
    _card_intervals_union

round_5 = partial(round, ndigits=5)


class TestMetrics:

    @pytest.mark.parametrize('s,stream_types', zip(range(20), cycle(zip((Stream, DiStream), (Link, DiLink)))))
    def test_equalities(self, s, stream_types):

        stream_type, link_type = stream_types
        stream = generate_stream(stream_type, link_type, s)

        assert round_5(sum((contribution_of_node(stream, n) for n in V(stream)))) == round_5(number_of_nodes(stream))

        assert round_5(sum((contribution_of_link(stream, u, v)
                            for u in E(stream) for v in E(stream)[u]))) == round_5(number_of_links(stream))

        assert round_5(coverage(stream)) == \
               round_5(number_of_nodes(stream) / card_V(stream)) == \
               round_5(node_duration(stream) / card_T(stream))

        assert round_5(number_of_nodes(stream) * card_T(stream)) == \
               round_5(node_duration(stream) * card_V(stream)) == \
               round_5(card_W(stream))

        assert round_5(number_of_links(stream) * card_T(stream)) == \
               round_5(link_duration(stream) * _card_set_unordered_pairs_distinct_elements(card_V(stream))) == \
               round_5(card_E(stream))

    @pytest.mark.parametrize('intervals,card_union', [
        ([[Interval(0, 5), Interval(10, 20)], [Interval(8, 9), Interval(19, 25)]], 21),
        ([[Interval(0, 1), Interval(2, 3)], [Interval(2, 3), Interval(4, 5)]], 3),
        ([[Interval(0, 10), Interval(20, 30)], [Interval(9, 21), Interval(22, 25)]], 30)
    ])
    def test_card_union(self, intervals, card_union):

        tree_1, tree_2 = map(lambda x: IntervalTree(x), intervals)
        assert _card_intervals_union(tree_1, tree_2) == card_union

    @pytest.mark.parametrize('s,stream_types', zip(range(20), cycle(zip((Stream, DiStream), (Link, DiLink)))))
    def test_uniformity(self, s, stream_types):

        stream_type, link_type = stream_types
        stream = generate_stream(stream_type, link_type, s)
        u_range = range(12)

        union_acc = 0
        intersection_acc = 0
        for u in u_range:
            for v in u_range:
                if u != v:
                    card_union = _card_intervals_union(T_u(stream, u), T_u(stream, v))
                    card_intersection = card_T_u(stream, u) + card_T_u(stream, v) - card_union
                    assert round_5(uniformity_of_nodes(stream, u, v)) == round_5(card_intersection / card_union)
                    union_acc += card_union
                    intersection_acc += card_intersection

        assert round_5(uniformity(stream)) == round_5(intersection_acc / union_acc)

    @pytest.mark.parametrize('s', list(range(20)))
    def test_degree(self, s):

        stream = generate_stream(Stream, Link, s)
        assert round_5(2 * number_of_links(stream)) == round_5(sum((degree(stream, u) for u in stream.nodes)))

    @pytest.mark.parametrize('s', list(range(20)))
    def test_avg_degree(self, s):

        stream = generate_stream(Stream, Link, s)
        assert round_5(average_degree(stream)) == \
               round_5(sum(contribution_of_node(stream, u) * degree(stream, u) for u in stream.nodes)
                       / number_of_nodes(stream))

    @pytest.mark.parametrize('s', list(range(20)))
    def test_avg_node_degree(self, s):

        stream = generate_stream(Stream, Link, s)
        card_w_stream = card_W(stream)
        assert round_5(average_node_degree(stream)) == \
               round_5(sum((card_T_u(stream, u) * degree(stream, u) / card_w_stream for u in V(stream))))

    @pytest.mark.parametrize('s', list(range(5)))
    def test_instantaneous_degree(self, s):

        stream = generate_stream(Stream, Link, s)
        for u in V(stream):
            neighborhood = list(stream.neighborhood(u))
            for t in map(lambda i: Interval(i.interval.left, i.interval.left, 'both'), choices(neighborhood, k=5)):
                assert instantaneous_degree(stream, u, t) == max(len(
                    set(flatten(((u, v)
                                 for _, u, v in
                                 filter(lambda x: contains_interval(x.interval, t), neighborhood))))) - 1,
                                                                 0)

    @pytest.mark.parametrize('s', list(range(5)))
    def test_average_time_degree(self, s):
        stream = generate_stream(Stream, Link, s, n_links=50, t_range=range(15), u_range=range(7))
        assert round_5(truediv(sum((mul(card_V_t(stream, pd.Interval(t, t, 'both')),
                                        degree_at_t(stream, pd.Interval(t, t, 'both')))
                                    for interval in T(stream)
                                    for t in split_in_instants(interval, stream.instant_duration))),
                               card_W(stream))) == \
               round_5(average_time_degree(stream))

    @pytest.mark.parametrize('s', list(range(5)))
    def test_degree_of_stream(self, s):
        stream = generate_stream(Stream, Link, s)
        assert round_5(sum((degree(stream, u) for u in V(stream))) / card_V(stream)) == \
               round_5(degree_of_stream(stream))

    @pytest.mark.parametrize('s', list(range(5)))
    def test_average_expected_degree(self, s):
        stream = generate_stream(Stream, Link, s)
        assert round_5(truediv(2 * card_E(stream), card_W(stream))) == \
               round_5(average_expected_degree(stream))
