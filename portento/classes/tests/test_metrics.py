import pytest
import random
from pandas import Interval
from functools import partial

from portento import Stream, Link
from portento.classes.functions import *
from portento.classes.functions import _card_set_unordered_pairs_distinct_elements, \
    _card_intervals_union


def generate_random_links(n, t_range, u_range):
    for i in range(n):
        random_t = random.choice(t_range)
        random_delta = random.choice(range(1, 6))

        interval = Interval(random_t, random_t + random_delta, 'left')

        u = random.choice(u_range)
        v = u
        while v == u:
            v = random.choice(u_range)

        yield Link(interval=interval, u=u, v=v)


class TestMetrics:

    @pytest.mark.parametrize('s', list(range(10)))
    def test_equalities(self, s):
        n_links = 200
        t_range = range(50)  # range for time instants
        u_range = range(12)  # range for nodes
        random.seed(s)
        round_5 = partial(round, ndigits=5)

        stream = Stream(list(generate_random_links(n_links, t_range, u_range)))

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

    @pytest.mark.parametrize('s', list(range(10)))
    def test_uniformity(self, s):
        n_links = 200
        t_range = range(50)  # range for time instants
        u_range = range(12)  # range for nodes
        random.seed(s)
        round_5 = partial(round, ndigits=5)

        stream = Stream(list(generate_random_links(n_links, t_range, u_range)))
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

