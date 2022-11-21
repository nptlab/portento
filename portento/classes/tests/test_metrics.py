import pytest
import random
from pandas import Interval
from functools import partial

from portento import Stream, Link
from portento.classes.functions import *
from portento.classes.functions import _card_set_unordered_pairs_distinct_elements


class TestMetrics:

    def test_metrics(self):
        pass

    @pytest.mark.parametrize('s', list(range(20)))
    def test_equalities(self, s):
        n_links = 200
        t_range = range(50)  # range for time instants
        u_range = range(12)  # range for nodes
        random.seed(s)
        round_5 = partial(round, ndigits=5)

        def generate_random_links(n):
            for i in range(n):
                random_t = random.choice(t_range)
                random_delta = random.choice(range(1, 6))

                interval = Interval(random_t, random_t + random_delta, 'left')

                u = random.choice(u_range)
                v = u
                while v == u:
                    v = random.choice(u_range)

                yield Link(interval=interval, u=u, v=v)

        stream = Stream(list(generate_random_links(n_links)))

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
