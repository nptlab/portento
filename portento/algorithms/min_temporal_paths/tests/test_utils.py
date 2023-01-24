import pytest
from operator import itemgetter
import random
from sortedcontainers import SortedKeyList
from portento.classes import Stream, DiStream
from portento.utils import Link, DiLink
from portento.algorithms.min_temporal_paths.utils import prepare_for_path_computation, remove_dominated_elements
from .random_stream import generate_stream


class TestUtils:

    @pytest.mark.parametrize('s', list(range(5)))
    def test_prepare_di_link(self, s):
        stream = generate_stream(DiStream, DiLink, s)
        prepared = list(prepare_for_path_computation(stream, [stream.stream_presence.root.full_interval]))
        assert prepared == sorted(prepared, key=itemgetter(0))

        prepared = list(prepare_for_path_computation(stream, [stream.stream_presence.root.full_interval],
                                                     reverse=True))
        assert prepared == sorted(prepared, key=itemgetter(0), reverse=True)

    @pytest.mark.parametrize('s', list(range(5)))
    def test_prepare_link(self, s):
        stream = generate_stream(Stream, Link, s)
        prepared = list(prepare_for_path_computation(stream, [stream.stream_presence.root.full_interval]))
        assert prepared == sorted(prepared, key=itemgetter(0))

        prepared = list(prepare_for_path_computation(stream, [stream.stream_presence.root.full_interval],
                                                     reverse=True))
        assert prepared == sorted(prepared, key=itemgetter(0), reverse=True)

    @pytest.mark.parametrize('s', list(range(20)))
    def test_domination(self, s):
        random.seed(s)
        tuples_list = SortedKeyList(key=itemgetter(0))
        for t in (random.choices(range(-10, 10), k=2) for _ in range(50)):
            new_tuples_list = tuples_list.copy()
            for c in new_tuples_list:
                if t > c:
                    break
                if t <= c:
                    new_tuples_list.remove(c)

            assert remove_dominated_elements(tuples_list, t) == new_tuples_list

