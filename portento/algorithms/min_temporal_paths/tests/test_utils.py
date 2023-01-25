import pytest
from operator import itemgetter
import random
from sortedcontainers import SortedKeyList
from portento.classes import Stream, DiStream
from portento.utils import Link, DiLink
from portento.algorithms.min_temporal_paths.utils import prepare_for_path_computation, find_le, find_le_idx, \
    update_on_new_candidate, filter_out_candidate, dominates
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

    @pytest.mark.parametrize('tuples,dom_a,res', [
        ([(3, 12), (4, 14), (5, 16)], 15, [(4, 14), (5, 16)]),
        ([(3, 12), (4, 14), (5, 16)], 13, [(3, 12), (4, 14), (5, 16)]),
        ([(3, 12), (4, 14), (5, 16)], 17, [(5, 16)]),
        ([], 1, [])
    ])
    def test_filter_out_candidate(self, tuples, dom_a, res):
        s = SortedKeyList(tuples, key=itemgetter(1))
        assert list(filter_out_candidate(s, find_le_idx(s, dom_a))) == res

    @pytest.mark.parametrize('tuple_1,tuple_2,res', [
        ((1, 0), (2, 1), False),
        ((0, 10), (10, 0), False),
        ((10, 0), (0, 10), True),
        ((10, 0), (11, 10), False),
        ((1, 0), (0, 0), True),
        ((10, 10), (9, 10), True)
    ])
    def test_domination_fun(self, tuple_1, tuple_2, res):
        assert dominates(tuple_1, tuple_2) == res

    @pytest.mark.parametrize('tuples,candidate,res', [
        ([], (0, 1), [(0, 1)]),
        ([(10, 10)], (9, 10), [(10, 10)]),
        ([(9, 10)], (10, 11), [(9, 10), (10, 11)]),
        ([(10, 10)], (10, 9), [(10, 9)])
    ])
    def test_update_on_new_candidate(self, tuples, candidate, res):
        s = SortedKeyList(tuples, key=itemgetter(1))
        assert list(update_on_new_candidate(s, candidate, True)) == res
