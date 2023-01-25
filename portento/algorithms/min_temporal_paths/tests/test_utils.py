import pytest
from operator import itemgetter
import random
from sortedcontainers import SortedKeyList
from portento.classes import Stream, DiStream
from portento.utils import Link, DiLink
from portento.algorithms.min_temporal_paths.utils import prepare_for_path_computation, find_le, \
    update_on_new_candidate, filter_out_candidate
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


    def test_remove(self):
        s = SortedKeyList([(3, 12), (4, 14), (5, 15)], key=itemgetter(1))
        c = 15
        assert list(filter_out_candidate(s, c)) == [(4, 14), (5, 15)]
