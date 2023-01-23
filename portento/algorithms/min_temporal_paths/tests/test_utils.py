import pytest
from operator import itemgetter
from portento.classes import Stream, DiStream
from portento.utils import Link, DiLink
from portento.algorithms.min_temporal_paths.utils import _prepare_for_path_computation
from .random_stream import generate_stream


class TestUtils:

    @pytest.mark.parametrize('s', list(range(5)))
    def test_prepare_di_link(self, s):
        stream = generate_stream(DiStream, DiLink, s)
        prepared = list(_prepare_for_path_computation(stream))
        assert prepared == sorted(prepared, key=itemgetter(0))

        prepared = list(_prepare_for_path_computation(stream, reverse=True))
        assert prepared == sorted(prepared, key=itemgetter(0), reverse=True)

    @pytest.mark.parametrize('s', list(range(5)))
    def test_prepare_link(self, s):
        stream = generate_stream(Stream, Link, s)
        prepared = list(_prepare_for_path_computation(stream))
        assert prepared == sorted(prepared, key=itemgetter(0))

        prepared = list(_prepare_for_path_computation(stream, reverse=True))
        assert prepared == sorted(prepared, key=itemgetter(0), reverse=True)
