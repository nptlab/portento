import pytest

from functools import partial
from portento import Stream, Link, card_T, card_T_u, degree
from .random_stream import generate_stream


round_5 = partial(round, ndigits=5)


class TestNeighborhood:

    @pytest.mark.parametrize('s', range(5))
    def test_card(self, s):
        stream = generate_stream(Stream, Link, s)

        for u in iter(stream.nodes):
            assert stream.neighborhood(u).stream_presence_len() == card_T_u(stream, u)
