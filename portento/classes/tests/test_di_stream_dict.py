import pytest
from functools import partial
from pandas import Interval

from portento import DiStreamDict, StreamDict
from portento.utils import DiLink, Link


class TestDiStreamDict:

    def test_di_stream_dict(self):
        DiLink_10_5 = partial(DiLink, u=10, v=5)
        di_links = [DiLink_10_5(interval=Interval(0, 1)), DiLink_10_5(interval=Interval(1, 2, 'both'))]

        di_stream_dict = DiStreamDict(di_links)

        assert di_stream_dict[(10, 5)] == [DiLink(Interval(0, 2), 10, 5)]

        assert di_stream_dict[(5, 10)] == list()

        with pytest.raises(TypeError):
            di_stream_dict.add(Link(Interval(0, 1), 5, 10))

        with pytest.raises(TypeError):
            StreamDict(di_links)
