import pandas as pd
import pytest
from sortedcontainers import SortedSet

from portento.convert import *


@pytest.fixture
def data():
    return {"intervals": [pd.Interval(2, 3),
                          pd.Interval(0, 1),
                          pd.Interval(0, 1),
                          pd.Interval(1, 2)],
            "i": [0, 0, 1, 2],
            "j": [1, 1, 2, 0]}


class TestFromPandasStream:

    def test_import(self, data):
        df = pd.DataFrame(data)
        stream = from_pandas_stream(df, *data.keys())
        assert set(stream) - set(map(lambda x: Link(*x), zip(data["intervals"], data["i"], data["j"]))) == set()

    def test_exception(self):
        with pytest.raises(TypeError):
            wrong_data = {"interval": [1, 2], "source": 0, "target": 1}
            df = pd.DataFrame(wrong_data)
            from_pandas_stream(df)


class TestToPandasStream:

    def test_output(self, data):
        stream = portento.Stream(list(map(lambda x: Link(*x), zip(data["intervals"], data["i"], data["j"]))))
        df = to_pandas_stream(stream, *data.keys())
        assert df.equals(pd.DataFrame(list(map(lambda link: (*link,), list(stream))),
                                      columns=[col for col in data.keys()]))
