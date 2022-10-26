import pytest

from pandas import Interval

from portento.utils import Link, IntervalContainer


class TestLink:

    @pytest.mark.parametrize('attrs', [
        (Interval(0, 1), None, 3),
        (Interval(0, 1), 3, None),
        (Interval(0, 1), None, None)
    ])
    def test_value_exception(self, attrs):
        with pytest.raises(ValueError):
            Link(*attrs)

    @pytest.mark.parametrize('attrs', [
        (Interval(0, 1), [1, 2], 3),
        (Interval(0, 1), 3, {1, 2})
    ])
    def test_type_exception(self, attrs):
        with pytest.raises(TypeError):
            Link(*attrs)


@pytest.fixture
def links():
    return [Link(Interval(1, 2), 1, 2),
            Link(Interval(0, 1), 0, 3),
            Link(Interval(0, 1), 0, 1)]


@pytest.fixture
def links_2():
    return [Link(Interval(0, 3), 1, 2),
            Link(Interval(1, 3), 0, 3),
            Link(Interval(2, 4), 0, 1)]


class TestIntervalContainer:

    def test_add(self, links_2):
        interval_container = IntervalContainer(0)
        interval_container_edge = IntervalContainer(2, 1)
        assert [interval_container.add(link) for link in links_2] == [False, True, True]
        assert [interval for interval in interval_container] == [Interval(1, 4)]

        assert [interval_container_edge.add(link) for link in links_2] == [True, False, False]
