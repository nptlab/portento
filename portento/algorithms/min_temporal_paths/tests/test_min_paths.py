import pytest
from pandas import Interval
from .random_stream import generate_stream
from portento.classes import Stream, DiStream
from portento.utils import Link, DiLink
from portento.algorithms.min_temporal_paths.earliest_arrival import earliest_arrival_time
from portento.algorithms.min_temporal_paths.latest_departure import latest_departure_time
from portento.algorithms.min_temporal_paths.fastest_path import fastest_path_duration
from portento.algorithms.min_temporal_paths.shortest_path import shortest_path_distance


class TestMinPaths:

    @pytest.mark.parametrize('intervals,source,target,res', [
        ([DiLink(Interval(0, 1), 0, 1), DiLink(Interval(1, 3), 1, 2)], 0, 2, 3),
        ([DiLink(Interval(0, 1), 0, 1), DiLink(Interval(1, 2), 1, 2), DiLink(Interval(4, 6), 0, 2)], 0, 2, 3),
        ([DiLink(Interval(0, 1), 0, 1), DiLink(Interval(3, 5), 1, 2), DiLink(Interval(1, 2), 0, 2)], 0, 2, 3)
    ])
    def test_earliest_arrival_di_stream(self, intervals, source, target, res):
        stream = DiStream(intervals)
        assert earliest_arrival_time(stream, source)[target] == res

    @pytest.mark.parametrize('intervals,source,target,res', [
        ([Link(Interval(0, 1), 1, 0), Link(Interval(1, 3), 2, 1)], 0, 2, 3),
        ([Link(Interval(0, 3), 0, 1), Link(Interval(0, 3), 0, 2)], 1, 2, 3),
        ([Link(Interval(0, 1), 0, 1), Link(Interval(0, 1), 0, 2)], 0, 2, float('inf'))
    ])
    def test_earliest_arrival_stream(self, intervals, source, target, res):
        stream = Stream(intervals)
        assert earliest_arrival_time(stream, source)[target] == res

    @pytest.mark.parametrize('intervals,target,source,res', [
        ([DiLink(Interval(0, 5), 0, 1), DiLink(Interval(4, 6), 1, 2)], 2, 0, 4),
        ([DiLink(Interval(0, 3), 0, 1), DiLink(Interval(2, 4), 1, 2), DiLink(Interval(0, 2), 0, 2)], 2, 0, 2),
        ([DiLink(Interval(0, 9), 0, 2), DiLink(Interval(0, 5), 1, 2), DiLink(Interval(3, 11), 0, 1)], 2, 0, 9),
        ([DiLink(Interval(0, 9), 0, 2), DiLink(Interval(0, 12), 1, 2), DiLink(Interval(3, 11), 0, 1)], 2, 0, 10)
    ])
    def test_latest_departure_di_stream(self, intervals, target, source, res):
        stream = DiStream(intervals)
        assert latest_departure_time(stream, target)[source] == res

    @pytest.mark.parametrize('intervals,target,source,res', [
        ([Link(Interval(0, 9), 2, 0), Link(Interval(0, 5), 1, 2), Link(Interval(3, 11), 1, 0)], 2, 0, 9),
        ([Link(Interval(0, 9), 2, 0), Link(Interval(0, 12), 1, 2), Link(Interval(3, 11), 1, 0)], 2, 0, 10)
    ])
    def test_latest_departure_stream(self, intervals, target, source, res):
        stream = Stream(intervals)
        assert latest_departure_time(stream, source)[target] == res

    @pytest.mark.parametrize('intervals,source,target,res', [

    ])
    def test_fastest_path_di_stream(self, intervals, source, target, res):
        stream = DiStream(intervals)
        assert fastest_path_duration(stream, source)[target] == res

    @pytest.mark.parametrize('intervals,source,target,res', [

    ])
    def test_fastest_path_stream(self, intervals, source, target, res):
        stream = Stream(intervals)
        assert fastest_path_duration(stream, source)[target] == res

    @pytest.mark.parametrize('intervals,source,target,res', [

    ])
    def test_shortest_path_di_stream(self, intervals, source, target, res):
        stream = DiStream(intervals)
        assert shortest_path_distance(stream, source)[target] == res

    @pytest.mark.parametrize('intervals,source,target,res', [

    ])
    def test_shortest_path_stream(self, intervals, source, target, res):
        stream = Stream(intervals)
        assert shortest_path_distance(stream, source)[target] == res
