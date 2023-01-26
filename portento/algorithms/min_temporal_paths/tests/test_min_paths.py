import pytest
from pandas import Interval
from .random_stream import generate_stream
from portento.classes import Stream, DiStream
from portento.utils import Link, DiLink
from portento.algorithms.min_temporal_paths.earliest_arrival import earliest_arrival_time
from portento.algorithms.min_temporal_paths.latest_departure import latest_departure_time
from portento.algorithms.min_temporal_paths.fastest_path import fastest_path_duration, fastest_path_duration_multipass
from portento.algorithms.min_temporal_paths.shortest_path import shortest_path_distance


class TestMinPaths:

    @pytest.mark.parametrize('links,source,target,res', [
        ([DiLink(Interval(0, 1), 0, 1), DiLink(Interval(1, 3), 1, 2)], 0, 2, 3),
        ([DiLink(Interval(0, 1), 0, 1), DiLink(Interval(1, 2), 1, 2), DiLink(Interval(4, 6), 0, 2)], 0, 2, 3),
        ([DiLink(Interval(0, 1), 0, 1), DiLink(Interval(3, 5), 1, 2), DiLink(Interval(1, 2), 0, 2)], 0, 2, 3)
    ])
    def test_earliest_arrival_di_stream(self, links, source, target, res):
        stream = DiStream(links)
        assert earliest_arrival_time(stream, source)[target] == res

    @pytest.mark.parametrize('links,source,target,res', [
        ([Link(Interval(0, 1), 1, 0), Link(Interval(1, 3), 2, 1)], 0, 2, 3),
        ([Link(Interval(0, 3), 0, 1), Link(Interval(0, 3), 0, 2)], 1, 2, 3),
        ([Link(Interval(0, 1), 0, 1), Link(Interval(0, 1), 0, 2)], 0, 2, float('inf'))
    ])
    def test_earliest_arrival_stream(self, links, source, target, res):
        stream = Stream(links)
        assert earliest_arrival_time(stream, source)[target] == res

    @pytest.mark.parametrize('links,target,source,res', [
        ([DiLink(Interval(0, 5), 0, 1), DiLink(Interval(4, 6), 1, 2)], 2, 0, 4),
        ([DiLink(Interval(0, 3), 0, 1), DiLink(Interval(2, 4), 1, 2), DiLink(Interval(0, 2), 0, 2)], 2, 0, 2),
        ([DiLink(Interval(0, 9), 0, 2), DiLink(Interval(0, 5), 1, 2), DiLink(Interval(3, 11), 0, 1)], 2, 0, 9),
        ([DiLink(Interval(0, 9), 0, 2), DiLink(Interval(0, 12), 1, 2), DiLink(Interval(3, 11), 0, 1)], 2, 0, 10)
    ])
    def test_latest_departure_di_stream(self, links, target, source, res):
        stream = DiStream(links)
        assert latest_departure_time(stream, target)[source] == res

    @pytest.mark.parametrize('links,target,source,res', [
        ([Link(Interval(0, 9), 2, 0), Link(Interval(0, 5), 1, 2), Link(Interval(3, 11), 1, 0)], 2, 0, 9),
        ([Link(Interval(0, 9), 2, 0), Link(Interval(0, 12), 1, 2), Link(Interval(3, 11), 1, 0)], 2, 0, 10)
    ])
    def test_latest_departure_stream(self, links, target, source, res):
        stream = Stream(links)
        assert latest_departure_time(stream, source)[target] == res

    @pytest.mark.parametrize('links,source,target,res', [
        ([DiLink(Interval(0, 2), 0, 1), DiLink(Interval(10, 12), 1, 2), DiLink(Interval(0, 2), 2, 0)], 0, 2, 10),
        ([DiLink(Interval(0, 2), 0, 1), DiLink(Interval(0, 12), 2, 0)], 2, 0, 1),
        ([DiLink(Interval(0, 2), 0, 1), DiLink(Interval(1, 3), 2, 0)], 0, 2, float('inf'))
    ])
    def test_fastest_path_multipass_di_stream(self, links, source, target, res):
        stream = DiStream(links)
        assert fastest_path_duration_multipass(stream, source)[target] == res

    @pytest.mark.parametrize('links,source,target,res', [
        ([Link(Interval(0, 2), 0, 1), Link(Interval(10, 12), 1, 2), Link(Interval(4, 6), 0, 2)], 0, 2, 1),
        ([Link(Interval(0, 2), 1, 0), Link(Interval(10, 12), 2, 1), Link(Interval(4, 6), 2, 0)], 0, 2, 1),
        ([Link(Interval(0, 2), 0, 1), Link(Interval(0, 12), 2, 3)], 2, 0, float('inf')),
        ([Link(Interval(0, 2), 0, 1), Link(Interval(10, 12), 1, 2)], 0, 2, 10)
    ])
    def test_fastest_path_multipass_stream(self, links, source, target, res):
        stream = Stream(links)
        assert fastest_path_duration_multipass(stream, source)[target] == res

    @pytest.mark.parametrize('links,source,target,res', [
        ([DiLink(Interval(0, 2), 0, 1), DiLink(Interval(10, 12), 1, 2), DiLink(Interval(0, 2), 2, 0)], 0, 2, 10),
        ([DiLink(Interval(0, 2), 0, 1), DiLink(Interval(0, 12), 2, 0)], 2, 0, 1),
        ([DiLink(Interval(0, 2), 0, 1), DiLink(Interval(1, 3), 2, 0)], 0, 2, float('inf'))
    ])
    def test_fastest_path_onepass_di_stream(self, links, source, target, res):
        stream = DiStream(links)
        assert fastest_path_duration(stream, source)[target] == res

    @pytest.mark.parametrize('links,source,target,res', [
        ([Link(Interval(0, 2), 0, 1), Link(Interval(10, 12), 1, 2), Link(Interval(4, 6), 0, 2)], 0, 2, 1),
        ([Link(Interval(0, 2), 1, 0), Link(Interval(10, 12), 2, 1), Link(Interval(4, 6), 2, 0)], 0, 2, 1),
        ([Link(Interval(0, 2), 0, 1), Link(Interval(0, 12), 2, 3)], 2, 0, float('inf')),
        ([Link(Interval(0, 2), 0, 1), Link(Interval(10, 12), 1, 2)], 0, 2, 10)
    ])
    def test_fastest_path_onepass_stream(self, links, source, target, res):
        stream = Stream(links)
        assert fastest_path_duration(stream, source)[target] == res

    @pytest.mark.parametrize('s', list(range(20)))
    def test_fastest_path_di_stream(self, s):
        stream = generate_stream(DiStream, DiLink, s, n_links=50, t_range=range(20), u_range=range(10))
        for source in stream.nodes:
            assert fastest_path_duration(stream, source) == fastest_path_duration_multipass(stream, source)

    @pytest.mark.parametrize('s', list(range(20)))
    def test_fastest_path_stream(self, s):
        stream = generate_stream(Stream, Link, s, n_links=50, t_range=range(20), u_range=range(10))
        for source in stream.nodes:
            assert fastest_path_duration(stream, source) == fastest_path_duration_multipass(stream, source)

    @pytest.mark.parametrize('links,source,target,res', [
        ([(DiLink(Interval(0, 2), 0, 1))], 0, 1, 1),
        ([(DiLink(Interval(0, 2), 1, 0))], 0, 1, float('inf')),
        ([(DiLink(Interval(9, 11), 0, 2)), DiLink(Interval(0, 2), 0, 1), DiLink(Interval(1, 3), 1, 2)], 0, 2, 1),
        ([DiLink(Interval(9, 11), 0, 1), DiLink(Interval(0, 2), 1, 2)], 0, 2, 2)
    ])
    def test_shortest_path_di_stream(self, links, source, target, res):
        stream = DiStream(links)
        assert shortest_path_distance(stream, source)[target] == res

    @pytest.mark.parametrize('links,source,target,res', [

    ])
    def test_shortest_path_stream(self, links, source, target, res):
        stream = Stream(links)
        assert shortest_path_distance(stream, source)[target] == res
