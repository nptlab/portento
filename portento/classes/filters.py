from abc import abstractmethod
from pandas import Interval
from typing import List, Callable, Union
from heapq import merge
from copy import deepcopy
from itertools import repeat

from portento.classes.streamdict import StreamDict
from portento.classes.stream import Stream
from portento.classes.streamtree import StreamTree, StreamTreeNode
from portento.utils import IntervalTree, IntervalTreeNode, Link, cut_interval


class Filter:

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def __getitem__(self, item):
        pass

    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass


class NoFilter(Filter):
    def __init__(self):
        pass

    def __getitem__(self, item):
        yield item

    def __call__(self, *args, **kwargs):
        return True


class TimeFilter(Filter):
    def __init__(self, list_of_intervals: List[Interval]):
        self._interval_tree = StreamTree(list_of_intervals)

    @property
    def interval_tree(self):
        return self._interval_tree

    def __getitem__(self, item: Interval):
        yield from self._ordered_visit(self.interval_tree.root, item)

    def _ordered_visit(self, node, item):
        if node.full_interval.overlaps(item):
            if node.left:
                yield from self._ordered_visit(node.left, item)
            if node.value.overlaps(item):
                yield cut_interval(item, node.value)
            if node.right:
                yield from self._ordered_visit(node.right, item)

    def __call__(self, *args, **kwargs):
        interval = args[0]
        if self._interval_tree.root:
            node_queue = list()
            node_queue.append(self.interval_tree.root)
            while node_queue:
                node = node_queue.pop()
                if node.full_interval.overlaps(interval):
                    if node.value.overlaps(interval):
                        return True
                    else:
                        if node.right:
                            node_queue.append(node.right)
                        if node.left:
                            node_queue.append(node.left)

        return False


def filter_by_time(tree_view: StreamTree, time_filter: Union[NoFilter, TimeFilter]):
    if tree_view.root:
        return iter(_filter_node_by_time(tree_view.root, time_filter))
    else:
        return iter()


def _filter_node_by_time(node: StreamTreeNode, time_filter):
    if time_filter(node.full_interval):
        if node.left:
            yield from _filter_node_by_time(node.left, time_filter)
        if time_filter(node.value):
            for interval in time_filter[node.value]:
                if hasattr(node, 'u'):
                    yield Link(interval, node.u, node.v)
                else:
                    yield interval
        if node.right:
            yield from _filter_node_by_time(node.right, time_filter)


class NodeFilter(Filter):
    def __init__(self, filter_nodes: Callable):
        self._filter = filter_nodes

    def __getitem__(self, item):
        if self._filter(item):
            return item
        else:
            raise Exception

    def __call__(self, *args, **kwargs):
        node = args[0]
        return self._filter(node)


def filter_by_nodes(dict_view: StreamDict, node_filter: Union[NoFilter, NodeFilter]):
    for link in merge(*(stream for u, links in dict_view.edges.items() if node_filter(u)
                        for v, stream in links.items() if node_filter(v))):
        yield link


def filter_stream(stream: Stream,
                  node_filter: Union[NoFilter, NodeFilter] = NoFilter,
                  time_filter: Union[NoFilter, TimeFilter] = NoFilter,
                  first='time'):
    if isinstance(node_filter, NoFilter) or first == 'time':  # time first slice
        yield from filter(lambda l: node_filter(l.u) and node_filter(l.v),
                          filter_by_time(stream.tree_view, time_filter))

    elif isinstance(time_filter, NoFilter) or first == 'node':  # node first slice
        yield from merge(*(map(lambda x: Link(*x),
                               zip(filter_by_time(links._intervals, time_filter), repeat(u), repeat(v)))
                           for u, adj in stream.edges.items() if node_filter(u)
                           for v, links in adj.items() if node_filter(v)
                           ))
    else:
        raise Exception


new_links = [Link(Interval(1.0, 3.0, 'both'), 'a', 'b'),
             Link(Interval(7.0, 8.0, 'both'), 'a', 'b'),
             Link(Interval(4.5, 7.5, 'both'), 'a', 'c'),
             Link(Interval(6.0, 9.0, 'both'), 'b', 'c'),
             Link(Interval(2.0, 3.0, 'both'), 'b', 'd'),
             Link(Interval(0.0, 4.0, 'both'), 'a', 'b'),
             Link(Interval(6.0, 9.0, 'both'), 'a', 'b'),
             Link(Interval(2.0, 5.0, 'both'), 'a', 'c'),
             Link(Interval(1.0, 8.0, 'both'), 'b', 'c'),
             Link(Interval(7.0, 10.0, 'both'), 'b', 'd'),
             Link(Interval(6.0, 9.0, 'both'), 'c', 'd')]

s = Stream(new_links)
print(
    [i for i in
     filter_stream(s, NodeFilter(lambda x: x in ['a', 'b', 'c']), TimeFilter([Interval(4.0, 6.0, 'both')]), 'node')])
