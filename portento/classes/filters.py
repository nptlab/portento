from abc import abstractmethod
from pandas import Interval
from typing import List, Callable, Union
from heapq import merge
from itertools import repeat
from sortedcontainers.sortedlist import SortedList

from portento.classes.streamdict import StreamDict
from portento.classes.stream import Stream
from portento.classes.streamtree import StreamTree, StreamTreeNode
from portento.utils import IntervalTree, IntervalTreeNode, Link, cut_interval


class Filter:
    """Abstract class for link filters.

    """

    @abstractmethod
    def __init__(self, *args):
        pass

    @abstractmethod
    def __getitem__(self, item):
        pass

    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass


class NoFilter(Filter):
    """A links filter that accepts any link.

    """

    def __init__(self):
        pass

    def __getitem__(self, item):
        yield item

    def __call__(self, *args, **kwargs):
        return True


class TimeFilter(Filter):
    """A links filter that tests the interval.
    A link is kept if the interval has an overlapping in the user-defined intervaltree.
    __init__ takes into input a list of intervals.
    """

    def __init__(self, list_of_intervals: List[Interval]):
        self._interval_tree = IntervalTree(list_of_intervals)

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


class NodeFilter(Filter):
    """A links filter that tests on nodes.
    Links with both nodes that respect the condition are kept.
    __init__ takes into input a boolean function over the nodes.
    """

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


def filter_by_time(stream_tree: StreamTree, time_filter: Union[NoFilter, TimeFilter]):

    if stream_tree.root:
        return iter(SortedList(_filter_node_by_time(stream_tree.root, time_filter)))
    else:
        return iter()


def _filter_node_by_time(node: Union[StreamTreeNode, IntervalTreeNode], time_filter):

    if time_filter(node.full_interval):
        if node.left:
            yield from _filter_node_by_time(node.left, time_filter)
        if time_filter(node.value):
            for interval in (time_filter[node.value]):
                if isinstance(node, StreamTreeNode):
                    yield Link(interval, node.u, node.v)
                else:
                    yield interval
        if node.right:
            yield from _filter_node_by_time(node.right, time_filter)


def filter_by_nodes(stream_dict: StreamDict, node_filter: Union[NoFilter, NodeFilter]):

    yield from merge(*(stream for u, links in stream_dict.edges.items() if node_filter(u)
                       for v, stream in links.items() if node_filter(v)))


def filter_stream(stream: Stream,
                  node_filter: Union[NoFilter, NodeFilter] = NoFilter(),
                  time_filter: Union[NoFilter, TimeFilter] = NoFilter(),
                  first='time'):

    if first == 'time':
        yield from filter(lambda l: node_filter(l.u) and node_filter(l.v),
                          filter_by_time(stream.tree_view, time_filter))

    elif first == 'node':
        yield from merge(*(map(lambda x: Link(*x),
                               zip(filter_by_time(links.interval_tree, time_filter), repeat(u), repeat(v)))
                           for u, adj in stream.edges.items() if node_filter(u)
                           for v, links in adj.items() if node_filter(v)
                           ))
    else:
        raise AttributeError("This method must be called with:\n a Stream, a NodeFilter, a TimeFilter and a string"
                             " with value \'node\' or \'time\'")
