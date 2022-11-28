from abc import abstractmethod
from pandas import Interval
from typing import List, Callable, Union
from heapq import merge
from itertools import repeat
from sortedcontainers.sortedlist import SortedList

from .streamdict import StreamDict
from .stream import Stream, DiStream
from .streamtree import StreamTree, StreamTreeNode
from portento.utils import IntervalTree, IntervalTreeNode, Link, DiLink, cut_interval


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


def filter_by_time(stream_tree: StreamTree, time_filter: Union[NoFilter, TimeFilter], link_type=Link):
    if stream_tree.root:
        return iter(SortedList(_filter_node_by_time(stream_tree.root, time_filter, link_type)))
    else:
        return iter([])


def _filter_node_by_time(node: Union[StreamTreeNode, IntervalTreeNode], time_filter: Union[NoFilter, TimeFilter],
                         link_type: Union[Link, DiLink]):
    if time_filter(node.full_interval):
        if node.left:
            yield from _filter_node_by_time(node.left, time_filter, link_type)
        if time_filter(node.value):
            yield from map(lambda i:
                           link_type(i, node.u, node.v) if isinstance(node, StreamTreeNode) else i,
                           time_filter[node.value])
        if node.right:
            yield from _filter_node_by_time(node.right, time_filter, link_type)


def filter_by_nodes(stream_dict: StreamDict, node_filter: Union[NoFilter, NodeFilter]):
    yield from merge(*(stream for u, links in stream_dict.edges.items() if node_filter(u)
                       for v, stream in links.items() if node_filter(v)))


def filter_by_link(stream_dict: StreamDict):
    pass


def filter_stream(stream: Stream,
                  node_filter: Union[NoFilter, NodeFilter] = NoFilter(),
                  time_filter: Union[NoFilter, TimeFilter] = NoFilter(),
                  first='time'):
    """A compounded slice over nodes and time.
    Function that returns an iterable over Links that respect both the node and the time filter.

    Parameters
    -------
    stream : Stream
        Stream over which the slicing is made.
    node_filter : Union[NoFilter, NodeFilter]
        Filter over nodes.
    time_filter : Union[NoFilter, TimeFilter]
        Filter over time.
    first : str['node', 'time']
        This parameter sets the first filter to apply.

    Returns
    -------
    Iterable[Link] : An iterable over the links that respect all filters.
    """
    return _filter(stream, Link, node_filter, time_filter, first)


def filter_di_stream(stream: DiStream,
                     node_filter: Union[NoFilter, NodeFilter] = NoFilter(),
                     time_filter: Union[NoFilter, TimeFilter] = NoFilter(),
                     first='time'):
    """A compounded slice over nodes and time.
        Function that returns an iterable over Links that respect both the node and the time filter.

        Parameters
        -------
        stream : DiStream
            DiStream over which the slicing is made.
        node_filter : Union[NoFilter, NodeFilter]
            Filter over nodes.
        time_filter : Union[NoFilter, TimeFilter]
            Filter over time.
        first : str['node', 'time']
            This parameter sets the first filter to apply.

        Returns
        -------
        Iterable[DiLink] : An iterable over the links that respect all filters.
        """
    return _filter(stream, DiLink, node_filter, time_filter, first)


def _filter(stream: Union[Stream, DiStream],
            link_type: Union[Link, DiLink],
            node_filter: Union[NoFilter, NodeFilter] = NoFilter(),
            time_filter: Union[NoFilter, TimeFilter] = NoFilter(),
            first='time'):
    if first == 'time':
        yield from filter(lambda l: node_filter(l.u) and node_filter(l.v),
                          filter_by_time(stream.tree_view, time_filter, link_type))

    elif first == 'node':
        yield from merge(*(map(lambda x: link_type(*x),
                               zip(filter_by_time(links.interval_tree, time_filter), repeat(u), repeat(v)))
                           for u, adj in stream.edges.items() if node_filter(u)
                           for v, links in adj.items() if node_filter(v)
                           ))
    else:
        raise AttributeError("This method must be called with:\n a Stream (or DiStream), a NodeFilter, a TimeFilter "
                             "and a string with value \'node\' or \'time\'")
