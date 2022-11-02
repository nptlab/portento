from pandas import Interval, Timestamp, Timedelta
from numpy import int64, float64
from collections.abc import Hashable
from typing import Optional, Iterable, Union
from random import shuffle  # TODO this will be removed as red-black trees are implemented

from portento.classes.streamtree import StreamTree
from portento.classes.streamdict import StreamDict
from portento.utils import Link, IntervalTree


# TODO put all correctness checks here (?)

class Stream:

    def __init__(self, links: Optional[Iterable[Link]] = list(), int_typ=type(None), **kwargs):
        self._dict = StreamDict(links, int_typ, **kwargs)
        self._tree = StreamTree(links)
        self._time_instants = IntervalTree(map(lambda l: l.interval, links))

    @property
    def tree_view(self):
        return self._tree

    @property
    def dict_view(self):
        return self._dict

    @property
    def stream_presence(self):
        return self._time_instants

    @property
    def nodes(self):
        return self.dict_view.nodes

    @property
    def edges(self):
        return self.dict_view.edges

    @property
    def interval_type(self):
        return self.dict_view.interval_type

    def __iter__(self):
        return iter(self.tree_view)

    def __contains__(self, item):
        return self.dict_view.__contains__(item)

    def __getitem__(self, item):
        return self.dict_view.__getitem__(item)

    def stream_presence_len(self):
        """The total time in which the stream has at least an active link.

        It is computed as the sum of the length of all time intervals in which the stream has at least an active link.
        If intervals have boundaries of the pandas Timestamp type, the number of total seconds is returned

        """
        if isinstance(self.stream_presence.length, Timedelta):
            return self.stream_presence.length.total_seconds()

        return self.stream_presence.length

    def node_presence(self, node: Hashable):
        """Return all time instances in which the node is alive (has at least an active link).

        Parameters
        ----------
        node : Node
            The node to query about.

        Returns
        -------
        time : IntervalContainer
            The IntervalContainer object containing Interval objects.

        """
        return self.dict_view.node_presence(node)

    def node_presence_len(self, node: Hashable):
        """The total time in which the node has at least an active link

        It is computed as the sum of the length of all time intervals in which the node has at least an active link.
        If intervals have boundaries of the pandas Timestamp type, the number of total seconds is returned

        """
        if isinstance(self.node_presence(node).length, Timedelta):
            return self.node_presence(node).length.total_seconds()

        return self.node_presence(node).length

    def link_presence(self, u: Hashable, v: Hashable):
        """Return all time instances in which the link (u, v) is active.

        Parameters
        ----------
        u, v : Hashable
            nodes

        Returns
        -------
        time : IntervalContainer
            The IntervalContainer object containing Interval objects.
        """
        return self.dict_view.edge_presence(u, v)

    def link_presence_len(self, u: Hashable, v: Hashable):
        """The total time in which the node has at least an active link

        It is computed as the sum of the length of all time intervals in which the node has at least an active link.
        If intervals have boundaries of the pandas Timestamp type, the number of total seconds is returned

        """
        if isinstance(self.link_presence(u, v).length, Timedelta):
            return self.link_presence(u, v).length.total_seconds()

        return self.link_presence(u, v).length

    def nodes_present_in_t(self, instant: Union[int, float, int64, float64, Timestamp, Timedelta]):
        """Return all nodes present during the specified instant.

        """
        interval = Interval(instant, instant, 'both')
        return self.time_based_slice(interval).nodes.keys()

    def links_present_in_t(self, instant: Union[int, float, int64, float64, Timestamp, Timedelta]):
        """Return all edges present during the specified instant.

        """
        interval = Interval(instant, instant, 'both')
        return [(u, v) for (u, edges) in self.time_based_slice(interval).edges.items()
                for v in edges.keys()]

    def add(self, link: Link):
        """Add a link to the stream.
        The link is added both to a StreamDict object and a StreamTree object.

        Parameters
        ----------
        link : Link
            The link to add.

        """
        self.dict_view.add(link)
        self.tree_view.add(link)
        self.stream_presence.add(link.interval)

    def node_based_slice(self, selected_nodes: Optional[Iterable[Hashable]] = None):
        """Return the sliced stream selecting some nodes.

        Parameters
        ----------
        selected_nodes : Iterable of nodes.
            The nodes to keep.

        Returns
        -------
        stream : Stream
            The resulting sliced stream.

        """
        l = list(self.dict_view.node_based_slice(selected_nodes))
        shuffle(l)
        return Stream(l)

    def time_based_slice(self, interval: Optional[Interval] = None):
        """Return the sliced stream over the temporal dimension.

        Parameters
        ----------
        interval : pandas Interval
            Time boundaries for the output stream

        Returns
        -------
        stream : Stream
            The resulting sliced stream.

        """
        l = list(self.tree_view.time_based_slice(interval))
        shuffle(l)
        return Stream(l)

    def slice(self, selected_nodes: Optional[Iterable[Hashable]] = None, interval: Optional[Interval] = None,
              first='node'):
        """Return the sliced stream on both the nodes and time.

        Parameters
        ----------
        selected_nodes : Iterable of nodes to keep.
        interval : pandas Interval
            Time boundaries for the output stream.
        first : str ('node' or 'time')
            Whether to slice upon nodes or time before.
            Choosing this value carefully may be crucial for performances.

        Returns
        -------
        stream : Stream
            The resulting sliced stream.

        """

        if selected_nodes:
            if interval:
                # both selected_nodes and interval are specified
                if first == 'node':
                    return self.node_based_slice(selected_nodes).time_based_slice(interval)
                elif first == 'time':
                    return self.time_based_slice(interval).node_based_slice(selected_nodes)
            else:
                # only selected_nodes
                return self.node_based_slice(selected_nodes)
        elif interval:
            # only the interval
            return self.time_based_slice(interval)
        else:
            return Stream()
