from itertools import tee
from collections.abc import Hashable
from typing import Optional, Iterable, Union

from .streamtree import StreamTree, DiStreamTree
from .streamdict import StreamDict, DiStreamDict
from portento.utils import Link, DiLink, IntervalTree


class Stream:
    """The stream class.

    """
    dict_view_container = StreamDict
    tree_view_container = StreamTree
    time_instants_container = IntervalTree

    def __init__(self, links: Optional[Iterable[Link]] = iter([]), instant_duration=1):
        links_for_dict, links_for_tree, links_for_time = tee(links, 3)
        self._dict = self.dict_view_container(links_for_dict, instant_duration=instant_duration)
        self._tree = self.tree_view_container(links_for_tree)
        self._time_instants = self.time_instants_container(map(lambda l: l.interval, links_for_time),
                                                           instant_duration=instant_duration)

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

        return self.link_presence(u, v).length

    def neighborhood(self, u):
        """Return the stream with all the links in the neighborhood of node u

        Parameters
        -------
        u: Node

        Returns
        -------
        stream : Stream
            Stream object of links in which u appears

        """
        return Stream(self[u])

    def add(self, link):
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


class DiStream(Stream):
    """The directed stream class.

    The order of the nodes counts.

    """
    dict_view_container = DiStreamDict
    tree_view_container = DiStreamTree
