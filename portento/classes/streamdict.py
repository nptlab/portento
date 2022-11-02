from heapq import merge
from collections.abc import Hashable
from typing import Optional, Iterable
from pandas import Timestamp, Timedelta
from functools import reduce

from portento.utils import IntervalContainer, Link, sort_nodes, compute_presence


class StreamDict:
    """The data structure for stream graphs that allows node-based slices

    """
    node_container_factory = dict
    edge_outer_container_factory = dict
    edge_inner_container_factory = dict
    data_container_factory = IntervalContainer

    def __init__(self, links: Optional[Iterable[Link]] = list(), int_typ=type(None), **kwargs):
        self._nodes = self.node_container_factory()
        self._edges = self.edge_outer_container_factory()
        self._reverse_edges = self.edge_outer_container_factory()
        self._interval_type = int_typ

        if 'nodes' in kwargs and 'edges' in kwargs and 'reverse_edges' in kwargs:
            self._nodes = kwargs['nodes']
            self._edges = kwargs['edges']
            self._reverse_edges = kwargs['reverse_edges']
        else:
            for link in links:
                self.add(link)

    @property
    def interval_type(self):
        return self._interval_type

    @property
    def nodes(self):
        return self._nodes

    @property
    def edges(self):
        return self._edges

    def __iter__(self):
        """Iterate over the sorted stream of links

        Returns
        -------
        iterable : Iterable
            An iterable over all the links of the stream.

        """
        for link in merge(*(stream for edges in self.edges.values() for stream in edges.values())):
            yield link

    def __contains__(self, item):
        return item in self.nodes

    def __getitem__(self, item):
        """Get links in a sorted list.

        Parameters
        ----------
        item : node or (node, node)
            A single node or a pair of nodes.
            If a single node is passed, this method returns all the links of this node.
            If a pair of nodes is passed, this method returns all links among these nodes.

        Returns
        -------
        list_of_links : list(Link)
            List of links.

        """
        if isinstance(item, tuple):
            if len(item) == 2 and all(map(lambda x: isinstance(x, Hashable), item)):
                u, v = sort_nodes(item)
                if u in self.edges:
                    if v in self.edges[u]:
                        return [link for link in self.edges[u][v]]
                    else:
                        if v in self.nodes:  # both nodes are in the stream but share no link
                            return list()
                raise ValueError("One of the given nodes is not in the stream")

        elif isinstance(item, Hashable):
            if item not in self.nodes:
                raise ValueError("The given node is not in the stream")
            else:
                edge_iter = iter([])
                edge_rev_iter = iter([])
                if item in self.edges:
                    edge_iter = iter(stream for stream in self.edges[item].values())
                if item in self._reverse_edges:
                    edge_rev_iter = iter(stream for stream in self._reverse_edges[item].values())

                return [link for link in merge(*edge_iter, *edge_rev_iter)]

        raise AttributeError("This method requires as input an Hashable object or a tuple of two Hashable objects")

    def node_presence(self, node: Hashable):
        return self.nodes[node]

    def edge_presence(self, u: Hashable, v: Hashable):
        u, v = sort_nodes((u, v))
        return self.edges[u][v]

    def node_based_slice(self, selected_nodes: Optional[Iterable[Hashable]] = None):
        """Create a node-based slice of the stream.

        Parameters
        ----------
        selected_nodes : nodes
            An iterable of nodes to keep.

        Returns
        -------
        sliced_stream : StreamDict
            The dictionary data structure of the stream limited to the selected nodes.
        """
        if selected_nodes:

            # TODO ask if this is ok
            slice_nodes = {u: intervals for u, intervals in self.nodes.items() if u in selected_nodes}

            slice_edges = {u: {v: intervals for v, intervals in adj.items() if v in selected_nodes}
                           for u, adj in self.edges.items() if u in selected_nodes and len(adj) > 0}

            slice_reverse_edges = {u: {v: intervals for v, intervals in adj.items() if v in selected_nodes}
                                   for u, adj in self._reverse_edges.items() if u in selected_nodes}

            slice_reverse_edges = {u: adj for u, adj in slice_reverse_edges.items() if len(adj) > 0}

            return StreamDict(nodes=slice_nodes, edges=slice_edges, reverse_edges=slice_reverse_edges)
        else:
            return StreamDict()

    def add(self, link: Link):
        """Add a link to the stream.

        Parameters
        ----------
        link : Link
            The link to insert in the stream.

        """
        if not isinstance(link, Link):
            raise TypeError("Tried to insert a non-link object in a stream.")
        if isinstance(link.interval.left, self.interval_type):
            self._add(link)
        elif isinstance(None, self.interval_type):  # this is the first insertion ever
            self._interval_type = type(link.interval.left)  # this can only be called once
            self._add(link)
        else:
            raise TypeError(f"Tried to insert a link with interval of boundaries of type "
                            f"{type(link.interval.left)} but links of the stream have type "
                            f"{self.interval_type}")

    def _add(self, link: Link):
        _, u, v = link

        if u not in self.nodes:
            self.nodes[u] = self.data_container_factory(u)

        if v not in self.nodes:
            self.nodes[v] = self.data_container_factory(v)

        if u not in self.edges:
            self.edges[u] = self.edge_inner_container_factory()

        if v not in self.edges[u]:
            self.edges[u][v] = self.data_container_factory(u, v)

        """if v not in self._reverse_edges:
            self._reverse_edges[v] = self.edge_inner_container_factory()

        if u not in self._reverse_edges[v]:
            self._reverse_edges[v][u] = self.data_container_factory(u, v)"""

        if v not in self._reverse_edges:
            self._reverse_edges[v] = self.edge_inner_container_factory()

        if u not in self._reverse_edges[v]:
            self._reverse_edges[v][u] = self.edges[u][v]

        self.nodes[u].add(link)
        self.nodes[v].add(link)
        self.edges[u][v].add(link)
        # self._reverse_edges[v][u].add(link)
