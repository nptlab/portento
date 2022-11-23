from heapq import merge
from collections.abc import Hashable
from typing import Optional, Iterable

from portento.utils import IntervalContainer, Link, sort_nodes


class StreamDict:
    """The data structure for stream graphs that allows node-based slices

    """
    node_container_factory = dict
    edge_outer_container_factory = dict
    edge_inner_container_factory = dict
    data_container_factory = IntervalContainer

    def __init__(self, links: Optional[Iterable[Link]] = list()):
        self._nodes = self.node_container_factory()
        self._edges = self.edge_outer_container_factory()
        self._reverse_edges = self.edge_outer_container_factory()

        for link in links:
            self.add(link)

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

    def add(self, link: Link):
        """Add a link to the stream.

        Parameters
        ----------
        link : Link
            The link to insert in the stream.

        """
        if not isinstance(link, Link):
            raise TypeError("Tried to insert a non-link object in a stream.")

        self._add(link)

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

        if v not in self._reverse_edges:
            self._reverse_edges[v] = self.edge_inner_container_factory()

        if u not in self._reverse_edges[v]:
            self._reverse_edges[v][u] = self.edges[u][v]

        self.nodes[u].add(link)
        self.nodes[v].add(link)
        self.edges[u][v].add(link)
