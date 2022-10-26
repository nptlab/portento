from typing import Optional, Iterable, Union
from pandas import Interval
from numpy import number
from dataclasses import dataclass, field
from .intervals_functions import cut_interval, merge_interval


@dataclass
class IntervalTreeNode:
    """The node class for the IntervalTree.

    """
    value: Interval
    parent: Optional['IntervalTreeNode'] = field(default=None, compare=False)
    left: Optional['IntervalTreeNode'] = field(default=None, compare=False)
    right: Optional['IntervalTreeNode'] = field(default=None, compare=False)
    full_interval: Interval = field(default=None, init=False, compare=False)
    time_instants: Union[int, float, number] = field(default=None, init=False, compare=False)

    def __post_init__(self):
        self.full_interval = self.value
        self.time_instants = self.value.length  # TODO ask for this

    def __iter__(self):
        """Iterate over the nodes depth-first visit.

        Returns
        -------
            An iterable over the ordered intervals.
        """
        # iteration is ordered
        if self:
            if self.left:
                yield from iter(self.left)
            yield self.value
            if self.right:
                yield from iter(self.right)

    def __str__(self):
        to_show = [self.value, self.full_interval, str(self.time_instants)]
        if self.left:
            to_show.append(self.left.value)
        if self.right:
            to_show.append(self.right.value)
        return str(to_show)

    @property
    def length(self):
        return self.value.length

    def overlaps(self, other):
        """Check if two nodes have overlapping intervals.

        Parameters
        ----------
        other : IntervalTreeNode

        Returns
        -------
            True if the intervals in the two nodes overlap.
        """
        return self.value.overlaps(other.value)

    def is_left(self):
        """Check if the node is left child of its parent.
        If the node has no parent, returns False

        Returns
        -------
            True if the node is left child of its parent
        """
        return self.parent is not None and self.parent.left is not None and self.value == self.parent.left.value

    def add(self, other):
        """Add a node recursively in the subtree with this node as route.
        Respects the structure of the Binary Search Tree.

        Parameters
        ----------
        other : IntervalTreeNode
            The node to insert to the subtree.

        """

        if other.value < self.value:
            if not self.left:
                other.parent = self
                self.left = other
                # update additional information recursively
                self._update_full_interval()
                self._update_time_instants()
            else:
                self.left.add(other)
        else:  # other.value > self.value
            if not self.right:
                other.parent = self
                self.right = other
                # update additional information recursively
                self._update_full_interval()
                self._update_time_instants()
            else:
                self.right.add(other)

    def all_overlaps(self, node, full_node=False):
        """Find all the nodes in the subtree that represent an interval overlapping with the one of the given node.

        Parameters
        ----------
        node : IntervalTreeNode
        full_node : Bool
            Whether to return the full node or the interval only.

        Returns
        -------
            An iterable over the nodes that have an interval overlapping with the one of the given node.
        """
        if self.full_interval.overlaps(node.full_interval):
            if self.overlaps(node):
                if full_node:
                    yield self
                else:
                    yield self._slice_cut(node)
            if self.left:
                yield from self.left.all_overlaps(node, full_node=full_node)
            if self.right:
                yield from self.right.all_overlaps(node, full_node=full_node)

    def minimum(self):
        """Navigate recursively the left children.

        Returns
        -------
            The minimum value of this subtree.
        """
        if self.left:
            return self.left.minimum()
        return self

    def _update_full_interval(self):
        # TODO one unique update operation, an higher-order function
        """Recursively update full_interval navigating through parents

        """
        new_full_interval = self.value
        if self.left:
            new_full_interval = merge_interval(new_full_interval, self.left.full_interval)
        if self.right:
            new_full_interval = merge_interval(new_full_interval, self.right.full_interval)

        self.full_interval = new_full_interval

        if self.parent:  # recursively update the full intervals
            self.parent._update_full_interval()

    def _update_time_instants(self):
        """Recursively update full_interval navigating through parents

        """
        new_time_instants = self.length
        if self.left:
            new_time_instants += self.left.time_instants
        if self.right:
            if not self.right.time_instants:
                print("self.", self)
                print("self.right", self.right)
                if self.right.left:
                    print("self.right.left", self.right.left)
                if self.right.right:
                    print("self.right.right", self.right.right)
            new_time_instants += self.right.time_instants

        self.time_instants = new_time_instants

        if self.parent:  # recursively update the time instants
            self.parent._update_time_instants()

    def _merge_values(self, other):
        return IntervalTreeNode(merge_interval(self.value, other.value))

    def _slice_cut(self, other):
        return cut_interval(self.value, other.value)


class IntervalTree:
    # TODO transform in proper red-black tree_view of intervals
    """The data structure that holds intervals.

    """

    value_type = Interval

    def __init__(self, data: Optional[Iterable[value_type]] = None):
        self._root = None
        if data:
            for d in data:
                self.add(d)

    def __iter__(self):
        if self.root:
            return iter(self.root)

        return iter(list())

    @property
    def root(self):
        return self._root

    @root.setter
    def root(self, new_root):
        self._root = new_root

    @property
    def length(self):
        """The summation of the length of all intervals in the tree

        """
        if self.root:
            return self.root.time_instants
        else:
            return 0

    def time_based_slice(self, interval: Optional[Interval] = None):
        """Return an IntervalTree

        Parameters
        ----------
        interval : Interval
            The interval required.

        Returns
        -------
        interval_tree : IntervalTree
        """
        if interval:
            node = self.__class__()._create_node(interval)
            return IntervalTree(self.root.all_overlaps(node))
        else:
            return self

    def add(self, datum: value_type):
        """Add an interval to the tree_view, merging it with the overlapping intervals

        Parameters
        ----------
        datum : Interval

        """
        datum_node = self._delete_overlapping_intervals(datum)
        if self.root:
            self.root.add(datum_node)
        else:
            self.root = datum_node

    def _delete_overlapping_intervals(self, datum: value_type):

        datum_node = self.__class__()._create_node(datum)

        if self.root:
            for overlapping_node in list(self.root.all_overlaps(datum_node, full_node=True)):
                # TODO probably will have to change this when implementing red-black trees
                datum_node = datum_node._merge_values(overlapping_node)
                self._delete(overlapping_node)

        return datum_node

    def _delete(self, node: IntervalTreeNode):
        transplant_parent = None

        if not node.left:
            transplant_node = node.right
            if transplant_node:
                transplant_parent = transplant_node.parent
            self._transplant(node, transplant_node)
        elif not node.right:
            transplant_node = node.left
            transplant_parent = transplant_node.parent
            if transplant_node:
                transplant_parent = transplant_node.parent
            self._transplant(node, transplant_node)
        else:  # has both children
            transplant_node = node.right.minimum()
            transplant_parent = transplant_node.parent
            if transplant_node.parent != node:
                self._transplant(transplant_node, transplant_node.right)
                transplant_node.right = node.right
                transplant_node.right.parent = transplant_node
            self._transplant(node, transplant_node)
            transplant_node.left = node.left
            transplant_node.left.parent = transplant_node

        if transplant_parent:
            transplant_parent._update_full_interval()
            transplant_parent._update_time_instants()

        if transplant_node:
            transplant_node._update_full_interval()
            transplant_node._update_time_instants()

    def _transplant(self, to_substitute: IntervalTreeNode, substitute: IntervalTreeNode):
        if not to_substitute.parent:
            self.root = substitute
        elif to_substitute.is_left():
            to_substitute.parent.left = substitute
        else:
            to_substitute.parent.right = substitute
        if substitute:
            substitute.parent = to_substitute.parent
            if not substitute.left and not substitute.right:
                substitute.full_interval = substitute.value
            else:
                substitute.full_interval = to_substitute.full_interval

    @classmethod
    def _create_node(cls, data):
        # data is assumed to be Interval
        return IntervalTreeNode(data)
