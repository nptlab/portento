from typing import Optional, Iterable, Union
from pandas import Interval
from numpy import number
from enum import Enum
from dataclasses import dataclass, field
from portento.utils.intervals_functions import cut_interval, merge_interval
import operator


# TODO delete when adding one by one (if overlaps than delete, merge and reinsert until reach a leaf)

class Color(Enum):
    RED = False
    BLACK = True


@dataclass
class IntervalTreeNode:
    """The node class for the IntervalTree.

    """
    value: Interval
    parent: Optional['IntervalTreeNode'] = field(default=None, compare=False)
    left: Optional['IntervalTreeNode'] = field(default=None, compare=False)
    right: Optional['IntervalTreeNode'] = field(default=None, compare=False)
    color: Color = field(default=Color.RED, init=False, compare=False)
    full_interval: Interval = field(default=None, init=False, compare=False)
    time_instants: Union[int, float, number] = field(default=None, init=False, compare=False)

    def __post_init__(self):
        self.full_interval = self.value
        self.time_instants = self.value.length

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
        return True if self.parent and self.parent.left and self.value == self.parent.left.value else False

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
                self._update_time_instants_addition()
            else:
                self.left.add(other)
        else:  # other.value > self.value
            if not self.right:
                other.parent = self
                self.right = other
                # update additional information recursively
                self._update_full_interval()
                self._update_time_instants_addition()
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

    def minimum(self, with_parent=False):
        """Navigate recursively the left children.

        Returns
        -------
            The minimum value of this subtree.
        """
        if self.left:
            return self.left.minimum(with_parent)

        return self if not with_parent else (self, (self.parent if self else None))

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

    def _compute_time_instants(self):
        self.time_instants = self.length + \
                             (self.left.time_instants if self.left else 0) + \
                             (self.right.time_instants if self.right else 0)

    def _update_time_instants(self, update_op):
        """Recursively update full_interval navigating through parents

        """
        parent = self.parent
        while parent:
            parent.time_instants = update_op(parent.time_instants, self.length)
            parent = parent.parent

    def _update_time_instants_add(self):
        self._update_time_instants(operator.add)

    def _update_time_instants_delete(self):
        self._update_time_instants(operator.sub)

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

    def add(self, datum: value_type):
        """Add an interval to the tree_view, merging it with the overlapping intervals

        Parameters
        ----------
        datum : Interval

        """
        datum_node = self.__class__()._create_node(datum)
        overlap = self._find_overlap(datum_node)
        while overlap:
            datum_node = self.__class__()._merge(datum_node, overlap)
            if not datum_node:
                raise Exception(f"Not datum! {overlap.value}")
            self._rb_delete(overlap)
            overlap = self._find_overlap(datum_node)

        self._add(datum_node)

    def _add(self, node):
        if self.root:
            self._add_in_subtree(self.root, node)
        else:
            self.root = node

        self._rb_insert_fixup(node)

    def _add_in_subtree(self, subtree, node):
        print(f"Add {node.value} in {(subtree.value, subtree.time_instants) if subtree else None}")
        if subtree.overlaps(node):
            raise Exception("This should not happen at this point. All overlapping nodes have been removed.")
        else:
            if node.value < subtree.value:
                if not subtree.left:
                    node.parent = subtree
                    subtree.left = node
                else:
                    self._add_in_subtree(subtree.left, node)
                    return
            else:  # other.value > self.value
                if not subtree.right:
                    node.parent = subtree
                    subtree.right = node
                else:
                    self._add_in_subtree(subtree.right, node)
                    return

        node._update_time_instants_add()

    def _rb_delete(self, node: IntervalTreeNode):
        print(f"deleting {node.value, node.time_instants}")
        if not node:
            raise AttributeError("The node to delete must be not None.")

        y = node
        y_original_color = Color.RED if y and y.color == Color.RED else Color.BLACK
        y._update_time_instants_delete()
        if not node.left:
            child = node.right
            is_left = node.is_left()
            parent = node.parent
            if parent:
                if is_left:
                    sibling = parent.right
                else:
                    sibling = parent.left
            else:
                sibling = None
            self._transplant(node, node.right)

        elif not node.right:
            child = node.left
            is_left = node.is_left()
            parent = node.parent
            if parent:
                if is_left:
                    sibling = parent.right
                else:
                    sibling = parent.left
            else:
                sibling = None
            self._transplant(node, node.left)

        else:  # node has 2 children
            y, parent = node.right.minimum(with_parent=True)  # y is the successor of node
            y_original_color = Color.RED if y and y.color == Color.RED else Color.BLACK
            child = y.right  # right child of node's successor in its subtree
            is_left = y.is_left()
            y._update_time_instants_delete()
            if y.parent == node:  # y == node.right
                if child:
                    child.parent = y
                parent = y
                is_left = y.is_left()  # TODO
            else:
                self._transplant(y, y.right)
                y.right = node.right
                if y.right:
                    y.right.parent = y

            self._transplant(node, y)
            y.left = node.left
            y.left.parent = y
            sibling = parent.right if is_left else parent.left
            y.color = node.color
            y._compute_time_instants()
            y._update_time_instants_add()

        if y_original_color.value:
            self._rb_recursive_delete_fixup(child, sibling, parent, is_left)

    def all_overlaps(self, interval: Interval):
        if interval:
            node = self.__class__()._create_node(interval)
            return IntervalTree(self.root.all_overlaps(node, full_node=True))
        else:
            return self

    def _delete_overlapping_intervals(self, datum: value_type):

        datum_node = self.__class__()._create_node(datum)

        if self.root:
            for overlapping_node in list(self.root.all_overlaps(datum_node, full_node=True)):
                # TODO probably will have to change this when implementing red-black trees
                datum_node = datum_node._merge_values(overlapping_node)
                self._rb_delete(overlapping_node)

        return datum_node

    def _find_overlap(self, node):
        return self._find_overlap_in_subtree(self.root, node)

    def _find_overlap_in_subtree(self, subtree, node):
        if subtree:
            if subtree.overlaps(node):
                return subtree
            else:
                overlap = self._find_overlap_in_subtree(subtree.left, node)
                if not overlap:
                    overlap = self._find_overlap_in_subtree(subtree.right, node)
                return overlap

        return None

    def _transplant(self, to_substitute: IntervalTreeNode, substitute: IntervalTreeNode):

        if not to_substitute.parent:
            self.root = substitute
        else:

            if to_substitute.is_left():
                to_substitute.parent.left = substitute
            else:
                to_substitute.parent.right = substitute

        if substitute:
            substitute.parent = to_substitute.parent
            substitute._compute_time_instants()

    def _left_rotate(self, node: IntervalTreeNode):
        print(
            f"Rotating Left node={node.value, node.time_instants}, pivot={(node.right.value, node.right.time_instants) if node.right else None}")
        pivot = node.right
        if pivot:
            node.right = pivot.left
            if pivot.left:
                pivot.left.parent = node
            pivot.parent = node.parent
            if not node.parent:
                self.root = pivot
            elif node.is_left():
                node.parent.left = pivot
            else:
                node.parent.right = pivot

            pivot.left = node
            node.parent = pivot

            # TODO this could be more efficient
            node._compute_time_instants()
            pivot._compute_time_instants()

        else:
            raise TypeError("Left rotation is impossible.")

    def _right_rotate(self, node: IntervalTreeNode):
        print(
            f"Rotating Right node={node.value, node.time_instants}, pivot={(node.left.value, node.left.time_instants) if node.left else None}")
        pivot = node.left
        if pivot:
            node.left = pivot.right
            if pivot.right:
                pivot.right.parent = node
            pivot.parent = node.parent
            if not node.parent:
                self.root = pivot
            elif node.is_left():
                node.parent.left = pivot
            else:
                node.parent.right = pivot

            pivot.right = node
            node.parent = pivot

            node._compute_time_instants()
            pivot._compute_time_instants()

        else:
            raise TypeError("Right rotation is impossible.")

    def _rb_insert_fixup(self, node: IntervalTreeNode):
        while node.parent and node.parent.color is Color.RED:
            grandparent = node.parent.parent
            if grandparent and node.parent.is_left():
                uncle = grandparent.right
                if uncle and uncle.color is Color.RED:  # case 1
                    node.parent.color = Color.BLACK
                    uncle.color = Color.BLACK
                    grandparent.color = Color.RED
                    node = grandparent
                else:
                    if not node.is_left():  # case 2
                        node = node.parent
                        self._left_rotate(node)
                    node.parent.color = Color.BLACK  # case 3
                    node.parent.parent.color = Color.RED
                    self._right_rotate(node.parent.parent)

            else:
                uncle = grandparent.left
                if uncle and uncle.color is Color.RED:  # case 1
                    node.parent.color = Color.BLACK
                    uncle.color = Color.BLACK
                    grandparent.color = Color.RED
                    node = grandparent
                else:
                    if node.is_left():  # case 2
                        node = node.parent
                        self._right_rotate(node)
                    node.parent.color = Color.BLACK  # case 3
                    node.parent.parent.color = Color.RED
                    self._left_rotate(node.parent.parent)

        self.root.color = Color.BLACK

    def _rb_recursive_delete_fixup(self, node: IntervalTreeNode,
                                   sibling: IntervalTreeNode,
                                   parent: IntervalTreeNode,
                                   is_left: bool):
        if node == self.root or (node.color == Color.RED if node else False):
            if node:
                node.color = Color.BLACK
        else:
            if is_left:
                if sibling.color == Color.RED if sibling else False:
                    # case 1: sibling is RED
                    # this becomes case 2, 3 or 4.
                    self.__delete_fixup_case_1(node, sibling, parent, is_left)

                elif sibling and \
                    (sibling.left.color.value if sibling.left else True) and \
                    (sibling.right.color.value if sibling.right else True):
                    # case 2: sibling is black with both children black
                    self.__delete_fixup_case_2(node, sibling, parent, is_left)

                elif sibling and (sibling.right.color.value if sibling.right else True):
                    # case 3: sibling is black with right child black and left child red
                    # this becomes case 4.
                    self.__delete_fixup_case_3(node, sibling, parent, is_left)

                else:
                    # case 4: sibling is black with right child red
                    self.__delete_fixup_case_4(node, sibling, parent, is_left)

            else:
                # as before with left and right swapped
                if sibling.color == Color.RED if sibling else False:
                    # case 1: sibling is RED
                    # this becomes case 2, 3 or 4.
                    self.__delete_fixup_case_1(node, sibling, parent, is_left)

                elif sibling and \
                    (sibling.left.color.value if sibling.left else True) and \
                    (sibling.right.color.value if sibling.right else True):
                    # case 2: sibling is black with both children black
                    self.__delete_fixup_case_2(node, sibling, parent, is_left)

                elif sibling and (sibling.left.color.value if sibling.left else True):
                    # case 3: sibling is black with left child black and right child red
                    # this becomes case 4.
                    self.__delete_fixup_case_3(node, sibling, parent, is_left)

                else:
                    # case 4: sibling is black with left child red
                    self.__delete_fixup_case_4(node, sibling, parent, is_left)

    def __delete_fixup_case_1(self, node, sibling, parent, is_left):
        # case 1: sibling is RED.
        # This becomes case 2, 3 or 4.
        if is_left:
            sibling.color = Color.BLACK
            parent.color = Color.RED
            self._left_rotate(parent)
            sibling = parent.right
        else:
            sibling.color = Color.BLACK
            parent.color = Color.RED
            self._right_rotate(parent)
            sibling = parent.left

        self._rb_recursive_delete_fixup(node, sibling, parent, is_left)

    def __delete_fixup_case_2(self, node, sibling, parent, is_left):
        # case 2: sibling is black with both children black.
        if is_left:
            sibling.color = Color.RED
            node = parent  # it can't be None
            parent = node.parent  # if this is None, then the node is the root
            is_left = node.is_left()
            if parent:
                sibling = parent.right if is_left else parent.left
            else:
                sibling = None
        else:
            sibling.color = Color.RED
            node = parent  # it can't be None
            parent = node.parent  # if this is None, then the node is the root
            is_left = node.is_left()
            if parent:
                sibling = parent.right if is_left else parent.left
            else:
                sibling = None

        self._rb_recursive_delete_fixup(node, sibling, parent, is_left)

    def __delete_fixup_case_3(self, node, sibling, parent, is_left):
        # case 3: right sibling is black with right child black and left child red or
        # case 3: left sibling is black with left child black and right child red
        # this becomes case 4.
        if is_left:
            sibling.left.color = Color.BLACK
            sibling.color = Color.RED
            self._right_rotate(sibling)
            sibling = parent.right
        else:
            sibling.right.color = Color.BLACK
            sibling.color = Color.RED
            self._left_rotate(sibling)
            sibling = parent.left

        self._rb_recursive_delete_fixup(node, sibling, parent, is_left)

    def __delete_fixup_case_4(self, node, sibling, parent, is_left):
        # case 4: right sibling is black with right child red
        # case 4: left sibling is black with left child red
        if is_left:
            if sibling:
                sibling.color = parent.color if parent else Color.BLACK
            if parent:
                parent.color = Color.BLACK
            if sibling and sibling.right:
                sibling.right.color = Color.BLACK

            self._left_rotate(parent)
            node = self.root
            parent = None
            is_left = True
            sibling = None

        else:
            if sibling:
                sibling.color = parent.color if parent else Color.BLACK
            if parent:
                parent.color = Color.BLACK
            if sibling and sibling.left:
                sibling.left.color = Color.BLACK

            self._right_rotate(parent)
            node = self.root
            parent = None
            is_left = True
            sibling = None

        self._rb_recursive_delete_fixup(node, sibling, parent, is_left)

    @classmethod
    def _create_node(cls, data):
        # data is assumed to be Interval
        return IntervalTreeNode(data)

    @classmethod
    def _merge(cls, node_1, node_2):
        # data is assumed to be Interval
        return IntervalTreeNode(merge_interval(node_1.value, node_2.value))
