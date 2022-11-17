from typing import Optional, Iterable, Union
from pandas import Interval
from numpy import number
from enum import Enum
from dataclasses import dataclass, field
from portento.utils.intervals_functions import cut_interval, merge_interval, contains_interval
import operator


# TODO change str

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
        if self.parent and self.parent.left:
            return self.value == self.parent.left.value
        return False

    def minimum(self, with_parent=False):
        """Navigate recursively the left children.

        Returns
        -------
            The minimum value of this subtree.
        """
        if self.left:
            return self.left.minimum(with_parent)

        return self if not with_parent else (self, (self.parent if self else None))

    def get_sibling(self):
        if self.parent:
            return self.parent.right if self.is_left() else self.parent.left
        else:
            return None

    def _compute_data(self):
        self._compute_time_instants()
        self._compute_full_interval()

    def _update_data_add(self):
        self._update_time_instants_add()
        self._update_full_interval_add()

    def _update_data_delete(self):
        self._update_time_instants_delete()
        self._update_full_interval_delete()

    def _compute_time_instants(self):
        self.time_instants = sum([self.length,
                                  (self.left.time_instants if self.left else 0),
                                  (self.right.time_instants if self.right else 0)])

    def _update_time_instants(self, update_op):
        """Iteratively update the count of time instants navigating through parents

        """
        parent = self.parent
        while parent:
            parent.time_instants = update_op(parent.time_instants, self.length)
            parent = parent.parent

    def _update_time_instants_add(self):
        self._update_time_instants(operator.add)

    def _update_time_instants_delete(self):
        self._update_time_instants(operator.sub)

    def _compute_full_interval(self):
        """Update full_interval.

        """
        self.full_interval = merge_interval(self.value,
                                            self.left.full_interval if self.left else None,
                                            self.right.full_interval if self.right else None)

    def _update_full_interval(self):
        """

        """
        parent = self.parent
        while parent:
            parent._compute_full_interval()
            parent = parent.parent

    def _update_full_interval_add(self):
        self._update_full_interval()

    def _update_full_interval_delete(self):

        if self.parent:
            sibling = self.get_sibling()

            self.parent.full_interval = merge_interval(self.parent.value,
                                                       sibling.full_interval if sibling else None,
                                                       self.left.full_interval if self.left else None,
                                                       self.right.full_interval if self.right else None)
            self.parent._update_full_interval()


class IntervalTree:
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
        datum_node = self._merge_all_overlap(datum_node)

        if datum_node:
            if self.root:
                self._add_in_subtree(self.root, datum_node)
            else:
                self.root = datum_node

            self._rb_insert_fixup(datum_node)

    def _add_in_subtree(self, subtree, node):
        if subtree.overlaps(node):
            raise Exception("This should not happen at this point. All overlapping nodes have been removed.")
        else:
            if node.value <= subtree.value:
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

        node._update_data_add()

    def _delete(self, node: IntervalTreeNode):
        if not node:
            raise AttributeError("The node to delete must be not None.")
        print(f"DELETING {node}")
        y = node
        y_original_color = y.color
        print(y_original_color)
        y._update_data_delete()

        if not node.left and not node.right:
            child, sibling, parent, is_left = self.__delete_node_has_no_children(y)

        elif not node.left:
            child, sibling, parent, is_left = self.__delete_node_has_no_child_left(y)

        elif not node.right:
            child, sibling, parent, is_left = self.__delete_node_has_no_child_right(y)

        else:  # node has 2 children
            y, parent = node.right.minimum(with_parent=True)  # y is the successor of node
            y_original_color = y.color
            y._update_data_delete()
            if parent == node:  # node.right has no left child
                child, sibling, parent, is_left = self.__delete_node_has_both_children_successor(node, y)
            else:
                child, sibling, parent, is_left = self.__delete_node_has_both_children_no_successor(node, y, parent)

            y.color = node.color
            y._compute_data()
            y._update_data_add()

        print(y_original_color)
        if y_original_color == Color.BLACK:
            self._rb_recursive_delete_fixup(child, sibling, parent, is_left)

    def __delete_node_has_no_children(self, node):
        print("NO CHILD")
        is_left = node.is_left()
        parent = node.parent
        sibling = node.get_sibling()
        self._transplant(node, None)
        return None, sibling, parent, is_left

    def __delete_node_has_no_child_left(self, node):
        print(f"NOT NODE LEFT")
        child = node.right
        self._transplant(node, child)
        return child, child.get_sibling(), child.parent, child.is_left()

    def __delete_node_has_no_child_right(self, node):
        print("NOT NODE RIGHT")
        child = node.left
        self._transplant(node, child)
        return child, child.get_sibling(), child.parent, child.is_left()

    def __delete_node_has_both_children_successor(self, node, y):
        print("YES SUCCESSOR")
        self._transplant(node, y)
        y.left = node.left
        y.left.parent = y
        print(f"{y.right}, \n{y.left}, \n{y}, \n{False}")
        return y.right, y.left, y, False

    def __delete_node_has_both_children_no_successor(self, node, y, parent):
        print("NO SUCCESSOR")
        child = y.right  # right child of node's successor in its subtree
        is_left = y.is_left()
        self._transplant(y, y.right)
        y.right = node.right
        if y.right:
            y.right.parent = y
        self._transplant(node, y)
        y.left = node.left
        y.left.parent = y
        print(f"parent (y) is: {parent}, {parent.left}, {parent.right}")
        sibling = parent.right if is_left else parent.left
        print(f"SIBLING IS {sibling}, {is_left}, {sibling.parent if sibling else None}")
        print(f"{child}, \n{sibling}, \n{parent}, \n{is_left}")
        return child, sibling, parent, is_left

    def _merge_all_overlap(self, node):

        overlap = self._find_overlap(node)

        while overlap:
            self._delete(overlap)
            node = self.__class__()._merge(node, overlap)
            overlap = self._find_overlap(node)

        return node

    def _find_overlap(self, node):
        return self._find_overlap_in_subtree(self.root, node)

    def _find_overlap_in_subtree(self, subtree, node):
        if subtree:
            if subtree.full_interval.overlaps(node.value):
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

    def _left_rotate(self, node: IntervalTreeNode):
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

            node._compute_data()
            pivot._compute_data()

        else:
            raise Exception(f"left rotation is impossible. {node}, {node.left}")

    def _right_rotate(self, node: IntervalTreeNode):
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

            node._compute_data()
            pivot._compute_data()

        else:
            raise Exception(f"left rotation is impossible. {node}, {node.right}")

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
                    print(f"node:{node},"
                          f"sibling:{sibling},"
                          f"parent:{parent}"
                          f"is_left:{is_left}")
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
                    print(f"node:{node},"
                          f"sibling:{sibling},"
                          f"parent:{parent}"
                          f"is_left:{is_left}")
                    self.__delete_fixup_case_4(node, sibling, parent, is_left)

    def __delete_fixup_case_1(self, node, sibling, parent, is_left):
        # case 1: sibling is RED.
        # This becomes case 2, 3 or 4.
        print("IN CASE 1")
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
        print("IN CASE 2")
        sibling.color = Color.RED
        is_left = parent.is_left()
        sibling = parent.parent.right if is_left else parent.parent.left if parent and parent.parent else None

        print(f"node:{node},"
              f"sibling:{sibling},"
              f"parent:{parent}"
              f"is_left:{is_left}")
        print("XXX")

        self._rb_recursive_delete_fixup(parent, sibling, parent.parent, is_left)

    def __delete_fixup_case_3(self, node, sibling, parent, is_left):
        # case 3: right sibling is black with right child black and left child red or
        # case 3: left sibling is black with left child black and right child red
        # this becomes case 4.
        print("IN CASE 3")
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
        # case 4: right sibling is black with right child red or
        # case 4: left sibling is black with left child red
        print("IN CASE 4")
        parent_original_color = parent.color if parent else Color.BLACK
        if parent:
            parent.color = Color.BLACK

        if is_left:
            if sibling:
                sibling.color = parent_original_color
                if sibling.right:
                    sibling.right.color = Color.BLACK

            else:
                raise Exception("LEFT ROTATE WON'T BE POSSIBLE")

            self._left_rotate(parent)

        else:
            if sibling:
                sibling.color = parent_original_color
                if sibling.left:
                    sibling.left.color = Color.BLACK
            else:
                raise Exception("LEFT ROTATE WON'T BE POSSIBLE")

            self._right_rotate(parent)

        print("NOW I'M THE ROOT!")
        self._rb_recursive_delete_fixup(self.root, None, None, True)

    @classmethod
    def _create_node(cls, data):
        # data is assumed to be Interval
        return IntervalTreeNode(data)

    @classmethod
    def _merge(cls, node_1, node_2):
        # data is assumed to be Interval
        return IntervalTreeNode(merge_interval(node_1.value, node_2.value))
