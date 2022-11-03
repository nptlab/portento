import pytest
from pandas import Interval

from portento.utils import IntervalTree, IntervalTreeNode


def black_root(tree: IntervalTree):
    if tree.root:
        return tree.root.color.value  # BLACK

    return True


def black_child(node: IntervalTreeNode):
    if not node.color.value:  # RED
        return all([not node.left or node.left.color.value, not node.right or node.right.color.value])


def count_n_black(node: IntervalTreeNode):
    if not node:
        return 1

    return count_n_black(node.left) + count_n_black(node.right)


def same_n_black(node: IntervalTreeNode):
    if not node:
        return True

    return count_n_black(node.left) == count_n_black(node.right)


class TestRedBlackTree:

    @pytest.mark.parametrize('intervals', [
        dict(zip(['x', 'alpha', 'y', 'beta', 'gamma'],
                 [Interval(2, 3),
                  Interval(0, 1),
                  Interval(6, 7),
                  Interval(4, 5),
                  Interval(8, 9)]
                 )),
    ])
    def test_rotate(self, intervals):
        tree = IntervalTree(intervals.values())
        tree._left_rotate(tree.root)

        assert tree.root.value == intervals['y']
        assert tree.root.left.value == intervals['x']
        assert tree.root.left.left.value == intervals['alpha']
        assert tree.root.left.right.value == intervals['beta']
        assert tree.root.right.value == intervals['gamma']

        tree._right_rotate(tree.root)

        assert tree.root.value == intervals['x']
        assert tree.root.right.value == intervals['y']
        assert tree.root.left.value == intervals['alpha']
        assert tree.root.right.left.value == intervals['beta']
        assert tree.root.right.right.value == intervals['gamma']
