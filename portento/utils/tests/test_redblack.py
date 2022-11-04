import pytest
from pandas import Interval
import random
from math import log2

from portento.utils import IntervalTree, IntervalTreeNode


def black_root(tree: IntervalTree):
    if tree.root:
        return tree.root.color.value  # BLACK

    return True


def red_has_black_child(node: IntervalTreeNode):
    if not node:
        return True

    if not node.color.value:  # RED
        return all([not node.left or node.left.color.value,
                    not node.right or node.right.color.value,
                    red_has_black_child(node.left) and red_has_black_child(node.right)])

    return red_has_black_child(node.left) and red_has_black_child(node.right)


def height(node: IntervalTreeNode):
    if not node:
        return 0
    return 1 + max(height(node.left), height(node.right))


def same_q_black_paths(node):
    if not node:
        return True
    return q_black_hidden(node, node.left, 0) == q_black_hidden(node, node.right, 0)


def q_black_hidden(node, node_next, q):
    new_q = q + (1 if node.color.value else 0)
    if not node_next:
        return new_q
    else:
        compute_left = q_black_hidden(node_next, node_next.left, new_q)
        compute_right = q_black_hidden(node_next, node_next.right, new_q)
        if compute_left == compute_right:
            return compute_left
        else:  # this will never happen
            raise Exception


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

    @pytest.mark.parametrize('s', list(range(20)))
    def test_properties(self, s):
        random.seed(s)
        n = 100

        intervals = random.sample([Interval(x, x+1) for x in range(n)], n)
        tree = IntervalTree()
        for interval in intervals:
            tree._rb_add(interval)
            assert black_root(tree)
            assert red_has_black_child(tree.root)
            assert same_q_black_paths(tree.root)
            assert height(tree.root) <= 2*log2(n+1)
