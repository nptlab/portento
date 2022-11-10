import itertools

import pytest
from pandas import Interval
import random
from math import log2

from portento.utils import IntervalTree, IntervalTreeNode, merge_interval


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
        else:  # this must never happen
            raise Exception(f"compute_left={compute_left}, compute_right={compute_right}")


def find(node, interval):
    if node:
        if node.value == interval:
            return node
        elif node.value > interval:
            return find(node.left, interval)
        else:
            return find(node.right, interval)

    raise Exception(f"{interval}")  # this must never happen


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
    def test_add_delete(self, s):
        random.seed(s)
        n = 127
        intervals = random.sample([Interval(x, x + 1) for x in range(n)], n)
        tree = IntervalTree()

        for n_added, interval in [(i+1, interval) for i, interval in enumerate(intervals)]:
            tree.add(interval)
            assert black_root(tree)
            assert red_has_black_child(tree.root)
            assert same_q_black_paths(tree.root)
            assert height(tree.root) <= 2 * log2(n_added + 1)

        intervals = random.sample(intervals, n)

        for n_deleted, interval in [(i+1, interval) for i, interval in enumerate(intervals)]:
            node = find(tree.root, interval)
            tree._rb_delete(node)
            assert black_root(tree)
            assert red_has_black_child(tree.root)
            assert same_q_black_paths(tree.root)
            assert height(tree.root) <= 2 * log2(n - n_deleted + 1)


    @pytest.mark.parametrize('s', list(range(20)))
    def test_update(self, s):

        def visit(n):
            if n:
                if n.left:
                    yield from visit(n.left)
                yield n
                if n.right:
                    yield from visit(n.right)

        random.seed(s)
        n = 190
        intervals = list(itertools.compress(map(lambda x: Interval(x, x+1, 'both'), range(n)), itertools.cycle([1,1,0])))
        tree = IntervalTree(intervals)
        intervals = random.sample(intervals, len(intervals))

        for interval in intervals:
            to_delete = tree._find_overlap(IntervalTreeNode(interval))
            if to_delete:
                tree._rb_delete(to_delete)
                nodes = list(visit(tree.root))
                for node in nodes:
                    all_instants = node.value.length
                    if node.left:
                        all_instants += node.left.time_instants
                    if node.right:
                        all_instants += node.right.time_instants

                    assert node.time_instants == all_instants, f"{node.value, node.time_instants}" \
                                                           f"{(node.left.value, node.left.time_instants) if node.left else None}" \
                                                           f"{(node.right.value, node.right.time_instants )if node.right else None}"

        """for interval in intervals:
            tree.add(interval)  # reinsert interval
            nodes = list(visit(tree.root))
            for node in nodes:
                all_instants = node.value.length
                if node.left:
                    all_instants += node.left.time_instants
                if node.right:
                    all_instants += node.right.time_instants

                assert node.time_instants == all_instants, f"{node.value, node.time_instants}" \
                                                           f"{node.left.value, node.left.time_instants if node.left else None}" \
                                                           f"{node.right.value, node.right.time_instants if node.right else None}"
                                                           """
