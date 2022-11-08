import pytest
from pandas import Interval
from random import seed, shuffle
from portento.utils import IntervalTree, merge_interval


@pytest.fixture
def intervals():  # no overlapping intervals
    return [Interval(5, 10),
            Interval(10, 20),
            Interval(0, 1),
            Interval(50, 100),
            Interval(2, 3)]


@pytest.fixture
def tree(intervals):
    tree = IntervalTree(intervals)
    return tree


class TestBinarySearchTree:

    def test_iter(self, tree, intervals):
        assert list(tree) == sorted(intervals)

    def test_add(self, tree, intervals):
        assert tree._root.value == intervals[0]
        assert tree._root.right.value == intervals[1]
        assert tree._root.left.value == intervals[2]
        assert tree._root.right.right.value == intervals[3]
        assert tree._root.left.right.value == intervals[4]

        tree.add(Interval(10, 11, 'both'))
        assert tree._root.value == intervals[3]
        assert tree._root.left.right.right.value == Interval(5, 20)

    def test_full_interval(self, tree):
        assert tree._root.full_interval == Interval(0, 100)
        assert tree._root.right.full_interval == Interval(10, 100)
        assert tree._root.left.full_interval == Interval(0, 3)
        assert tree._root.right.right.full_interval == Interval(50, 100)
        assert tree._root.left.right.full_interval == Interval(2, 3)

        tree.add(Interval(10, 11, 'both'))
        assert tree._root.left.full_interval == Interval(0, 20)
        assert tree._root.left.right.full_interval == Interval(2, 20)
        assert tree._root.left.right.right.full_interval == Interval(5, 20)

    @pytest.mark.parametrize('s', list(range(20)))
    def test_update(self, s):

        def visit(n):
            if n:
                if n.left:
                    yield from visit(n.left)
                yield n
                if n.right:
                    yield from visit(n.right)

        seed(s)
        intervals = [Interval(x, x+1, 'both') for x in range(100)]
        shuffle(intervals)
        tree = IntervalTree(intervals)
        for interval in intervals:
            tree._delete_overlapping_intervals(interval)
            nodes = list(visit(tree.root))
            for node in nodes:
                full = node.full_interval
                all_instants = node.value.length
                if node.left:
                    full = merge_interval(full, node.left.full_interval)
                    all_instants += node.left.time_instants
                    assert node.full_interval.left <= node.left.full_interval.left
                    assert node.left.time_instants < node.time_instants
                if node.right:
                    full = merge_interval(full, node.right.full_interval)
                    all_instants += node.right.time_instants
                    assert node.full_interval.right >= node.right.full_interval.right
                    assert node.right.time_instants < node.time_instants

                assert node.full_interval == full
                assert node.time_instants == all_instants

        for interval in intervals:
            tree.add(interval)  # reinsert interval
            nodes = list(visit(tree.root))
            for node in nodes:
                full = node.full_interval
                all_instants = node.value.length
                if node.left:
                    full = merge_interval(full, node.left.full_interval)
                    all_instants += node.left.time_instants
                    assert node.full_interval.left <= node.left.full_interval.left
                    assert node.left.time_instants < node.time_instants
                if node.right:
                    full = merge_interval(full, node.right.full_interval)
                    all_instants += node.right.time_instants
                    assert node.full_interval.right >= node.right.full_interval.right
                    assert node.right.time_instants < node.time_instants

                assert node.full_interval == full
                assert node.time_instants == all_instants
