from portento.utils import *
from pandas import Interval
import random
import itertools


def find(node, interval):
    if node:
        if node.value == interval:
            return node
        elif node.value > interval:
            return find(node.left, interval)
        else:
            return find(node.right, interval)

    raise Exception(f"{interval}")  # this must never happen


def print_tree(n):
    if n:
        print(n.value, ": ", n.full_interval, "\n",
              "parent:", n.parent.value if n.parent else None, "\n",
              "left:", n.left.value if n.left else None, "right:", n.right.value if n.right else None, "\n",
              "=====")
        if n.left:
            print_tree(n.left)
        if n.right:
            print_tree(n.right)


intervals = [Interval(3, 4), Interval(1, 2), Interval(5, 6), Interval(7, 8)]
tree = IntervalTree(intervals)
print_tree(tree.root)
print("XXXXXXXXXXXXXXXXXXXXXXXX")
tree._rb_delete(tree.root.right)
print_tree(tree.root)
"""s = 6
random.seed(s)
n = 6
intervals = random.sample([Interval(x, x + 1) for x in range(n)], n)
tree = IntervalTree()
intervals = list(
            itertools.compress(map(lambda x: Interval(x, x + 1, 'right'), range(n)), itertools.cycle([1, 0, 1, 1, 0])))
# tree = IntervalTree(intervals)

for new_interval in intervals:
    tree.add(new_interval)
    print_tree(tree.root)
    print("XXXXXXXXXX")

tree.add(Interval(1, 2))
print_tree(tree.root)

intervals = random.sample(intervals, len(intervals))
print("::::::::::NOW DELETING")
for new_interval in [Interval(3, 4)]:
    if tree.root:
        tree._rb_delete(find(tree.root, new_interval))
        print_tree(tree.root)
        print("XXXXXXXXXX")"""

"""for new_interval in intervals:
    tree.add(new_interval)

intervals = random.sample(intervals, n)"""

"""print("XXXXXXXXXX")
for new_interval in intervals:
    node_found = find(tree.root, new_interval)
    tree._rb_delete(node_found)
    print_tree(tree.root)
    print("XXXXXXXXXX")"""
