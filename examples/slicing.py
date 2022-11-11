"""
======================
Slicing
======================

Presenting commands to slice a stream graph.

Data was taken from: http://www.sociopatterns.org.
"""

# TODO this dataset has classes, show with functional programming the aggregation of nodes of the same class
from os import path

import pandas as pd
import pickle
from numpy import int64
from random import shuffle

import portento
import portento.utils
import itertools

"""DATA_DIR = path.join('sociopatterns', 'data')
CSV_DIR = 'csv'
PICKLE_DIR = 'pickled_stream'
SCHOOL_FILE = 'primaryschool.csv.gz'
SCHOOL_STREAM_PICKLE = 'school_stream'

THIS_FILE = path.join(DATA_DIR, CSV_DIR, SCHOOL_FILE)
STREAM_PICKLE = path.join(DATA_DIR, PICKLE_DIR, SCHOOL_STREAM_PICKLE)

school_df = pd.read_csv(THIS_FILE, compression='gzip', sep='\t', header=None)
school_df.columns = ["t", "i", "j", "C_i", "C_j"]
school_df.t = school_df.t.apply(lambda x: pd.Interval(x-20, x, 'both'))

if not path.exists(STREAM_PICKLE):
    pass
    # stream = portento.from_pandas_stream(school_df, *["t", "i", "j"])
    # pickle.dump(stream, open(STREAM_PICKLE, 'wb'))
else:
    stream = pickle.load(open(STREAM_PICKLE, 'rb'))

print(school_df[:10])
a = 3
b = a
a = 4
print(a, b)
print(pd.Interval(1, 1, 'both').length)"""

tree = portento.utils.IntervalTree()
tree.add(pd.Interval(0, 1, 'both'))
print("=====")
print(tree.root.value, tree.root.time_instants)
tree.add(pd.Interval(2, 3, 'both'))
print("=====")
print(tree.root.value, tree.root.time_instants)
print(tree.root.right.value, tree.root.right.time_instants)
tree.add(pd.Interval(0, 2, 'left'))
tree.add(pd.Interval(9, 12, 'left'))
print("=====")
print(tree.root.value, tree.root.time_instants)
print(tree.root.left.value, tree.root.left.time_instants)
print(tree.root.right.value, tree.root.right.time_instants)
tree.add(pd.Interval(13, 21, 'left'))
print("=====")
print(tree.root.value, tree.root.time_instants)
print(tree.root.left.value, tree.root.left.time_instants)
print(tree.root.right.value, tree.root.right.time_instants)
print(tree.root.right.right.value, tree.root.right.right.time_instants)
tree.add(pd.Interval(25, 26, 'left'))
print("=====")
print(tree.root.value, tree.root.time_instants, tree.root.color)
print(tree.root.left.value, tree.root.left.time_instants, tree.root.left.color)
print(tree.root.right.value, tree.root.right.time_instants, tree.root.right.color)
print(tree.root.right.right.value, tree.root.right.right.time_instants, tree.root.right.right.color)
print(tree.root.right.left.value, tree.root.right.left.time_instants, tree.root.right.left.color)
tree.add(pd.Interval(20, 25, 'both'))
print("=====")
print(tree.root.value, tree.root.time_instants, tree.root.color)
print(tree.root.left.value, tree.root.left.time_instants, tree.root.left.color)
print(tree.root.right.value, tree.root.right.time_instants, tree.root.right.color)
print(tree.root.right.right.value, tree.root.right.right.time_instants, tree.root.right.right.color)
tree.add(pd.Interval(0, 4, 'both'))
print("=====")
print(tree.root.value, tree.root.time_instants)
print(tree.root.left.value, tree.root.left.time_instants)
print(tree.root.right.value, tree.root.right.time_instants)
tree.add(pd.Interval(27, 28, 'both'))
print("=====")
print(tree.root.value, tree.root.time_instants)
print(tree.root.left.value, tree.root.left.time_instants)
print(tree.root.right.value, tree.root.right.time_instants)
tree.add(pd.Interval(29, 39, 'both'))
print("=====")
print(tree.root.value, tree.root.time_instants)
print(tree.root.left.value, tree.root.left.time_instants)
print(tree.root.right.value, tree.root.right.time_instants)
print(tree.root.right.left.value, tree.root.right.left.time_instants)
print(tree.root.right.right.value, tree.root.right.right.time_instants)
tree._rb_delete(tree.root)
print("=====")
print(tree.root.value, tree.root.time_instants)
print(tree.root.left.value, tree.root.left.time_instants)
print(tree.root.right.value, tree.root.right.time_instants)


