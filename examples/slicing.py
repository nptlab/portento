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
import portento
import portento.utils
import itertools

DATA_DIR = path.join('sociopatterns', 'data')
CSV_DIR = 'csv'
PICKLE_DIR = 'pickled_stream'
SCHOOL_FILE = 'primaryschool.csv.gz'
SCHOOL_STREAM_PICKLE = 'school_stream'

THIS_FILE = path.join(DATA_DIR, CSV_DIR, SCHOOL_FILE)
STREAM_PICKLE = path.join(DATA_DIR, PICKLE_DIR, SCHOOL_STREAM_PICKLE)

school_df = pd.read_csv(THIS_FILE, compression='gzip', sep='\t', header=None)
school_df.columns = ["t", "i", "j", "C_i", "C_j"]
school_df.t = school_df.t.apply(lambda x: pd.Interval(x - 20, x, 'both'))


def find(node, interval, u, v):
    if node:
        if node.value == interval and node.u == u and node.v == v:
            raise Exception(f"found the deleted one! {interval, u, v}")
        elif node.value > interval:
            return find(node.left, interval, u, v)
        else:
            return find(node.right, interval, u, v)


def print_tree(n):
    if n:
        print(n, "\n",
              "parent:", n.parent if n.parent else None, "\n",
              "left:", n.left if n.left else None, "right:", n.right if n.right else None, "\n",
              "=====")
        if n.left:
            print_tree(n.left)
        if n.right:
            print_tree(n.right)


if not path.exists(STREAM_PICKLE):
    stream = portento.from_pandas_stream(school_df, "t", ["i", "C_i"], ["j", "C_j"])
    print(list(stream)[:10])
    pickle.dump(stream, open(STREAM_PICKLE, 'wb'))
else:
    stream = pickle.load(open(STREAM_PICKLE, 'rb'))

print(list(stream)[:10])
