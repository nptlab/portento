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
school_df.t = school_df.t.apply(lambda x: pd.Interval(x-20, x, 'both'))

if not path.exists(STREAM_PICKLE):
    stream = portento.from_pandas_stream(school_df, *["t", "i", "j"])
    pickle.dump(stream, open(STREAM_PICKLE, 'wb'))
else:
    stream = pickle.load(open(STREAM_PICKLE, 'rb'))

print(school_df[:10])
print(list(iter(stream))[:10])
