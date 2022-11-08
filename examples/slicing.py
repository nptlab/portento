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

DATA_DIR = path.join('sociopatterns', 'data')
CSV_DIR = 'csv'
PICKLE_DIR = 'pickled_stream'
SCHOOL_FILE = 'primaryschool.csv.gz'
SCHOOL_STREAM_PICKLE = 'school_stream'

THIS_FILE = path.join(DATA_DIR, CSV_DIR, SCHOOL_FILE)
STREAM_PICKLE = path.join(DATA_DIR, PICKLE_DIR, SCHOOL_STREAM_PICKLE)

school_df = pd.read_csv(THIS_FILE, compression='gzip', sep='\t', header=None)
school_df.columns = ["t", "i", "j", "C_i", "C_j"]

if not path.exists(STREAM_PICKLE):
    pass
    # stream = portento.from_pandas_stream(school_df, *FINAL_COLUMNS)  # specify columns names as parameters (in order,
    # interval, source and destination)
    # pickle.dump(stream, open(STREAM_PICKLE, 'wb'))
else:
    stream = pickle.load(open(STREAM_PICKLE, 'rb'))

print(school_df[:10])
