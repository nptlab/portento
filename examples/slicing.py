"""
======================
Slicing
======================

Presenting commands to slice a stream graph.

Data was taken from: http://www.sociopatterns.org.
"""

from os import path

import pandas as pd
import pickle
from functools import partial

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
school_df.t = school_df.t.apply(lambda x: pd.Interval(x - 20, x, 'both'))

if not path.exists(STREAM_PICKLE):
    stream = portento.from_pandas_stream(school_df, "t", ["i", "C_i"], ["j", "C_j"])
    pickle.dump(stream, open(STREAM_PICKLE, 'wb'))
else:
    stream = pickle.load(open(STREAM_PICKLE, 'rb'))

print("* The first columns of the original dataframe:")
print(school_df[:10])

print("* Overlapping intervals are merged during the creation of the stream object:")
print('\n'.join([str(link) for link in stream][:10]))

print("* The entity in a stream (aka the node) can be described by many attributes: in this case,"
      " the tuple of tuples (name_of_attr, value_of_attr).\n This format is handy to extract values"
      " calling dict() over the tuple.")
print('\n'.join([str(portento.Link(link.interval, dict(link.u)["i"], dict(link.v)["i"])) for link in stream][:10]))

print("* This is useful when slicing by nodes.")
node_filter = portento.NodeFilter(lambda x: 1799 < dict(x)["i"] < 1851)
filtered_stream = portento.slice_stream(stream, node_filter=node_filter, first='node')
print('\n'.join([str(link) for link in filtered_stream][:10]))

print("* A stream can be sliced over its temporal dimension.\n For instance, you can get"
      " a stream of snapshots in certain timestamps.")
time_filter = portento.TimeFilter(list(
    map(lambda x: pd.Interval(x, x, 'both'), range(31200, 148100, 10000))))
filtered_stream = list(portento.slice_stream(stream, time_filter=time_filter, first='time'))
print('\n'.join([str(link) for link in filtered_stream][:10]))
print("=====")
print('\n'.join([str(link) for link in filtered_stream][-10:]))

print("* Slices can be compounded. The order of slicing (node first or time first) does not change the result.")
print("- NODE FIRST:")
filtered_stream = list(portento.slice_stream(stream, node_filter=node_filter, time_filter=time_filter, first='node'))
print('\n'.join([str(link) for link in filtered_stream][:10]))

print("- TIME FIRST:")
filtered_stream = list(portento.slice_stream(stream, node_filter=node_filter, time_filter=time_filter, first='time'))
print('\n'.join([str(link) for link in filtered_stream][:10]))

print("* These are all the classes represented in the dataset:")
classes = sorted(school_df.C_i.unique())
print(classes)
print("* You may want to create many slices of the stream with the same time slice and different node slice:")
partial_filter = partial(portento.slice_stream,
                         stream=stream,
                         time_filter=portento.TimeFilter([pd.Interval(31200, 61200, 'both')]),
                         first='node')

for c in classes:
    filtered_stream = list(partial_filter(node_filter=portento.NodeFilter(lambda x: dict(x)["C_i"] == c)))
    print(f"{c}:")
    print('\n'.join([str(link) for link in filtered_stream][:5]))
    print("=====")
    print('\n'.join([str(link) for link in filtered_stream][-5:]))
    print("XXXXX")

print("* Remember that the result of a filter is an iterable over links. If you need further analysis,"
      " initialize a stream object.")
