"""
======================
Presentation of the basic interface
======================

Presenting the basic commands to query a stream graph.

Data was taken from: http://www.sociopatterns.org/datasets/contact-patterns-in-a-village-in-rural-malawi/
"""

from os import path

import pandas as pd
import pickle
from numpy import int64
from random import shuffle

import portento

DATA_DIR = path.join('sociopatterns', 'data')
CSV_DIR = 'csv'
PICKLE_DIR = 'pickled_stream'
MALAWI_FILE = 'tnet_malawi_pilot.csv.gz'
MALAWI_STREAM_PICKLE = 'malawi_stream'

THIS_FILE = path.join(DATA_DIR, CSV_DIR, MALAWI_FILE)
STREAM_PICKLE = path.join(DATA_DIR, PICKLE_DIR, MALAWI_STREAM_PICKLE)

FINAL_COLUMNS = ['t', 'i', 'j']


def import_malawi_data_as_df():
    malawi = pd.read_csv(THIS_FILE, compression='gzip', header=0, sep=',', index_col=0)
    malawi = malawi.drop('day', axis=1)
    malawi.columns = FINAL_COLUMNS
    malawi.t = malawi.t.apply(lambda x: pd.Interval(x, x, 'both'))
    return malawi


malawi_df = import_malawi_data_as_df()

if not path.exists(STREAM_PICKLE):
    stream = portento.from_pandas_stream(malawi_df, *FINAL_COLUMNS)  # specify columns names as parameters (in order,
    # interval, source and destination)
    pickle.dump(stream, open(STREAM_PICKLE, 'wb'))
else:
    stream = pickle.load(open(STREAM_PICKLE, 'rb'))

print("* The first columns of the original dataframe:")
print(malawi_df[:10])

print("* A stream can be imported from a pandas dataframe with at least three columns:"
      "one of pandas intervals and two for nodes in the link.")
print('\n'.join([str(link) for link in stream][:10]))

print("\n* Get all links of a node (here just 10):")
list_links_node = list(stream[59])
print(list_links_node[:10])

print("\n* Get all links among a pair of nodes (here just 10):")
list_links_edge = list(stream[79, 9])
print(list_links_edge[:10])

print("\n* The links are unordered, so the order of the nodes is not relevant when calling __getitem__:")
list_links_edge_2 = list(stream[9, 79])
print(list_links_edge_2[:10])

print("\n* If a node that is not present is specified, an exception is raised:")
try:
    print(list(stream[59, 'a']))
except Exception as e:
    print(e)

print("\n* If two nodes do not share a link, an empty list is returned:")
print(list(stream[59, 79]))

print("\n* It is possible to extract node presence: all intervals in which the node is link to another:")
node_presence = list(stream.node_presence(79))
print(node_presence[:10])

print("\n* When adding a link, the interval is merged with other overlapping intervals "
      "both in the edge and in nodes presence:")
node_71_presence = list(stream.node_presence(71))
node_79_presence = list(stream.node_presence(79))
print(node_71_presence[:10])
print(node_79_presence[:10])
print(list(stream[71, 79]))
stream.add(portento.Link(pd.Interval(int64(0), int64(93760), 'right'), 71, 79))
print("*** After addition:")
node_71_presence = list(stream.node_presence(71))
node_79_presence = list(stream.node_presence(79))
print(node_71_presence[:10])
print(node_79_presence[:10])
print(list(stream[71, 79]))

print("\n* A node is created as a link with that node is inserted in the stream.")
try:
    print(list(stream[5000]))
except Exception as e:
    print(e)
stream.add(portento.Link(pd.Interval(int64(0), int64(10)), 5000, 71))
print("*** After addition:")
print(list(stream[5000]))

print("\n* This kind of data structure offers the possibility to define node-based slices:")
nodes = list(map(lambda x: int64(x), [9, 31, 37, 59, 65]))
links = list(portento.slice_stream(stream,
                                   node_filter=portento.NodeFilter(lambda x: x in nodes),
                                   first='time'))
shuffle(links)
sliced_stream = portento.Stream(links)
print(list(stream[59])[:10])
print(list(sliced_stream[59])[:10])

print("\n* A stream can be transported to a pandas dataframe:")
df = portento.to_pandas_stream(sliced_stream)
print(df[:10])
