from os import path
from itertools import accumulate, repeat
from operator import mul
from pandas import Interval

STREAM_PICKLE_PATH = path.join(".", "stream_pickle")
ADD_PERFORMANCE_PATH = path.join(".", "add_performance_res")
SLICE_PERFORMANCE_PATH = path.join(".", "slice_performance_res")
PATH_PERFORMANCE_PATH = path.join(".", "path_performance_res")

ADD_COMMAND = "stream.add(link)"
SETUP_ADD = "; ".join(['from pickle import load',
                       'from os.path import join',
                       'from pandas import Interval',
                       'from portento import Link'])

SLICE_COMMAND = "list(slice_stream(stream, node_filter, time_filter, slice_type))"
SETUP_SLICE = "; ".join(['from pickle import load',
                         'from portento import slice_stream, TimeFilter, NodeFilter',
                         'from pandas import Interval'])

PATH_COMMAND = ["earliest_arrival_time(stream, node)",
                # "fastest_path_duration_multipass(stream, node)",
                "fastest_path_duration(stream, node)",
                "latest_departure_time(stream, node)",
                "shortest_path_distance(stream, node)"]
PATH_NAMES = ["earliest_arrival_time",
              # "fastest_path_duration_multipass",
              "fastest_path_duration",
              "latest_departure_time",
              "shortest_path_distance"]
SETUP_PATH = "; ".join(['from pickle import load',
                        'from portento.algorithms.min_temporal_paths import earliest_arrival_time, '
                        'fastest_path_duration, '
                        'latest_departure_time, '
                        'shortest_path_distance'])

CMD_REP = 100
TEST_REP = 100
CMD_REP_PATH = CMD_REP // 10

TEST_REP_ADD = TEST_REP  # repetition of the same test. DEFAULT: 100
MAX_LINKS = 10000  # max number of links. DEFAULT: 10000
MEASUREMENT_MILESTONE = 100  # each n links, a test of performance is made. DEFAULT: 100
TIME_BOUND = Interval(0, 100000, 'both')  # time boundary of the stream. DEFAULT: Interval(0, 99999, 'both')

N_NODES = list(map(lambda x: 100 * x, [1, 5, 10, 50]))  # number of nodes in the stream.
# DEFAULT: list(map(lambda x: 100 * x, [1, 5, 10, 50]))
PERC_MEAN_INT_LEN = [0.001 * i for i in accumulate(repeat(10, 4), mul)]  # percentage of the length of a link
# based on TIME_BOUND. DEFAULT: [0.001 * i for i in accumulate(repeat(10, 4), mul)]
UNIT_MEASURE = 10e3  # milliseconds

TEST_REP_SLICE = 1

PERC_NODES = [25, 50, 75]
PERC_INTERVAL = [25, 50, 75]
SLICE_TYPES = ['time', 'node']

N_NODES_PATH = 1
